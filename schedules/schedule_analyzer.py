#!/usr/bin/env python3
from pathlib import Path

import gtfs_kit
import pyproj
import shapely

from shapely.ops import split
from sqlalchemy.orm import Session
from geoalchemy2.shape import to_shape, from_shape

from realtime.rtmodel import *


class ScheduleAnalyzer:
    # Clark / Lake
    LOOP_MIDPOINT = (41.885737, -87.630886)
    # geometry lengths are in meters
    CHICAGO = 'EPSG:26916'
    FEET_TO_METERS = 0.3048

    def __init__(self, schedule_location: Path):
        self.schedule_location = schedule_location
        self.feed = gtfs_kit.read_feed(self.schedule_location,
                                       dist_units='ft')
        schedule_datestr = schedule_location.name.replace('cta_gtfs_', '').replace('.zip', '')
        self.schedule_date = datetime.datetime.strptime(schedule_datestr, '%Y%m%d').date()
        print(f'Schedule: {self.schedule_date}')
        self.engine = db_init(dev=True)
        self.xfm = pyproj.Transformer.from_crs('EPSG:4326', self.CHICAGO)
        self.joined_shapes = None
        self.geo_shapes = self.feed.get_shapes(as_gdf=True).to_crs(self.CHICAGO).set_index('shape_id')

    def schedule_start(self):
        return self.feed.get_dates()[0]

    def update_db(self):
        feed = self.feed
        shape_df = self.shape_trips_joined()
        schedule_dt = datetime.datetime.combine(self.schedule_date, datetime.time())
        with Session(self.engine) as session:
            for _, row in shape_df.iterrows():
                route_id = row.route_id.lower()
                shape_id = int(row.shape_id)
                pattern = session.get(Pattern, shape_id)
                if pattern:
                    continue
                print(f'Inserting shape {row.shape_id}')
                dist_feet = row.geometry.length / self.FEET_TO_METERS
                pattern = Pattern(
                    id=shape_id,
                    rt=route_id,
                    updated=schedule_dt,
                    length=dist_feet
                )
                session.add(pattern)
                sequence = 1
                for stop_id_str, stop_name in row.stop_list:
                    stop_id = int(stop_id_str)
                    stop = session.get(Stop, stop_id)
                    if not stop:
                        stop_info = feed.stops[feed.stops.stop_id == stop_id_str].iloc[0]
                        stop_geom = shapely.Point(stop_info.stop_lon, stop_info.stop_lat)
                        stop = Stop(
                            id=stop_id,
                            stop_name=stop_name,
                            geom=from_shape(stop_geom)
                            #geom=f'POINT({stop_info.stop_lon} {stop_info.stop_lat})'
                        )
                        session.add(stop)
                    stop_point = to_shape(stop.geom)
                    coord_point = shapely.Point(self.xfm.transform(stop_point.y, stop_point.x))
                    distance = row.geometry.line_locate_point(coord_point)
                    pattern_stop = PatternStop(
                        sequence=sequence,
                        distance=distance,
                        pattern_id=shape_id,
                        stop_id=stop_id
                    )
                    session.add(pattern_stop)
                    sequence += 1
            session.commit()
        print(f'Database updated')

    def calc_midpoint(self, shape):
        splitlen = shape.line_locate_point(shapely.Point(self.xfm.transform(*self.LOOP_MIDPOINT)))
        #shape.line_locate_point(shapely.Point(xfm.transform(*LOOP_MIDPOINT)))
        splitpoint = shape.interpolate(splitlen)
        splitsnap = shapely.snap(shape, splitpoint, tolerance=1)
        segments = split(splitsnap, splitpoint)
        inbound, outbound = segments.geoms
        return inbound, outbound

    def train_shapes(self):
        feed = self.feed
        return feed.shapes[feed.shapes.shape_id.str.startswith('3')].shape_id.unique()

    def train_trips(self):
        feed = self.feed
        return feed.trips[feed.trips.shape_id.str.startswith('3')]

    def shape_trips(self):
        train_trips = self.train_trips()
        return train_trips.groupby(['route_id', 'shape_id']).first()

    def shape_services(self):
        return sa.train_trips()[['shape_id', 'service_id']].drop_duplicates().join(
            self.feed.calendar.set_index('service_id'), on='service_id')

    def shape_trips_joined(self):
        if self.joined_shapes is not None:
            return self.joined_shapes
        counts_df = self.train_trips().groupby(['route_id', 'shape_id']).count()[['service_id']].rename(
            columns={'service_id': 'count'})
        shape_with_counts = self.shape_trips().join(counts_df)
        shape_with_counts['stop_list'] = shape_with_counts.apply(
            lambda x: [(z.stop_id, z.stop_name) for _, z in self.get_stop_list(x.trip_id).iterrows()], axis=1)
        shape_with_counts['stop_count'] = shape_with_counts.apply(lambda x: len(x.stop_list), axis=1)
        shape_with_counts['first_stop_name'] = shape_with_counts.apply(lambda x: x.stop_list[0][1], axis=1)
        shape_with_counts['last_stop_name'] = shape_with_counts.apply(lambda x: x.stop_list[-1][1], axis=1)
        shape_with_counts['first_stop_id'] = shape_with_counts.apply(lambda x: x.stop_list[0][0], axis=1)
        shape_with_counts['last_stop_id'] = shape_with_counts.apply(lambda x: x.stop_list[-1][0], axis=1)
        shape_with_counts = shape_with_counts.reset_index().join(sa.geo_shapes, on='shape_id')
        self.joined_shapes = shape_with_counts
        return self.joined_shapes

    def stop_sequence(self, trip_id: str):
        feed = self.feed
        result = (feed.stop_times[feed.stop_times.trip_id == trip_id].
                  join(feed.stops.set_index('stop_id')[['stop_name']], on='stop_id'))
        return result

    def get_stop_list(self, trip_id):
        feed = self.feed
        return feed.stop_times[feed.stop_times.trip_id == trip_id].join(
            feed.stops.set_index('stop_id')[['stop_name']], on='stop_id')

    def shape_list(self):
        train_summary = self.shape_trips()
        feed = self.feed
        train_summary['stop_list'] = train_summary.apply(
            lambda x: [(z.stop_id, z.stop_name) for _, z in self.get_stop_list(x.trip_id).iterrows()], axis=1)
        return train_summary

    def train_stops(self):
        """
        train platforms (matches stop in train feed)
        - feed.stops[(feed.stops.location_type == 0) & ~(feed.stops.parent_station.isna()) & (feed.stops.stop_id.str.startswith('3'))]
        train stations (matches station in train feed)
        - feed.stops[feed.stops.location_type == 1]
        aux entrances
        - feed.stops[feed.stops.location_type == 2]
        :return:

        For now, group by and use parent station
        GTFS doesn't have specific station entrances. Maybe use OSM data eventually

        """
        pass


if __name__ == "__main__":
    schedule_file = Path('~/datasets/transit/cta_gtfs_20250206.zip').expanduser()
    sa = ScheduleAnalyzer(schedule_file)
    sa.update_db()
