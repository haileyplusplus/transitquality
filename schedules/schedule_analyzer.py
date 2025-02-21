#!/usr/bin/env python3
from pathlib import Path

import gtfs_kit
import pyproj
import shapely

from shapely.ops import split
from sqlalchemy.orm import Session
from geoalchemy2.shape import to_shape, from_shape

from realtime.rtmodel import *


class ShapeManager:
    # Clark / Lake
    #LOOP_MIDPOINT = (41.885737, -87.630886)
    # Washington / Wabash
    LOOP_MIDPOINT = (41.88322, -87.626189)
    # geometry lengths are in meters
    CHICAGO = 'EPSG:26916'
    FEET_TO_METERS = 0.3048
    XFM = pyproj.Transformer.from_crs('EPSG:4326', CHICAGO)
    FRONT_DIRECTIONS = {
        308500017: 5,  # Brown Kimball-Kimball
        308500034: 1,  # Orange Midway-Midway
        308500024: 5,  # Purple Linden-Linden
        308500036: 5,  # Purple Linden-Linden
        308500102: 1,  # Purple Howard-Howard
        308500035: 1,  # Pink 54th - 54th
        308500033: 1,  # Yellow Howard - Howard
    }

    def __init__(self, row):
        self.row = row
        self.shape = row.geometry
        self.front = None
        self.back = None
        self.split_length = None
        if row.first_stop_name == row.last_stop_name:
            self.calc_midpoint()

    def calc_midpoint(self):
        shape = self.shape
        loop_midpoint = shapely.Point(self.XFM.transform(*self.LOOP_MIDPOINT))
        distance = shape.distance(loop_midpoint)
        if distance > 50:
            loop_midpoint = shape.interpolate(0.5, normalized=1)
            distance = shape.distance(loop_midpoint)
            if distance > 50:
                return
        splitlen = shape.line_locate_point(loop_midpoint)
        self.split_length = splitlen
        print(f'Splitting shape {self.row.shape_id} at len {splitlen}')
        #shape.line_locate_point(shapely.Point(xfm.transform(*LOOP_MIDPOINT)))
        splitpoint = shape.interpolate(splitlen)
        print(f'Calculating midpoint: distance from line is {distance}')
        splitsnap = shapely.snap(shape, splitpoint, tolerance=1)
        segments = split(splitsnap, splitpoint)
        self.front, self.back = segments.geoms

    def get_distance_along_shape(self, previous_distance, stop_point):
        coord_point = shapely.Point(self.XFM.transform(stop_point.y, stop_point.x))
        if self.front is None:
            distance = self.shape.line_locate_point(coord_point)
            return distance
        if previous_distance >= self.split_length:
            # look in the second part
            distance = self.back.line_locate_point(coord_point)
            distance += self.front.length
            return distance
        if previous_distance <= 0.9 * self.split_length:
            # only in first
            distance = self.front.line_locate_point(coord_point)
            return distance
        # otherwise try both
        front_distance = self.front.line_locate_point(coord_point)
        back_distance = self.back.line_locate_point(coord_point) + self.front.length
        if front_distance <= previous_distance:
            return back_distance
        return front_distance

    def get_distance_along_shape_dc(self, direction_change, stop_point):
        coord_point = shapely.Point(self.XFM.transform(stop_point.y, stop_point.x))
        if self.front is None:
            distance = self.shape.line_locate_point(coord_point)
            return distance
        dist_from_front = self.front.distance(stop_point)
        dist_from_back = self.back.distance(stop_point)
        use_front = None
        if dist_from_front > 50:
            use_front = False
        if dist_from_back > 50:
            use_front = True
        if use_front is None:
            if direction_change == 0:
                use_front = True
            else:
                use_front = False
        if use_front:
            # only in first
            distance = self.front.line_locate_point(coord_point)
            return distance
        distance = self.back.line_locate_point(coord_point)
        distance += self.front.length
        return distance

    def get_distance_along_shape_direction(self, direction, train_point, debug=False):
        coord_point = shapely.Point(self.XFM.transform(train_point.y, train_point.x))
        if debug:
            print(f'Get distance for {direction} point {train_point} coord point {coord_point}')
        if self.front is None:
            distance = self.shape.line_locate_point(coord_point)
            return distance
        dist_from_front = self.front.distance(coord_point)
        dist_from_back = self.back.distance(coord_point)
        use_front = None
        if dist_from_front > 50:
            use_front = False
        if dist_from_back > 50:
            use_front = True
        if use_front is None:
            use_front = int(direction) == self.FRONT_DIRECTIONS.get(int(self.row.shape_id))
        if debug:
            print(f'  Use front: {use_front} dist from front {dist_from_front} back {dist_from_back}')
        if use_front:
            # only in first
            distance = self.front.line_locate_point(coord_point)
            return distance
        distance = self.back.line_locate_point(coord_point)
        distance += self.front.length
        return distance


class ScheduleAnalyzer:
    def __init__(self, schedule_location: Path, engine=None):
        self.schedule_location = schedule_location
        self.feed = gtfs_kit.read_feed(self.schedule_location,
                                       dist_units='ft')
        schedule_datestr = schedule_location.name.replace('cta_gtfs_', '').replace('.zip', '')
        self.schedule_date = datetime.datetime.strptime(schedule_datestr, '%Y%m%d').date()
        print(f'Schedule: {self.schedule_date}')
        self.engine = engine
        self.joined_shapes = None
        self.geo_shapes = self.feed.get_shapes(as_gdf=True).to_crs(ShapeManager.CHICAGO).set_index('shape_id')
        self.managed_shapes = {}
        self.setup_shapes()

    def get_pattern(self, rt: str, last_station: int, train_point: shapely.Point):
        """
        Shapely point is assumed to be lat/lon
        :param rt:
        :param last_station:
        :param train_point:
        :return:
        """
        j = self.shape_trips_joined()
        candidates = j[(j.last_stop_id == str(last_station)) & (j.route_id.str.lower() == rt)]
        if len(candidates) == 1:
            return candidates.iloc[0].shape_id
        if candidates.empty:
            return None
        rdist = None
        transformed = ShapeManager.XFM.transform(train_point.y, train_point.x)
        train_point_chicago = shapely.Point(*transformed)
        for _, c in candidates.iterrows():
            dist = c.geometry.distance(train_point_chicago)
            key = (dist, c.shape_id)
            if rdist is None:
                rdist = key
            elif key < rdist:
                rdist = key
        if rdist is None:
            return None
        if rdist[0] > 200:
            return None
        return rdist[1]

    def schedule_start(self):
        return self.feed.get_dates()[0]

    def setup_shapes(self):
        shape_df = self.shape_trips_joined()
        for _, row in shape_df.iterrows():
            shape_id = int(row.shape_id)
            self.managed_shapes[shape_id] = ShapeManager(row)

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
                dist_feet = row.geometry.length / ShapeManager.FEET_TO_METERS
                pattern = Pattern(
                    id=shape_id,
                    rt=route_id,
                    updated=schedule_dt,
                    length=dist_feet
                )
                session.add(pattern)
                sequence = 1
                shape_manager = ShapeManager(row)
                previous_distance = 0
                first_headsign = None
                for stop_id_str, stop_name, stop_headsign in row.stop_list:
                    if first_headsign is None:
                        first_headsign = stop_headsign
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
                    distance = shape_manager.get_distance_along_shape(previous_distance, stop_point)
                    previous_distance = distance
                    #coord_point = shapely.Point(ShapeManager.XFM.transform(stop_point.y, stop_point.x))
                    #distance = row.geometry.line_locate_point(coord_point)
                    direction_change = 0
                    if isinstance(stop_headsign, str) and stop_headsign != first_headsign:
                        direction_change = 1
                    if not isinstance(stop_headsign, str):
                        stop_headsign = ''
                    pattern_stop = PatternStop(
                        sequence=sequence,
                        distance=distance,
                        pattern_id=shape_id,
                        stop_id=stop_id,
                        direction_change=direction_change,
                        stop_headsign=stop_headsign
                    )
                    session.add(pattern_stop)
                    sequence += 1
            session.commit()
        print(f'Database updated')

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
            lambda x: [(z.stop_id, z.stop_name, z.stop_headsign) for _, z in self.get_stop_list(x.trip_id).iterrows()], axis=1)
        shape_with_counts['stop_count'] = shape_with_counts.apply(lambda x: len(x.stop_list), axis=1)
        shape_with_counts['first_stop_name'] = shape_with_counts.apply(lambda x: x.stop_list[0][1], axis=1)
        shape_with_counts['last_stop_name'] = shape_with_counts.apply(lambda x: x.stop_list[-1][1], axis=1)
        shape_with_counts['first_stop_id'] = shape_with_counts.apply(lambda x: x.stop_list[0][0], axis=1)
        shape_with_counts['last_stop_id'] = shape_with_counts.apply(lambda x: x.stop_list[-1][0], axis=1)
        shape_with_counts = shape_with_counts.reset_index().join(self.geo_shapes, on='shape_id')
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
    sa = ScheduleAnalyzer(schedule_file, engine=db_init(dev=False))
    #sa.update_db()
