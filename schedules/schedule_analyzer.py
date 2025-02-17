#!/usr/bin/env python3
from pathlib import Path

import gtfs_kit
import pyproj
import shapely

from shapely.ops import split


class ScheduleAnalyzer:
    # Clark / Lake
    LOOP_MIDPOINT = (41.885737, -87.630886)
    CHICAGO = 'EPSG:26916'

    def __init__(self, schedule_location: Path):
        self.schedule_location = schedule_location
        self.feed = gtfs_kit.read_feed(self.schedule_location,
                                       dist_units='mi')
        self.xfm = pyproj.Transformer.from_crs('EPSG:4326', self.CHICAGO)
        self.joined_shapes = None

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
