#!/usr/bin/env python3
from pathlib import Path

import gtfs_kit
import pyproj
import shapely

from shapely.ops import split
from sqlalchemy import select, func
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

    def __init__(self, pattern):
        self.pattern = pattern
        self.shape = to_shape(pattern.geom)
        self.front = None
        self.back = None
        self.split_length = None
        if pattern.first_stop_name == pattern.last_stop_name:
            self.calc_midpoint()

    @staticmethod
    def transform(point: shapely.Point):
        coord_point = shapely.Point(ShapeManager.XFM.transform(point.y, point.x))
        return coord_point

    def length(self):
        return self.shape.length

    def needs_loop_detection(self):
        return self.front is not None

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
        print(f'Splitting shape {self.pattern.pattern_id} at len {splitlen}')
        #shape.line_locate_point(shapely.Point(xfm.transform(*LOOP_MIDPOINT)))
        splitpoint = shape.interpolate(splitlen)
        print(f'Calculating midpoint: distance from line is {distance}')
        splitsnap = shapely.snap(shape, splitpoint, tolerance=1)
        segments = split(splitsnap, splitpoint)
        self.front, self.back = segments.geoms

    # we need more sophisticated statistical tooling to predict which of the two points is a better fit for our desired
    # monotonically increasing sequence
    def get_distance_along_shape(self, previous_distance, stop_point, debug=False):
        coord_point = shapely.Point(self.XFM.transform(stop_point.y, stop_point.x))
        midpoint = self.shape.length / 2
        x = self.shape.line_locate_point(coord_point)
        if not self.needs_loop_detection():
            return x
        complement = self.shape.length - x
        dx = abs(x - previous_distance)
        dcomplement = abs(complement - previous_distance)
        # if dx < dcomplement:
        #     rv = x
        # else:
        #     rv = complement
        # if rv < previous_distance:
        #     rv = previous_distance
        if x < previous_distance and complement < previous_distance:
            rv = previous_distance
        else:
            rv = min([y for y in {x, complement} if y >= previous_distance])
        delta = abs(rv - previous_distance)
        if delta > 4000:
            if dx < dcomplement:
                rv = x
            else:
                rv = complement

        # return the smallest that is >= previous
        midpoint_distance = abs(midpoint - x)
        #rv = x
        # if midpoint_distance < 500:
        #     # don't correct
        #     rv = x
        # elif previous_distance < midpoint:
        #     rv = min(x, complement)
        # else:
        #     rv = max(x, complement)
        # if rv < previous_distance:
        #     rv = max(x, complement)
        if debug:
            print(f'Point prev {int(previous_distance):5} midpoint {int(midpoint):5}  {int(x):5}  {int(complement):5}   mpd {int(midpoint_distance):5}   rv {int(rv):5}')
        return rv

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

    def initialize_previous(self, direction):
        use_front = int(direction) == self.FRONT_DIRECTIONS.get(int(self.pattern.pattern_id))
        if use_front:
            return 0
        return (self.shape.length / 2) + 1

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
            use_front = int(direction) == self.FRONT_DIRECTIONS.get(int(self.pattern.pattern_id))
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
        schedule_datestr = schedule_location.name.replace('cta_gtfs_', '').replace('.zip', '')
        self.schedule_date = datetime.datetime.strptime(schedule_datestr, '%Y%m%d').date()
        self.engine = engine
        self.joined_shapes = None
        self.feed = None
        self.geo_shapes = None
        self.managed_shapes = {}
        #self.setup_shapes()

    def load_feed(self):
        if self.feed is not None:
            return
        self.feed = gtfs_kit.read_feed(self.schedule_location,
                                       dist_units='ft')
        print(f'Schedule: {self.schedule_date}')
        self.geo_shapes = self.feed.get_shapes(as_gdf=True).to_crs(ShapeManager.CHICAGO).set_index('shape_id')

    def get_pattern(self, rt: str, last_station: int, train_point: shapely.Point):
        """
        Shapely point is assumed to be lat/lon
        :param rt:
        :param last_station:
        :param train_point:
        :return:
        """
        if train_point.x == 0 or train_point.y == 0:
            # need more input geometry sanitization
            print(f'Invalid train point {train_point}')
            return None
        with Session(self.engine) as session:
            #print(f'rt {rt}  last station {last_station}  train point {train_point}')
            stmt = (select(TrainPatternDetail)
                    .join(Stop, TrainPatternDetail.first_stop_id == Stop.id)
                    .where(TrainPatternDetail.route_id == rt)
                    .where(TrainPatternDetail.last_stop_id == last_station)
                    .where(TrainPatternDetail.pattern_id.not_in({308500036, 308500102}))
                    .where(func.ST_Distance(
                            Stop.geom.ST_Transform(26916), func.ST_Transform(from_shape(train_point, srid=4326), 26916)
                        ) < 1000
                        )
                    )
            s = session.scalars(stmt)
            candidates = s.all()
            #candidates = j[(j.last_stop_id == str(last_station)) & (j.route_id.str.lower() == rt)]
            #candidates = candidates[~candidates.shape_id.isin({'308500036', '308500102'})]
            if len(candidates) == 1:
                return candidates[0].pattern_id
            if not candidates:
                return None
            rdist = None
            transformed = ShapeManager.XFM.transform(train_point.y, train_point.x)
            train_point_chicago = shapely.Point(*transformed)
            for c in candidates:
                shape_geom = c.geom
                dist = shape_geom.distance(train_point_chicago)
                key = (dist, c.pattern_id)
                if rdist is None:
                    rdist = key
                elif key < rdist:
                    rdist = key
            if rdist is None:
                return None
            if rdist[0] > 200:
                return None
            return rdist[1]

    # def schedule_start(self):
    #     return self.feed.get_dates()[0]

    def setup_shapes(self):
        # shape_df = self.shape_trips_joined()
        # for _, row in shape_df.iterrows():
        #     shape_id = int(row.shape_id)
        #     self.managed_shapes[shape_id] = ShapeManager(row)
        with Session(self.engine) as session:
            #print(f'rt {rt}  last station {last_station}  train point {train_point}')
            #j = self.shape_trips_joined()
            stmt = (select(TrainPatternDetail)
                    .where(TrainPatternDetail.pattern_id.not_in({308500036, 308500102}))
                    )
            rows = session.scalars(stmt)
            for pattern in rows:
                pattern_id = pattern.pattern_id
                self.managed_shapes[pattern_id] = ShapeManager(pattern)


    def update_db(self):
        self.load_feed()
        feed = self.feed
        shape_df = self.shape_trips_joined()
        schedule_dt = datetime.datetime.combine(self.schedule_date, datetime.time())
        with Session(self.engine) as session:
            for _, row in shape_df.iterrows():
                route_id = row.route_id.lower()
                shape_id = int(row.shape_id)
                pattern_detail = session.get(TrainPatternDetail, shape_id)
                if not pattern_detail:
                    detail = TrainPatternDetail(
                        pattern_id=shape_id,
                        route_id=route_id,
                        pattern_length_meters=row.geometry.length,
                        service_id=int(row.service_id),
                        direction_id=int(row.direction_id),
                        direction=row.direction,
                        schedule_instance_count=row['count'],
                        stop_count=row.stop_count,
                        first_stop_name=row.first_stop_name,
                        last_stop_name=row.last_stop_name,
                        first_stop_id=row.first_stop_id,
                        last_stop_id=row.last_stop_id,
                        geom=row.geometry.wkt
                    )
                    session.add(detail)
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

    # def train_shapes(self):
    #     feed = self.feed
    #     return feed.shapes[feed.shapes.shape_id.str.startswith('3')].shape_id.unique()

    def train_trips(self):
        self.load_feed()
        feed = self.feed
        return feed.trips[feed.trips.shape_id.str.startswith('3')]

    def shape_trips(self):
        self.load_feed()
        train_trips = self.train_trips()
        return train_trips.groupby(['route_id', 'shape_id']).first()

    # def shape_services(self):
    #     return sa.train_trips()[['shape_id', 'service_id']].drop_duplicates().join(
    #         self.feed.calendar.set_index('service_id'), on='service_id')

    def shape_trips_joined(self):
        if self.joined_shapes is not None:
            return self.joined_shapes
        self.load_feed()
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
        self.load_feed()
        feed = self.feed
        result = (feed.stop_times[feed.stop_times.trip_id == trip_id].
                  join(feed.stops.set_index('stop_id')[['stop_name']], on='stop_id'))
        return result

    def get_stop_list(self, trip_id):
        self.load_feed()
        feed = self.feed
        return feed.stop_times[feed.stop_times.trip_id == trip_id].join(
            feed.stops.set_index('stop_id')[['stop_name']], on='stop_id')

    def shape_list(self):
        self.load_feed()
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
    sa = ScheduleAnalyzer(schedule_file, engine=db_init(dev=True))
    #sa.update_db()
