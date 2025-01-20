#!/usr/bin/env python3

import argparse
from pathlib import Path
import datetime
import pickle # temporary
import json

import gtfs_kit
import pandas as pd
import geopandas as gpd
import shapely
from gtfs_kit import Feed

from bundler.bundlereader import BundleReader, Route
from bundler.schedule_writer import ScheduleWriter
from backend.util import Util


def fix_interpolation(orig_df: pd.DataFrame):
    df = orig_df.reset_index()
    #df['tmstmp'] = df['tmstmp'].apply(lambda x: int(x))
    samefirst = df[df.tmstmp == df.iloc[0].tmstmp]
    #samelast = df[df.tmstmp == df.iloc[-1].tmstmp]
    next_ = df.iloc[len(samefirst)]
    deltas = next_ - samefirst.iloc[-1]
    #if deltas.tmstmp < self.EPSILON:
    #    return None
    v = deltas.pdist / deltas.tmstmp
    ref = samefirst.iloc[-1]
    for x in range(len(samefirst)):
        calc = df.iloc[x]
        corrected_tmstmp = ref.tmstmp - ((ref - calc).pdist / v)
        #col_index = orig_df.columns.get_loc('tmstmp')
        orig_df.iloc[x] = corrected_tmstmp


class TrainManager:
    CHICAGO = 'EPSG:26916'  # unit: meters

    def __init__(self):
        pass

    @staticmethod
    def applysplit(x):
        if x['prevdest'] is None:
            return 1
        # entering the loop can never be a new trip
        if x['prevdest'] == 'Loop':
            return 0
        if x['destNm'] != x['prevdest']:
            return 1
        return 0

    def split_trips(self, vs):
        vs['prevdest'] = vs['destNm'].shift(1)
        vs['trip_id'] = vs.apply(self.applysplit, axis=1)
        cur_trip_id = 0
        for i in range(len(vs)):
            if vs.iloc[i].trip_id == 1:
                cur_trip_id += 1
            vs.iloc[i, vs.columns.get_loc('trip_id')] = cur_trip_id


class TrainTripsHandler:
    def __init__(self, routex: Route,
                 day: str,
                 vehicle_df: pd.DataFrame, feed: Feed,
                 writer: ScheduleWriter):
        self.route = routex
        self.day = day
        self.vehicle_id = vehicle_df.rn.unique()[0]
        naive_day = datetime.datetime.strptime(self.day, '%Y%m%d')
        self.next_day_thresh = Util.CTA_TIMEZONE.localize(naive_day + datetime.timedelta(days=1))
        vehicle_df_filt = vehicle_df[vehicle_df.lat != '0']
        self.vehicle_df = vehicle_df_filt.sort_values('prdt').copy()
        m = TrainManager()
        m.split_trips(self.vehicle_df)
        self.vehicle_df['tmstmp'] = self.vehicle_df.loc[:, 'prdt'].apply(lambda x: int(Util.CTA_TIMEZONE.localize(
            datetime.datetime.strptime(x, '%Y-%m-%dT%H:%M:%S')).timestamp()))
        self.trip_ids = list(self.vehicle_df.trip_id.unique())
        self.error = None
        self.feed = feed
        self.output_df = pd.DataFrame()
        self.writer = writer
        self.shape = None
        self.reference_trip = None
        self.rt_geo_trip_utm = None
        self.stops_seen = set([])
        #self.get_shape()

    def record_error(self, trip_id, msg):
        self.error = f'{trip_id}: {msg}'
        print(self.error)

    @staticmethod
    def write_all_stops(feed, writer):
        fs = feed.stops
        stops = fs[(fs.parent_station != '<NA>') | (fs.location_type != 0)]
        for _, row in stops.iterrows():
            writer.write('stops', {
                'stop_id': row.stop_id,
                'stop_name': row.stop_name,
                'stop_lat': row.stop_lat,
                'stop_lon': row.stop_lon,
                'location_type': row.location_type,
                'parent_station': row.parent_station,
                'wheelchair_boarding': row.wheelchair_boarding,
            })

    def get_shape(self, trip_id):
        run = self.vehicle_id
        daily_trips = self.feed.get_trips(self.day)
        route_trips = daily_trips[daily_trips.route_id.str.lower() == self.route.route]
        run_trips = route_trips[(route_trips.schd_trip_id == f'R{run}')]
        services = run_trips[['route_id', 'shape_id', 'schd_trip_id']].drop_duplicates()
        geo_shapes = self.feed.get_shapes(as_gdf=True).to_crs(TrainManager.CHICAGO).set_index('shape_id')
        rt_trip = self.vehicle_df[self.vehicle_df.trip_id == trip_id]
        rt_geo_trip = gpd.GeoDataFrame(rt_trip, geometry=gpd.points_from_xy(x=rt_trip.lon, y=rt_trip.lat), crs='EPSG:4326')
        rt_geo_trip_utm = rt_geo_trip.to_crs(TrainManager.CHICAGO)
        if len(services.shape_id.unique()) == 1:
            shape_id = services.iloc[0].shape_id
            self.reference_trip = run_trips[run_trips.shape_id == shape_id].iloc[0].trip_id
            self.shape = geo_shapes.loc[shape_id].geometry
            rt_geo_trip_utm['pdist'] = rt_geo_trip_utm.apply(lambda x: self.shape.line_locate_point(x.geometry) * 3.28084,
                                                             axis=1)
            self.rt_geo_trip_utm = rt_geo_trip_utm
        rt_first = rt_geo_trip_utm.iloc[0].geometry
        rt_last = rt_geo_trip_utm.iloc[-1].geometry
        run_geo = services.join(geo_shapes, on='shape_id')
        run_geo['first'] = run_geo.apply(lambda x: x.geometry.line_locate_point(rt_first), axis=1)
        run_geo['last'] = run_geo.apply(lambda x: x.geometry.line_locate_point(rt_last), axis=1)
        run_geo['len'] = run_geo.apply(lambda x: x.geometry.length, axis=1)
        run_geo['tot'] = (run_geo['len'] - run_geo['last']) + run_geo['first']
        minval = run_geo['tot'].min()
        if minval > 2000:
            #print(f'rt')
            #print(rt_geo_trip)
            #print(run_geo)
            #raise ValueError(f'No shape within threshold 1000m. Closest: {minval}m')
            print(f'Run {run} trip {trip_id}: No shape within threshold 1000m. Closest: {minval}m')
            return False
        shape = run_geo[run_geo['tot'] == minval].iloc[0]
        self.reference_trip = run_trips[run_trips.shape_id == shape.shape_id].iloc[0].trip_id
        self.shape = shape.geometry
        #print(self.shape)
        #print(rt_geo_trip_utm)

        def geofn(x):
            geo = x.geometry
            #print(geo)
            rv = self.shape.line_locate_point(geo) * 3.28084
            #print(geo, rv)
            return rv
        rt_geo_trip_utm['pdist'] = rt_geo_trip_utm.apply(geofn, axis=1)
        self.rt_geo_trip_utm = rt_geo_trip_utm
        return True

    def gtfs_time(self, ts: datetime.datetime):
        if ts >= self.next_day_thresh:
            hour = ts.hour + 24
            return ts.strftime(f'{hour:02d}:%M:%S')
        return ts.strftime('%H:%M:%S')

    def process_all_trips(self):
        for trip_id in self.trip_ids:
            self.writer.write('trips', {
                'route_id': self.route.route,
                'service_id': self.day,
                'trip_id': f'{self.day}.{self.vehicle_id}.{trip_id}',
            })
            self.process_trip(trip_id)
        self.write_all_stops(self.feed, self.writer)

    def process_trip(self, trip_id: str, debug=False):
        stops = []
        stop_index = {}
        try:
            result = self.get_shape(trip_id)
            if not result:
                return False
        #except shapely.errors.GEOSException:
        except ValueError:
            print(f'Error parsing run {self.vehicle_id} trip {trip_id}')
            return False

        df = self.rt_geo_trip_utm[self.rt_geo_trip_utm.trip_id == trip_id]
        # meters to feet
        # 3.28084
        feed_stops = self.feed.stop_times[self.feed.stop_times.trip_id == self.reference_trip].join(self.feed.stops.set_index('stop_id'), on='stop_id')
        # TODO: rename
        vehicles_df = df[['tmstmp', 'pdist']]

        #print(feed_stops)
        for i, ps in feed_stops.iterrows():
            #print(ps)
            stop_index[ps.stop_id] = ps
            stops.append({
                'stpid': ps.stop_id,
                'seq': ps.stop_sequence,
                'pdist': ps.shape_dist_traveled,
            })
            self.stops_seen.add(ps.stop_id)
        if not stops:
            self.record_error(trip_id=trip_id, msg='Missing stops')
            return False
        stops_df = pd.DataFrame(stops)
        minval = vehicles_df.pdist.min()
        maxval = vehicles_df.pdist.max()
        beginnings = vehicles_df[vehicles_df.pdist == minval]
        endings = vehicles_df[vehicles_df.pdist == maxval]
        begin_drop = len(beginnings) - 1
        end_drop = len(endings) - 1
        filtered = vehicles_df
        if end_drop > 0:
            filtered = vehicles_df.drop(vehicles_df.tail(end_drop).index)
        if begin_drop > 0:
            filtered = filtered.drop(filtered.head(begin_drop).index)
        if filtered.empty:
            self.record_error(trip_id=trip_id, msg='Interpolated vehicle error')
            return False
        pattern_template = pd.DataFrame(index=stops_df.pdist, columns={'tmstmp': float('NaN')})
        # need spline interpolations or something else
        # https://stackoverflow.com/questions/71215630/how-do-i-use-pandas-to-interpolate-on-the-first-few-rows-of-a-dataframe
        #itarget = pd.concat([pattern_template, filtered.set_index('pdist')]).sort_index().tmstmp.astype('float')
        #itarget.to_csv(f'/tmp/interpolate-{self.vehicle_id}-{trip_id}')
        combined = pd.concat([pattern_template, filtered.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        #print(combined)
        fix_interpolation(combined)
        combined = combined.groupby(combined.index).last()
        df = stops_df.set_index('pdist').assign(tmstmp=combined.apply(
            lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.fromtimestamp(int(x)))
        ))
        if debug:
            return df
        #stop_interpolation = []
        stopseq = set([])
        #print(df)
        for _, row in df.iterrows():
            pattern_stop = stop_index[row.stpid]
            # TODO: log error and debug this
            if pattern_stop.stop_sequence in stopseq:
                continue
            interpolated_timestamp = self.gtfs_time(row.tmstmp)
            self.writer.write('stop_times', {
                'trip_id': f'{self.day}.{self.vehicle_id}.{trip_id}',
                'arrival_time': interpolated_timestamp,
                'departure_time': interpolated_timestamp,
                'stop_id': pattern_stop.stop_id,
                'stop_sequence': pattern_stop.stop_sequence,
                'shape_dist_traveled': pattern_stop.shape_dist_traveled,
            })
            stopseq.add(pattern_stop.stop_sequence)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bundles')
    parser.add_argument('--bundle_file', type=str,
                        help='File with bus/train scrape data.')
    parser.add_argument('--routes', type=str,
                        help='Comma-separated list of routes.')
    parser.add_argument('--gtfs_file', type=str,
                        help='Applicable GTFS file.')
    args = parser.parse_args()
    gtfs_file=Path(args.gtfs_file).expanduser()
    feed = gtfs_kit.read_feed(gtfs_file, dist_units='ft')
    ds = feed.calendar_dates.iloc[0].date
    print(f'First date: {ds}')
    bundle_file = Path(args.bundle_file).expanduser()
    day = datetime.datetime.strptime(bundle_file.name, 'bundle-%Y%m%d.tar.lz')
    daystr = day.strftime('%Y%m%d')
    print(f'Routes: {args.routes}')
    if not args.routes:
        routes = None
    else:
        routes = args.routes.split(',')
    d = {}
    routeidx = args.routes.replace(',', '_')
    tmppath = Path(f'/tmp/jsoncachep_{routeidx}')
    daily_trips = feed.get_trips(day.strftime('%Y%m%d'))
    if tmppath.exists():
        with tmppath.open('rb') as jfh:
            d = pickle.load(jfh)
    else:
        r = BundleReader(bundle_file, routes)
        r.process_bundle_file()
        for route, vsamp in r.generate_vehicles():
            d.setdefault(route, []).append(vsamp)
            #print(route)
            #print(vsamp)
            #break
        with tmppath.open('wb') as jfh:
            pickle.dump(d, jfh)
    key, runs = next(iter(d.items()))
    writer = ScheduleWriter(Path('/tmp/take2'), daystr)
    for vsamp in runs:
        th = TrainTripsHandler(key, daystr, vsamp, feed, writer)
        th.process_all_trips()
    #route_trips = daily_trips[daily_trips.route_id.str.lower() == key.route]
    #for route, vsamp in r.generate_vehicles():
    #    th = TrainTripsHandler(route, r.day, vsamp, mpm, writer)
    #    th.process_all_trips()
    # run_trips = route_trips[route_trips.schd_trip_id == f'R{run}']
    #     geo_shapes = feed.get_shapes(as_gdf=True, use_utm=True)
    #     run_trip = pd.DataFrame()
    #     gtrip = gpd.GeoDataFrame(run_trip, geometry=gpd.points_from_xy(x=run_trip.lon, y=run_trip.lat), crs='EPSG:4326')
    #     gtriputm = gtrip.to_crs('EPSG:32616')
