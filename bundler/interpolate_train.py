#!/usr/bin/env python3

import argparse
from pathlib import Path
import datetime
import pickle # temporary
import json

import gtfs_kit
import pandas as pd

from bundler.bundlereader import BundleReader, Route
from bundler.schedule_writer import ScheduleWriter
from backend.util import Util


class MemoryPatternManager:
    pass


class TrainManager:
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


class TripsHandler:
    # incomplete! fix for trains
    def __init__(self, routex: Route,
                 day: str,
                 vehicle_df: pd.DataFrame, mpm: MemoryPatternManager,
                 writer: ScheduleWriter):
        self.route = routex
        self.day = day
        self.vehicle_id = vehicle_df.vid.unique()[0]
        naive_day = datetime.datetime.strptime(self.day, '%Y%m%d')
        self.next_day_thresh = Util.CTA_TIMEZONE.localize(naive_day + datetime.timedelta(days=1))
        self.vehicle_df = vehicle_df
        self.vehicle_df['tmstmp'] = self.vehicle_df.loc[:, 'tmstmp'].apply(lambda x: int(Util.CTA_TIMEZONE.localize(
            datetime.datetime.strptime(x, '%Y%m%d %H:%M:%S')).timestamp()))
        self.trip_ids = list(vehicle_df.origtatripno.unique())
        self.error = None
        self.mpm = mpm
        self.output_df = pd.DataFrame()
        self.writer = writer

    def record_error(self, trip_id, msg):
        self.error = f'{trip_id}: {msg}'
        print(self.error)

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

    def process_trip(self, trip_id: str, debug=False):
        stops = []
        stop_index = {}
        df = self.vehicle_df[self.vehicle_df.origtatripno == trip_id]
        # TODO: rename
        vehicles_df = df[['tmstmp', 'pdist']]
        pids = df['pid'].unique()
        if len(pids) != 1:
            self.record_error(trip_id, f'Wrong number of pattern ids: {pids}')
            return
        pid = pids[0]
        for ps in self.mpm.get_stops(pid):
            stop_index[ps.stop.stop_id] = ps
            stops.append({
                'stpid': ps.stop.stop_id,
                'seq': ps.sequence_no,
                'pdist': ps.pattern_distance,
            })
        if not stops:
            self.record_error(trip_id=trip_id, msg='Missing stops')
            return False
        stops_df = pd.DataFrame(stops)
        #vehicles_df = pd.DataFrame([{'pdist': x.pattern_distance,
        #                             'tmstmp': int(x.timestamp.timestamp())} for x in positions]).sort_values('tmstmp')
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
        combined = pd.concat([pattern_template, filtered.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        combined = combined.groupby(combined.index).last()
        df = stops_df.set_index('pdist').assign(tmstmp=combined.apply(
            lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.fromtimestamp(int(x)))
        ))
        if debug:
            return df
        #stop_interpolation = []
        stopseq = set([])
        for _, row in df.iterrows():
            pattern_stop = stop_index[row.stpid]
            # TODO: log error and debug this
            if pattern_stop.sequence_no in stopseq:
                continue
            interpolated_timestamp = self.gtfs_time(row.tmstmp)
            self.writer.write('stop_times', {
                'trip_id': f'{self.day}.{self.vehicle_id}.{trip_id}',
                'arrival_time': interpolated_timestamp,
                'departure_time': interpolated_timestamp,
                'stop_id': pattern_stop.stop_id,
                'stop_sequence': pattern_stop.sequence_no,
                'shape_dist_traveled': pattern_stop.pattern_distance,
            })
            stopseq.add(pattern_stop.sequence_no)


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
    print(f'Routes: {args.routes}')
    if not args.routes:
        routes = None
    else:
        routes = args.routes.split(',')
    d = {}
    tmppath = Path('/tmp/jsoncachep')
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
    runs = next(iter(d.values()))
