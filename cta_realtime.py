#!/usr/bin/env python3

import sys
import argparse
import datetime
import logging
from pathlib import Path

import pandas as pd
import geopandas as gpd

import gtfs_kit

logger = logging.getLogger(__name__)


class FeedWrapper:
    def __init__(self, feed: gtfs_kit.Feed, datestr: str):
        self.feed = feed
        self.datestr = datestr
        self.cache = {}

    def get_timetable(self, route):
        rv = self.cache.get(route)
        if rv:
            return rv
        rv = self.feed.build_route_timetable(route, self.datestr)
        self.cache[route] = rv
        return rv

    def get_stop_patterns(self, route: str):
        stats = self.feed.compute_trip_stats([route])
        stop_patterns = stats.groupby(['stop_pattern_name'])
        # g.iloc[(g['distance'] - key).abs().argsort()][:1]
        return stop_patterns.first()

    def get_closest_pattern(self, route, dist):
        patterns = self.get_stop_patterns(route)
        return patterns.iloc[(patterns['distance'] - dist).abs().argsort()][:2]

    def get_trip(self, trip_id):
        return self.feed.trips[self.feed.trips.trip_id == trip_id]

    def get_trip_stops(self, trip_id):
        return self.feed.stop_times[self.feed.stop_times.trip_id == trip_id]


class RealtimeConverter:
    def __init__(self, rt_path: Path, fw: FeedWrapper, start: datetime.date, end: datetime.date):
        self.rt_path = rt_path
        self.fw = fw
        self.start = start
        self.end = end
        self.days = {}
        self.output_stop_times = pd.DataFrame()

    def process(self):
        rtfile = self.rt_path / self.start.strftime('%Y-%m-%d.csv')
        rtdf = pd.read_csv(rtfile, low_memory=False)
        self.days[self.start] = rtdf

    def process_trip(self, date, route, pid, sched_stops, single_rt_trip):
        logger.debug(f'  -- process trip -- ')
        # this interpolation isn't quite right: maybe need to set the index and use that
        # fixed with below
        # pd.concat([spf, s3], ignore_index=True).sort_values(['pdist']).interpolate(method='linear')[1:].astype(int)[:50]
        # sp['unixts'] = sp.apply(lambda x: int(datetime.datetime.strptime(x.tmstmp, '%Y%m%d %H:%M').timestamp()), axis=1)
        # intermediate2 = pd.concat([spf, s3], ignore_index=True).sort_values(['pdist']).set_index('pdist').interpolate(method='index')[1:].astype(int)
        # trip_stops from gtfs
        single_sched = pd.concat([sched_stops['shape_dist_traveled'].rename('pdist'), sched_stops['stop_id']],
                                 axis=1).reset_index().drop(columns=['index'])
        def timefn(x):
            # TODO: figure out 24-hour cutover
            rawdate = datetime.datetime.strptime(x, '%Y%m%d %H:%M')
            if rawdate.hour < 4:
                rawdate += datetime.timedelta(days=1)
            return int(rawdate.timestamp())
        times = single_rt_trip['tmstmp'].apply(timefn)
        single_rt1 = pd.concat([times, single_rt_trip['pdist']], axis=1)

        fakerows = []
        if single_rt1.iloc[0].pdist != 0:
            deltas = single_rt1.iloc[1] - single_rt1.iloc[0]
            v = deltas.pdist / deltas.tmstmp
            nt = int(single_rt1.iloc[0].tmstmp - single_rt1.iloc[0].pdist / v)
            #fakerow = pd.DataFrame([{'tmstmp': nt, 'pdist': 0}])
            fakerows.append({'tmstmp': nt, 'pdist': 0})
        if single_rt1.iloc[-1].pdist < single_sched.iloc[-1].pdist:
            deltas = single_rt1.iloc[-1] - single_rt1.iloc[-2]
            v = deltas.pdist / deltas.tmstmp
            nt = int(single_rt1.iloc[-1].tmstmp + single_rt1.iloc[-1].pdist / v)
            fakerows.append({'tmstmp': nt, 'pdist': single_sched.iloc[-1].pdist})
        if fakerows:
            fdf = pd.DataFrame(fakerows)
            single_rt = pd.concat([fdf, single_rt1]).sort_values(['pdist'], ignore_index=True)
        else:
            single_rt = single_rt1
        single_rt['stop_id'] = -1
        logger.debug(single_rt)
        logger.debug(single_sched)
        logger.debug(' ====== ')
        combined = pd.concat([single_rt, single_sched],
                             ignore_index=True).sort_values(
            ['pdist']).set_index('pdist')
        logger.debug(combined)
        #combined = combined.infer_objects(copy=True)
        combined['stop_id'] = combined['stop_id'].astype(int)
        interpolated = combined.interpolate(method='index')[1:].astype(int)
        return interpolated

    def apply_to_template(self, sched_stops: pd.DataFrame, trip_pattern_output: pd.DataFrame, rt_trip_id):
        ndf = sched_stops.copy().reset_index()
        ndf = ndf.drop(columns=['index'])
        ndf['stop_id'] = ndf['stop_id'].astype(int)
        print(trip_pattern_output['stop_id'])
        if not (ndf['stop_id'] == trip_pattern_output['stop_id']).all():
            print(f'Pattern mismatch')
            return

        def to_gtfs_time(unix_timestamp: int):
            ts = datetime.datetime.fromtimestamp(unix_timestamp)
            hour = ts.hour
            if hour < 4:
                hour += 24
            return f'{hour:02d}:{ts.minute:02d}:{ts.second:02d}'

        times = trip_pattern_output['tmstmp'].apply(to_gtfs_time)
        print(times.rename('arrival_time'))
        print(ndf['arrival_time'])
        ndf['arrival_time'] = times.rename('arrival_time')
        ndf['departure_time'] = times.rename('departure_time')
        ndf['trip_id'] = rt_trip_id
        print(ndf)
        return ndf

    def process_pattern(self, date, route, pid):
        df: pd.DataFrame = self.days.get(date)
        if df.empty:
            return False
        pdf = df.query(f'rt == "{route}" and pid == {pid}')
        approx_len = pdf.pdist.max()
        schedule_patterns = self.fw.get_closest_pattern(route, approx_len)
        representative_trip = schedule_patterns.iloc[0].trip_id
        sched_stops = self.fw.get_trip_stops(representative_trip)
        logger.debug(f'Scheduled stops: {sched_stops}')
        rt_trips = pdf.tatripid.unique()
        for rt_trip_id in rt_trips:
            logger.debug(f' ==== T ====')
            single_rt_trip = pdf[pdf.tatripid == rt_trip_id]
            r = self.process_trip(date, route, pid, sched_stops, single_rt_trip)
            output = r[r.stop_id != -1].reset_index()
            #output.to_csv('/tmp/p1.csv')
            print(output)
            print()
            ndf = self.apply_to_template(sched_stops, output, rt_trip_id)
            # need to rewrite trips too
            self.output_stop_times = pd.concat([self.output_stop_times, ndf])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Collect information about photos into a central db.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--route', type=str, nargs=1, default=['72'],
                        help='Route to analyze.')
    parser.add_argument('--pattern', type=int, nargs=1, default=[10918],
                        help='Stop pattern to analyze.')
    parser.add_argument('--output_file', type=str, nargs=1, default=['~/tmp/transit/new_stop_times.txt'],
                        help='Stop pattern to analyze.')
    args = parser.parse_args()
    logging.basicConfig()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    print(f'Starting')
    output_file = Path(args.output_file[0]).expanduser()
    sched_path = Path('~/datasets/transit').expanduser()
    feed = gtfs_kit.read_feed(sched_path / 'google_transit_2024-05-18.zip', 'mi')
    fw = FeedWrapper(feed, '20240509')
    start = datetime.date(2024, 5, 9)
    rt = RealtimeConverter(Path('~/tmp/transit').expanduser(),
                           fw,
                           start,
                           start)
    rt.process()
    rt.process_pattern(start, args.route[0], args.pattern[0])
    rt.output_stop_times.to_csv(output_file, index=False)
