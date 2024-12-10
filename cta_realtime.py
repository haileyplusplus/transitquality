#!/usr/bin/env python3
import json
import sys
import argparse
import datetime
import logging
from pathlib import Path
import math
import tempfile
import zipfile

import pandas as pd
import geopandas as gpd

import gtfs_kit
from tqdm import tqdm

logger = logging.getLogger(__name__)


class FeedWrapper:
    def __init__(self, feed: gtfs_kit.Feed, datestr: str):
        self.feed = feed
        self.datestr = datestr
        self.cache = {}
        self.stats_cache = {}

    def get_timetable(self, route):
        rv = self.cache.get(route)
        if rv:
            return rv
        rv = self.feed.build_route_timetable(route, self.datestr)
        self.cache[route] = rv
        return rv

    def get_stop_patterns(self, route: str):
        stats = self.stats_cache.get(route, pd.DataFrame())
        if stats.empty:
            stats = self.feed.compute_trip_stats([route])
            self.stats_cache[route] = stats
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


class RealtimeManager:
    RT_SERVICE_PREFIX = 9000

    def __init__(self, rt_path: Path, start: datetime.date, days: int):
        self.rt_path = rt_path
        self.start = start
        self.num_days = days
        self.rt_data = {}

    def load_data(self):
        for x in range(self.num_days):
            d = self.start + datetime.timedelta(days=x)
            rtfile = self.rt_path / d.strftime('%Y-%m-%d.csv')
            rtdf = pd.read_csv(rtfile, low_memory=False)
            self.rt_data[x] = rtdf

    def calc_service_id(self, offset=None, date=None):
        if offset is None:
            offset = (date - self.start).days
        return str(self.RT_SERVICE_PREFIX + offset)

    def get(self, offset):
        return self.rt_data.get(offset)

    def get_all(self):
        for k, v in sorted(self.rt_data.items()):
            yield k, v

    def generate_services(self) -> pd.DataFrame:
        dd = []
        for x in range(self.num_days):
            service_id = self.RT_SERVICE_PREFIX + x
            date = self.start + datetime.timedelta(days=x)
            dd.append({'service_id': service_id,
                       'date': date.strftime('%Y%m%d'),
                       'exception_type': 1})
        return pd.DataFrame(dd)


class RealtimeConverter:
    EPSILON = 0.001

    def __init__(self, rt_path: Path, fw: FeedWrapper, start: datetime.date, num_days: int):
        self.rt_path = rt_path
        self.fw = fw
        self.start = start
        self.rt_manager = RealtimeManager(rt_path, start, num_days)
        self.output_stop_times = pd.DataFrame()
        self.output_trips = pd.DataFrame()
        self.errors = []
        self.trips_attempted = 0
        self.trips_processed = 0
        self.trips_seen = set([])

    def process(self):
        self.rt_manager.load_data()
        # rtfile = self.rt_path / self.start.strftime('%Y-%m-%d.csv')
        # rtdf = pd.read_csv(rtfile, low_memory=False)
        # self.days[self.start] = rtdf

    def frame_interpolation(self, single_rt1: pd.DataFrame, single_sched: pd.DataFrame) -> pd.DataFrame | None:
        if single_rt1.empty or len(single_rt1) < 2:
            return None
        fakerows = []
        maxval = single_rt1.pdist.max()
        beginnings = single_rt1[single_rt1.pdist == 0]
        endings = single_rt1[single_rt1.pdist == maxval]
        begin_drop = len(beginnings) - 1
        end_drop = len(endings) - 1
        if end_drop > 0:
            single_rt1.drop(single_rt1.tail(end_drop).index, inplace=True)
        if begin_drop > 0:
            single_rt1.drop(single_rt1.head(begin_drop).index, inplace=True)
        if len(single_rt1) < 2:
            return None
        try:
            if single_rt1.iloc[0].pdist != 0:
                deltas = single_rt1.iloc[1] - single_rt1.iloc[0]
                if deltas.tmstmp < self.EPSILON:
                    return None
                v = deltas.pdist / deltas.tmstmp
                if v < self.EPSILON:
                    return None
                ntf = single_rt1.iloc[0].tmstmp - single_rt1.iloc[0].pdist / v
                if math.isnan(ntf):
                    return None
                nt = int(ntf)
                #fakerow = pd.DataFrame([{'tmstmp': nt, 'pdist': 0}])
                fakerows.append({'tmstmp': nt, 'pdist': 0})
            if single_rt1.iloc[-1].pdist < single_sched.iloc[-1].pdist:
                deltas = single_rt1.iloc[-1] - single_rt1.iloc[-2]
                if deltas.tmstmp < self.EPSILON:
                    return None
                v = deltas.pdist / deltas.tmstmp
                if v < self.EPSILON:
                    return None
                ntf = single_rt1.iloc[-1].tmstmp + single_rt1.iloc[-1].pdist / v
                if math.isnan(ntf):
                    return None
                nt = int(ntf)
                fakerows.append({'tmstmp': nt, 'pdist': single_sched.iloc[-1].pdist})
            if fakerows:
                fdf = pd.DataFrame(fakerows)
                return pd.concat([fdf, single_rt1]).sort_values(['pdist'], ignore_index=True)
            else:
                return single_rt1
        except OverflowError as e:
            self.errors.append(e)
        return None

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
        single_rt = self.frame_interpolation(single_rt1, single_sched)
        if single_rt is None:
            return None
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
        logger.debug(trip_pattern_output['stop_id'])
        if not (ndf['stop_id'] == trip_pattern_output['stop_id']).all():
            print(f'Pattern mismatch')
            return None

        def to_gtfs_time(unix_timestamp: int):
            ts = datetime.datetime.fromtimestamp(unix_timestamp)
            hour = ts.hour
            if hour < 4:
                hour += 24
            return f'{hour:02d}:{ts.minute:02d}:{ts.second:02d}'

        times = trip_pattern_output['tmstmp'].apply(to_gtfs_time)
        logger.debug(times.rename('arrival_time'))
        logger.debug(ndf['arrival_time'])
        ndf['arrival_time'] = times.rename('arrival_time')
        ndf['departure_time'] = times.rename('departure_time')
        ndf['trip_id'] = rt_trip_id
        logger.debug(ndf)
        return ndf

    def process_pattern(self, df: pd.DataFrame, date, route, pid):
        pdf = df.query(f'rt == "{route}" and pid == {pid}')
        approx_len = pdf.pdist.max()
        schedule_patterns = self.fw.get_closest_pattern(route, approx_len)
        representative_trip = schedule_patterns.iloc[0].trip_id
        sched_stops = self.fw.get_trip_stops(representative_trip)
        logger.debug(f'Scheduled stops: {sched_stops}')
        rt_trips = pdf.tatripid.unique()
        sched_trip = self.fw.get_trip(representative_trip)
        for rt_trip_id in rt_trips:
            self.trips_attempted += 1
            logger.debug(f' ==== T ====')
            single_rt_trip = pdf[pdf.tatripid == rt_trip_id]
            r = self.process_trip(date, route, pid, sched_stops, single_rt_trip)
            if r is None:
                self.errors.append(['process', date.strftime('%Y%m%d'), route, str(pid), str(rt_trip_id)])
                continue
            service_id = self.rt_manager.calc_service_id(date=date)
            new_trip_id = f'{rt_trip_id}-{service_id}'
            # TODO: consider renumbering these
            if new_trip_id in self.trips_seen:
                self.errors.append(['repeated_trip', date.strftime('%Y%m%d'), route, str(pid), str(rt_trip_id)])
                continue
            self.trips_seen.add(new_trip_id)
            output = r[r.stop_id != -1].reset_index()
            #output.to_csv('/tmp/p1.csv')
            #print(output)
            #print()
            ndf = self.apply_to_template(sched_stops, output, new_trip_id)
            if ndf is None:
                self.errors.append(['template', date.strftime('%Y%m%d'), route, str(pid), str(rt_trip_id)])
                continue
            # need to rewrite trips too
            self.output_stop_times = pd.concat([self.output_stop_times, ndf])
            rewrite_rt_trip = sched_trip.copy().reset_index().drop(columns=['index'])
            rewrite_rt_trip['service_id'] = service_id
            rewrite_rt_trip['trip_id'] = new_trip_id
            rewrite_rt_trip['block_id'] = single_rt_trip.iloc[0].tablockid.replace(' ', '')
            #print(single_rt_trip)
            #print(rewrite_rt_trip)
            self.output_trips = pd.concat([self.output_trips, rewrite_rt_trip])
            self.trips_processed += 1

    def process_route(self, route):
        # date = self.start
        # for x in range(self.)
        # df: pd.DataFrame = self.days.get(date)
        work = []
        for offset, df in self.rt_manager.get_all():
            date = self.start + datetime.timedelta(days=offset)
            pdf = df.query(f'rt == "{route}"')
            patterns = pdf.pid.unique()
            for p in patterns:
                work.append((pdf, date, route, p))
        for w in tqdm(work):
            self.process_pattern(*w)

    def output_summary(self):
        print(f'Trips attempted: {self.trips_attempted:6d}')
        print(f'Trips processed: {self.trips_processed:6d}')

    def write_files(self, output_dir: Path):
        self.output_stop_times.to_csv(output_dir / 'new_stop_times.txt', index=False)
        self.output_trips.to_csv(output_dir / 'new_trips.txt', index=False)
        self.rt_manager.generate_services().to_csv(output_dir / 'new_calendar_dates.txt', index=False)

    def write_zip(self, output_dir: Path):
        startstr = self.start.strftime('%Y%m%d')
        with tempfile.TemporaryDirectory() as tempdir:
            tmp_path = Path(tempdir)
            zipfilename = output_dir / f'cta_rt_sched_{startstr}.zip'
            self.output_stop_times.to_csv(tmp_path / 'stop_times.txt', index=False)
            self.output_trips.to_csv(tmp_path / 'trips.txt', index=False)
            self.rt_manager.generate_services().to_csv(tmp_path / 'calendar_dates.txt', index=False)
            feed = self.fw.feed
            for dfname in {'agency', 'calendar', 'frequencies', 'routes', 'shapes', 'stops', 'transfers'}:
                df = getattr(feed, dfname)
                df.to_csv(tmp_path / f'{dfname}.txt', index=False)
            with zipfile.ZipFile(zipfilename, 'w') as zf:
                for fn in tmp_path.glob('*.txt'):
                    zf.write(fn, arcname=fn.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert realtime bus status feeds to GTFS format.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--route', type=str, nargs=1, default=['72'],
                        help='Route to analyze.')
    parser.add_argument('--pattern', type=int, nargs=1,
                        #, default=[10918],
                        help='Stop pattern to analyze.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/tmp/transit'],
                        help='Output directory for generated files.')
    parser.add_argument('--num_days', type=int, nargs=1,
                        default=[7],
                        #, default=[10918],
                        help='Number of days to analyze.')
    args = parser.parse_args()
    logging.basicConfig()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    print(f'Starting')
    output_dir = Path(args.output_dir[0]).expanduser()
    runtime = datetime.datetime.now()
    runstr = runtime.strftime('%Y%m%d%H%M%S')
    error_file = output_dir / f'errors-{runstr}.json'
    sched_path = Path('~/datasets/transit').expanduser()
    feed = gtfs_kit.read_feed(sched_path / 'google_transit_2024-05-18.zip', 'mi')
    # TODO: match schedule patterns to actual days
    fw = FeedWrapper(feed, '20240506')
    start = datetime.date(2024, 5, 6)
    num_days = args.num_days[0]
    rt = RealtimeConverter(Path('~/tmp/transit').expanduser(),
                           fw,
                           start,
                           num_days)
    rt.process()
    #
    if args.pattern:
        raise ValueError('Not supported')
        #rt.process_pattern(start, args.route[0], args.pattern[0])
    else:
        rt.process_route(args.route[0])
    #rt.write_files(output_dir)
    rt.write_zip(output_dir)
    print(len(rt.errors), 'errors')
    with open(error_file, 'w') as efh:
        json.dump(rt.errors, efh, indent=4)
    rt.output_summary()
