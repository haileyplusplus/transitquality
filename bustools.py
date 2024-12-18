#!/usr/bin/env python3
import dataclasses
import sys
import argparse
import datetime
from pathlib import Path
import json
import math

from backend.util import Util

import gtfs_kit
import pendulum
import pandas as pd
import tqdm


def stst(x: int):
    s = x % 60
    tm = x // 60
    h = tm // 60
    m = tm % 60
    return f'{h:02d}:{m:02d}:{s:02d}'


class PatternManager:
    def __init__(self, datadir: Path):
        self.datadir = datadir
        self.summary_df = None
        self.df = None
        self.unknown_version = 0
        self.errors = 0

    def initialize(self):
        self.df['pdist'] = self.df.apply(lambda x: int(x.pdist), axis=1)

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['ptr'][0]
        df = pd.DataFrame(top['pt'])
        df['pid'] = top['pid']
        del top['pt']
        self.summary_df = pd.concat([self.summary_df, pd.DataFrame([top])], ignore_index=True)
        self.df = pd.concat([self.df, df], ignore_index=True)

    def get(self, pid: int):
        return self.df[self.df.pid == pid]

    def get_stops(self, pid: int):
        return self.df.query(f'pid == {pid} and typ =="S"')

    def parse_day(self, day: str):
        pattern_dir = self.datadir / day
        for f in pattern_dir.glob('t*.json'):
            #print(f'Reading {f}')
            with open(f) as fh:
                try:
                    p = json.load(fh)
                    if 'bustime-response' in p:
                        self.parse_bustime_response(p)
                    else:
                        self.parse_v2(p)
                except json.JSONDecodeError:
                    continue

    def parse_v2(self, v2dict):
        if v2dict.get('v') != '2.0':
            self.unknown_version += 1
            return
        for req in v2dict.get('requests', []):
            r = req.get('response', {})
            if 'bustime-response' not in r:
                self.errors += 1
                continue
            self.parse_bustime_response(r)

    def report(self):
        print(f'Errors: {self.errors}, unknown version: {self.unknown_version}')


class PredictionManager(PatternManager):
    def __init__(self, *argsz):
        super().__init__(*argsz)

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['prd']
        self.df = pd.concat([self.df, pd.DataFrame(top)], ignore_index=True)
        #self.df = pd.concat([self.df, pd.DataFrame([top])], ignore_index=True)


class VehicleManager(PatternManager):
    def __init__(self, *argsz):
        super().__init__(*argsz)

    def get_trip(self, tatripid: str):
        return self.df[self.df.tatripid == tatripid].sort_values('tmstmp')

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        # may need work for 24h+
        df['sched'] = df.apply(
            lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(x.stsd, '%Y-%m-%d') + datetime.timedelta(seconds=x.stst)),
            axis=1)
        df['tmstmp'] = df.apply(lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(x.tmstmp, '%Y%m%d %H:%M:%S')),
                                axis=1)
        df = df.drop(columns=['lat', 'lon', 'hdg', 'origtatripno', 'tablockid', 'zone', 'mode', 'psgld', 'stst', 'stsd'])
        return df

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['vehicle']
        this_df = pd.DataFrame(top)
        this_df = self.preprocess(this_df)
        self.df = pd.concat([self.df, this_df], ignore_index=True)
        #self.df = pd.concat([self.df, pd.DataFrame([top])], ignore_index=True)


"""
Trip analysis (one or dataframe)
trip predicted start time: prdtm of min value for trip. origtatripno and tatripid both correlate between predictions and vehicles

trip actual start time: 
vdf[vdf.origtatripno == '259146355'].sort_values('tmstmp')[:50], last value with min pdist

trip average speed
trip sched vs actual arrival
departure headway
midpoint headway
arrival headway
"""


@dataclasses.dataclass
class Managers:
    vm: VehicleManager
    pm: PatternManager
    dm: PredictionManager

    def initialize(self):
        self.pm.initialize()


class RealtimeConverter:
    EPSILON = 0.001

    def __init__(self, manager: Managers):
        self.manager = manager
        self.output_stop_times = pd.DataFrame()
        self.output_trips = pd.DataFrame()
        self.errors = []
        self.trips_attempted = 0
        self.trips_processed = 0
        self.trips_seen = set([])

    @staticmethod
    def frame_interpolation(single_rt1: pd.DataFrame, single_sched: pd.DataFrame) -> pd.DataFrame | None:
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
                if deltas.tmstmp < RealtimeConverter.EPSILON:
                    return None
                v = deltas.pdist / deltas.tmstmp
                if v < RealtimeConverter.EPSILON:
                    return None
                ntf = single_rt1.iloc[0].tmstmp - single_rt1.iloc[0].pdist / v
                if math.isnan(ntf):
                    return None
                nt = int(ntf)
                #fakerow = pd.DataFrame([{'tmstmp': nt, 'pdist': 0}])
                fakerows.append({'tmstmp': nt, 'pdist': 0})
            if single_rt1.iloc[-1].pdist < single_sched.iloc[-1].pdist:
                deltas = single_rt1.iloc[-1] - single_rt1.iloc[-2]
                if deltas.tmstmp < RealtimeConverter.EPSILON:
                    return None
                v = deltas.pdist / deltas.tmstmp
                if v < RealtimeConverter.EPSILON:
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
            return None
            #self.errors.append(e)
        return None

    def process_trip1(self, tatripid: str):
        v = self.manager.vm.get_trip(tatripid).drop(columns=['sched', 'dly', 'des', 'vid', 'tatripid', 'rt'])
        v['tmstmp'] = v.apply(lambda x: int(x.tmstmp.timestamp()), axis=1)
        pattern = v.iloc[0].pid
        v = v.drop(columns='pid')
        p = self.manager.pm.get_stops(pattern).drop(columns=['typ', 'stpnm', 'lat', 'lon'])
        return v, p

    def interpolate(self, tatripid: str):
        # v = self.manager.vm.get_trip(tatripid).drop(columns=['sched', 'dly', 'des', 'vid'])
        # v['tmstmp'] = v.apply(lambda x: int(x.tmstmp.timestamp()), axis=1)
        # pattern = v.iloc[0].pid
        # p = self.manager.pm.get_stops(pattern)
        v, p = self.process_trip1(tatripid)
        # single_rt = self.frame_interpolation(v, p)
        # if single_rt is None:
        #     return None
        pattern_template = pd.DataFrame(index=p.pdist,
                                        columns={'tmstmp': float('NaN')})
        combined = pd.concat([pattern_template, v.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        # #single_rt['stpid'] = -1
        # combined = pd.concat([single_rt, p],
        #                      ignore_index=True).sort_values(
        #     ['pdist']).set_index('pdist')
        # combined['stpid'] = combined['stpid'].astype(int)
        # interpolated = combined.interpolate(method='index')[1:].astype(int)
        combined = combined.groupby(combined.index).last()
        df = p.set_index('pdist').assign(tmstmp=combined.apply(lambda x: datetime.datetime.fromtimestamp(int(x))))
        return df

    def process_trip(self, tatripid: str):
        return self.interpolate(tatripid).reset_index()
        #interpolated.
        #interpolated = interpolated[interpolated.stpid != -1]
        #z = interpolated.apply(lambda x: str(x.stpid), axis=1).set_index('stpid')



    def apply_to_template(self, sched_stops: pd.DataFrame, trip_pattern_output: pd.DataFrame, rt_trip_id):
        logger.debug(f'apply_to_template {rt_trip_id}')
        logger.debug(f'sched stops {sched_stops}')
        logger.debug(f'trip pattern output {trip_pattern_output}')
        ndf = sched_stops.sort_values(['stop_sequence']).copy().reset_index()
        ndf = ndf.drop(columns=['index'])
        ndf['stop_id'] = ndf['stop_id'].astype(int)
        if not (ndf['stop_id'] == trip_pattern_output['stop_id']).all():
            #print(f'Pattern mismatch')
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
        rt_trips = pdf.tatripid.unique()
        schedule_patterns = self.fw.get_closest_pattern(route, date.strftime('%Y%m%d'), approx_len)
        if schedule_patterns.empty:
            # TODO: log error
            return
        # now we need to choose the right direction by looking at which stops the rt ends are closest to
        rt_begin = pdf.sort_values('pdist').iloc[0]
        rt_end = pdf.sort_values('pdist').tail(1).iloc[0]
        dists = []
        for _, pattern in schedule_patterns.iterrows():
            dist = 0
            start = self.fw.get_stop(pattern.start_stop_id).geometry.iloc[0]
            end = self.fw.get_stop(pattern.end_stop_id).geometry.iloc[0]
            dist += rt_begin.geometry.distance(start)
            dist += rt_end.geometry.distance(end)
            dists.append((dist, pattern.name))
        dists.sort()
        representative_trip = schedule_patterns.loc[dists[0][1]].trip_id
        sched_stops = self.fw.get_trip_stops(representative_trip)
        logger.debug(f'Scheduled stops: {sched_stops}')
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
                sj = json.loads(sched_stops.to_json())
                self.errors.append(['template', sj, date.strftime('%Y%m%d'), route, str(pid), str(rt_trip_id)])
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

    def process_route(self, route, pattern=None):
        # date = self.start
        # for x in range(self.)
        # df: pd.DataFrame = self.days.get(date)
        work = []
        for offset, df in self.rt_manager.get_all():
            date = self.start + datetime.timedelta(days=offset)
            pdf = df.query(f'rt == "{route}"')
            patterns = pdf.pid.unique()
            if pattern is not None:
                work.append((pdf, date, route, pattern))
                continue
            for p in patterns:
                work.append((pdf, date, route, p))
        for w in tqdm(work):
            self.process_pattern(*w)


class SingleTripAnalyzer:
    def __init__(self, managers: Managers):
        self.managers = managers

    def analyze_trip(self, tripid):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--schedule_dir', type=str, nargs=1, default=['~/datasets/transit'],
                        help='Directory containing schedule files.')
    parser.add_argument('--data_dir', type=str, nargs=1, default=['~/transit/bustracker/raw'],
                        help='Output directory for generated files.')
    parser.add_argument('--day',  type=str, nargs='*', help='Day to summarize (YYYYmmdd)')
    args = parser.parse_args()
    print(args)
    datadir = Path(args.data_dir[0]).expanduser()
    vm = VehicleManager(datadir / 'getvehicles')
    pm = PatternManager(datadir / 'getpatterns')
    predm = PredictionManager(datadir / 'getpredictions')
    for day in datadir.glob('202?????'):
        pm.parse_day(day.name)
    for day in args.day:
        print(f'Parsing day {day}')
        #pm.parse_day(day)
        predm.parse_day(day)
        vm.parse_day(day)
    pm.report()
    m = Managers(vm=vm, pm=pm, dm=predm)
    m.initialize()
    rtc = RealtimeConverter(m)
    t = rtc.process_trip('88357800')