#!/usr/bin/env python3

import dataclasses
import itertools
import sys
import argparse
import datetime
from pathlib import Path
import json
import math
import random

from backend.util import Util

import gtfs_kit
import pendulum
import pandas as pd
import tqdm


CACHEDIR = Path('~/transit/cache').expanduser()
CACHEDIR.mkdir(exist_ok=True)


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
        self.filter_time = None
        self.filter_end = None
        self.filtered_out = 0
        self.filetime = None

    def initialize(self):
        self.df['pdist'] = self.df.apply(lambda x: int(x.pdist), axis=1)

    def all_stops(self):
        joined = self.df[self.df.typ == 'S'].join(self.summary_df.set_index('pid'), on='pid')
        for _, x in joined.iterrows():
            yield x

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

    def set_filetime(self, ft):
        if self.filetime and self.filetime >= ft:
            return
        self.filetime = ft

    def get_filetime(self):
        return self.filetime

    def parse_all_days(self, process_fn=None):
        for day in self.datadir.glob('202?????'):
            self.parse_day(day.name, process_fn)

    def parse_day(self, day: str, process_fn=None):
        if process_fn is None:
            process_fn = self.parse_bustime_response
        pattern_dir = self.datadir / day
        for f in tqdm.tqdm(pattern_dir.glob('t*.json')):
            if f.name.startswith('ttscrape'):
                _, cmd, rawts = f.name.split('-')
                data_ts = datetime.datetime.strptime(rawts, '%Y%m%d%H%M%Sz.json').replace(tzinfo=datetime.UTC)
            else:
                data_ts = datetime.datetime.strptime(f'{day}{f.name}', '%Y%m%dt%H%M%Sz.json').replace(tzinfo=datetime.UTC)
            self.set_filetime(data_ts)
            if self.filter_end and data_ts >= self.filter_end:
                self.filtered_out += 1
                continue
            if self.filter_time and data_ts <= self.filter_time:
                self.filtered_out += 1
                continue
            #print(f'Reading {f}')
            with open(f) as fh:
                try:
                    p = json.load(fh)
                    if 'bustime-response' in p:
                        process_fn(p)
                    else:
                        self.parse_v2(p, process_fn)
                except json.JSONDecodeError:
                    continue

    def parse_v2(self, v2dict, process_fn):
        if v2dict.get('v') != '2.0':
            self.unknown_version += 1
            return
        for req in v2dict.get('requests', []):
            r = req.get('response', {})
            if 'bustime-response' not in r:
                self.errors += 1
                continue
            process_fn(r)

    def report(self):
        print(f'Errors: {self.errors}, unknown version: {self.unknown_version}')


class PredictionManager(PatternManager):
    def __init__(self, *argsz):
        super().__init__(*argsz)

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['prd']
        self.df = pd.concat([self.df, pd.DataFrame(top)], ignore_index=True)


class VehicleManager(PatternManager):
    def __init__(self, *argsz):
        super().__init__(*argsz)

    def get_trip(self, origtatripno: str, day: str):
        return self.df.query(f'origtatripno == "{origtatripno}" and day == {day}').sort_values('tmstmp')

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        # may need work for 24h+
        df['sched'] = df.apply(
            lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(x.stsd, '%Y-%m-%d') + datetime.timedelta(seconds=x.stst)),
            axis=1)
        df['tmstmp'] = df.apply(lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(x.tmstmp, '%Y%m%d %H:%M:%S')),
                                axis=1)
        # 'origtatripno',
        df = df.drop(columns=['lat', 'lon', 'hdg', 'tablockid', 'zone', 'mode', 'psgld', 'stst', 'stsd'])
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

bus 74 has suspiciously low trip ids - maybe use origtatripid
"""


@dataclasses.dataclass
class Managers:
    vm: VehicleManager
    pm: PatternManager
    dm: PredictionManager

    def initialize(self):
        self.pm.initialize()


class Trip:
    def __init__(self, d: dict):
        self.trip_id = d['origtatripno']
        self.day = d['stsd'].replace('-', '')
        self.sched = Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(d['stsd'], '%Y-%m-%d') + datetime.timedelta(seconds=d['stst']))
        self.des = d['des']
        self.rt = d['rt']
        self.pattern_id = d['pid']
        self.vehicle_id = d['vid']

    def key(self):
        return self.trip_id, self.day

    def out(self):
        return {'day': self.day,
                'origtatripno': self.trip_id,
                'sched': self.sched.isoformat(),
                'des': self.des,
                'rt': self.rt,
                'pid': self.pattern_id,
                'vid': self.vehicle_id
                }


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

    def process_trip(self, summary, times):
        if summary.empty:
            return None
        day = summary.iloc[0].day
        origtatripno = summary.iloc[0].origtatripno
        if times.empty:
            self.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_trip',
                                'msg': 'Missing raw times'})
            return None
        v = times.drop(columns='day').copy()
        #print(v)
        #v['tmstmp'] = v.apply(lambda x: datetime.datetime.fromisoformat(x.tmstmp), axis=1)
        v['tmstmp'] = v.apply(lambda x: int(x.tmstmp.timestamp()), axis=1)
        pattern = summary.iloc[0].pid
        #v = v.drop(columns='pid')
        stops = self.manager.pm.get_stops(pattern)
        if stops.empty:
            self.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_trip', 'msg': 'Missing stops'})
            return None
        p = stops.drop(columns=['typ', 'stpnm', 'lat', 'lon'])
        if v is None:
            self.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_trip', 'msg': 'Missing times'})
            return None
        pattern_template = pd.DataFrame(index=p.pdist, columns={'tmstmp': float('NaN')})
        combined = pd.concat([pattern_template, v.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        combined = combined.groupby(combined.index).last()
        px = self.manager.pm.get_stops(pattern)
        df = px.set_index('pdist').assign(tmstmp=combined.apply(lambda x: datetime.datetime.fromtimestamp(int(x))))
        if df.empty:
            self.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_trip',
                                'msg': 'Missing interpolation'})
            return None
        df['day'] = day
        df['origtatripno'] = origtatripno
        df = df.reset_index()
        df = df[['day', 'origtatripno', 'pdist', 'seq', 'stpid', 'stpnm', 'pid', 'tmstmp']]
        return df


class TransitCache:
    CACHE_FILENAME = CACHEDIR / 'transit-cache.json'
    TRIPS_FILENAME = CACHEDIR / 'trips.json'
    STOP_TIMES_FILENAME = CACHEDIR / 'stop_times.csv'
    RT_TRIPS_FILENAME = CACHEDIR / 'rt_trips.csv'

    def __init__(self, managers: Managers, rtc):
        self.manager = managers
        self.rtc = rtc
        self.stops_by_id ={}
        self.stops_index = {}
        self.cache: dict = {'last_updated': None}
        self.new_trip_list = []
        self.new_stop_list = []
        self.trip_ids = set([])
        self.days = set([])
        #self.rt_trip_ids = set([])
        self.trips_df = pd.DataFrame()
        self.stops_df = pd.DataFrame()
        self.rt_trips_df = pd.DataFrame()
        self.count = 0
        self.load()

    def process_stop_row(self, row: pd.Series):
        if row.stpid in self.stops_by_id:
            return
        self.stops_by_id[row.stpid] = f'{row.stpnm} -> {row.rtdir}'
        indexable = row.stpnm.lower().replace('&', '').replace('.', '').split(' ')
        indexable = [x.strip() for x in indexable if x.strip()]
        for name in indexable:
            self.stops_index.setdefault(name, set([])).add(row.stpid)

    def get_trip(self, trip_id, day):
        return (self.trips_df.query(f'origtatripno == "{trip_id}" and day == {day}'),
                self.stops_df.query(f'origtatripno == "{trip_id}" and day == {day}'))

    def new_trips(self):
        return [x.out() for x in self.new_trip_list]

    def maybe_add_trip(self, trip_item: dict):
        tt = Trip(trip_item)
        # stop info
        # tmstmp, pdist, dly
        if tt.key() not in self.trip_ids:
            self.trip_ids.add(tt.key())
            self.new_trip_list.append(tt)
        self.new_stop_list.append({
            'day': tt.day,
            'origtatripno': tt.trip_id,
            'tmstmp': Util.CTA_TIMEZONE.localize(
                datetime.datetime.strptime(trip_item['tmstmp'], '%Y%m%d %H:%M:%S')),
            'pdist': trip_item['pdist'],
            'dly': trip_item['dly']
        })

    def process_rt_trips(self, limit=0):
        existing_rt_trips = set([])
        if not self.rt_trips_df.empty:
            existing_rt_trips = set(self.rt_trips_df[['origtatripno', 'day']].itertuples(index=False, name=None))
        if self.trips_df.empty:
            print(f'No previously existing trips to process')
            return
        existing_trips = set(self.trips_df[['origtatripno', 'day']].itertuples(index=False, name=None))
        needed = existing_trips - existing_rt_trips
        if limit > 0:
            needed = set(list(needed)[:limit])
        for n in tqdm.tqdm(needed):
            origtatripno, day = n
            summary, times = self.get_trip(origtatripno, day)
            if summary.empty:
                rtc.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_rt_trips',
                                   'msg': 'Missing trip summary'})
                continue
            df = self.rtc.process_trip(summary, times)
            if df is None:
                #print(f'Skipped {n}')
                continue
            self.rt_trips_df = pd.concat([self.rt_trips_df, df], ignore_index=True)

    def load(self):
        if not self.CACHE_FILENAME.exists():
            return None
        with open(self.CACHE_FILENAME) as fh:
            self.cache = json.load(fh)
        self.trips_df = pd.read_json(self.TRIPS_FILENAME)
        self.trips_df['origtatripno'] = self.trips_df['origtatripno'].astype(str)
        self.days = set(self.trips_df['day'])
        self.stops_df = pd.read_csv(self.STOP_TIMES_FILENAME, low_memory=False)
        self.stops_df['origtatripno'] = self.stops_df['origtatripno'].astype(str)
        for stop in self.manager.pm.all_stops():
            self.process_stop_row(stop)
        if self.RT_TRIPS_FILENAME.exists():
            self.rt_trips_df = pd.read_csv(self.RT_TRIPS_FILENAME, low_memory=False)
            self.rt_trips_df['origtatripno'] = self.rt_trips_df['origtatripno'].astype(str)
            self.rt_trips_df['stpid'] = self.rt_trips_df['stpid'].astype(str)
            self.rt_trips_df['tmstmp'] = self.rt_trips_df.apply(lambda x: datetime.datetime.fromisoformat(x.tmstmp),
                                                                axis=1)

        self.trip_ids = set(self.trips_df['pid'])
        u = self.cache.get('last_updated')
        if u:
            self.manager.vm.filter_time = datetime.datetime.fromisoformat(u)
            print(f'Loading changes since {u}')

    def store(self):
        with open(self.CACHE_FILENAME, 'w') as wfh:
            json.dump(self.cache, wfh)
        df = pd.DataFrame(self.new_trips())
        self.trips_df = pd.concat([self.trips_df, df], ignore_index=True)
        self.trips_df.to_json(self.TRIPS_FILENAME)
        if self.new_stop_list:
            stops_df = pd.DataFrame(self.new_stop_list)
            if self.STOP_TIMES_FILENAME.exists():
                prev_stops_df = pd.read_csv(self.STOP_TIMES_FILENAME, low_memory=False)
                stops_df = pd.concat([prev_stops_df, stops_df], ignore_index=True)
            stops_df.to_csv(self.STOP_TIMES_FILENAME, index=False)
            self.stops_df = stops_df

    def store_rt(self):
        if not self.rt_trips_df.empty:
            self.rt_trips_df.to_csv(self.RT_TRIPS_FILENAME, index=False)
        ts = Util.utcnow().strftime('%Y%m%d%H%M%Sz')
        errors_fn = CACHEDIR / f'errors-{ts}.csv'
        error_df = pd.DataFrame(self.rtc.errors)
        error_df.to_csv(errors_fn)

    def process_fn(self, bd):
        self.count += 1
        top = bd['bustime-response']['vehicle']
        if not top:
            return
        for item in top:
            self.maybe_add_trip(item)

    def run(self, process=False, limit=0):
        for day in self.manager.vm.datadir.glob('202?????'):
            self.manager.vm.parse_day(day.name, self.process_fn)
        latest = self.manager.vm.get_filetime()
        self.cache['last_updated'] = latest.isoformat()
        self.store()
        if process:
            self.process_rt_trips(limit)
            self.store_rt()
        print(f'Processed {self.count}')

    def explore_route(self, rt, day=None, dir=None):
        if day is None:
            day = datetime.datetime.now().strftime('%Y%m%d')
        df = (tc.trips_df.query(f'rt == "{rt}" and day == {day}').sort_values('sched').
              join(m.pm.summary_df.set_index('pid'), on='pid', rsuffix='_pt'))
        if dir is not None:
            return df[df.rtdir == dir]
        return df

    def get_trip_info(self, origtatripno, day=None):
        if day is None:
            day = datetime.datetime.now().strftime('%Y%m%d')
        return tc.rt_trips_df.query(f'origtatripno == "{origtatripno}" and day == {day}').sort_values('tmstmp')

    def get_stop_info(self, stpid, day=None):
        if day is None:
            day = int(datetime.datetime.now().strftime('%Y%m%d'))
        return tc.rt_trips_df.query(f'stpid == "{stpid}" and day == {day}').join(
            tc.trips_df[tc.trips_df.day == day][['origtatripno', 'des', 'rt', 'vid']].set_index(
                'origtatripno'), on='origtatripno', rsuffix='_r').sort_values('tmstmp')

    def get_trip_keys(self, rt=False):
        if rt:
            df = self.rt_trips_df
        else:
            df = self.trips_df
        return set(df[['origtatripno', 'day']].itertuples(index=False, name=None))

    def find_stops(self, *args):
        candidates = set([])
        for x in args:
            key = x.strip().lower()
            val = self.stops_index.get(key)
            #print(f'Looking for {key}: {len(val)}')
            if not val:
                return None
            if not candidates:
                candidates |= val
            else:
                candidates &= val
        for c in candidates:
            nm = self.stops_by_id[c]
            print(f'{c:7}: {nm}')
        return candidates

    def make_travel_df(self, orig, dest, day=None):
        src_df = self.get_stop_info(orig, day=day)
        if src_df.empty:
            return {}, pd.DataFrame()
        dest_df = self.get_stop_info(dest, day=day)
        if dest_df.empty:
            return {}, pd.DataFrame()
        ddf = dest_df[dest_df.day == day].set_index('origtatripno')[['pdist', 'seq', 'stpid', 'stpnm', 'tmstmp']]
        joined = src_df.join(ddf, on='origtatripno', rsuffix='_dest')
        joined['travel_dist'] = joined['pdist_dest'] - joined['pdist']
        joined['travel_time'] = joined['tmstmp_dest'] - joined['tmstmp']
        head = joined.iloc[0]
        if head.seq > head.seq_dest:
            print(f'Out of order')
            return {}, pd.DataFrame()
        d = {'dist': head.travel_dist, 'rt': head.rt, 'vid': head.vid, 'pid': head.pid,
             'stpid': head.stpid, 'stpid_dest': head.stpid_dest, 'orig': head.stpnm, 'dest': head.stpnm_dest}
        return d, joined[['tmstmp', 'tmstmp_dest', 'travel_time']]

    def simulate(self, orig, dest, start_hour, iters=100):
        THRESH = datetime.timedelta(hours=1)
        df = pd.DataFrame()
        for d in self.days:
            _, tdf = self.make_travel_df(orig, dest, day=d)
            df = pd.concat([df, tdf], ignore_index=True)
        days = set(self.rt_trips_df['day'])
        p = [x for x in itertools.product(list(days), range(60))]
        random.shuffle(p)
        outcomes = []
        for x in p[:iters]:
            d, m = x
            ts = datetime.datetime.strptime(f'{d}{start_hour:02d}{m:02d}', '%Y%m%d%H%M')
            trips = df[df.tmstmp >= ts]
            if trips.empty:
                continue
            next_trip = trips.iloc[0]
            wait = next_trip.tmstmp - ts
            travel = next_trip.travel_time
            total = wait + travel
            if total > THRESH:
                continue
            outcomes.append({'start': ts, 'board': next_trip.tmstmp, 'wait': wait, 'travel': travel, 'total': total})
        out_df = pd.DataFrame(outcomes)
        return out_df


class SingleTripAnalyzer:
    def __init__(self, managers: Managers):
        self.managers = managers

    def analyze_trip(self, tripid):
        pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--update', action='store_true',
                        help='Update cache.')
    parser.add_argument('--process_rt_trips', action='store_true',
                        help='Interpolate rt trips.')
    parser.add_argument('--limit_rt', type=int, default=0,
                        help='Limit rt trips.')
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
    # for day in args.day:
    #     print(f'Parsing day {day}')
    #     #pm.parse_day(day)
    #     #predm.parse_day(day)
    #     vm.parse_day(day)
    #pm.report()
    m = Managers(vm=vm, pm=pm, dm=predm)
    m.initialize()
    rtc = RealtimeConverter(m)
    tc = TransitCache(m, rtc)
    if args.update:
        tc.run(args.process_rt_trips, args.limit_rt)
    #summary, times = tc.get_trip('20241217', '88357800')
    #z = rtc.process_trip(summary, times)
    #print(z)
    #x = tc.rt_trips_df[tc.rt_trips_df.stpid == 1225].sort_values('tmstmp')
    #print(x)
