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
        self.filtered_out = 0
        self.filetime = None

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

    def set_filetime(self, ft):
        if self.filetime and self.filetime >= ft:
            return
        self.filetime = ft

    def get_filetime(self):
        return self.filetime

    def parse_day(self, day: str, process_fn=None):
        if process_fn is None:
            process_fn = self.parse_bustime_response
        pattern_dir = self.datadir / day
        for f in pattern_dir.glob('t*.json'):
            if f.name.startswith('ttscrape'):
                _, cmd, rawts = f.name.split('-')
                data_ts = datetime.datetime.strptime(rawts, '%Y%m%d%H%M%Sz.json').replace(tzinfo=datetime.UTC)
            else:
                data_ts = datetime.datetime.strptime(f'{day}{f.name}', '%Y%m%dt%H%M%Sz.json').replace(tzinfo=datetime.UTC)
            self.set_filetime(data_ts)
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

    def get_trip(self, tatripid: str, day: str):
        return self.df.query(f'tatripid == "{tatripid}" and day == {day}').sort_values('tmstmp')

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


class Trip:
    def __init__(self, d: dict):
        self.trip_id = d['tatripid']
        self.day = d['stsd'].replace('-', '')
        self.sched = Util.CTA_TIMEZONE.localize(datetime.datetime.strptime(d['stsd'], '%Y-%m-%d') + datetime.timedelta(seconds=d['stst']))
        self.des = d['des']
        self.rt = d['rt']
        self.pattern_id = d['pid']
        self.vehicle_id = d['vid']

    def key(self):
        return self.day, self.trip_id

    def out(self):
        return {'day': self.day,
                'tatripid': self.trip_id,
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

    def process_trip1(self, summary, times):
        #v = self.manager.vm.get_trip(tatripid).drop(columns=['sched', 'dly', 'des', 'vid', 'tatripid', 'rt'])
        #tatripid: str, day: str
        #summary, times = self.tc.get_trip(tatripid, day)
        v = times.drop(columns='day').copy()
        #print(v)
        v['tmstmp'] = v.apply(lambda x: datetime.datetime.fromisoformat(x.tmstmp), axis=1)
        v['tmstmp'] = v.apply(lambda x: int(x.tmstmp.timestamp()), axis=1)
        pattern = summary.iloc[0].pid
        #v = v.drop(columns='pid')
        stops = self.manager.pm.get_stops(pattern)
        p = stops.drop(columns=['typ', 'stpnm', 'lat', 'lon'])
        return v, p, pattern

    def process_trip(self, summary, times):
        v, p, pattern = self.process_trip1(summary, times)
        pattern_template = pd.DataFrame(index=p.pdist, columns={'tmstmp': float('NaN')})
        combined = pd.concat([pattern_template, v.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        combined = combined.groupby(combined.index).last()
        px = self.manager.pm.get_stops(pattern)
        df = px.set_index('pdist').assign(tmstmp=combined.apply(lambda x: datetime.datetime.fromtimestamp(int(x))))
        return df.reset_index()


class TransitCache:
    CACHE_FILENAME = CACHEDIR / 'transit-cache.json'
    TRIPS_FILENAME = CACHEDIR / 'trips.json'
    STOP_TIMES_FILENAME = CACHEDIR / 'stop_times.csv'

    def __init__(self, managers: Managers):
        self.manager = managers
        self.cache: dict = {'last_updated': None}
        self.new_trip_list = []
        self.new_stop_list = []
        self.new_rt_list = []
        self.trip_ids = set([])
        #self.rt_trip_ids = set([])
        self.trips_df = pd.DataFrame()
        self.stops_df = pd.DataFrame()
        self.rt_trips = pd.DataFrame()
        self.count = 0
        self.load()

    def get_trip(self, trip_id, day):
        return (self.trips_df.query(f'tatripid == "{trip_id}" and day == {day}'),
                self.stops_df.query(f'tatripid == "{trip_id}" and day == {day}'))

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
            'tatripid': tt.trip_id,
            'tmstmp': Util.CTA_TIMEZONE.localize(
                datetime.datetime.strptime(trip_item['tmstmp'], '%Y%m%d %H:%M:%S')),
            'pdist': trip_item['pdist'],
            'dly': trip_item['dly']
        })

    def load(self):
        if not self.CACHE_FILENAME.exists():
            return None
        with open(self.CACHE_FILENAME) as fh:
            self.cache = json.load(fh)
        self.trips_df = pd.read_json(self.TRIPS_FILENAME)
        self.stops_df = pd.read_csv(self.STOP_TIMES_FILENAME, low_memory=False)
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
        new_stops_df = pd.DataFrame(self.new_stop_list)
        self.stops_df = pd.concat([self.stops_df, new_stops_df], ignore_index=True)
        self.stops_df.to_csv(self.STOP_TIMES_FILENAME, index=False)

    def process_fn(self, bd):
        self.count += 1
        top = bd['bustime-response']['vehicle']
        if not top:
            return
        for item in top:
            self.maybe_add_trip(item)

    def run(self):
        for day in self.manager.vm.datadir.glob('202?????'):
            self.manager.vm.parse_day(day.name, self.process_fn)
        latest = self.manager.vm.get_filetime()
        self.cache['last_updated'] = latest.isoformat()
        self.store()
        print(f'Processed {self.count}')


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
    # for day in args.day:
    #     print(f'Parsing day {day}')
    #     #pm.parse_day(day)
    #     #predm.parse_day(day)
    #     vm.parse_day(day)
    #pm.report()
    m = Managers(vm=vm, pm=pm, dm=predm)
    m.initialize()
    tc = TransitCache(m)
    #tc.run()
    rtc = RealtimeConverter(m)
    summary, times = tc.get_trip('88357800', '20241217')
    z = rtc.process_trip(summary, times)
