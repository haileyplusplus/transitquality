#!/usr/bin/env python3
import dataclasses
import sys
import argparse
import datetime
from pathlib import Path
import json

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
        return self.df[self.df.pid == pid][self.df.typ == 's']

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
