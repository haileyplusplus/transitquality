#!/usr/bin/env python3

import sys
import argparse
import datetime
from pathlib import Path
import json

import gtfs_kit
import pendulum
import pandas as pd
import tqdm


class PatternManager:
    def __init__(self, datadir: Path):
        self.datadir = datadir
        self.summary_df = None
        self.pattern_df = None
        self.unknown_version = 0
        self.errors = 0

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['ptr'][0]
        df = pd.DataFrame(top['pt'])
        df['pid'] = top['pid']
        del top['pt']
        self.summary_df = pd.concat([self.summary_df, pd.DataFrame([top])], ignore_index=True)
        self.pattern_df = pd.concat([self.pattern_df, df], ignore_index=True)

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


class VehicleManager:
    def __init__(self, outdir: Path, prefix: str):
        self.outdir = outdir
        self.df = pd.DataFrame()
        self.parse(prefix)

    def parse(self, prefix: str):
        total = pd.DataFrame()
        for f in self.outdir.glob(f'ttscrape-getvehicles-{prefix}*.json'):
            df0 = pd.read_json(f)
            df = pd.DataFrame.from_records(df0['bustime-response']['vehicle'])
            total = pd.concat([total, df], ignore_index=True)
        self.df = total


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
    #vm = VehicleManager(datadir, '202412151')
    pm = PatternManager(datadir / 'getpatterns')
    for day in args.day:
        print(f'Parsing day {day}')
        pm.parse_day(day)
    pm.report()
