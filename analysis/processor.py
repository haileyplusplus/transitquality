#!/usr/bin/env python3

import dataclasses
import itertools
import sys
import argparse
import datetime
from pathlib import Path
import json

from backend.util import Util
from analysis.datamodels import db_initialize, Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip, VehiclePosition, StopInterpolation, File, FileParse, Error


class Processor:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        db_initialize()
        self.processed = 0
        self.inserted = 0

    def update(self):
        self.find_files('getvehicles', self.data_dir / 'getvehicles')
        self.find_files('getvehicles', self.data_dir / 'chnghostbuses')
        self.find_files('getpatterns', self.data_dir / 'getpatterns')
        return self.processed, self.inserted

    def find_files(self, command: str, start_dir: Path):
        for root, directories, files in start_dir.walk():
            relative_path = root.relative_to(self.data_dir)
            for f in files:
                relative_path_str = relative_path.as_posix()
                if f.endswith('.json') and f.startswith('t'):
                    self.processed += 1
                    previous = File.select().where(File.relative_path == relative_path_str).where(File.filename == f)
                    if previous.exists():
                        continue
                    start_timestr = f'{root.name}{f}'
                    data_ts = datetime.datetime.strptime(start_timestr,
                                                         '%Y%m%dt%H%M%Sz.json').replace(tzinfo=datetime.UTC)
                elif f.endswith('.csv') and f.startswith('20'):
                    previous = File.select().where(File.relative_path == relative_path_str).where(File.filename == f)
                    self.processed += 1
                    if previous.exists():
                        continue
                    data_ts = datetime.datetime.strptime('%Y-%m-%d.csv',
                                                         '%Y%m%dt%H%M%Sz.json'
                                                         ).replace(tzinfo=Util.CTA_TIMEZONE)
                else:
                    continue
                file_model = File(relative_path=relative_path_str,
                                  filename=f,
                                  command=command,
                                  start_time=data_ts)
                file_model.save(force_insert=True)
                self.inserted += 1


    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['vehicle']

    def parse_bustime_response(self, brdict: dict):
        top = brdict['bustime-response']['ptr'][0]


    def parse_all_days(self, process_fn=None):
        for day in self.datadir.glob('202?????'):
            self.parse_day(day.name, process_fn)

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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--data_dir', type=str, nargs=1, default=['/transit/s3'],
                        help='Input directory for files')
    parser.add_argument('--day',  type=str, nargs='*', help='Day to summarize (YYYYmmdd)')
    args = parser.parse_args()
    print(args)
    datadir = Path(args.data_dir[0]).expanduser()