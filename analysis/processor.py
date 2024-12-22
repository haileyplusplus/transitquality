#!/usr/bin/env python3

import dataclasses
import itertools
from functools import partial
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
        self.find_files('getpatterns', self.data_dir / 'getpatterns')
        self.find_files('getvehicles', self.data_dir / 'getvehicles')
        self.find_files('getvehicles', self.data_dir / 'chnghostbuses')
        return self.processed, self.inserted

    def find_files(self, command: str, start_dir: Path):
        for root, directories, files in start_dir.walk():
            relative_path = root.relative_to(self.data_dir)
            for f in files:
                relative_path_str = relative_path.as_posix()
                if f.endswith('.json') and f.startswith('tt'):
                    self.processed += 1
                    previous = File.select().where(File.relative_path == relative_path_str).where(File.filename == f)
                    if previous.exists():
                        continue
                    start_timestr = f
                    data_ts = datetime.datetime.strptime(start_timestr,
                                                         'ttscrape-getpatterns-%Y%m%d%H%M%Sz.json').replace(tzinfo=datetime.UTC)
                elif f.endswith('.json') and f.startswith('t'):
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
                    data_ts = datetime.datetime.strptime(f,
                                                         '%Y-%m-%d.csv',
                                                         ).replace(tzinfo=Util.CTA_TIMEZONE)
                else:
                    continue
                file_model = File(relative_path=relative_path_str,
                                  filename=f,
                                  command=command,
                                  start_time=data_ts)
                file_model.save(force_insert=True)
                self.inserted += 1

    def parse_new_patterns(self):
        existing = FileParse.select(FileParse.file_id).where(FileParse.parse_success)
        files = File.select().where(File.command == 'getpatterns')
        previous = Pattern.select().count()
        for f in files:
            if f in existing:
                continue
            self.parse_file(f)
        patterns = Pattern.select().count()
        return {'files': len(files), 'previous': previous, 'patterns': patterns}

    @staticmethod
    def parse_getvehicles_response(file_ts: datetime.datetime, brdict: dict):
        top = brdict['bustime-response']['vehicle']

    @staticmethod
    def get_direction(dirname):
        dir_ = Direction.get_or_none(Direction.direction_id == dirname)
        if dir_ is None:
            dir_ = Direction(direction_id=dirname)
            dir_.save(force_insert=True)
        return dir_

    @staticmethod
    def parse_getpatterns_response(file_ts: datetime.datetime, brdict: dict):
        top = brdict['bustime-response']['ptr'][0]
        pid = int(top['pid'])
        existing = Pattern.get_or_none(Pattern.pattern_id == pid)
        if existing is not None:
            return
        p = Pattern(pattern_id=pid,
                    direction=Processor.get_direction(top['rtdir']),
                    timestamp=file_ts,
                    length=int(top['ln']))
        p.save(force_insert=True)
        for d in top['pt']:
            typ = d['typ']
            if typ == 'W':
                w = Waypoint(
                    pattern=p,
                    sequence_no=int(d['seq']),
                    lat=d['lat'],
                    lon=d['lon'],
                    distance=int(d['pdist'])
                )
                w.save(force_insert=True)
            elif typ == 'S':
                stop_id = str(d['stpid'])
                stop = Stop.get_or_none(Stop.stop_id == stop_id)
                if stop is None:
                    stop = Stop(stop_id=stop_id,
                                stop_name=d['stpnm'],
                                lat=d['lat'],
                                lon=d['lon']
                                )
                    stop.save(force_insert=True)
                pattern_stop = PatternStop(
                    pattern=p,
                    stop=stop,
                    sequence_no=int(d['seq']),
                    pattern_distance=int(d['pdist'])
                )
                pattern_stop.save(force_insert=True)
            else:
                raise ValueError(f'Unexpected pattern type {typ}')

    def parse_file(self, file_model: File):
        f: Path = self.data_dir / file_model.relative_path / file_model.filename
        attempt = FileParse(file_id=file_model,
                            parse_time=Util.utcnow(),
                            parse_stage='first',
                            parse_success=False)
        attempt.save(force_insert=True)
        if not f.exists():
            error = Error(parse_attempt=attempt,
                          error_class='Missing file')
            error.save(force_insert=True)
            return False
        process_fn = getattr(self, f'parse_{file_model.command}_response')
        success = False
        with open(f) as fh:
            try:
                p = json.load(fh)
                if 'bustime-response' in p:
                    applied = partial(process_fn, file_model.start_time, p)
                    success = self.parse_single(applied, attempt)
                else:
                    if p.get('v') != '2.0':
                        error = Error(parse_attempt=attempt,
                                      error_class='Version error')
                        error.save(force_insert=True)
                        return False
                    reqno = 0
                    for req in p.get('requests', []):
                        r = req['response']
                        request_time = datetime.datetime.fromisoformat(req['request_time'])
                        applied = partial(process_fn, request_time, r)
                        if self.parse_single(applied, attempt):
                            success = True
                        reqno += 1
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                error = Error(parse_attempt=attempt,
                              error_class='Parse error',
                              error_content=str(e))
                error.save(force_insert=True)
                return False
        attempt.parse_success = success
        attempt.save()
        return True

    def parse_single(self, applied, attempt):
        try:
            applied()
        except (ValueError, KeyError) as e:
            error = Error(parse_attempt=attempt,
                          error_class='Parse error',
                          error_content=str(e))
            error.save(force_insert=True)
            return False
        return True

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