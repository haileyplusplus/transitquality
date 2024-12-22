#!/usr/bin/env python3

import dataclasses
import itertools
from functools import partial
import sys
import argparse
import datetime
from pathlib import Path
import json

import peewee

from backend.util import Util
from analysis.datamodels import db_initialize, Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip, VehiclePosition, StopInterpolation, File, FileParse, Error


class Processor:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.db = db_initialize()
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

    def parse_new_vehicles(self, limit=None):
        existing = FileParse.select(FileParse.file_id).where(FileParse.parse_success)
        files = File.select().where(File.command == 'getvehicles')
        previous = VehiclePosition.select().count()
        count = 0
        for f in files:
            if limit and count >= limit:
                break
            if f in existing:
                continue
            self.parse_file(f)
            count += 1
        v = VehiclePosition.select().count()
        return {'files': len(files), 'previous': previous, 'vehicles': v}

    def add_trip(self, v):
        r = Route.get_or_none(Route.route_id == v['rt'])
        if r is None:
            r = Route.create(route_id=v['rt'], timestamp=Util.utcnow(), active=True)
        pid = int(v['pid'])
        p = Pattern.get_or_none(Pattern.pattern_id == pid)
        if p is None:
            p = Pattern.create(pattern_id=pid, route=r)
        schedule_time = Util.CTA_TIMEZONE.localize(
            datetime.datetime.strptime(
                v['stsd'], '%Y-%m-%d') + datetime.timedelta(seconds=v['stst'])),
        t = Trip.create(
            vehicle_id=v['vid'],
            route=r,
            pattern=p,
            destination=v['des'],
            ta_block_id=v['tablockid'],
            ta_trip_id=v['tatripid'],
            origtatripno=v['origtatripno'],
            zone=v['zone'],
            mode=v['mode'],
            passenger_load=v['psgld'],
            schedule_local_day=v['stsd'],
            schedule_time=schedule_time
        )
        return t

    def parse_getvehicles_response(self, file_ts: datetime.datetime, brdict: dict):
        top = brdict['bustime-response']['vehicle']
        trips = set([v['origtatripno'] for v in top])
        existing = Trip.select(Trip.origtatripno).where(Trip.origtatripno << trips)
        #trips = trips - set([x.origtatripno for x in existing])
        existing_tripids = {x.origtatripno: x for x in existing}
        positions = []
        for v in top:
            ota = v['origtatripno']
            if ota not in existing_tripids:
                t = self.add_trip(v)
                existing_tripids[ota] = t
            else:
                t = existing_tripids[ota]
            positions.append(VehiclePosition(
                trip=t,
                lat=v['lat'],
                lon=v['lon'],
                heading=int(v['hdg']),
                timestamp=Util.CTA_TIMEZONE.localize(
                    datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')),
                pattern_distance=int(v['pdist']),
                delay=v['dly']
            ))
        VehiclePosition.bulk_create(positions, batch_size=100)

    @staticmethod
    def get_direction(dirname):
        dir_ = Direction.get_or_none(Direction.direction_id == dirname)
        if dir_ is None:
            dir_ = Direction.create(direction_id=dirname)
        return dir_

    def parse_getpatterns_response(self, file_ts: datetime.datetime, brdict: dict):
        top = brdict['bustime-response']['ptr'][0]
        pid = int(top['pid'])
        try:
            # existing = Pattern.get_or_none(Pattern.pattern_id == pid)
            # if existing is not None:
            #     return
            p = Pattern.create(
                pattern_id=pid,
                direction=Processor.get_direction(top['rtdir']),
                timestamp=file_ts,
                length=int(top['ln']))
        except peewee.IntegrityError:
            return
        waypoints = []
        pattern_stops = []
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
                #w.save(force_insert=True)
                waypoints.append(w)
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
                #pattern_stop.save(force_insert=True)
                pattern_stops.append(pattern_stop)
            else:
                raise ValueError(f'Unexpected pattern type {typ}')
        Waypoint.bulk_create(waypoints, batch_size=100)
        PatternStop.bulk_create(pattern_stops, batch_size=100)

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
            with self.db.atomic():
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