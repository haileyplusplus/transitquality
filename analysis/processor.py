#!/usr/bin/env python3
import csv
import dataclasses
import itertools
from functools import partial
import sys
import argparse
import datetime
from pathlib import Path
import json
from typing import Iterable
import logging

import peewee

from backend.util import Util
from analysis.datamodels import db_initialize, Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip, VehiclePosition, StopInterpolation, File, FileParse, Error


logger = logging.getLogger(__file__)


class BaseParser:
    def __init__(self, db, attempt):
        self.attempt = attempt
        self.db = db
        self.iter = 0
        self.data_time = None
        self.success = False

    def set_data_time(self, data_time: datetime.datetime):
        self.data_time = data_time

    def set_iter(self, iter_no: int):
        self.iter = iter_no

    def finalize(self):
        pass

    def parse_inner(self, brdict):
        pass

    def parse_single(self, brdict):
        try:
            with self.db.atomic():
                self.parse_inner(brdict)
        except (ValueError, KeyError) as e:
            error = Error(parse_attempt=self.attempt,
                          error_class='Parse error',
                          error_content=str(e))
            error.save(force_insert=True)
            return False
        return True


class PatternParser(BaseParser):
    def __init__(self, db, attempt):
        super().__init__(db, attempt)

    @staticmethod
    def get_direction(dirname):
        dir_ = Direction.get_or_none(Direction.direction_id == dirname)
        if dir_ is None:
            dir_ = Direction.create(direction_id=dirname)
        return dir_

    def parse(self, brdict):
        self.parse_single(brdict)

    def finalize(self):
        return self.success

    def parse_inner(self, brdict):
        top = brdict['bustime-response']['ptr'][0]
        pid = int(top['pid'])
        try:
            # existing = Pattern.get_or_none(Pattern.pattern_id == pid)
            # if existing is not None:
            #     return
            p = Pattern.create(
                pattern_id=pid,
                direction=self.get_direction(top['rtdir']),
                timestamp=self.data_time,
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
                self.success = False
                raise ValueError(f'Unexpected pattern type {typ}')
        Waypoint.bulk_create(waypoints, batch_size=100)
        PatternStop.bulk_create(pattern_stops, batch_size=100)
        self.success = True


class WorkingTrip:
    def __init__(self, ota: str, v):
        self.ota = ota
        self.template = v
        if 'stst' in v:
            schedule_time = Util.CTA_TIMEZONE.localize(
                datetime.datetime.strptime(
                    v['stsd'], '%Y-%m-%d') + datetime.timedelta(seconds=v['stst']))
            schedule_local_day = v['stsd']
        else:
            schedule_time = None
            schedule_local_day = v['data_date']
        self.trip_model = Trip(
            vehicle_id=v['vid'],
            destination=v['des'],
            ta_block_id=v['tablockid'],
            ta_trip_id=v['tatripid'],
            origtatripno=v['origtatripno'],
            zone=v['zone'],
            mode=v.get('mode'),
            passenger_load=v.get('psgld'),
            schedule_local_day=schedule_local_day,
            schedule_time=schedule_time
        )
        self.positions = []
        self.needs_insert = True
        self.add_position(v)

    def replace_model(self, new_model: Trip):
        self.needs_insert = False
        self.trip_model = new_model

    def insert_trip_model(self):
        if not self.needs_insert:
            return
        v = self.template
        r = Route.get_or_none(Route.route_id == v['rt'])
        if r is None:
            r = Route.create(route_id=v['rt'], timestamp=Util.utcnow(), active=True)
        pid = int(v['pid'])
        p = Pattern.get_or_none(Pattern.pattern_id == pid)
        if p is None:
            p = Pattern.create(pattern_id=pid, route=r)
        self.trip_model.route = r
        self.trip_model.pattern = p
        self.trip_model.save(force_insert=True)
        self.needs_insert = False

    def finalize(self):
        for position in self.positions:
            position.trip = self.trip_model

    def add_position(self, v):
        self.positions.append(VehiclePosition(
            lat=v['lat'],
            lon=v['lon'],
            heading=int(v['hdg']),
            timestamp=Util.CTA_TIMEZONE.localize(
                datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')),
            pattern_distance=int(v['pdist']),
            delay=v['dly']
        ))

    def __lt__(self, other):
        return self.ota < other.ota

    def __eq__(self, other):
        return self.ota == other.ota

    def __hash__(self):
        return hash(self.ota)


class VehicleParser(BaseParser):
    def __init__(self, db, attempt):
        super().__init__(db, attempt)
        self.by_trip: dict[str, WorkingTrip] = {}

    def parse(self, brdict, override=None):
        if override is not None:
            top = override
        else:
            top = brdict['bustime-response']['vehicle']
        for v in top:
            ota = v['origtatripno']
            self.by_trip.setdefault(ota, WorkingTrip(ota, v)).add_position(v)

    def finalize(self):
        all_ids = list(self.by_trip.keys())
        existing: Iterable[Trip] = Trip.select().where(Trip.origtatripno << all_ids)
        for existing_trip_model in existing:
            trip: WorkingTrip | None = self.by_trip.get(existing_trip_model.origtatripno)
            if trip is not None:
                trip.replace_model(existing_trip_model)
        positions = []
        for v in self.by_trip.values():
            v.insert_trip_model()
            v.finalize()
            positions += v.positions
        VehiclePosition.bulk_create(positions, batch_size=100)
        return True


class Processor:
    PARSERS = {'getvehicles': VehicleParser,
               'getpatterns': PatternParser}

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
        #existing = FileParse.select(FileParse.file_id).where(FileParse.parse_success)
        #files = File.select().join(FileParse).where(File.command == 'getvehicles')
        # never_attempted = (File.select().join(FileParse).where(File.file_id == FileParse.file_id).
        #                    where(File.command == 'getvehicles').where(FileParse.parse_time.is_null()))
        # print(never_attempted)
        # succeeded = (File.select(File.file_id).join(FileParse).where(File.file_id == FileParse.file_id).
        #              where(File.command == 'getvehicles').where(FileParse.parse_stage == 'first').
        #              where(FileParse.parse_success))
        previous = VehiclePosition.select().count()
        # sql_retry = "select file.file_id from file left join fileparse on file.file_id = fileparse.file_id where file.command = 'getvehicles' and fileparse.parse_stage = 'first' and not fileparse.parse_success and file.file_id not in (select distinct file_id from fileparse where parse_stage = 'first' and parse_success);"
        success = FileParse.select(FileParse.file_id).where(FileParse.parse_stage == 'first').where(FileParse.parse_success)
        print(f'success {success}')
        needed = File.select().where(File.command == 'getvehicles').where(File.file_id.not_in(success))
        print(f'needed {needed}')
        count = 0
        #existing_ids = set([x.file_id for x in existing])
        #logger.info(f'Existing ids for files: {existing_ids}')
        for f in needed:
            if limit and count >= limit:
                break
            # if f.file_id in existing_ids:
            #     continue
            self.parse_file(f)
            count += 1
        v = VehiclePosition.select().count()
        return {'needed': len(needed), 'previous': previous, 'vehicles': v, 'prev_success': len(success)}

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
        #process_fn = getattr(self, f'parse_{file_model.command}_response')
        parser_class = self.PARSERS[file_model.command]
        parser = parser_class(self.db, attempt)
        success = False
        with open(f) as fh:
            try:
                # csv reader
                if file_model.filename.endswith('.csv'):
                    print(f'Reading csv file {file_model.filename}')
                    reader = csv.DictReader(fh)
                    parser.set_data_time(file_model.start_time)
                    for row in reader:
                        tmstmp = row['tmstmp']
                        row['tmstmp'] = f'{tmstmp}:00'
                        row['pdist'] = float(row['pdist'])
                        try:
                            parser.parse(None, override=[row])
                        except (KeyError, ValueError) as e:
                            error = Error(parse_attempt=attempt,
                                          error_class='Parse error',
                                          error_key=str(row)[:250],
                                          error_content=str(e))
                            error.save(force_insert=True)
                    attempt.parse_success = parser.finalize()
                    attempt.save()
                    return
                p = json.load(fh)
                if 'bustime-response' in p:
                    parser.set_data_time(file_model.start_time)
                    #applied = partial(process_fn, file_model.start_time, p)
                    #success = self.parse_single(applied, attempt)
                    parser.parse(p)
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
                        parser.set_data_time(request_time)
                        parser.set_iter(reqno)
                        parser.parse(r)
                        #applied = partial(process_fn, request_time, r)
                        #if self.parse_single(applied, attempt):
                        #    success = True
                        reqno += 1
            except (json.JSONDecodeError, ValueError, KeyError) as e:
                error = Error(parse_attempt=attempt,
                              error_class='Parse error',
                              error_content=str(e))
                error.save(force_insert=True)
                return False
        attempt.parse_success = parser.finalize()
        attempt.save()

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