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
from types import SimpleNamespace
from typing import Iterable
import logging
import pandas as pd

import peewee
from playhouse.shortcuts import model_to_dict

from backend.util import Util
from analysis.datamodels import db_initialize, Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip, VehiclePosition, StopInterpolation, File, FileParse, Error, TimetableView


logger = logging.getLogger(__file__)


PROJROOT = Path(__file__).parent.parent


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
        m = model_to_dict(self.trip_model, recurse=False)
        del m['trip_id']
        #print(m)
        new_trip_model = Trip.insert(**m).on_conflict_ignore().execute()
        if new_trip_model is None:
            new_trip_model = Trip.get(Trip.schedule_local_day == self.trip_model.schedule_local_day,
                                      Trip.origtatripno == self.trip_model.origtatripno)
        self.trip_model = new_trip_model
        # try:
        #     self.trip_model.save(force_insert=True)
        # except peewee.IntegrityError:
        #     self.trip_model = Trip.get(Trip.schedule_local_day == self.trip_model.schedule_local_day,
        #                                Trip.origtatripno == self.trip_model.origtatripno)
        self.needs_insert = False

    def finalize(self):
        for position in self.positions:
            position.trip = self.trip_model

    def add_position(self, v):
        self.positions.append(SimpleNamespace(
            lat=v['lat'],
            lon=v['lon'],
            heading=int(v['hdg']),
            timestamp=Util.CTA_TIMEZONE.localize(
                datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')),
            pattern_distance=int(v['pdist']),
            delay=v['dly'],
            trip=None
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
        day = self.data_time.strftime('%Y-%m-%d')
        existing: Iterable[Trip] = Trip.select().where(Trip.schedule_local_day == day).where(Trip.origtatripno << all_ids)
        for existing_trip_model in existing:
            trip: WorkingTrip | None = self.by_trip.get(existing_trip_model.origtatripno)
            if trip is not None:
                trip.replace_model(existing_trip_model)
        positions = []
        for v in self.by_trip.values():
            v.insert_trip_model()
            v.finalize()
            positions += v.positions
        VehiclePosition.insert_many([vars(x) for x in positions]).on_conflict_ignore().execute()
        #VehiclePosition.bulk_create(positions, batch_size=100)
        return True


class Processor:
    PARSERS = {'getvehicles': VehicleParser,
               'getpatterns': PatternParser}

    def __init__(self, data_dir: Path):
        self.data_dir = data_dir
        self.db = None
        self.processed = 0
        self.inserted = 0

    def open(self):
        if self.db is None:
            self.db = db_initialize()

    def close(self):
        if self.db is None:
            return False
        self.db.close()
        self.db = None
        return True

    def update(self):
        self.find_files('getpatterns', self.data_dir / 'getpatterns')
        self.find_files('getvehicles', self.data_dir / 'getvehicles')
        self.find_files('getvehicles', self.data_dir / 'chnghostbuses')
        return self.processed, self.inserted

    def get_trip_json(self, tripid: int):
        #date = datetime.datetime.strptime(datestr, '%Y-%m-%d')
        trips = TimetableView.select().where(TimetableView.trip_id == tripid).order_by(TimetableView.interpolated_timestamp)
                 #.where(TimetableView.schedule_time.date() == date))
        def upd(x):
            d = model_to_dict(x)
            d.update({'day': x.interpolated_timestamp.strftime('%Y-%m-%d')})
            return d
        return [upd(x) for x in trips]

    def get_stop_json(self, stop_id: str, route_id: str, day: str):
        date = datetime.datetime.strptime(day, '%Y-%m-%d').replace(tzinfo=Util.CTA_TIMEZONE)
        # Use 3am as cutover time
        date += datetime.timedelta(hours=3)
        tomorrow = date + datetime.timedelta(days=1)
        trips = (TimetableView.select().where(TimetableView.stop_id == stop_id).
                 where(TimetableView.route_id == route_id).
                 where(TimetableView.interpolated_timestamp >= date).
                 where(TimetableView.interpolated_timestamp < tomorrow).
                 order_by(TimetableView.interpolated_timestamp))
        return [model_to_dict(x) for x in trips]
        #.where(TimetableView.schedule_time.date() == date))

    def get_route_json(self):
        # TODO: properly integrate with database
        routes_df = pd.read_csv(PROJROOT / 'data' / 'routes.txt')
        rv = []
        for _, row in routes_df.iterrows():
            if row.route_type == 3:
                rv.append({'route_id': row.route_id,
                           'name': row.route_long_name
                           })
        return rv

    def get_day_json(self):
        days = Trip.select(Trip.schedule_local_day).distinct().order_by(Trip.schedule_local_day)
        return [x.schedule_local_day for x in days]

    def get_daily_trips_json(self, route_id: str, day: str):
        trips = Trip.select().where(Trip.route == route_id).where(Trip.schedule_local_day == day).order_by(Trip.schedule_time)
        directions = set([])
        for t in trips:
            p = t.pattern
            if p:
                d = p.direction
                if d:
                    directions.add(d.direction_id)
        return {'trips': [model_to_dict(x) for x in trips], 'directions': list(directions)}

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
                        row['pid'] = float(row['pid'])

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


class RealtimeConverter:
    EPSILON = 0.001

    def __init__(self):
        self.output_stop_times = pd.DataFrame()
        self.output_trips = pd.DataFrame()
        self.errors = []
        self.trips_attempted = 0
        self.trips_processed = 0
        self.trips_seen = set([])

    def process_trips_for_route(self, route_id: str):
        route_trips = Trip.select(Trip.trip_id).where(Trip.route == route_id)
        attempted = 0
        converted = 0
        for t in route_trips:
            result = self.process_trip(t.trip_id)
            attempted += 1
            if result:
                converted += 1
        return {'attempted': attempted, 'converted': converted}

    def record_error(self, trip_id=None, msg=None):
        # TODO: fix this
        error = Error(parse_attempt=1,
                      error_class='Process interpolation',
                      error_message=msg,
                      error_key=trip_id)
        error.save(force_insert=True)

    def process_trip(self, trip_id: int):
        #summary, times
        trip: Trip | None = Trip.get_or_none(Trip.trip_id == trip_id)
        if trip is None:
            return False
        if trip.has_interpolation:
            return False
        day = trip.schedule_local_day
        origtatripno = trip.origtatripno
        positions = VehiclePosition.select(
            VehiclePosition.timestamp,
            VehiclePosition.pattern_distance
        ).where(VehiclePosition.trip == trip).order_by(VehiclePosition.timestamp)
        if not positions.exists():
            self.record_error(trip_id=trip_id, msg='Missing raw times')
            return False
        #print(f'Ts: {positions[0].timestamp}')
        stops = []
        stop_index = {}
        for ps in trip.pattern.stops:
            stop_index[ps.stop.stop_id] = ps
            stops.append({
                'stpid': ps.stop.stop_id,
                'seq': ps.sequence_no,
                'pdist': ps.pattern_distance,
            })
        if not stops:
            self.record_error(trip_id=trip_id, msg='Missing stops')
            return False
        #v['tmstmp'] = v.apply(lambda x: int(x.tmstmp.timestamp()), axis=1)
        #pattern = summary.iloc[0].pid
        stops_df = pd.DataFrame(stops)
        #print(f'Stops: {stops_df}')
        vehicles_df = pd.DataFrame([{'pdist': x.pattern_distance, 'tmstmp': int(x.timestamp.timestamp())} for x in positions]).sort_values('tmstmp')
        # The tracker gives us updates while waiting to depart and after arrival, so just ignore these
        minval = vehicles_df.pdist.min()
        maxval = vehicles_df.pdist.max()
        beginnings = vehicles_df[vehicles_df.pdist == minval]
        endings = vehicles_df[vehicles_df.pdist == maxval]
        begin_drop = len(beginnings) - 1
        end_drop = len(endings) - 1
        if end_drop > 0:
            vehicles_df.drop(vehicles_df.tail(end_drop).index, inplace=True)
        if begin_drop > 0:
            vehicles_df.drop(vehicles_df.head(begin_drop).index, inplace=True)
        if vehicles_df.empty:
            self.record_error(trip_id=trip_id, msg='Interpolated vehicle error')
            return False
        #print(f'Vehicles: {vehicles_df}')
        pattern_template = pd.DataFrame(index=stops_df.pdist, columns={'tmstmp': float('NaN')})
        combined = pd.concat([pattern_template, vehicles_df.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        combined = combined.groupby(combined.index).last()
        #print(combined)

        #px = self.manager.pm.get_stops(pattern)
        df = stops_df.set_index('pdist').assign(tmstmp=combined.apply(lambda x: datetime.datetime.fromtimestamp(int(x))))
        #print('return df', df)
        stop_interpolation = []
        for _, row in df.iterrows():
            stop_interpolation.append(SimpleNamespace(
                trip=trip,
                pattern_stop=stop_index[row.stpid],
                interpolated_timestamp=row.tmstmp
            ))
        StopInterpolation.insert_many([vars(x) for x in stop_interpolation]).on_conflict_ignore().execute()
        trip.has_interpolation = True
        trip.save()
        return True
        # if df.empty:
        #     self.errors.append({'day': day, 'origtatripno': origtatripno, 'fn': 'process_trip',
        #                         'msg': 'Missing interpolation'})
        #     return None
        # df['day'] = day
        # df['origtatripno'] = origtatripno
        # df = df.reset_index()
        # df = df[['day', 'origtatripno', 'pdist', 'seq', 'stpid', 'stpnm', 'pid', 'tmstmp']]
        # return df


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