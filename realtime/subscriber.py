#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

import requests
import shapely
import sqlalchemy
from geoalchemy2.shape import to_shape
from sqlalchemy import select, func, text
from sqlalchemy.orm import Session
import redis
import redis.asyncio as redis_async
from prometheus_client import start_http_server, Counter

from backend.util import Util
from realtime.rtmodel import *
from realtime.load_patterns import load_routes, S3Getter
from realtime.redisclean import Cleaner
from interfaces import ureg, Q_

from schedules.schedule_analyzer import ScheduleAnalyzer, ShapeManager


class DatabaseUpdater:
    def __init__(self, subscriber):
        self.subscriber = subscriber
        self.r = redis.Redis()

    def subscriber_callback(self, data):
        pass


class TrainUpdater(DatabaseUpdater):
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.schedule_analyzer = kwargs['schedule_analyzer']
        self.schedule_analyzer.engine = self.subscriber.engine
        self.schedule_analyzer.setup_shapes()
        self.prediction_bundle_counter = Counter('transit_train_prediction_bundle',
                                                 'Bundles of train predictions')
        self.prediction_counter = Counter('transit_train_prediction_individual', 'Individual train predictions')
        self.new_prediction_counter = Counter('transit_train_new_prediction_individual',
                                              'Individual train predictions newly added to db')
        self.position_bundle_counter = Counter('transit_train_position_bundle',
                                               'Bundles of train positions')
        self.position_counter_total = Counter('transit_train_position_individual_total',
                                              'Total individual train position updates')
        self.position_counter_success = Counter('transit_train_position_individual_success',
                                                'Successful individual train position updates')
        self.redis_position_counter = Counter('transit_train_redis_position_success',
                                              'Successful train position redis update')
        self.redis_position_error = Counter('transit_train_redis_position_error',
                                            'Errors updating train Redis position')
        self.duplicate_key_counter = Counter('transit_train_position_duplicate_key_error', 'Duplicate key error')
        self.route_error_counter = Counter('transit_train_position_unknown_route_error', 'Unknown route')
        self.missing_position_counter = Counter('transit_train_position_missing_error',
                                                'Train position missing in database error')
        self.invalid_position_counter = Counter('transit_train_position_invalid_error',
                                                'Train position invalid in database error')
        #self.refresh(hours=8)


    """
    On each train update
     - record the train position
     - update the current position
     - detect end of trip and, if so, perform end of trip logic
     
    End of trip signals
     - within $thresh of end (crow distance) AND
       - $thresh time elapsed
       - or there is a new update with a different destination / direction
         - Loop headsign -> something else is not end of trip. any other mismatch 
           is a new trip. But the end of route threshold search should mostly make this moot.
       
    On completed trip
     - check for completeness
     - detect pattern
     - assign new pattern id
    """
    def find_finalized_trips(self):
        finish_thresh = datetime.timedelta(minutes=5)
        with Session(self.subscriber.engine) as session:
            local_now = Util.ctanow()
            # only consider one run at a time
            runs = {}
            stmt = (select(TrainPosition)
                    .join(Stop, TrainPosition.dest_station == Stop.id)
                    .where(TrainPosition.completed.is_(False))
                    .where(func.ST_Distance(
                            Stop.geom.ST_Transform(26916), TrainPosition.geom.ST_Transform(26916)
                        ) < 1000
                        )
                    .order_by(TrainPosition.run, TrainPosition.timestamp)
                    )
            try:
                for pos in session.scalars(stmt):
                    key = (pos.run, pos.dest_name, pos.direction)
                    previous = runs.get(key)
                    if not previous or pos.timestamp > previous.timestamp:
                        runs[key] = pos
            except sqlalchemy.exc.InternalError as e:
                print(f'Internal error: {e}')
            count = 0
            succeeded = 0
            for k, v in runs.items():
                count += 1
                #print(f'Found {v.run} ts {v.timestamp.isoformat()}')
                ts = v.timestamp.replace(tzinfo=Util.CTA_TIMEZONE)
                update_age = local_now - ts
                if update_age < finish_thresh:
                    #print(f'Skipping fresh update for {k}')
                    continue
                success = self.finalize_trip(session, v)
                if success:
                    succeeded += 1
            session.commit()

    def finalize_trip(self, session, end_position):
        run = end_position.run
        # may want to use an ORM join but let's try this for now
        stmt = (select(TrainPosition)
                .where(TrainPosition.run == run)
                .where(TrainPosition.completed.is_(False))
                .where(TrainPosition.timestamp <= end_position.timestamp)
                .order_by(TrainPosition.timestamp))
        points = session.scalars(stmt).all()
        if not points:
            return None
        # process outliers
        run_length = 0
        prev_key = None
        elide = []
        first_run = True
        # also detect big time gaps here
        for i, p in enumerate(points):
            key = (p.rt, p.dest_station, p.dest_name, p.direction)
            if prev_key and prev_key != key:
                if run_length == 1 and not first_run:
                    elide.append(i - 1)
                run_length = 0
                first_run = False
            run_length += 1
            prev_key = key
        p = points[0]
        #if len(elide) > 1:
        #    print(f'Too many outliers in {p.rt} {p.run} {p.timestamp.isoformat()} with {len(points)} points: {elide}')
        #    return None
        removed = 0
        for e in elide:
            remove_index = e - removed
            removed += 1
            points[remove_index].completed = True
            points.pop(remove_index)
        if removed > 0:
            session.commit()
        prev_dest_name = points[-1].dest_name
        start_position = None
        i = len(points) - 1
        prev_point = None
        for p in reversed(points):
            if p.dest_name != prev_dest_name and p.dest_name != 'Loop':
                start_position = prev_point
                i += 1
                break
            if prev_point and abs(p.timestamp - prev_point.timestamp) > datetime.timedelta(minutes=10):
                start_position = prev_point
                i += 1
                break
            prev_dest_name = p.dest_name
            i -= 1
            prev_point = p
        if start_position is None:
            i = 0
            start_position = points[0]
        if end_position.route.id in {'y', 'p'}:
            min_points = 4
        else:
            min_points = 7
        if len(points[i:]) <= min_points:
            #print(f'Not enough points for trip {run} ts {end_position.timestamp.isoformat()}')
            return None
        stop_name = session.get(Stop, points[i].next_stop).stop_name
        end_stop_name = session.get(Stop, points[-1].next_stop).stop_name
        #print(f'Looking for pattern (run {run}) from stop {stop_name} to {end_stop_name} with {len(points)} points index {i}')

        stmt = (session.query(TrainPatternDetail, Stop, func.ST_Distance(
                    Stop.geom.ST_Transform(26916), func.ST_Transform(start_position.geom, 26916)
                    ).label('stop_dist'))
                .join(Stop, TrainPatternDetail.first_stop_id == Stop.id)
                .where(TrainPatternDetail.route_id == end_position.rt)
                .where(TrainPatternDetail.last_stop_id == end_position.dest_station)
                .where(TrainPatternDetail.pattern_id.not_in({308500036, 308500102}))
                #.where(text('stop_dist < 1000')
                    #)
                )
        pattern_result = stmt.all()
        pattern_id = None
        duplicate = False
        dists = [999999999999]
        first_stop = None
        last_stop = None
        match = None
        for pr, stop, d in pattern_result:
            #print(f' Pattern result: {pr.pattern_id} stop {stop.stop_name} dist {d}')
            #print(f'  md: {type(pr)} {dir(pr)}')
            dists.append(d)
            first_stop = pr.first_stop_name
            last_stop = pr.last_stop_name
            if d < 4000:
                if match is not None:
                    if d >= match[2]:
                        continue
                match = (pr, stop, d)
        if match:
            pattern_id = match[0].pattern_id
        else:
            pattern_id = None
        next_trip_id = session.query(func.max(TrainPosition.synthetic_trip_id)).scalar()
        if next_trip_id is None:
            next_trip_id = 0
        else:
            next_trip_id += 1

        if pattern_id is None:
            print(f'No p match run {end_position.run} rt {end_position.route.id} starting at {start_position.timestamp.isoformat()}: '
                  f'patterns {len(pattern_result)} points {len(points[i:])} min dist {min(dists)} '
                  f'pat {first_stop}-{last_stop} pt {stop_name}-{end_stop_name}')
            for point in points[i:]:
                point.synthetic_trip_id = next_trip_id
                point.completed = True
            session.commit()
            return False
        try:
            # get pattern stops
            pattern_stmt = (select(PatternStop)
                            .where(PatternStop.pattern_id == int(pattern_id)))
            stop_distances = {}
            for ps in session.scalars(pattern_stmt):
                stop_distances[ps.stop_id] = ps.distance

            debug = run == 409
            # if current.current_pattern is None:
            #     print(f'Error finding pattern for run {run}')
            #     continue
            #pattern_id = pattern_result[0].pattern_id
            shape_manager = self.schedule_analyzer.managed_shapes[pattern_id]
            previous_distance = 0
            previous_larger = False
            # clean up and discard outliers
            for point in points[i:]:
                train_point = to_shape(point.geom)
                #train_distance = shape_manager.get_distance_along_shape(previous_distance, train_point, debug=debug)
                point.pattern = pattern_id
                point.synthetic_trip_id = next_trip_id
                point.completed = True

                if point.next_stop_distance < 100 and not point.approaching:
                    # insert into error table
                    continue

                stop_pattern_distance = stop_distances.get(point.next_stop)
                if stop_pattern_distance is None:
                    print(f'Warning: couldn\'t find stop pattern distance for stop {point.next_stop} pattern {pattern_id} computing trip {next_trip_id} at {point.timestamp.isoformat()}. Using fallback (prev larger {previous_larger})')
                    #continue

                previous_larger, train_distance = shape_manager.get_distance_along_shape_anchor(stop_pattern_distance, train_point, previous_larger)
                point.pattern_distance = train_distance

                redis_key = f'trainposition:{pattern_id}:{run}-{next_trip_id}'
                if not self.r.exists(redis_key):
                    self.r.ts().create(redis_key, retention_msecs=60 * 60 * 24 * 1000)
                self.r.ts().add(redis_key, int(point.timestamp.timestamp()), train_distance)
                self.redis_position_counter.inc()
            return True
        except redis.exceptions.ResponseError as e:
            print(f'Redis summarizer error: {e}')
            self.redis_position_error.inc()
        except KeyError as e:
            print(f'Bad pattern: {e}')
        except shapely.errors.GEOSException as e:
            print(f'GEOS error: {e}')
        return False

    def subscriber_callback(self, data):
        #print(f'Finding finalized trips data len {len(str(data))}')
        self.position_bundle_counter.inc()
        self.find_finalized_trips()
        with Session(self.subscriber.engine) as session:
            routes = data['route']
            for route in routes:
                rt = route['@name']
                route_db = session.get(Route, rt)
                if route_db is None:
                    print(f'Unknown route {route_db}')
                    self.route_error_counter.inc()
                    continue
                if 'train' not in route:
                    self.route_error_counter.inc()
                    continue
                if isinstance(route['train'], dict):
                    trains = [route['train']]
                else:
                    trains = route['train']
                for v in trains:
                    self.position_counter_total.inc()
                    run = int(v['rn'])
                    timestamp = datetime.datetime.strptime(v['prdt'], '%Y-%m-%dT%H:%M:%S')
                    existing = session.get(TrainPosition, {'run': run, 'timestamp': timestamp})
                    if existing is not None:
                        self.missing_position_counter.inc()
                        continue
                    lat = v['lat']
                    lon = v['lon']
                    if abs(float(lat)) < 1 or abs(float(lon)) < 1:
                        #print(f'Invalid point {lat} {lon} in run {run}')
                        self.invalid_position_counter.inc()
                        continue
                    geom = f'POINT({lon} {lat})'
                    key = (run, timestamp)
                    if session.get(TrainPosition, key):
                        continue
                    #latest_update = session.query(TrainPosition).order_by(TrainPosition.timestamp.desc()).one_or_none()
                    upd = TrainPosition(
                        run=run,
                        timestamp=timestamp,
                        dest_station=int(v['destSt']),
                        dest_name=v['destNm'],
                        direction=int(v['trDr']),
                        next_station=int(v['nextStaId']),
                        next_stop=int(v['nextStpId']),
                        arrival=datetime.datetime.strptime(v['arrT'], '%Y-%m-%dT%H:%M:%S'),
                        approaching=int(v['isApp']),
                        delayed=int(v['isDly']),
                        geom=geom,
                        heading=int(v['heading']),
                        route=route_db,
                        completed=False,
                    )
                    session.add(upd)
                    self.position_counter_success.inc()
                    next_stop = session.get(Stop, upd.next_stop)
                    if next_stop:
                        llpoint = shapely.Point(lon, lat)
                        upd.next_stop_distance = ShapeManager.geom_distance(llpoint, to_shape(next_stop.geom))
                    current = session.get(CurrentTrainState, run)
                    if not current:
                        current = CurrentTrainState(
                            id=run,
                            last_update=timestamp,
                            update_count=0
                        )
                        session.add(current)
                    elif timestamp <= current.last_update:
                        continue
                    #prev_dest_name = current.dest_station_name
                    current.last_update = timestamp
                    dest_station = int(v['destSt'])
                    current.dest_station_name = v['destNm']
                    current.direction = int(v['trDr'])
                    current.next_station = int(v['nextStaId'])
                    current.next_stop = int(v['nextStpId'])
                    current.next_arrival = datetime.datetime.strptime(v['arrT'], '%Y-%m-%dT%H:%M:%S')
                    current.approaching = int(v['isApp'])
                    current.delayed = int(v['isDly'])
                    current.geom = geom
                    current.heading = int(v['heading'])
                    current.route = route_db
                    #train_point = shapely.Point(lon, lat)
                    # start_of_trip = None
                    if current.update_count is None:
                        current.update_count = 0
                    if dest_station == 0 and current.dest_station_name == 'UIC-Halsted':
                        dest_station = 30069
                    current.dest_station = dest_station
                    upd.dest_station = dest_station
            session.commit()

    def prediction_callback(self, data):
        self.prediction_bundle_counter.inc()
        with Session(self.subscriber.engine) as session:
            if 'eta' not in data:
                print(f'Error reading prediction from {data}')
                return
            for estimate in data['eta']:
                self.prediction_counter.inc()
                station_id = int(estimate['staId'])
                destination = estimate['destNm']
                destination_stop_id = int(estimate['destSt'])
                key = (station_id, estimate['rt'], destination)
                prediction = session.get(TrainPrediction, key)
                if not prediction:
                    prediction = TrainPrediction(
                        station_id=station_id,
                        route=estimate['rt'],
                        destination=destination,
                    )
                    session.add(prediction)
                prediction.stop_id = int(estimate['stpId'])
                prediction.destination_stop_id = destination_stop_id
                prediction.stop_description = estimate['stpDe']
                prediction.run = int(estimate['rn']),
                prediction.timestamp = datetime.datetime.strptime(
                    estimate['prdt'], '%Y-%m-%dT%H:%M:%S'),
                prediction.predicted_time = datetime.datetime.strptime(
                    estimate['arrT'], '%Y-%m-%dT%H:%M:%S')
            session.commit()


class BusUpdater(DatabaseUpdater):
    def __init__(self, *args):
        super().__init__(*args)
        self.cleanup_iteration = 0
        self.cleanup_counter = Counter('transit_bus_cleanup_base', 'Standard cleanup run')
        self.cleanup_position_counter = Counter('transit_bus_cleanup_position', 'Clean up positions')
        self.cleanup_redis_counter = Counter('transit_bus_cleanup_redis', 'Clean up old redis entries')
        self.prediction_bundle_counter = Counter('transit_bus_prediction_bundle',
                                                 'Bundles of bus predictions')
        self.prediction_counter = Counter('transit_bus_prediction_individual', 'Individual bus predictions')
        self.new_prediction_counter = Counter('transit_bus_new_prediction_individual',
                                              'Individual bus predictions newly added to db')
        self.position_bundle_counter = Counter('transit_bus_position_bundle',
                                               'Bundles of bus positions')
        self.position_counter_total = Counter('transit_bus_position_individual_total',
                                              'Total individual bus position updates')
        self.position_counter_success = Counter('transit_bus_position_individual_success',
                                                'Successful individual bus position updates')
        self.redis_position_counter = Counter('transit_bus_redis_position_success',
                                              'Successful bus position redis update')
        self.redis_position_error = Counter('transit_bus_redis_position_error',
                                            'Errors updating bus Redis position')
        self.duplicate_key_counter = Counter('transit_bus_position_duplicate_key_error', 'Duplicate key error')
        self.route_error_counter = Counter('transit_bus_position_unknown_route_error', 'Unknown route')
        self.missing_position_counter = Counter('transit_bus_position_missing_error',
                                                'Bus position missing in database error')

    def periodic_cleanup(self):
        """
        select count(*) from active_trip where (now() at time zone 'America/Chicago' - active_trip.timestamp) > make_interval(hours => 24);

        select count(*) from current_vehicle_state where ((select max(last_update) from current_vehicle_state) - current_vehicle_state.last_update) > make_interval(mins => 5);
        update bus_position set completed = true where origtatripno not in (select origtatripno from current_vehicle_state);
        :return:
        """
        start = datetime.datetime.now()
        self.cleanup_counter.inc()
        with Session(self.subscriber.engine) as session:
            session.execute(text('DELETE from current_vehicle_state where ((select max(last_update) from '
                                 'current_vehicle_state) - current_vehicle_state.last_update)'
                                 ' > make_interval(mins => 5)'))
            session.execute(text('DELETE from current_train_state where ((select max(last_update) from '
                                 'current_train_state) - current_train_state.last_update)'
                                 ' > make_interval(mins => 10)'))
            #session.execute(text('update bus_position set completed = true where origtatripno not in '
            #                     '(select origtatripno from current_vehicle_state)'))
            session.execute(text('UPDATE pattern t2 SET rt = t1.rt '
                                 'FROM bus_position t1 WHERE t2.id = t1.pid'))
            if self.cleanup_iteration % 10 == 0:
                self.cleanup_position_counter.inc()
                session.execute(text('delete from bus_position where timestamp < '
                                     '(select max(timestamp) - interval \'24 hours\' from bus_position)'))
                session.execute(text('delete from train_position where timestamp < '
                                     '(select max(timestamp) - interval \'24 hours\' from train_position)'))
            self.cleanup_iteration += 1
            session.commit()
        if self.cleanup_iteration % 500 == 0:
            self.cleanup_redis_counter.inc()
            host = None
            print(f'  Cleaning up redis {host}')
            c = Cleaner(host=host)
            c.clean()
        finish = datetime.datetime.now()
        td = finish - start
        print(f'Cleanup run {self.cleanup_iteration} took {td}')

    def finish_past_trips(self):
        with Session(self.subscriber.engine) as session:
            vids = select(BusPosition.vid, func.min(BusPosition.timestamp).label("ts")).group_by(BusPosition.vid).order_by("ts", "vid")
            count = 0
            for vid in session.scalars(vids):
                self.finish_past_trip(vid)
                count += 1
                if count > 20:
                    break
            print(f'Finished {count} past trips')

    def finish_past_trip(self, vid):
        include_current = False
        with Session(self.subscriber.engine) as session:
            existing_vehicle_state = session.get(CurrentVehicleState, vid)
            if not existing_vehicle_state:
                print(f'No state for {vid}')
                return
            current_key = (existing_vehicle_state.pid, existing_vehicle_state.origtatripno)
            if (datetime.datetime.now() - existing_vehicle_state.last_update) > datetime.timedelta(minutes=15):
                include_current = True
            statement = select(BusPosition).where(BusPosition.vid == vid).order_by(BusPosition.timestamp)
            prev_key = None
            current_trip = None
            for trip_item in session.scalars(statement):
                key = (trip_item.pid, trip_item.origtatripno)
                if not include_current and current_key == key:
                    break
                if key != prev_key:
                    ts = trip_item.timestamp.strftime('%Y%m%d%H%M%S')
                    current_trip_id = f'{ts}.{vid}.{trip_item.pid}'
                    current_trip = session.get(Trip, current_trip_id)
                    if current_trip is None:
                        current_trip = Trip(
                            id=current_trip_id,
                            rt=trip_item.route.id,
                            pid=trip_item.pid
                        )
                        session.add(current_trip)
                nt = TripUpdate(
                    timestamp=trip_item.timestamp,
                    distance=trip_item.pdist,
                    trip=current_trip
                )
                session.add(nt)
                session.delete(trip_item)
                prev_key = key
            session.commit()

    def bus_prediction_callback(self, data):
        with Session(self.subscriber.engine) as session:
            self.prediction_bundle_counter.inc()
            for v in data:
                stop_id = int(v['stpid'])
                destination = v['des']
                route = v['rt']
                key = (stop_id, destination, route)
                prediction = session.get(BusPrediction, key)
                self.prediction_counter.inc()
                if not prediction:
                    prediction = BusPrediction(
                        stop_id=stop_id,
                        destination=destination,
                        route=route
                    )
                    session.add(prediction)
                # eventually check timestamps before overwrite
                prediction.origtatripno = v['origtatripno']
                prediction.prediction_type = v['typ']
                prediction.origin = v['stpnm']
                prediction.vehicle_id = v['vid']
                prediction.direction = v['rtdir']
                prediction.block_id = v['tablockid']
                prediction.delay = v['dly']
                if v['prdctdn'] == 'DUE':
                    prediction.prediction = 1
                elif v['prdctdn'] == 'DLY':
                    prediction.prediction = -1
                else:
                    prediction.prediction = int(v['prdctdn'])
                prediction.timestamp = datetime.datetime.strptime(
                    v['tmstmp'], '%Y%m%d %H:%M')
            session.commit()

    def subscriber_callback(self, data):
        #print(f'Bus {len(data)}')
        #self.finish_past_trips()
        with Session(self.subscriber.engine) as session:
            self.position_bundle_counter.inc()
            for v in data:
                self.position_counter_total.inc()
                vid = int(v['vid'])
                timestamp = datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')
                existing = session.get(BusPosition, {'vid': vid, 'timestamp': timestamp})
                route = session.get(Route, v['rt'])
                if route is None:
                    print(f'Unknown route {route}')
                    self.route_error_counter.inc()
                    continue
                if existing is not None:
                    self.missing_position_counter.inc()
                    continue
                lat = v['lat']
                lon = v['lon']
                geom = f'POINT({lon} {lat})'
                key = (vid, timestamp)
                if session.get(BusPosition, key):
                    self.duplicate_key_counter.inc()
                    continue
                self.position_counter_success.inc()
                upd = BusPosition(
                    vid=vid,
                    timestamp=timestamp,
                    #lat=float(v['lat']),
                    #lon=float(v['lon']),
                    geom=geom,
                    pid=v['pid'],
                    route=route,
                    pdist=v['pdist'],
                    tatripid=v['tatripid'],
                    origtatripno=v['origtatripno'],
                    tablockid=v['tablockid'],
                    destination=v['des'],
                    completed=False
                )
                bus_position = Q_(int(v['pdist']), 'ft')
                redis_key = f'busposition:{v["pid"]}:{v["origtatripno"]}'
                try:
                    if not self.r.exists(redis_key):
                        self.r.ts().create(redis_key, retention_msecs=60 * 60 * 24 * 1000)
                    self.r.ts().add(redis_key, int(timestamp.timestamp()), bus_position.to(ureg.meters).m)
                    self.redis_position_counter.inc()
                except redis.exceptions.ResponseError as e:
                    print(f'Redis summarizer error: {e}')
                    self.redis_position_error.inc()
                session.add(upd)
                pattern = session.get(Pattern, v['pid'])
                if pattern is None:
                    pattern = Pattern(
                        id=v['pid'],
                        updated=datetime.datetime.fromtimestamp(0),
                        route=route,
                        length=0,
                    )
                    session.add(pattern)
                existing_state = session.get(CurrentVehicleState, vid)
                if not existing_state:
                    current_state = CurrentVehicleState(
                        id=vid,
                        last_update=timestamp,
                        #lat=float(v['lat']),
                        #lon=float(v['lon']),
                        geom=geom,
                        route=route,
                        distance=v['pdist'],
                        origtatripno=v['origtatripno'],
                        pattern=pattern,
                        destination=v['des'],
                    )
                    session.add(current_state)
                elif timestamp > existing_state.last_update:
                    existing_state.last_update = timestamp
                    existing_state.lat = float(v['lat'])
                    existing_state.lon = float(v['lon'])
                    existing_state.route = route
                    existing_state.distance = v['pdist']
                    existing_state.origtatripno = v['origtatripno']
                    existing_state.pattern = pattern
                    existing_state.destination = v['des']
            session.commit()


class Subscriber:
    def __init__(self, host, schedule_analyzer):
        self.host = host
        self.engine = db_init(Config('local'))
        schedule_analyzer.engine = self.engine
        self.train_updater = TrainUpdater(self, schedule_analyzer=schedule_analyzer)
        self.bus_updater = BusUpdater(self)
        self.redis_client = redis_async.Redis(host=self.host)
        self.handle_refresh()

    def handle_refresh(self):
        hours = os.getenv('REFRESH_HOURS')
        if hours is None:
            hours = 4
        else:
            hours = int(hours)
        cmds = os.getenv('REFRESH')
        if not cmds:
            print(f'No refresh configured')
            return
        cmdlist = cmds.strip().split(',')
        for cmd in cmdlist:
            print(f'Refreshing {hours} hours of {cmd}')
        self.refresh(cmdlist, hours)

    def refresh(self, cmds=(), hours=5):
        if not cmds:
            print(f'Empty command set. Not refreshing')
        now = Util.utcnow()
        getter = S3Getter()
        for x in reversed(range(hours)):
            dt = now - datetime.timedelta(hours=x)
            daystr = dt.strftime("%Y%m%d")
            for cmd in cmds:
                self.s3_refresh(getter, cmd, daystr, dt.hour)
        getter.stats()

    def s3_refresh(self, getter, cmd, daystr, hour):
        #cmd = 'ttpositions.aspx'
        prefix = f'bustracker/raw/{cmd}/{daystr}/t{hour:02d}'
        print(f'Getting prefix {prefix}')
        keys = getter.list_with_prefix(prefix)
        refreshed = 0
        for k in keys.get('Contents', []):
            #print(f'Refreshing {k["Key"]}')
            jd = getter.get_json_contents(k['Key'])
            datalist = jd['requests']
            refreshed += 1
            self.handler(datalist, cmd)
            # for item in datalist:
            #     response = item['response']
            #     self.subscriber_callback(response['ctatt'])
        return {'refreshed': refreshed}

    async def periodic_cleanup(self):
        while True:
            self.bus_updater.periodic_cleanup()
            await asyncio.sleep(60)

    def handler(self, data, topic):
        print(f'Received {topic} data len {len(str(data))} first {str(data)[:100]}')
        datalist = data
        for item in datalist:
            response = item['response']
            if 'getvehicles' in topic:
                self.bus_updater.subscriber_callback(response['bustime-response']['vehicle'])
            elif 'ttpositions' in topic:
                self.train_updater.subscriber_callback(response['ctatt'])
            elif 'ttarrivals' in topic:
                self.train_updater.prediction_callback(response['ctatt'])
            elif 'getpredictions' in topic:
                self.bus_updater.bus_prediction_callback(response['bustime-response']['prd'])
            else:
                print(f'Warning! Unexpected topic {topic}')

    def catchup(self):
        response = requests.get(f'http://{self.host}:8002/train-bundle')
        if response.status_code != 200:
            print(f'Error getting train bundle: {response.status_code}')
            return
        train_bundle = response.json()['train_bundle']
        for k, v in train_bundle.items():
            self.handler(v, f'catchup-{k}')
        response = requests.get(f'http://{self.host}:8002/bus-bundle')
        if response.status_code != 200:
            print(f'Error getting bus bundle: {response.status_code}')
            return
        bus_bundle = response.json()['bus_bundle']
        for k, v in bus_bundle.items():
            self.handler(v, f'catchup-{k}')

    async def catchup_wrapper(self):
        print('catching up')
        self.catchup()
        print('caught up')

    async def start_clients(self):
        while True:
            print(f'Creating subscriber task')
            print(f'Starting listener')
            pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)
            channels = ['getvehicles', 'ttpositions.aspx', 'getpredictions', 'ttarrivals.aspx']
            await pubsub.subscribe(*[f'channel:{channel}' for channel in channels])
            print(f'Starting async')
            while True:
                try:
                    message = await pubsub.get_message(ignore_subscribe_messages=True)
                except redis.exceptions.ConnectionError as e:
                    print(f'Redis connection error: {e}. Retrying')
                    time.sleep(5)
                    break
                if message is not None:
                    channel = message['channel'].decode('utf-8')
                    data = message['data'].decode('utf-8')
                    self.handler(json.loads(data), channel)


async def main(host: str):
    load_routes()
    print(f'Loaded data')
    print(f'Starting subscriber')
    schedule_file = Path('/app/cta_gtfs_20250206.zip').expanduser()
    schedule_analyzer = ScheduleAnalyzer(schedule_file, engine=None)
    subscriber = Subscriber(host, schedule_analyzer)
    async with asyncio.TaskGroup() as tg:
        client_task = tg.create_task(subscriber.start_clients())
        catchup_task = tg.create_task(subscriber.catchup_wrapper())
        cleanup_task = tg.create_task(subscriber.periodic_cleanup())
    print(client_task.result())
    print(cleanup_task.result())
    print(catchup_task.result())
    print(f'Tasks finished.')


if __name__ == "__main__":
    start_http_server(8109)
    asyncio.run(main(sys.argv[1]))
