#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import faulthandler
import json
import sys
import time
from pathlib import Path

import requests
import shapely
from sqlalchemy import select, delete, func, text
from sqlalchemy.orm import Session
import redis
import redis.asyncio as redis_async

from realtime.rtmodel import *
from realtime.load_patterns import load_routes, load, S3Getter

from schedules.schedule_analyzer import ScheduleAnalyzer

"""
Detecting a finished trip:
 - 99% of way through route
 - vid update with new pattern or trip no
 
Grouped trip key:
 - vid, route, pid, origtatripno, day of first update
"""

"""
Geo queries:

select stop_name, 
ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM('SRID=4326;POINT(-87.632892 41.903914)'::geometry, 26916) as dist
from stop ORDER BY dist limit 10;

"""


class DatabaseUpdater:
    def __init__(self, subscriber):
        self.subscriber = subscriber
        self.r = redis.Redis()

    def subscriber_callback(self, data):
        pass


class TrainUpdater(DatabaseUpdater):
    """
    Finding pattern for route

    trivial or mostly trivial: yellow, red, pink, brown, orange
    easy choice: green
    more complexity: blue, purple
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.schedule_analyzer = kwargs['schedule_analyzer']

    def subscriber_callback(self, data):
        with Session(self.subscriber.engine) as session:
            routes = data['route']
            for route in routes:
                rt = route['@name']
                route_db = session.get(Route, rt)
                if route_db is None:
                    print(f'Unknown route {route_db}')
                    continue
                if 'train' not in route:
                    continue
                if isinstance(route['train'], dict):
                    trains = [route['train']]
                else:
                    trains = route['train']
                for v in trains:
                    run = int(v['rn'])
                    timestamp = datetime.datetime.strptime(v['prdt'], '%Y-%m-%dT%H:%M:%S')
                    existing = session.get(TrainPosition, {'run': run, 'timestamp': timestamp})
                    if existing is not None:
                        continue
                    lat = v['lat']
                    lon = v['lon']
                    geom = f'POINT({lon} {lat})'
                    key = (run, timestamp)
                    if session.get(TrainPosition, key):
                        continue
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
                    current = session.get(CurrentTrainState, run)
                    if not current:
                        current = CurrentTrainState(
                            id=run,
                            last_update=timestamp
                        )
                        session.add(current)
                    elif timestamp <= current.last_update:
                        continue
                    current.last_update = timestamp
                    current.dest_station = int(v['destSt'])
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
                    train_point = shapely.Point(lon, lat)
                    if current.dest_station == current.next_stop:
                        upd.current_pattern = current.current_pattern
                        current.current_pattern = None
                    else:
                        if current.current_pattern is None:
                            if current.synthetic_trip_id is None:
                                current.synthetic_trip_id = 0
                            else:
                                current.synthetic_trip_id += 1
                        current.current_pattern = self.schedule_analyzer.get_pattern(
                            rt, current.dest_station, train_point)
                        upd.current_pattern = current.current_pattern
                    if current.current_pattern:
                        redis_key = f'trainposition:{current.current_pattern}:{run}-{current.synthetic_trip_id}'
                        try:
                            debug = run == 423
                            shape_manager = self.schedule_analyzer.managed_shapes[int(current.current_pattern)]
                            train_distance = shape_manager.get_distance_along_shape_direction(current.direction,
                                                                                              train_point, debug=debug)
                            current.pattern_distance = int(train_distance)
                            upd.pattern_distance = int(train_distance)
                            if not self.r.exists(redis_key):
                                self.r.ts().create(redis_key, retention_msecs=60 * 60 * 24 * 1000)
                            self.r.ts().add(redis_key, int(timestamp.timestamp()), train_distance)
                        except redis.exceptions.ResponseError as e:
                            print(f'Redis summarizer error: {e}')
                        except KeyError as e:
                            print(f'Bad pattern: {e}')
                        except shapely.errors.GEOSException as e:
                            print(f'GEOS error: {e}')
            session.commit()

    def prediction_callback(self, data):
        with Session(self.subscriber.engine) as session:
            for estimate in data['eta']:
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

    def periodic_cleanup(self):
        """
        select count(*) from active_trip where (now() at time zone 'America/Chicago' - active_trip.timestamp) > make_interval(hours => 24);

        select count(*) from current_vehicle_state where ((select max(last_update) from current_vehicle_state) - current_vehicle_state.last_update) > make_interval(mins => 5);
        update bus_position set completed = true where origtatripno not in (select origtatripno from current_vehicle_state);
        :return:
        """
        start = datetime.datetime.now()
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
                session.execute(text('delete from bus_position where timestamp < '
                                     '(select max(timestamp) - interval \'24 hours\' from bus_position)'))
                session.execute(text('delete from train_position where timestamp < '
                                     '(select max(timestamp) - interval \'24 hours\' from train_position)'))
            self.cleanup_iteration += 1
            session.commit()
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

    def s3_refresh(self, daystr, hour):
        cmd = 'getvehicles'
        getter = S3Getter()
        keys = getter.list_with_prefix(f'bustracker/raw/{cmd}/{daystr}/t{hour}')
        refreshed = 0
        for k in keys['Contents']:
            print(f'Refreshing {k["Key"]}')
            jd = getter.get_json_contents(k['Key'])
            datalist = jd['requests']
            refreshed += 1
            for item in datalist:
                response = item['response']
                self.subscriber_callback(response['bustime-response']['vehicle'])
        return {'refreshed': refreshed}


    """
        stop_id: Mapped[int] = mapped_column(primary_key=True)
    destination: Mapped[str] = mapped_column(primary_key=True)
    route: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime]
    origtatripno: Mapped[str]
    prediction: Mapped[int]
                {
                    "tmstmp": "20250214 21:58",
                    "typ": "D",
                    "stpnm": "Neva & North Ave",
                    "stpid": "845",
                    "vid": "8548",
                    "dstp": 763,
                    "rt": "72",
                    "rtdd": "72",
                    "rtdir": "Eastbound",
                    "des": "Pulaski",
                    "prdtm": "20250214 21:59",
                    "tablockid": "72 -853",
                    "tatripid": "1075659",
                    "origtatripno": "259624668",
                    "dly": false,
                    "dyn": 0,
                    "prdctdn": "DUE",
                    "zone": "",
                    "psgld": "",
                    "stst": 78300,
                    "stsd": "2025-02-14",
                    "flagstop": 0
                },

    """
    def bus_prediction_callback(self, data):
        with Session(self.subscriber.engine) as session:
            for v in data:
                stop_id = int(v['stpid'])
                destination = v['des']
                route = v['rt']
                key = (stop_id, destination, route)
                prediction = session.get(BusPrediction, key)
                if not prediction:
                    prediction = BusPrediction(
                        stop_id=stop_id,
                        destination=destination,
                        route=route
                    )
                    session.add(prediction)
                prediction.origtatripno = v['origtatripno']
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
            for v in data:
                vid = int(v['vid'])
                timestamp = datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')
                existing = session.get(BusPosition, {'vid': vid, 'timestamp': timestamp})
                route = session.get(Route, v['rt'])
                if route is None:
                    print(f'Unknown route {route}')
                    continue
                if existing is not None:
                    continue
                lat = v['lat']
                lon = v['lon']
                geom = f'POINT({lon} {lat})'
                key = (vid, timestamp)
                if session.get(BusPosition, key):
                    continue
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
                redis_key = f'busposition:{v["pid"]}:{v["origtatripno"]}'
                try:
                    if not self.r.exists(redis_key):
                        self.r.ts().create(redis_key, retention_msecs=60 * 60 * 24 * 1000)
                    self.r.ts().add(redis_key, int(timestamp.timestamp()), int(v['pdist']))
                except redis.exceptions.ResponseError as e:
                    print(f'Redis summarizer error: {e}')
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
        self.engine = db_init(local=True)
        self.train_updater = TrainUpdater(self, schedule_analyzer=schedule_analyzer)
        self.bus_updater = BusUpdater(self)
        self.redis_client = redis_async.Redis(host=self.host)

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
            channels = ['getvehicles', 'ttpositions.aspx', 'getpredictions']
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
    asyncio.run(main(sys.argv[1]))
