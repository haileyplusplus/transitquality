#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import sys

from fastapi_websocket_pubsub import PubSubClient
from sqlalchemy import select, delete, func, text
from sqlalchemy.orm import Session
import redis

from realtime.rtmodel import *
from realtime.load_patterns import load_routes, load, S3Getter


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
    def __init__(self, *args):
        super().__init__(*args)

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
                        #lat=float(v['lat']),
                        #lon=float(v['lon']),
                        geom=geom,
                        heading=int(v['heading']),
                        route=route_db,
                        completed=False,
                    )
                    session.add(upd)
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
            session.execute(text('update bus_position set completed = true where origtatripno not in '
                                 '(select origtatripno from current_vehicle_state)'))
            session.execute(text('UPDATE pattern t2 SET rt = t1.rt '
                                 'FROM bus_position t1 WHERE t2.id = t1.pid'))
            if self.cleanup_iteration % 10 == 0:
                session.execute(text('delete from bus_position where timestamp < '
                                     '(select max(timestamp) - interval \'24 hours\' from bus_position)'))
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
                if not self.r.exists(redis_key):
                    self.r.ts().create(redis_key, retention_msecs=60 * 60 * 24 * 1000)
                self.r.ts().add(redis_key, int(timestamp.timestamp()), int(v['pdist']))
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
    def __init__(self, host):
        self.host = host
        self.client = None
        self.engine = db_init()
        self.train_updater = TrainUpdater(self)
        self.bus_updater = BusUpdater(self)
        #print(f'Finishing past trip')
        #self.bus_updater.finish_past_trips(1744)

    async def periodic_cleanup(self):
        while True:
            self.bus_updater.periodic_cleanup()
            await asyncio.sleep(60)

    async def callback(self, data, topic):
        print(f'Received {topic} data len {len(str(data))}')
        if 'catchup' in topic:
            datalist = data
        else:
            datalist = [data]
        for item in datalist:
            response = item['response']
            if 'getvehicles' in topic:
                self.bus_updater.subscriber_callback(response['bustime-response']['vehicle'])
            elif 'ttpositions' in topic:
                self.train_updater.subscriber_callback(response['ctatt'])
            else:
                print(f'Warning! Unexpected topic {topic}')

    def initialize_clients(self):
        self.client = PubSubClient(
            ['getvehicles', 'ttpositions.aspx', 'catchup-getvehicles', 'catchup-ttpositions.aspx'],
            callback=self.callback)

    async def start_clients(self):
        self.client.start_client(f'ws://{self.host}:8002/pubsub')
        await self.client.wait_until_done()


def initialize(host: str):
    #load_routes(path='realtime/routes.json')
    #engine = load(path='/patterns')
    load_routes()
    #engine = load()
    print(f'Loaded data')
    subscriber = Subscriber(host)
    subscriber.initialize_clients()
    print(f'Starting subscriber')
    return subscriber


async def main(host: str):
    subscriber = initialize(host)
    async with asyncio.TaskGroup() as tg:
        client_task = tg.create_task(subscriber.start_clients())
        cleanup_task = tg.create_task(subscriber.periodic_cleanup())
    print(client_task.result())
    print(cleanup_task.result())
    print(f'Tasks finished.')
    #asyncio.run(subscriber.start_clients())


if __name__ == "__main__":
    asyncio.run(main(sys.argv[1]))
