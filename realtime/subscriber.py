#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import sys

from fastapi_websocket_pubsub import PubSubClient
from sqlalchemy import select, delete, func
from sqlalchemy.orm import Session

from realtime.rtmodel import *
from realtime.load_patterns import load_routes, load


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

    def periodic_cleanup(self):
        """
        select count(*) from active_trip where (now() at time zone 'America/Chicago' - active_trip.timestamp) > make_interval(hours => 24);
        :return:
        """
        pass

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
                session.add(upd)
                pattern = session.get(Pattern, v['pid'])
                if pattern is None:
                    pattern = Pattern(
                        id=v['pid'],
                        updated=datetime.datetime.fromtimestamp(0),
                        route=route
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


if __name__ == "__main__":
    load_routes(path='realtime/routes.json')
    engine = load(path='/patterns')
    print(f'Loaded data')
    subscriber = Subscriber(sys.argv[1])
    subscriber.initialize_clients()
    print(f'Starting subscriber')
    asyncio.run(subscriber.start_clients())
