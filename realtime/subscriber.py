#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import sys

from fastapi_websocket_pubsub import PubSubClient
from sqlalchemy.orm import Session

from realtime.rtmodel import *


"""
Detecting a finished trip:
 - 99% of way through route
 - vid update with new pattern or trip no
 
Grouped trip key:
 - vid, route, pid, origtatripno, day of first update
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
                if isinstance(route['train'], dict):
                    trains = [route['train']]
                else:
                    trains = route['train']
                for v in trains:
                    run = int(v['rn'])
                    timestamp = datetime.datetime.strptime(v['prdt'], '%Y-%m-%dT%H:%M:%S')
                    existing = session.get(ActiveTrain, {'run': run, 'timestamp': timestamp})
                    if existing is not None:
                        continue
                    upd = ActiveTrain(
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
                        lat=float(v['lat']),
                        lon=float(v['lon']),
                        heading=int(v['heading']),
                        route=route_db,
                    )
                    session.add(upd)
            session.commit()


class BusUpdater(DatabaseUpdater):
    def __init__(self, *args):
        super().__init__(*args)

    def subscriber_callback(self, data):
        #print(f'Bus {len(data)}')
        with Session(self.subscriber.engine) as session:
            for v in data:
                vid = int(v['vid'])
                timestamp = datetime.datetime.strptime(v['tmstmp'], '%Y%m%d %H:%M:%S')
                existing = session.get(ActiveTrip, {'vid': vid, 'timestamp': timestamp})
                route = session.get(Route, v['rt'])
                if route is None:
                    print(f'Unknown route {route}')
                    continue
                if existing is not None:
                    continue
                upd = ActiveTrip(
                    vid=vid,
                    timestamp=timestamp,
                    lat=float(v['lat']),
                    lon=float(v['lon']),
                    pid=v['pid'],
                    route=route,
                    pdist=v['pdist'],
                    tatripid=v['tatripid'],
                    origtatripno=v['origtatripno'],
                    tablockid=v['tablockid'],
                    destination=v['des'],
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
                        lat=float(v['lat']),
                        lon=float(v['lon']),
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

    async def callback(self, data, topic):
        if topic == 'vehicles':
            self.bus_updater.subscriber_callback(data)
        elif topic == 'trains':
            self.train_updater.subscriber_callback(data)
        else:
            print(f'Warning! Unexpected topic {topic}')

    def initialize_clients(self):
        self.client = PubSubClient(['vehicles', 'trains'], callback=self.callback)

    async def start_clients(self):
        self.client.start_client(f'ws://{self.host}:8002/pubsub')
        await self.client.wait_until_done()


if __name__ == "__main__":
    subscriber = Subscriber(sys.argv[1])
    subscriber.initialize_clients()
    asyncio.run(subscriber.start_clients())
