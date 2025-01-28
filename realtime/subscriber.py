#!/usr/bin/env python3

"""
Subscribe to streaming updates and insert them into the database.
"""

import asyncio
import sys

from fastapi_websocket_pubsub import PubSubClient


class DatabaseUpdater:
    def __init__(self):
        pass

    def subscriber_callback(self, data):
        pass


class TrainUpdater(DatabaseUpdater):
    def __init__(self):
        super().__init__()

    def subscriber_callback(self, data):
        print(f'Train {len(data)}')


class BusUpdater(DatabaseUpdater):
    def __init__(self):
        super().__init__()

    def subscriber_callback(self, data):
        print(f'Bus {len(data)}')


class Subscriber:
    def __init__(self, host):
        self.host = host
        self.client = None
        self.train_updater = TrainUpdater()
        self.bus_updater = BusUpdater()

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
