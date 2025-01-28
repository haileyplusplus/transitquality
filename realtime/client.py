#!/usr/bin/env python3

import asyncio

from fastapi_websocket_pubsub import PubSubClient


async def updates(data, topic):
    datastr = str(data)
    print(f'Update on {topic}: {len(data)} str {len(datastr)} first {datastr[:100]}')


async def main():
    client = PubSubClient(['vehicles', 'trains'], callback=updates)
    client.start_client(f'ws://localhost:8002/pubsub')
    await client.wait_until_done()


if __name__ == "__main__":
    asyncio.run(main())
