#!/usr/bin/env python3

import asyncio
import sys
import json

from fastapi_websocket_pubsub import PubSubClient


async def updates(data, topic):
    #datastr = str(data)
    #print(f'Update on {topic}: {len(data)} str {len(datastr)} first {datastr[:100]}')
    print(json.dumps(data, indent=4))


TOPICS = [
    'getpredictions',
]

async def main(host):
    catchup_topics = [f'catchup-{x}' for x in TOPICS]
    client = PubSubClient(catchup_topics + TOPICS, callback=updates)
    client.start_client(f'ws://{host}:8002/pubsub')
    await client.wait_until_done()


if __name__ == "__main__":
    host = sys.argv[1]
    asyncio.run(main(host))
