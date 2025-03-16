# simple redis pubsub client

import asyncio

import redis.asyncio as redis


class Subscriber:
    def __init__(self):
        self.redis = redis.Redis(host='leonard.guineafowl-cloud.ts.net')

    async def reader(self, pubsub):
        while True:
            message = await pubsub.get_message()
            if message is not None:
                channel = message['channel'].decode('utf-8')
                if isinstance(message['data'], bytes):
                    data = message['data'].decode('utf-8')[:250]
                else:
                    data = message['data']
                print(f'Received message: {channel} data {data}')

    async def main(self):
        #async with self.redis.pubsub() as pubsub:
        pubsub = self.redis.pubsub()
        channels = ['getvehicles', 'ttpositions.aspx', 'getpredictions']
        # await pubsub.subscribe(**{f'channel:{channel}': self.message_handler for channel in channels})
        await pubsub.subscribe(*[f'channel:{channel}' for channel in channels])
        print(f'Starting async')
        reader = asyncio.create_task(self.reader(pubsub))

        await reader
        print(f'Done')


if __name__ == "__main__":
    s = Subscriber()
    asyncio.run(s.main())
