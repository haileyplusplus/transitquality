#!/usr/bin/env python3

import sys

import redis
import datetime


class Cleaner:
    def __init__(self, host):
        self.redis = redis.Redis(host=host)

    def clean(self):
        r = self.redis
        thresh = datetime.datetime.now() - datetime.timedelta(hours=24)
        ts = int(thresh.timestamp())
        print(f'Removing entries older than {ts}')
        for keytype in ['train', 'bus']:
            print(f'Cleaning {keytype}')
            empty = 0
            del_keys = r.keys(f'{keytype}position:*')
            print(len(del_keys), type(del_keys))
            p = r.pipeline()
            for k in del_keys:
                p.ts().delete(k, 0, ts)
            p.execute()
            for k in del_keys:
                p.ts().get(k)
            results = p.execute()
            for k, rv in zip(del_keys, results):
                if rv is None:
                    p.delete(k)
                    empty += 1
            p.execute()
            print(f'Removed {empty} empty keys')



if __name__ == "__main__":
    c = Cleaner(sys.argv[1])
    c.clean()
