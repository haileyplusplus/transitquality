#!/usr/bin/env python3
import argparse
import sys

from sqlalchemy import select, delete, func, text
from sqlalchemy.orm import Session
import redis
from realtime.rtmodel import *


def redis_delete(keytype):
    deleted = 0
    #del_keys = r.keys('trainposition:*')
    del_keys = r.keys(f'{keytype}position:*')
    print(f'Found {len(del_keys)} redis {keytype} keys to delete')
    pipe = r.pipeline()
    for key in del_keys:
        deleted += 1
        pipe.delete(key)
    pipe.execute()
    print(f'Redis deleted {deleted}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Reset test or prod realtime data.')
    parser.add_argument('--prod', action='store_true',
                        help='Reset prod instead of dev.')
    parser.add_argument('--bus', action='store_true',
                        help='Reset bus data.')
    parser.add_argument('--train', action='store_true',
                        help='Reset train data.')
    args = parser.parse_args()
    dev = not args.prod
    if dev:
        print(f'Resetting dev')
    else:
        print(f'Resetting prod')
    if not args.bus and not args.train:
        print(f'At least one of --bus or --train must be specified.')
        sys.exit(1)
    engine = db_init(dev=dev)
    with Session(engine) as session:
        if args.train:
            session.execute(text('delete from current_train_state'))
            session.execute(text('delete from train_position'))
        if args.bus:
            session.execute(text('delete from current_vehicle_state'))
            session.execute(text('delete from bus_position'))
        session.commit()
    print(f'Cleared db')
    if dev:
        r = redis.Redis(host='rttransit-1')
    else:
        r = redis.Redis(host='rttransit')
    #for key in r.scan_iter('trainposition:*'):
    if args.bus:
        redis_delete('bus')
    if args.train:
        redis_delete('train')
