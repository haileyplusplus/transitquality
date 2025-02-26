#!/usr/bin/env python3

from sqlalchemy import select, delete, func, text
from sqlalchemy.orm import Session
import redis
from realtime.rtmodel import *


if __name__ == "__main__":
    engine = db_init(dev=True)
    with Session(engine) as session:
        session.execute(text('delete from current_train_state'))
        session.execute(text('delete from train_position'))
        session.commit()
    print(f'Cleared db')
    r = redis.Redis(host='rttransit-1')
    deleted = 0
    #for key in r.scan_iter('trainposition:*'):
    del_keys = r.keys('trainposition:*')
    print(f'Found {del_keys} redis keys to delete')
    pipe = r.pipeline()
    for key in del_keys:
        deleted += 1
        pipe.delete(key)
    pipe.execute()
    print(f'Redis deleted {deleted}')
