#!/usr/bin/env python3

from pathlib import Path
import json


from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session
from s3path import S3Path
import boto3
import botocore
from botocore.config import Config
from botocore import UNSIGNED

from tools.patternhistory import PatternHistory
from realtime.rtmodel import *
import backend.util


TRAIN_ROUTES = [
    {'rt': 'red', 'rtnm': 'Red Line'},
    {'rt': 'blue', 'rtnm': 'Blue Line'},
    {'rt': 'brn', 'rtnm': 'Brown Line'},
    {'rt': 'g', 'rtnm': 'Green Line'},
    {'rt': 'org', 'rtnm': 'Orange Line'},
    {'rt': 'p', 'rtnm': 'Purple Line'},
    {'rt': 'pink', 'rtnm': 'Pink Line'},
    {'rt': 'y', 'rtnm': 'Yellow Line'},
]


class S3Getter:
    def __init__(self):
        self.cachedir = Path('/tmp/s3cache')
        self.cachedir.mkdir(exist_ok=True)
        self.client = boto3.client(
            's3', region_name='us-east-2',
            config=Config(signature_version=UNSIGNED)
        )
        self.bucket = 'transitquality2024'
        self.fetched = 0
        self.cached = 0

    def stats(self):
        print(f'In this session, retrieved {self.fetched} directly and {self.cached} from cache.')

    def list_with_prefix(self, prefix):
        return self.client.list_objects(Bucket=self.bucket, Prefix=prefix)

    def get_json_contents(self, key):
        cache_key = key.replace('/', '_')
        cached_path = self.cachedir / cache_key
        if cached_path.exists():
            with cached_path.open() as fh:
                self.cached += 1
                return json.load(fh)
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        data = obj['Body'].read()
        raw_str = data.decode('utf-8')
        with cached_path.open('w') as wfh:
            wfh.write(raw_str)
        self.fetched += 1
        return json.loads(raw_str)


def load_routes():
    engine = db_init(backend.util.Config('local'))
    getter = S3Getter()
    j = getter.get_json_contents('bustracker/raw/getroutes/20250107/t025330z.json')
    routes = j['requests'][0]['response']['bustime-response']['routes']
    routes += TRAIN_ROUTES
    with Session(engine) as session:
        for rt in routes:
            rt_id: str = rt['rt']
            route_db = session.get(Route, rt_id)
            if not route_db:
                route_db = Route(
                    id=rt_id,
                    name=rt['rtnm']
                )
                session.add(route_db)
        session.commit()


def load():
    ph = PatternHistory(Path())
    getter = S3Getter()
    keys = getter.list_with_prefix('bustracker/raw/getpatterns/2025')
    for k in keys['Contents']:
        jd = getter.get_json_contents(k['Key'])
        ph.read_json(jd)
    engine = db_init(backend.util.Config('prod'))
    count = 0
    with Session(engine) as session:
        for maxts, pattern_obj in ph.latest_patterns():
            count += 1
            if count % 100 == 0:
                print(f'Read {count} patterns so far')
            pid = pattern_obj['pid']
            updated = maxts
            pattern = session.get(Pattern, pid)
            if pattern:
                #print(f'Repeat pattern {pid}')
                if updated <= pattern.updated.replace(tzinfo=datetime.UTC):
                    continue
                pattern.updated = updated
                pattern.length = pattern_obj['ln']
                stmt = delete(PatternStop).where(PatternStop.pattern_id.in_([pid]))
                session.execute(stmt)
            else:
                pattern = Pattern(id=pid,
                                  updated=updated,
                                  length=pattern_obj['ln'])
                session.add(pattern)
            for pattern_stop_obj in pattern_obj['pt']:
                if pattern_stop_obj['typ'] != 'S':
                    continue
                stop_id = int(pattern_stop_obj['stpid'])
                stop = session.get(Stop, stop_id)
                if stop is None:
                    lat = lat=pattern_stop_obj['lat']
                    lon = pattern_stop_obj['lon']
                    geom = f'POINT({lon} {lat})'
                    stop = Stop(id=stop_id,
                                stop_name=pattern_stop_obj['stpnm'],
                                geom=geom,
                                #lat=pattern_stop_obj['lat'],
                                #lon=pattern_stop_obj['lon']
                                )
                    session.add(stop)
                pattern_stop = PatternStop(pattern=pattern, stop=stop,
                                           sequence=pattern_stop_obj['seq'],
                                           distance=pattern_stop_obj['pdist'])
                session.add(pattern_stop)
        session.commit()
    return engine


if __name__ == "__main__":
    load_routes()
    engine = load()
    with engine.connect() as conn:
        print(conn.execute(select(func.count('*')).select_from(Pattern)).all())
        print(conn.execute(select(func.count('*')).select_from(Stop)).all())
        print(conn.execute(select(func.count('*')).select_from(PatternStop)).all())
