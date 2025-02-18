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

# boto3.setup_default_session(
#     region_name='us-east-2',
# )


# BUCKET = S3Path('/transitquality2024/bustracker/raw')
# BUCKET.client = client


class S3Getter:
    def __init__(self):
        self.client = boto3.client(
            's3', region_name='us-east-2',
            config=Config(signature_version=UNSIGNED)
        )
        self.bucket = 'transitquality2024'

    def list_with_prefix(self, prefix):
        return self.client.list_objects(Bucket=self.bucket, Prefix=prefix)

    def get_json_contents(self, key):
        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        data = obj['Body'].read()
        return json.loads(data.decode('utf-8'))


def load_routes():
    # path='~/transit/s3/getroutes/20250107/t025330z.json'
    engine = db_init(local=True)
    #r = Path(path).expanduser()
    #r = BUCKET / 'getroutes/20250107/t025330z.json'
    #with r.open() as fh:
    if True:
        getter = S3Getter()
        #j = json.load(fh)
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
    # path='~/transit/s3/getpatterns'
    #pattern_path = Path(path).expanduser()
    #pattern_path = BUCKET / 'getpatterns'
    #print(f'Pattern path: {pattern_path} exists {pattern_path.exists()}')
    #ph = PatternHistory(pattern_path)
    ph = PatternHistory(Path())
    getter = S3Getter()
    keys = getter.list_with_prefix('bustracker/raw/getpatterns/2025')
    for k in keys['Contents']:
        jd = getter.get_json_contents(k['Key'])
        ph.read_json(jd)
    #ph.traverse()
    engine = db_init()
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
