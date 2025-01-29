#!/usr/bin/env python3

from pathlib import Path
import json

from sqlalchemy import select, func, delete
from sqlalchemy.orm import Session

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


def load_routes(path='~/transit/s3/getroutes/20250107/t025330z.json'):
    engine = db_init()
    r = Path(path).expanduser()
    with r.open() as fh:
        j = json.load(fh)
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


def load(path='~/transit/s3/getpatterns'):
    pattern_path = Path(path).expanduser()
    print(f'Pattern path: {pattern_path} exists {pattern_path.exists()}')
    ph = PatternHistory(pattern_path)
    ph.traverse()
    engine = db_init()
    with Session(engine) as session:
        for maxts, pattern_obj in ph.latest_patterns():
            pid = pattern_obj['pid']
            updated = maxts
            pattern = session.get(Pattern, pid)
            if pattern:
                #print(f'Repeat pattern {pid}')
                if updated <= pattern.updated.replace(tzinfo=datetime.UTC):
                    continue
                pattern.updated = updated
                pattern.length = pattern_obj['ln']
                stmt = delete(PatternStop).where(PatternStop.pattern_id.is_(pid))
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
                    stop = Stop(id=stop_id,
                                stop_name=pattern_stop_obj['stpnm'],
                                lat=pattern_stop_obj['lat'],
                                lon=pattern_stop_obj['lon'])
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
