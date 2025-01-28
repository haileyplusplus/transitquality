#!/usr/bin/env python3

from pathlib import Path

from sqlalchemy import select, func
from sqlalchemy.orm import Session

from tools.patternhistory import PatternHistory
from realtime.rtmodel import *


def load():
    ph = PatternHistory(Path('~/transit/s3/getpatterns').expanduser())
    ph.traverse()
    engine = db_init()
    with Session(engine) as session:
        for pattern_obj in ph.latest_patterns():
            pid = pattern_obj['pid']
            if session.get(Pattern, pid):
                print(f'Repeat pattern {pid}')
                continue
            pattern = Pattern(id=pid,
                              rt=pattern_obj['ln'])
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
    engine = load()
    with engine.connect() as conn:
        print(conn.execute(select(func.count('*')).select_from(Pattern)).all())
        print(conn.execute(select(func.count('*')).select_from(Stop)).all())
        print(conn.execute(select(func.count('*')).select_from(PatternStop)).all())
