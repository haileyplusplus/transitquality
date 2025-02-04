
from fastapi import FastAPI
from contextlib import asynccontextmanager
import datetime
import logging
import json
import sys

from sqlalchemy import text
from sqlalchemy.orm import Session

from realtime.rtmodel import db_init
from realtime.queries import QueryManager, StopEstimates

logger = logging.getLogger(__file__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    print('serving')
    yield
    logger.info(f'App lifespan done')


app = FastAPI(lifespan=lifespan)
engine = db_init(echo=True)
qm = QueryManager(engine)


@app.get('/')
def main():
    return {'appname': 'Real-time database manager (dev)'}


@app.get('/status')
def status():
    return {'status': 'running'}


@app.get('/nearest-stops')
def nearest_stops(lat: float, lon: float):
    rv = []
    with Session(engine) as session:
        query = (
            'select pattern_id, rt, id, stop_name, dist from (select DISTINCT ON (pattern_id) pattern_id, rt, id, stop_name, dist from (select stop.id, pattern_stop.pattern_id, stop_name, pattern.rt, '  
            'ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist '
            'from stop inner join pattern_stop on stop.id = pattern_stop.stop_id inner join pattern on pattern_stop.pattern_id = pattern.id ORDER BY dist) ' 
            'WHERE dist < :thresh) order by dist'
        )
        result = session.execute(text(query), {"lat": float(lat), "lon": float(lon), "thresh": 1000})
        for row in result:
            rv.append({'pattern': row.pattern_id,
                       'route': row.rt,
                       'stop_id': row.id,
                       'stop_name': row.stop_name,
                       'dist': row.dist})
    return {'results': rv}


@app.get('/nearest-estimates')
def nearest_estimates(lat: float, lon: float):
    start = datetime.datetime.now()
    results = qm.nearest_stop_vehicles(lat, lon)
    end = datetime.datetime.now()
    latency = int((end - start).total_seconds())
    return {'results': results, 'start': start.isoformat(), 'latency': latency,
            'lat': lat, 'lon': lon}


@app.get('/detail')
def detail(pid: int, stop_dist: int):
    start = datetime.datetime.now()
    detail = qm.detail(pid, stop_dist)
    end = datetime.datetime.now()
    latency = int((end - start).total_seconds())
    return {'detail': detail, 'start': start.isoformat(), 'latency': latency}


@app.post('/estimates/')
def estimates(stop_estimates: StopEstimates):
    pattern_estimate_dict = qm.get_estimates(stop_estimates.estimates)
    rv = []
    for k, v in pattern_estimate_dict.items():
        rv.append({'pattern': k, 'estimate': v})
    return {'estimates': rv}
