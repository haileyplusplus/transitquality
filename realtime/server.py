
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging
import json
import sys

from sqlalchemy import text
from sqlalchemy.orm import Session

from realtime.subscriber import initialize
from realtime.rtmodel import db_init

logger = logging.getLogger(__file__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    subscriber = initialize('leonard.guineafowl-cloud.ts.net')
    #async with asyncio.TaskGroup() as tg:
    client_task = asyncio.create_task(subscriber.start_clients())
    cleanup_task = asyncio.create_task(subscriber.periodic_cleanup())
    print('serving')
    yield
    client_task.cancel()
    cleanup_task.cancel()
    logger.info(f'App lifespan done')


app = FastAPI(lifespan=lifespan)
engine = db_init()


@app.get('/')
def main():
    return {'appname': 'Real-time database manager'}


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
