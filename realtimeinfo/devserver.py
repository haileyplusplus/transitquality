from pathlib import Path
from typing import Optional

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import datetime
import logging

from sqlalchemy import text
from sqlalchemy.orm import Session
from prometheus_client import make_asgi_app

from interfaces.estimates import BusResponse, TrainResponse, StopEstimates, StopEstimate, \
    EstimateResponse, DetailRequest, CombinedResponse, CombinedOutput, PositionInfo, CombinedEstimateRequest
from realtimeinfo.assembly import NearStopQuery
from realtime.rtmodel import db_init
from realtimeinfo.queries import QueryManager, TrainQuery
from backend.util import Config

from schedules.schedule_analyzer import ScheduleAnalyzer

logger = logging.getLogger(__file__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    logger.debug('serving')
    yield
    logger.info(f'App lifespan done')


app = FastAPI(lifespan=lifespan)
metrics_app = make_asgi_app()
app.mount('/metrics', metrics_app)


connection_config = Config('prod')
app.add_middleware(CORSMiddleware,
                   allow_origins=connection_config.allowed_origins,
                   allow_credentials=True,
                   allow_methods=["*"],
                   allow_headers=["*"]
                   )

engine = db_init(connection_config, echo=False)
qm = QueryManager(engine, connection_config)
# fix for prod
schedule_file = Path('/app/cta_gtfs_20250206.zip')
sa = ScheduleAnalyzer(schedule_file, engine=engine)
sa.setup_shapes()


@app.get('/')
def main():
    return {'appname': 'Real-time database manager (dev)'}


@app.get('/status')
def status():
    return {'status': 'running'}


# deprecated
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
def nearest_estimates(lat: float, lon: float) -> BusResponse:
    start = datetime.datetime.now()
    results = qm.nearest_stop_vehicles(lat, lon)
    end = datetime.datetime.now()
    latency = int((end - start).total_seconds())
    return BusResponse(
        results=results,
        start=start,
        latency=latency,
        lat=lat,
        lon=lon
    )


@app.get('/nearest-trains')
def nearest_trains(lat: float, lon: float) -> TrainResponse:
    if sa is None:
        return TrainResponse(results=[])
    tq = TrainQuery(engine, sa)
    return TrainResponse(results=tq.get_relevant_stops(lat, lon))


@app.post('/detail')
async def detail(request: DetailRequest):
    start = datetime.datetime.now()
    detail = await qm.detail(request)
    end = datetime.datetime.now()
    latency = int((end - start).total_seconds())
    return {'detail': detail, 'start': start.isoformat(), 'latency': latency}


@app.get('/single-estimate/')
async def single_estimate(pattern_id: int, stop_position: str, vehicle_position: str, vehicle_id: Optional[int], debug: bool=False) -> EstimateResponse:
    return await qm.get_estimates(
        StopEstimates(
            estimates=[StopEstimate(
                        debug=debug,
                        pattern_id=pattern_id,
                        stop_position=stop_position,
                        vehicle_positions=[
                            PositionInfo(
                                vehicle_position=vehicle_position,
                                vehicle_id=vehicle_id
                            )],
                        )],
            recalculate_positions=True
        ), schedule_analyzer=sa
    )


@app.post('/estimates/')
async def estimates(stop_estimates: StopEstimates) -> EstimateResponse:
    return await qm.get_estimates(stop_estimates)


@app.get('/combined-estimate-raw')
async def combined_estimate_raw(lat: float, lon: float) -> CombinedResponse:
    logger.debug(f'Running query combined estimate 1')
    q = NearStopQuery(qm, sa, lat=lat, lon=lon, do_conversion=False)
    logger.debug(f'Running query combined estimate 2')
    response = await q.run_query()
    return CombinedResponse(response=response)


@app.post('/combined-estimate')
async def combined_estimate(request: CombinedEstimateRequest) -> CombinedOutput:
    q = NearStopQuery(qm, sa, lat=request.lat, lon=request.lon, do_conversion=True)
    response = await q.run_query()
    return CombinedOutput(response=response)
