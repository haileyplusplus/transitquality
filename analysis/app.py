from analysis.processor import Processor, RealtimeConverter
import signal
import asyncio
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging
import json
import sys


from playhouse.shortcuts import model_to_dict
import gitinfo

app = FastAPI()
LOCALDIR = Path(__file__).parent.parent
START_TIME = datetime.datetime.now(datetime.UTC)

logger = logging.getLogger(__file__)

processor = Processor(data_dir=Path('/transitdata'))

@app.get('/status')
def status():
    d = {}
    fn = LOCALDIR / 'data' / 'buildinfo.json'
    if fn.exists():
        with open(fn) as fh:
            git = json.load(fh)
    else:
        git = {}
    d['build'] = git
    d['started'] = START_TIME.isoformat()
    return d

@app.get('/runprocessor')
def runprocessor():
    processor.open()
    p, i = processor.update()
    return {'processed': p, 'inserted': i}


@app.get('/parsepatterns')
def parsepatterns():
    processor.open()
    start = datetime.datetime.now(datetime.UTC)
    d = processor.parse_new_patterns()
    finish = datetime.datetime.now(datetime.UTC)
    d['finish'] = finish.isoformat()
    d['elapsed'] = int((finish - start).total_seconds() * 1000)
    return d


@app.get('/parsevehicles/{limit}')
def parsevehicles(limit: int):
    processor.open()
    start = datetime.datetime.now(datetime.UTC)
    d = processor.parse_new_vehicles(limit=limit)
    finish = datetime.datetime.now(datetime.UTC)
    d['finish'] = finish.isoformat()
    d['elapsed'] = int((finish - start).total_seconds() * 1000)
    return d


@app.get('/close')
def close():
    closed = processor.close()
    finish = datetime.datetime.now(datetime.UTC)
    return {'closed': closed, 'finish': finish.isoformat()}


@app.get('/interpolate/{tripid}')
def interpolate(tripid: int):
    processor.open()
    rtc = RealtimeConverter()
    success = rtc.process_trip(tripid)
    return {'trip': tripid, 'success': success}


@app.get('/interpolate_route/{routeid}')
def interpolate(routeid: str):
    processor.open()
    rtc = RealtimeConverter()
    start = datetime.datetime.now(datetime.UTC)
    d = {'routes': {}}
    for route in routeid.split(','):
        rd = rtc.process_trips_for_route(route)
        d['routes'][route] = rd
    finish = datetime.datetime.now(datetime.UTC)
    routes = [routeid]
    d['finish'] = finish.isoformat()
    d['elapsed'] = int((finish - start).total_seconds() * 1000)
    return d


@app.get('/trip/{tripid}')
def trip(tripid: int):
    processor.open()
    finish = datetime.datetime.now(datetime.UTC)
    return {'trips': processor.get_trip_json(tripid),
            'finish': finish.isoformat()}


@app.get('/stops/{stop_id}')
def get_stop(stop_id: str, route_id: str, day: str):
    processor.open()
    finish = datetime.datetime.now(datetime.UTC)
    return {'stops': processor.get_stop_json(stop_id, route_id, day),
            'finish': finish.isoformat()}


@app.get('/routes')
def get_routes():
    finish = datetime.datetime.now(datetime.UTC)
    return {'routes': processor.get_route_json(),
            'finish': finish.isoformat()}

@app.get('/days')
def get_routes():
    processor.open()
    finish = datetime.datetime.now(datetime.UTC)
    return {'days': processor.get_day_json(),
            'finish': finish.isoformat()}


@app.get('/trips/{route_id}')
def get_daily_trips(route_id: str, day: str):
    processor.open()
    finish = datetime.datetime.now(datetime.UTC)
    d = processor.get_daily_trips_json(route_id, day)
    d.update({'finish': finish.isoformat()})
    return d


@app.get('/rawstop/{stop_id}')
def get_raw_stop(stop_id: str, route_id: str):
    processor.open()
    return processor.get_raw_stop(stop_id, route_id)


@app.get('/headways/{stop_id}')
def get_stop_headways(stop_id: str, route_id: str):
    processor.open()
    return processor.analyze_stop(stop_id, route_id)
