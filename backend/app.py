from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging
import json


from playhouse.shortcuts import model_to_dict
import gitinfo

from backend.busscraper2 import BusScraper, Runner, ScrapeState
from backend.scrapemodels import db_initialize, Route, Pattern, Stop, Count
from backend.s3client import S3Client
from analysis.processor import Processor, RealtimeConverter
import signal
import asyncio
import os

logger = logging.getLogger(__file__)

LOCALDIR = Path(__file__).parent.parent

db_initialize()
#outdir = Path('~/transit/scraping/bustracker').expanduser()
#outdir = Path('/transit/scraping/bustracker')
outdir = Path('/transit/scraping/bustracker')
# os.mkdir('/transit/scraping')
# os.mkdir('/transit/scraping/bustracker')
# os.mkdir('/transit/scraping/bustracker/logs')
# outdir.mkdir(parents=True, exist_ok=True)
# logdir = outdir / 'logs'
# logdir.mkdir(parents=True, exist_ok=True)
ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key='', debug=False,
                fetch_routes=False)
runner = Runner(ts)
#signal.signal(signal.SIGINT, runner.exithandler)
#signal.signal(signal.SIGTERM, runner.exithandler)

START_TIME = datetime.datetime.now(datetime.UTC)

processor = Processor(data_dir=Path('/transitdata'))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    yield
    logger.info(f'App lifespan done')
    runner.syncstop()
    logger.info(f'Syncstop done')


app = FastAPI(lifespan=lifespan)


def sshelper(d: dict):
    if 'scrape_state' not in d:
        return
    ss = d['scrape_state']
    try:
        d['scrape_state'] = ScrapeState(ss).name
    except ValueError:
        pass
    return d


@app.get('/')
def main():
    return {'appname': 'Bus scraper control'}


@app.get('/routeinfo')
def routeinfo():
    routes = Route.select().order_by(['scrape_state', 'last_scrape_attempt', 'route_id'])
    routeinfos = []
    for r in routes:
        routeinfos.append(sshelper(model_to_dict(r)))
    return {'route_info': routeinfos}


@app.get('/patterninfo')
def patterninfo():
    items = Pattern.select().order_by(['scrape_state', 'last_scrape_attempt', 'pattern_id'])
    infos = []
    for r in items:
        infos.append(sshelper(model_to_dict(r)))
    return {'pattern_info': infos}


@app.get('/stopinfo')
def stopinfo():
    items = Stop.select().order_by(['scrape_state', 'last_scrape_attempt', 'stop_id'])
    infos = []
    for r in items:
        infos.append(sshelper(model_to_dict(r)))
    return {'stop_info': infos}


@app.get('/countinfo')
def countinfo():
    items = Count.select().order_by(['day', 'command'])
    infos = []
    for r in items:
        infos.append(model_to_dict(r))
    return {'count_info': infos}


@app.get('/status')
def status():
    d = runner.status()
    fn = LOCALDIR / 'data' / 'buildinfo.json'
    if fn.exists():
        with open(fn) as fh:
            git = json.load(fh)
    else:
        git = {}
    d['build'] = git
    d['started'] = START_TIME.isoformat()
    return d


@app.get('/setkey/{key}')
def setkey(key: str):
    ts.set_api_key(key)
    return {'result': 'success'}


@app.get('/start')
async def start(background_tasks: BackgroundTasks):
    if not ts.has_api_key():
        return {'result': 'error', 'message': 'API key must first be set'}
    runner.syncstart()
    # check whether running
    #asyncio.run(runner.start())
    background_tasks.add_task(runner.loop)
    return {'result': 'success'}


@app.get('/stop')
def stop():
    #asyncio.run(runner.stop())
    runner.syncstop()
    return {'result': 'success'}


@app.get('/tests3/{testarg}')
def tests3(testarg: str):
    client = S3Client()
    client.write_api_response(datetime.datetime.now(), 'test', testarg)


@app.get('/loghead')
def loghead():
    v = ts.requestor.readlog(tail=False)
    return {'log_contents': v}


@app.get('/logtail')
def logtail():
    v = ts.requestor.readlog(tail=True)
    return {'log_contents': v}


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
