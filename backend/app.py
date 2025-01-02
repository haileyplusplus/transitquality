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

from backend.busscraper2 import BusScraper
from backend.trainscraper2 import TrainScraper
from backend.scraper_interface import ScrapeState
from backend.runner import Runner
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
tracker_env = os.getenv('TRACKERWRITE')
if tracker_env == 's3':
    write_local = False
elif tracker_env == 'local':
    write_local = True
else:
    print(f'Unexpected value for TRACKERWRITE env var: {tracker_env}')
    sys.exit(1)
# logdir.mkdir(parents=True, exist_ok=True)
bus_scraper = BusScraper(outdir, datetime.timedelta(seconds=60), debug=False,
                         fetch_routes=False, write_local=write_local)
bus_runner = Runner(bus_scraper)
train_scraper = TrainScraper(outdir, datetime.timedelta(seconds=60), write_local=write_local)
train_runner = Runner(train_scraper)

#signal.signal(signal.SIGINT, runner.exithandler)
#signal.signal(signal.SIGTERM, runner.exithandler)

START_TIME = datetime.datetime.now(datetime.UTC)

processor = Processor(data_dir=Path('/transitdata'))


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    yield
    logger.info(f'App lifespan done')
    bus_runner.syncstop()
    train_runner.syncstop()
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
    d = {}
    fn = LOCALDIR / 'data' / 'buildinfo.json'
    if fn.exists():
        with open(fn) as fh:
            git = json.load(fh)
    else:
        git = {}
    d['build'] = git
    d['started'] = START_TIME.isoformat()
    for x in {bus_runner, train_runner}:
        d[x.scraper.get_name()] = x.status()
    return d


@app.get('/setkey/{key}')
def setkey(key: str, trainkey: str | None = None):
    bus_scraper.set_api_key(key)
    if trainkey is not None:
        train_scraper.set_api_key(trainkey)
    return {'result': 'success'}


@app.get('/startbus')
async def startbus(background_tasks: BackgroundTasks):
    if not bus_scraper.has_api_key():
        return {'result': 'error', 'message': 'API key must first be set'}
    bus_runner.syncstart()
    #train_runner.syncstart()
    # check whether running
    #asyncio.run(runner.start())
    background_tasks.add_task(bus_runner.loop)
    #background_tasks.add_task(train_runner.loop)
    return {'result': 'success'}


@app.get('/starttrain')
async def starttrain(background_tasks: BackgroundTasks):
    if not train_scraper.has_api_key():
        return {'result': 'error', 'message': 'API key must first be set'}
    #bus_runner.syncstart()
    train_runner.syncstart()
    # check whether running
    #asyncio.run(runner.start())
    #background_tasks.add_task(bus_runner.loop)
    background_tasks.add_task(train_runner.loop)
    return {'result': 'success'}


@app.get('/stop')
def stop():
    #asyncio.run(runner.stop())
    bus_runner.syncstop()
    train_runner.syncstop()
    return {'result': 'success'}


@app.get('/tests3/{testarg}')
def tests3(testarg: str):
    client = S3Client()
    client.write_api_response(datetime.datetime.now(), 'test', testarg)


@app.get('/loghead')
def loghead():
    v = bus_scraper.requestor.readlog(tail=False)
    return {'log_contents': v}


@app.get('/logtail')
def logtail():
    v = bus_scraper.requestor.readlog(tail=True)
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
