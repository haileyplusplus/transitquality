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
    write_local = False
    #sys.exit(1)
# logdir.mkdir(parents=True, exist_ok=True)
bus_scraper = BusScraper(outdir, datetime.timedelta(seconds=60), debug=False,
                         fetch_routes=False, write_local=write_local)
bus_runner = Runner(bus_scraper)
train_scraper = TrainScraper(outdir, datetime.timedelta(seconds=60), write_local=write_local)
train_runner = Runner(train_scraper)

#signal.signal(signal.SIGINT, runner.exithandler)
#signal.signal(signal.SIGTERM, runner.exithandler)

START_TIME = datetime.datetime.now(datetime.UTC)



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
