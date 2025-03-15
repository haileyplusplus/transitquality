from functools import lru_cache

import botocore
from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging
import json


import redis.asyncio as redis
from playhouse.shortcuts import model_to_dict
from pydantic_settings import BaseSettings, SettingsConfigDict

from backend.busscraper2 import BusScraper
from backend.trainscraper2 import TrainScraper
from backend.scraper_interface import ScrapeState
from backend.runner import Runner
from backend.scrapemodels import db_initialize, Route, Pattern, Stop, Count
from backend.s3client import S3Client
import os


from backend.util import Util

from schedules.schedule_manager import ScheduleManager

logger = logging.getLogger(__file__)


LOCALDIR = Path(__file__).parent.parent


class SubscriptionManager:
    def __init__(self):
        self.redis_client = redis.Redis(host='memstore')
        task = asyncio.create_task(self.redis_client.ping())
        print(f'create task {task}')
        task.add_done_callback(lambda x: print(f'ping status: {x}'))

    def common_callback(self, command, bundles):
        channel_name = f'channel:{command}'
        objlist = bundles[command]
        asyncio.create_task(self.redis_client.publish(
            channel_name, json.dumps([objlist[-1]])
        ))


class Settings(BaseSettings):
    # TODO: put bucket settings here
    model_config = SettingsConfigDict(secrets_dir='/run/secrets')
    bus_api_key: str
    train_api_key: str


class ScraperManager:
    START_TIME = datetime.datetime.now(datetime.UTC)

    def __init__(self):
        db_initialize()
        outdir = Path('/transit/scraping/bustracker')
        tracker_env = os.getenv('TRACKERWRITE')
        if tracker_env == 's3':
            write_local = False
        elif tracker_env == 'local':
            write_local = True
        else:
            print(f'Unexpected value for TRACKERWRITE env var: {tracker_env}')
            write_local = False
        self.subscription_manager = SubscriptionManager()
        self.bus_scraper = BusScraper(outdir, datetime.timedelta(seconds=60), debug=False,
                                 fetch_routes=False, write_local=write_local,
                                 callback=self.subscription_manager.common_callback)
        self.bus_runner = Runner(self.bus_scraper)
        self.train_scraper = TrainScraper(outdir, datetime.timedelta(seconds=60),
                                     write_local=write_local, callback=self.subscription_manager.common_callback)
        self.train_runner = Runner(self.train_scraper)
        s = Settings()
        self.bus_scraper.set_api_key(s.bus_api_key)
        self.train_scraper.set_api_key(s.train_api_key)


scraper_manager = ScraperManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    print('App starting up in lifespan')
    bus_runner = scraper_manager.bus_runner
    train_runner = scraper_manager.train_runner
    bus_runner.syncstart()
    bus_task = asyncio.create_task(bus_runner.loop())
    train_runner.syncstart()
    train_task = asyncio.create_task(train_runner.loop())
    yield
    print('App lifespan done')
    logger.info(f'App lifespan done')
    bus_task.cancel()
    bus_runner.syncstop()
    train_task.cancel()
    train_runner.syncstop()
    logger.info(f'Syncstop done')
    print('Tasks stopped')


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
    d['started'] = ScraperManager.START_TIME.isoformat()
    for x in {scraper_manager.bus_runner, scraper_manager.train_runner}:
        d[x.scraper.get_name()] = x.status()
    return d


@app.get('/stop')
def stop():
    scraper_manager.bus_runner.syncstop()
    scraper_manager.train_runner.syncstop()
    return {'result': 'success'}


@app.get('/tests3/{testarg}')
def tests3(testarg: str):
    client = S3Client()
    client.write_api_response(datetime.datetime.now(), 'test', testarg)


@app.get('/loghead')
def loghead():
    v = scraper_manager.bus_scraper.requestor.readlog(tail=False)
    return {'log_contents': v}


@app.get('/logtail')
def logtail():
    v = scraper_manager.bus_scraper.requestor.readlog(tail=True)
    return {'log_contents': v}


@app.get('/bus-bundle')
def bus_bundle():
    request_time = Util.utcnow()
    return {'bus_bundle': scraper_manager.bus_scraper.get_bundle(),
            'request_time': request_time.isoformat()}


@app.get('/train-bundle')
def train_bundle():
    request_time = Util.utcnow()
    return {'train_bundle': scraper_manager.train_scraper.get_bundle(),
            'request_time': request_time.isoformat()}


@app.get('/schedule_update')
def schedule_update():
    try:
        schedule_manager = ScheduleManager()
        return schedule_manager.retrieve()
    except botocore.exceptions.NoCredentialsError as e:
        return {'schedule_update': 'failed',
                'error': str(e)}
