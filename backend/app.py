from functools import lru_cache

from fastapi import FastAPI, BackgroundTasks
from fastapi.responses import StreamingResponse
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging
import json
import sys


import redis.asyncio as redis
from playhouse.shortcuts import model_to_dict
from pydantic_settings import BaseSettings, SettingsConfigDict
from fastapi_websocket_pubsub import PubSubEndpoint
from fastapi_websocket_rpc import RpcChannel

from backend.busscraper2 import BusScraper
from backend.trainscraper2 import TrainScraper
from backend.scraper_interface import ScrapeState
from backend.runner import Runner
from backend.scrapemodels import db_initialize, Route, Pattern, Stop, Count
from backend.s3client import S3Client
import os

from backend.util import Util

logger = logging.getLogger(__file__)

# don't log every message the subscriber sends
logging.getLogger('PubSubServer').setLevel(logging.WARNING)

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

#
# def pubsub_callback(obj):
#     #pub.sendMessage('vehicles', obj)
#     asyncio.create_task(endpoint.publish(['vehicles'], data=obj))
#     #print(obj)
#
#
# def train_callback(obj):
#     asyncio.create_task(endpoint.publish(['trains'], data=obj))


class SubscriptionManager:
    def __init__(self):
        self.needs_init = set([])
        self.endpoint = None
        self.redis_client = redis.Redis(host='memstore')
        #ping_status = await asyncio.run(self.redis_client.ping()
        #print(f'Redis ping status: {)}')
        #self.redis_pubsub = self.redis_client.pubsub()
        task = asyncio.create_task(self.redis_client.ping())
        print(f'create task {task}')
        task.add_done_callback(lambda x: print(f'ping status: {x}'))

    def create_endpoint(self, app):
        async def connection_callback(channel: RpcChannel):
            self.needs_init.add('bus')
            self.needs_init.add('train')

        endpoint = PubSubEndpoint(on_connect=[connection_callback])
        endpoint.register_route(app, '/pubsub')
        self.endpoint = endpoint
        return self.endpoint

    def common_callback(self, command, bundles):
        if command == 'ttpositions.aspx':
            conntype = 'train'
        else:
            conntype = 'bus'
        channel_name = f'channel:{command}'
        #print(f'publishing to redis channel {channel_name}')
        if conntype in self.needs_init:
            for k, v in bundles.items():
                if k == command:
                    datalist = v[:-1]
                else:
                    datalist = v
                asyncio.create_task(self.endpoint.publish([f'catchup-{k}'],
                                                          data=datalist))
                asyncio.create_task(self.redis_client.publish(
                    channel_name, json.dumps(datalist)
                ))
            self.needs_init.discard(conntype)
        objlist = bundles[command]
        asyncio.create_task(self.endpoint.publish([command], data=objlist[-1]))
        asyncio.create_task(self.redis_client.publish(
            channel_name, json.dumps([objlist[-1]])
        ))


subscription_manager = SubscriptionManager()


bus_scraper = BusScraper(outdir, datetime.timedelta(seconds=60), debug=False,
                         fetch_routes=False, write_local=write_local,
                         callback=subscription_manager.common_callback)
bus_runner = Runner(bus_scraper)
train_scraper = TrainScraper(outdir, datetime.timedelta(seconds=60),
                             write_local=write_local, callback=subscription_manager.common_callback)
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


class Settings(BaseSettings):
    # TODO: put bucket settings here
    model_config = SettingsConfigDict(secrets_dir='/run/secrets')
    bus_api_key: str
    train_api_key: str


app = FastAPI(lifespan=lifespan)
print(f'Registering app route for pubsub')
subscription_manager.create_endpoint(app)


def apply_settings():
    s = Settings()
    bus_scraper.set_api_key(s.bus_api_key)
    train_scraper.set_api_key(s.train_api_key)


apply_settings()


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
    return {'command': 'setkey', 'result': 'success'}


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
    return {'command': 'startbus', 'result': 'success'}


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
    return {'command': 'starttrain', 'result': 'success'}


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


@app.get('/bus-bundle')
def bus_bundle():
    request_time = Util.utcnow()
    return {'bus_bundle': bus_scraper.get_bundle(),
            'request_time': request_time.isoformat()}


@app.get('/train-bundle')
def train_bundle():
    request_time = Util.utcnow()
    return {'train_bundle': train_scraper.get_bundle(),
            'request_time': request_time.isoformat()}