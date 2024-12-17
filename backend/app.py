from fastapi import FastAPI, BackgroundTasks
from contextlib import asynccontextmanager
import asyncio
from pathlib import Path
import datetime
import logging

from backend.busscraper2 import BusScraper, Runner
from backend.scrapemodels import db_initialize
from backend.s3client import S3Client
import signal
import asyncio
import os

logger = logging.getLogger(__file__)


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f'App starting up')
    yield
    logger.info(f'App lifespan done')
    runner.syncstop()
    logger.info(f'Syncstop done')


app = FastAPI(lifespan=lifespan)


@app.get('/')
def main():
    return {'appname': 'Bus scraper control'}


@app.get('/status')
def status():
    return runner.status()


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
