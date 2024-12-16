from fastapi import FastAPI, BackgroundTasks
import asyncio
from pathlib import Path
import datetime

from busscraper2 import BusScraper, Runner
from scrapemodels import db_initialize
import signal
import asyncio

app = FastAPI()

db_initialize()
outdir = Path('~/transit/scraping/bustracker').expanduser()
outdir.mkdir(parents=True, exist_ok=True)
ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key='', debug=False,
                fetch_routes=False)
runner = Runner(ts)
#signal.signal(signal.SIGINT, runner.exithandler)
#signal.signal(signal.SIGTERM, runner.exithandler)


@app.get('/')
def main():
    return {'appname': 'Bus scraper control'}


@app.get('/status')
def status():
    return {'status': 'status goes here'}


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
