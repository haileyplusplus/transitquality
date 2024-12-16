from fastapi import FastAPI
import asyncio
from pathlib import Path
import datetime

from busscraper2 import BusScraper, Runner
from scrapemodels import db_initialize

app = FastAPI()

db_initialize()
outdir = Path('~/transit/scraping/bustracker').expanduser()
outdir.mkdir(parents=True, exist_ok=True)
ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key='', debug=False,
                fetch_routes=False)
runner = Runner(ts)


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
def start():
    if not ts.has_api_key():
        return {'result': 'error', 'message': 'API key must first be set'}
    return {'result': 'success'}


@app.get('/stop')
def stop():
    return {'result': 'success'}
