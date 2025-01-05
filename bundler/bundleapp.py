import threading

from fastapi import FastAPI, BackgroundTasks
from s3path import S3Path

from bundler.bundler import Bundler


app = FastAPI()


class BundleManager:
    BUCKET = S3Path('/transitquality2024/bustracker/raw')

    def __init__(self):
        self.mutex = threading.Lock()
        self.bundler = None

    def create_bundler(self, day: str):
        success = False
        bundler = None
        with self.mutex:
            if self.bundler is None or self.bundler.done():
                self.bundler = Bundler(self.BUCKET, day)
                success = True
                bundler = self.bundler
        if bundler is not None:
            bundler.scan_day()
        return success

    def bundle_status(self):
        with self.mutex:
            if self.bundler is None:
                return {'active': False}
            return self.bundler.status()


manager = BundleManager()


@app.get('/')
def main():
    return {'appname': 'Scraper bundler'}


@app.get('/status')
def status():
    return manager.bundle_status()


@app.get('/bundle/{day}')
async def bundle(day: str, background_tasks: BackgroundTasks):
    background_tasks.add_task(manager.create_bundler, day)
    return {'day': day, 'started': True}
