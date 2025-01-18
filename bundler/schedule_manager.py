#!/usr/bin/env python3
import json

# https://www.transitchicago.com/downloads/sch_data/

from s3path import S3Path

import requests
import tempfile
import datetime
from dateutil.parser import parse


class ScheduleManager:
    BUCKET = S3Path('/transitquality2024/schedules/cta')
    URL = 'https://www.transitchicago.com/downloads/sch_data/google_transit.zip'

    def __init__(self):
        self.most_recent = self.most_recent_schedule()
        self.state = None
        self.read_state()

    def most_recent_schedule(self):
        #path = self.BUCKET / '*.zip'
        schedules = list(self.BUCKET.glob('cta_gtfs_20??????.zip'))
        if not schedules:
            return None
        schedules.sort()
        try:
            return datetime.datetime.strptime(schedules[-1].name, 'cta_gtfs_%Y%m%d.zip')
        except ValueError:
            return None

    def read_state(self):
        state = self.BUCKET / 'state.json'
        if not state.exists():
            return False
        self.state = json.loads(state.read_text(encoding='utf-8'))
        return True

    def status(self):
        return self.state

    def write_state(self):
        state = self.BUCKET / 'state.json'
        state.write_text(json.dumps(dict(self.state)), encoding='utf-8')

    def retrieve(self):
        needs_update = self.poll()
        if not needs_update:
            return {'retrieve': 'no update needed'}
        with tempfile.TemporaryFile() as tfh:
            resp = requests.get(self.URL, stream=True)
            if resp.status_code != 200:
                return {'retrieve': 'error',
                        'status_code': resp.status_code}
            try:
                dt = parse(resp.headers.get('Last-Modified'))
            except ValueError as e:
                return {'retrieve': 'error', 'message': str(e)}
            outname = dt.strftime('cta_gtfs_%Y%m%d.zip')
            outpath = self.BUCKET / outname
            for chunk in resp.iter_content(chunk_size=16384):
                tfh.write(chunk)
            tfh.seek(0)
            outpath.write_bytes(tfh.read())
            self.state = resp.headers
        self.write_state()
        return {'retrieve': 'success'}

    def poll(self):
        h = requests.head(self.URL)
        try:
            #dt = parse(h.headers.get('Last-Modified'))
            tag = h.headers['ETag']
            needs_update = True
            if self.state and self.state['ETag'] == tag:
                needs_update = False
            #self.state = h.headers
            return needs_update
        except ValueError:
            return False
