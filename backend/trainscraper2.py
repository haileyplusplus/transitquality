#!/usr/bin/env python3

import argparse
import os
import datetime
import logging
from pathlib import Path
import json
import time

import requests

from backend.scrapemodels import ErrorMessage
from backend.scraper_interface import ScraperInterface, ParserInterface, ResponseWrapper
from backend.util import Util
from backend.requestor import Requestor

logger = logging.getLogger(__file__)


"""
{"ctatt":{"tmst":"2024-12-31T17:26:15","TimeStamp":"2024-12-31T17:26:15","errCd":"106","errNm":"Invalid route identifier: 'bxn'"}}
"""


class TrainParser(ParserInterface):
    @staticmethod
    def parse_success(response: requests.Response, command: str) -> ResponseWrapper:
        json_response = response.json()
        if not isinstance(json_response, dict):
            logging.error(f'Received invalid JSON response: {str(json_response)[:100]}')
            return ResponseWrapper.PERMANENT_ERROR
        app_error_code = json_response.get('ctatt', {}).get('errCd')
        if app_error_code is None:
            logging.error(f'Could not parse error code from JSON response: {str(json_response)[:100]}')
            return ResponseWrapper.PERMANENT_ERROR
        if app_error_code == '0':
            return ResponseWrapper(json_dict=json_response['ctatt'])
        logging.error(f'Application error {app_error_code}: {str(json_response)[:100]}')
        return ResponseWrapper(error_dict=json_response['ctatt'])

    @staticmethod
    def parse_error(bustime_response: dict):
        code = bustime_response.get('errCd')
        nm = bustime_response.get('errNm')
        msg = f'{code}: {nm}'
        model = ErrorMessage.get_or_none(ErrorMessage.text == msg)
        errortime = Util.utcnow()
        if model is None:
            model = ErrorMessage(text=msg, last_seen=errortime)
            model.count = model.count + 1
            # model.insert()
            model.save(force_insert=True)
        else:
            model.count = model.count + 1
            model.last_seen = errortime
            model.save()


class TrainScraper(ScraperInterface):
    BASE_URL = 'https://lapi.transitchicago.com/api/1.0'
    ERROR_REST = datetime.timedelta(minutes=30)
    TERMINAL_STATIONS = [
        # Green
        40290,
        40720,
        40020,

        # Red
        40900,
        40450,

        # Blue
        40890,
        40350,
        40390,

        # Orange
        40930,

        # Brown
        41290,

        # Pink
        40580,

        # Purple
        41050,

        # Yellow
        40140,
    ]

    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta,
                 write_local=False, callback=None):
        super().__init__()
        self.start_time = datetime.datetime.now()
        self.api_key = None
        self.write_local = write_local
        self.last_scraped = datetime.datetime.fromtimestamp(0).replace(tzinfo=datetime.UTC)
        self.next_scrape = None
        self.output_dir = output_dir
        self.parser = TrainParser()
        self.requestor = Requestor(self.BASE_URL, output_dir, output_dir, self.parser,
                                   debug=False, write_local=write_local, callback=callback)
        self.scrape_interval = scrape_interval
        self.callback = callback
        self.night = False
        logger.info('Train scraper')
        #self.locations_url = f'https://lapi.transitchicago.com/api/1.0/ttpositions.aspx?key={self.api_key}&rt=Red,Blue,Brn,G,Org,P,Pink,Y&outputType=JSON'
        #self.initialize_logging()

    def get_requestor(self):
        return self.requestor

    def get_bundle_status(self) -> dict:
        d = self.requestor.bundler.status()
        d['last_scraped'] = self.last_scraped.isoformat()
        return d

    def get_write_local(self):
        return self.write_local

    def initialize(self):
        logger.info('Initialize train scraper')

    def get_name(self) -> str:
        return 'train'

    def initialize_logging(self):
        logdir = self.output_dir / 'logs'
        logdir.mkdir(parents=True, exist_ok=True)
        datestr = self.start_time.strftime('%Y%m%d%H%M%S')
        logfile = logdir / f'train-scraper-{datestr}.log'
        logging.basicConfig(filename=logfile,
                            filemode='a',
                            format='%(asctime)s: %(message)s',
                            datefmt='%Y%m%d %H:%S',
                            level=logging.INFO)

    def check_interval(self):
        hour = datetime.datetime.now().hour
        if hour <= 4:
            if not self.night:
                self.night = True
                logging.info(f'Entering night mode.')
                return
        if self.night:
            self.night = False
            logging.info(f'Exiting night mode.')

    def get_scrape_interval(self):
        if self.night:
            return datetime.timedelta(minutes=5)
        return self.scrape_interval

    def scrape_one(self):
        scrape_time = Util.utcnow()
        if scrape_time < (self.last_scraped + self.get_scrape_interval()):
            return
        cmd = 'ttpositions.aspx'
        # response doesn't affect scraping logic but we do want to publish it to
        # any subscribers
        #response = (
        self.requestor.make_request(
            cmd, rt='Red,Blue,Brn,G,Org,P,Pink,Y', outputType='JSON', noformat=1)
        #if self.callback and response.ok():
        #    d = response.payload()
        #    self.callback(d)
        for mapid in self.TERMINAL_STATIONS:
            self.requestor.make_request(
                'ttarrivals.aspx',
                mapid=mapid,
                outputType='JSON',
                noformat=1
            )
        self.last_scraped = scrape_time

    def do_shutdown(self):
        self.requestor.bundler.output()

    def set_api_key(self, api_key):
        self.requestor.api_key = api_key

    def has_api_key(self):
        if not self.requestor.api_key:
            return False
        return True
