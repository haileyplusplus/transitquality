#!/usr/bin/env python3

import argparse
import os
import datetime
import logging
from pathlib import Path
import json
import time

import requests


logger = logging.getLogger(__file__)


class TrainScraper:
    ERROR_REST = datetime.timedelta(minutes=30)

    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta, api_key: str):
        self.start_time = datetime.datetime.now()
        self.api_key = api_key
        self.last_scraped = None
        self.next_scrape = None
        self.output_dir = output_dir
        self.scrape_interval = scrape_interval
        self.night = False
        self.locations_url = f'https://lapi.transitchicago.com/api/1.0/ttpositions.aspx?key={self.api_key}&rt=Red,Blue,Brn,G,Org,P,Pink,Y&outputType=JSON'
        self.initialize_logging()

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

    def parse_success(self, json_response):
        if not isinstance(json_response, dict):
            logging.error(f'Received invalid JSON response: {str(json_response)[:100]}')
            return None
        app_error_code = json_response.get('ctatt', {}).get('errCd')
        if app_error_code is None:
            logging.error(f'Could not parse error code from JSON response: {str(json_response)[:100]}')
            return None
        if app_error_code == '0':
            return 0
        logging.error(f'Application error {app_error_code}: {str(json_response)[:100]}')
        return app_error_code

    def scrape_one(self):
        self.last_scraped = datetime.datetime.now()
        result = requests.get(self.locations_url)
        if result.status_code != 200:
            logging.error(f'Received non-ok HTTP status code {result.status_code}')
            self.next_scrape = datetime.datetime.now() + self.ERROR_REST
            return
        rj = result.json()
        if self.parse_success(rj) != 0:
            self.next_scrape = datetime.datetime.now() + self.ERROR_REST
            return
        self.parse(rj)
        self.next_scrape = self.last_scraped + self.get_scrape_interval()

    def parse(self, json_response):
        datestr = self.last_scraped.strftime('%Y%m%d%H%M%S')
        filename = self.output_dir / f'ttscrape-{datestr}.json'
        with open(filename, 'w') as ofh:
            json.dump(json_response, ofh)

    def loop(self):
        while True:
            self.scrape_one()
            interval = self.next_scrape - datetime.datetime.now()
            time.sleep(interval.total_seconds())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Train Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/transit/traintracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Train tracker API key.')
    args = parser.parse_args()
    if not args.api_key:
        print(f'API key required')
    ts = TrainScraper(Path(args.output_dir[0]).expanduser(), datetime.timedelta(seconds=60), api_key=args.api_key[0])
    logging.info(f'Initializing scraping every {ts.scrape_interval.total_seconds()} seconds.')
    ts.loop()
