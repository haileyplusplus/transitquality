
import argparse
import os
import datetime
import logging
from pathlib import Path
import json
import time

import requests

from backend.scrapemodels import Route, Pattern, Count, ErrorMessage, db_initialize, Stop
from backend.util import Util
from backend.s3client import S3Client
from backend.scraper_interface import ScraperInterface, ScrapeState, ResponseWrapper, ParserInterface

logger = logging.getLogger(__file__)


class Bundler:
    VERSION = '2.0'
    BATCH_TIME = datetime.timedelta(minutes=5)

    def __init__(self, write_local=False, s3client=None, rawdatadir=None, callback=None):
        self.bundles = {}
        self.write_local = write_local
        self.s3client = s3client
        self.rawdatadir = rawdatadir
        self.last_write_time = Util.utcnow()
        self.callback = callback

    def maybe_write(self):
        elapsed = Util.utcnow() - self.last_write_time
        if elapsed < self.BATCH_TIME:
            return
        self.output()

    def output(self):
        if not self.bundles:
            logger.info(f'No bundles to write')
            return
        self.last_write_time = Util.utcnow()
        commands = ','.join(self.bundles.keys())
        logger.info(f'Writing bundle with commands {commands}')
        for command, v in self.bundles.items():
            if not v:
                continue
            req_time = datetime.datetime.fromisoformat(v[0]['request_time'])
            dumpdict = {'v': self.VERSION, 'command': command, 'requests': v}
            if not self.s3client:
                datestr = req_time.strftime('%Y%m%d%H%M%Sz')
                filename = f'ttscrape-{command}-{datestr}.json'
                with open(self.rawdatadir / filename, 'w') as ofh:
                    json.dump(dumpdict, ofh)
            else:
                logging.debug(f'Writing {command} to s3')
                response = self.s3client.write_api_response(req_time, command, json.dumps(dumpdict))
                logging.debug(f'S3 response: {response}')
        self.bundles = {}

    def record(self, command: str, request_args: dict, request_time: datetime.datetime,
               response_time: datetime.datetime, response_dict: dict):
        bl = self.bundles.setdefault(command, [])
        latency = response_time - request_time
        bl.append({'request_args': request_args,
                   'request_time': request_time.isoformat(),
                   'latency_ms': latency.total_seconds() * 1000, 'response': response_dict})
        if self.callback:
            self.callback(command, self.bundles)

    def status(self):
        d = {'last_write_time': self.last_write_time}
        for k, v in self.bundles.items():
            d[k] = len(v)
        return d


class Requestor:
    """
    Low-level scraper that gets, sends, and logs requests; also handles logging.
    Handling periodic scraping is done elsewhere.
    """
    ERROR_REST = datetime.timedelta(minutes=30)
    LOG_PAYLOAD_LIMIT = 200

    """
    Useful things to scrape:
    gettime()
    getvehicles(rt, tmres='s') # up to 10 comma-separated routes
    getroutes()
    getdirections(rt) # 1 redundant
    getstops(rt, dir) # redundant with getpatterns
    getpatterns(rt) # 1
     or getpatterns(pid) # up to 10

    Also: getpredictions()
    """

    def __init__(self, base_url: str,
                 output_dir: Path, rawdatadir: Path, parser: ParserInterface,
                 debug=False, write_local=False, callback=None):
        self.start_time = Util.utcnow()
        self.api_key = None
        self.output_dir = output_dir
        self.rawdatadir = rawdatadir
        self.request_count = 0
        self.debug = debug
        self.shutdown = False
        self.write_local = write_local
        self.parser = parser
        self.base_url = base_url
        if self.write_local:
            self.s3client = None
        else:
            self.s3client = S3Client()
        self.bundler = Bundler(self.write_local, self.s3client, rawdatadir=self.rawdatadir,
                               callback=callback)
        self.logfile = None
        self.initialize_logging()

    def cancel(self):
        self.shutdown = True

    def readlog(self, tail=False):
        if self.logfile is None:
            return 'no log file'
        if not self.logfile.exists():
            return f'{self.logfile} does not exist'
        with open(self.logfile, 'r') as lfh:
            if tail:
                lfh.seek(-1000, os.SEEK_END)
                content = lfh.read()
                nl = content.find('\n')
                return content[nl+1:]
            else:
                rv = []
                count = 10
                for line in lfh:
                    rv.append(line)
                    count -= 1
                    if count <= 0:
                        break
                return '\n'.join(rv)

    def initialize_logging(self):
        logdir = self.output_dir / 'logs'
        logdir.mkdir(parents=True, exist_ok=True)
        datestr = self.start_time.strftime('%Y%m%d%H%M%Sz')
        logfile = logdir / f'bus-scraper-{datestr}.log'
        self.logfile = logfile
        loglink = logdir / 'latest.log'
        loglink.unlink(missing_ok=True)
        loglink.symlink_to(logfile.name)
        level = logging.INFO
        if self.debug:
            level = logging.DEBUG
        logging.basicConfig(filename=logfile,
                            filemode='a',
                            format='%(asctime)s: %(message)s',
                            datefmt='%Y%m%d %H:%M:%S',
                            level=level)
        logger.info(f'Initialize requestor. Local file mode: {self.write_local}')

    def make_request(self, command, **kwargs) -> ResponseWrapper:
        """
        Makes a request by appending command to BASE_URL. Automatically adds api key and JSON format to arg dict.
        :param command:
        :param kwargs:
        :return: JSON response dict, or int if application or server error
        """
        if self.shutdown:
            return ResponseWrapper.permanent_error()
        # eventually should probably be in another thread
        self.bundler.maybe_write()
        req_day = Util.ctanow().date()
        c = Count.get_or_none((Count.day == req_day) & (Count.command == command))
        if c is None:
            c = Count(day=req_day, command=command, requests=1)
            c.save(force_insert=True)
        else:
            c.requests = c.requests + 1
            c.save()
        params = kwargs
        params['key'] = self.api_key
        if params.get('noformat'):
            del params['noformat']
        else:
            params['format'] = 'json'
        trunc_response = '(unavailable)'
        self.request_count += 1
        request_args = kwargs.copy()
        del request_args['key']
        logging.info(f'Request {self.request_count:6d}: cmd {command} args {request_args}')
        try:
            request_time = Util.utcnow()
            response = requests.get(f'{self.base_url}/{command}', params=params, timeout=10)
            trunc_response = response.text[:Requestor.LOG_PAYLOAD_LIMIT]
            result = self.parser.parse_success(response, command)
            response_time = Util.utcnow()
            if result.ok():
                self.bundler.record(command, request_args, request_time,
                                    response_time, response.json())
                if result.get_error_dict():
                    c.partial_errors = c.partial_errors + 1
                    c.save()
            else:
                c.app_errors = c.app_errors + 1
                c.save()
            return result
        except requests.exceptions.Timeout:
            logging.warning(f'Request timed out.')
            c.errors = c.errors + 1
            c.save()
            return ResponseWrapper.transient_error()
        except requests.JSONDecodeError:
            c.errors = c.errors + 1
            c.save()
            logging.warning(f'Unable to decode JSON payload: {trunc_response}')
            return ResponseWrapper.permanent_error()
        except requests.exceptions.ConnectionError:
            c.errors = c.errors + 1
            c.save()
            logging.warning(f'Connection error')
            time.sleep(30)
            return ResponseWrapper.transient_error()
            # TODO: exponential backoff

