#!/usr/bin/env python3

import argparse
import os
import datetime
import logging
from pathlib import Path
from enum import IntEnum
import json
import time
import pytz
import sys
import signal
from typing import Iterable, Tuple
from abc import ABC, abstractmethod
import asyncio
import threading

import requests

from backend.scrapemodels import Route, Pattern, Count, ErrorMessage, db_initialize, Stop
from backend.util import Util
from backend.s3client import S3Client

# import pandas as pd


logger = logging.getLogger(__file__)


class ResponseWrapper:
    """
    Simple wrapper to encapsulate response return values
    """
    TRANSIENT_ERROR = -1
    PERMANENT_ERROR = -2
    RATE_LIMIT_ERROR = -3
    PARTIAL_ERROR = -4

    def __init__(self, json_dict=None, error_code=None, error_dict=None):
        self.json_dict = json_dict
        self.error_code = error_code
        self.error_dict = error_dict

    def __str__(self):
        jds = str(self.json_dict)[:300]
        return f'ResponseWrapper: dict {jds} code {self.error_code} dict {self.error_dict}'

    @classmethod
    def transient_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.TRANSIENT_ERROR)

    @classmethod
    def permanent_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.PERMANENT_ERROR)

    @classmethod
    def rate_limit_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.RATE_LIMIT_ERROR)

    def ok(self):
        # return isinstance(self.json_dict, dict)
        return self.json_dict is not None

    def get_error_dict(self):
        return self.error_dict

    def get_error_code(self):
        return self.error_code

    def payload(self):
        return self.json_dict


class Bundler:
    VERSION = '2.0'
    BATCH_TIME = datetime.timedelta(minutes=5)

    def __init__(self, write_local=False, s3client=None, rawdatadir=None):
        self.bundles = {}
        self.write_local = write_local
        self.s3client = s3client
        self.rawdatadir = rawdatadir
        self.last_write_time = Util.utcnow()

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


class Requestor:
    """
    Low-level scraper that gets, sends, and logs requests; also handles logging.
    Handling periodic scraping is done elsewhere.
    """
    BASE_URL = 'http://www.ctabustracker.com/bustime/api/v3'
    ERROR_REST = datetime.timedelta(minutes=30)
    LOG_PAYLOAD_LIMIT = 200
    COMMAND_RESPONSE_SCHEMA = {
        'gettime': ('tm', str),
        # 'getvehicles': ('vehicle', list[dict[str, str | int | bool]]),
        # 'getvehicles': ('vehicle', list[dict]),
        # 'getroutes': ('routes', list[dict[str, str]]),
        # 'getpatterns': ('ptr', list[dict]),
        # 'getstops': ('stops', list[dict]),
        # 'getdirections': ('directions', list[dict]),
        # 'getpredictions': ('prd', list[dict]),
        'getvehicles': ('vehicle', list),
        'getroutes': ('routes', list),
        'getpatterns': ('ptr', list),
        'getstops': ('stops', list),
        'getdirections': ('directions', list),
        'getpredictions': ('prd', list),
    }

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

    def __init__(self, output_dir: Path, rawdatadir: Path, api_key: str, debug=False, write_local=False):
        self.start_time = Util.utcnow()
        self.api_key = api_key
        self.output_dir = output_dir
        self.rawdatadir = rawdatadir
        self.request_count = 0
        self.debug = debug
        self.shutdown = False
        self.write_local = write_local
        if self.write_local:
            self.s3client = None
        else:
            self.s3client = S3Client()
        self.bundler = Bundler(self.write_local, self.s3client, rawdatadir=self.rawdatadir)
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

    @staticmethod
    def parse_success(response: requests.Response, command: str) -> ResponseWrapper:
        trunc_response = response.text[:Requestor.LOG_PAYLOAD_LIMIT]
        if response.status_code != 200:
            logging.error(f'Non-successful status code: {response.status_code}. Response: {trunc_response}')
            if response.status_code == 429:
                # too many requests
                return ResponseWrapper.rate_limit_error()
            return ResponseWrapper.permanent_error()
        json_response = response.json()
        if not isinstance(json_response, dict):
            logging.error(f'Received invalid JSON response: {trunc_response}')
            return ResponseWrapper.permanent_error()
        bustime_response = json_response.get('bustime-response')
        if not isinstance(bustime_response, dict) or not bustime_response:
            logging.error(f'Unexpected response format: {trunc_response}')
        expected, _ = Requestor.COMMAND_RESPONSE_SCHEMA.get(command, (None, None))
        if expected in bustime_response:
            # partial errors are possible
            app_error = bustime_response.get('error')
            error_dict = Requestor.parse_error(bustime_response)
            return ResponseWrapper(json_dict=bustime_response[expected],
                                   error_dict=error_dict)
        if 'error' in bustime_response:
            error_dict = Requestor.parse_error(bustime_response)
            app_error = bustime_response['error']
            errorstr = str(app_error)
            logging.error(f'Application error: {trunc_response}')
            if 'transaction limit' in errorstr.lower():
                return ResponseWrapper.rate_limit_error()
            if 'API' in errorstr:
                return ResponseWrapper.permanent_error()
            if 'internal server error' in errorstr.lower():
                return ResponseWrapper.permanent_error()
            return ResponseWrapper(error_dict=error_dict)
        logging.error(f'Unexpected response schema: {trunc_response}')
        return ResponseWrapper.permanent_error()

    @staticmethod
    def parse_error(bustime_response: dict):
        if 'error' not in bustime_response:
            return None
        error_list = bustime_response.get('error', [])
        rv = {'rt': [], 'stpid': [], 'other': []}
        if not isinstance(error_list, list):
            return {'other': [str(error_list)]}
        errortime = Util.utcnow()
        for e in error_list:
            msg = e.get('msg')
            model = ErrorMessage.get_or_none(ErrorMessage.text == msg)
            if model is None:
                model = ErrorMessage(text=msg, last_seen=errortime)
                model.count = model.count + 1
                #model.insert()
                model.save(force_insert=True)
            else:
                model.count = model.count + 1
                model.save()
            rt = e.get('rt')
            stpid = e.get('stpid')
            if rt:
                rv['rt'].append(rt)
            elif stpid:
                rv['stpid'].append(stpid)
            else:
                rv['other'].append(msg)
        return rv

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
        params['format'] = 'json'
        trunc_response = '(unavailable)'
        self.request_count += 1
        request_args = kwargs.copy()
        del request_args['key']
        logging.info(f'Request {self.request_count:6d}: cmd {command} args {request_args}')
        try:
            request_time = Util.utcnow()
            response = requests.get(f'{self.BASE_URL}/{command}', params=params, timeout=10)
            trunc_response = response.text[:Requestor.LOG_PAYLOAD_LIMIT]
            result = self.parse_success(response, command)
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


class ScrapeState(IntEnum):
    ACTIVE = 0
    PENDING = 1
    ATTEMPTED = 2
    PAUSED = 3
    INACTIVE = 4
    NEEDS_SCRAPING = 5


class ScraperInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def do_shutdown(self):
        pass

    @abstractmethod
    def get_write_local(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def scrape_one(self):
        pass


class ScrapeTask(ABC):
    def __init__(self, models):
        self.model_dict = {}
        ids = []
        for m in models:
            key = self.get_key(m)
            ids.append(key)
            self.model_dict[key] = m
        self.ids = ','.join(sorted([str(x) for x in ids]))

    @abstractmethod
    def get_key(self, model):
        pass

    @abstractmethod
    def handle_response(self, response: list):
        pass

    @abstractmethod
    def handle_errors(self, error_dict: dict):
        pass

    @abstractmethod
    def get_scrape_params(self) -> Tuple[str, dict]:
        pass

    def scrape(self, requestor: Requestor):
        scrapetime = Util.utcnow()
        for m in self.model_dict.values():
            m.scrape_state = ScrapeState.ATTEMPTED
            m.last_scrape_attempt = scrapetime
            m.save()
        cmd, kwargs = self.get_scrape_params()
        res = requestor.make_request(cmd, **kwargs)
        if res.ok():
            self.handle_response(res.payload())
        if res.get_error_dict():
            self.handle_errors(res.get_error_dict())
        # TODO: handle rate limiting error


class PatternTask(ScrapeTask):
    def __init__(self, models: Iterable[Pattern]):
        super().__init__(models)

    def get_key(self, model: Pattern):
        return model.pattern_id

    def get_scrape_params(self) -> Tuple[str, dict]:
        return 'getpatterns', {'pid': self.ids}

    def handle_response(self, response: list):
        for pattern in response:
            pattern_id = pattern.get('pid')
            model: Pattern = self.model_dict.get(pattern_id)
            if model is None:
                logger.warning(f'Received unexpected pattern id in response: {pattern_id}')
                continue
            model.length = pattern.get('ln')
            model.direction = pattern.get('rtdir')
            min_seq = None
            stop_id = None
            stop_name = None
            for stop in pattern.get('pt', []):
                if not isinstance(stop, dict):
                    break
                if stop.get('typ') != 'S':
                    continue
                if 'seq' not in stop:
                    continue
                if 'stpid' not in stop:
                    continue
                seq = stop.get('seq')
                if not isinstance(seq, int):
                    continue
                if min_seq is None or seq < min_seq:
                    min_seq = seq
                    stop_id = stop.get('stpid')
                    stop_name = stop.get('stpnm')
            if stop_id is not None:
                model.first_stop = stop_id
                model.scrape_state = ScrapeState.ACTIVE
                model.save()
                stop_model = Stop.get_or_none(Stop.stop_id == stop_id)
                insert = False
                if stop_model is None:
                    stop_model = Stop(stop_id=stop_id, stop_name=stop_name)
                    insert = True
                stop_model.scrape_state = ScrapeState.ACTIVE
                stop_model.save(force_insert=insert)


    def handle_errors(self, error_dict: dict):
        pass


class VehicleTask(ScrapeTask):
    def __init__(self, models: Iterable[Route]):
        super().__init__(models)

    def get_key(self, model: Route):
        return model.route_id

    def get_scrape_params(self) -> Tuple[str, dict]:
        return 'getvehicles', {'rt': self.ids, 'tmres': 's'}

    def handle_response(self, response: list):
        logger.debug(f'Handling vehicle response: {response}')
        route_ids = set([])
        resp_time = Util.utcnow()
        pattern_ids = set([])
        for v in response:
            rt = v.get('rt')
            if not rt:
                continue
            route_ids.add(rt)
            pid = v.get('pid')
            if pid:
                pattern_ids.add((rt, pid))
        for r in route_ids:
            m = self.model_dict.get(r)
            if m is None:
                logger.warning(f'Unexpected route response in query: {r}')
                continue
            m.last_scrape_success = resp_time
            m.scrape_state = ScrapeState.ACTIVE
            m.save()
        logger.debug(f'Scraped pattern ids: {pattern_ids}')
        for p in pattern_ids:
            rt, pid = p
            pattern = Pattern.get_or_none(Pattern.pattern_id == pid)
            #if not Pattern.select().where(Pattern.pattern_id == pid).exists():
            if pattern is None:
                rtm = self.model_dict.get(rt)
                if rtm is None:
                    logger.warning(f'Mismatch with route {rt} pattern {pid}')
                    continue
                m = Pattern(pattern_id=pid,
                            route=rtm,
                            scrape_state=ScrapeState.NEEDS_SCRAPING,
                            predicted_time=resp_time)
                logger.debug(f'Inserting pattern {m}  pid {pid} route {rtm} {rt}')
                m.save(force_insert=True)
            else:
                pattern.predicted_time = resp_time
                pattern.save()
                if pattern.first_stop is not None:
                    stop_model = Stop.get_or_none(Stop.stop_id == pattern.first_stop)
                    if stop_model is None:
                        logger.warning(f'Pattern {pid} for route {rt} missing first stop in db for {pattern.first_stop}')
                        continue
                    if stop_model.scrape_state != ScrapeState.ACTIVE:
                        stop_model.scrape_state = ScrapeState.ACTIVE
                        stop_model.save()

    def handle_errors(self, error_dict: dict):
        for r in error_dict.get('rt', []):
            m = self.model_dict.get(r)
            if m is None:
                continue
            m.scrape_state = ScrapeState.PAUSED
            m.save()


class PredictionTask(ScrapeTask):
    def __init__(self, models: Iterable[Stop]):
        super().__init__(models)

    def get_key(self, model: Stop):
        return model.stop_id

    def get_scrape_params(self) -> Tuple[str, dict]:
        return 'getpredictions', {'stpid': self.ids}

    def handle_errors(self, error_dict: dict):
        for r in error_dict.get('stpid', []):
            m = self.model_dict.get(r)
            if m is None:
                continue
            m.scrape_state = ScrapeState.PAUSED
            m.save()

    def handle_response(self, prediction_list: list):
        for m in self.model_dict.values():
            m.predicted_time = None
            m.save()
        scrapetime = Util.utcnow()
        for prd in prediction_list:
            if not isinstance(prd, dict):
                continue
            if prd['typ'] != 'D':
                continue
            stpid = prd.get('stpid')
            if not stpid:
                continue
            m = self.model_dict.get(stpid)
            if m is None:
                logger.warning(f'Unexpected stop id in response: {stpid}')
                continue
            prediction = prd.get('prdctdn')
            try:
                t = datetime.datetime.strptime(prd.get('prdtm'),
                                               '%Y%m%d %H:%M').astimezone(Util.CTA_TIMEZONE)
                if isinstance(m.predicted_time, str):
                    existing_prediction = Util.read_datetime(m.predicted_time)
                    if t > existing_prediction:
                        continue
                else:
                    m.predicted_time = t
                    m.save()
            except ValueError:
                continue
            predno = None
            if prediction == 'DUE':
                predno = 0
            elif prediction.isdigit():
                predno = int(prediction)
            if predno:
                m.minutes_predicted = predno
                m.scrape_state = ScrapeState.ACTIVE
                m.last_scrape_success = scrapetime
                m.save()


class Routes:
    def __init__(self, requestor):
        self.requestor = requestor
        self.routes = {}

    def initialize(self, fetch_routes=False):
        routes = Route.select()
        for r in routes:
            self.routes[r.route_id] = r
        if len(self.routes) > 0 and not fetch_routes:
            return True
        routesresp = self.requestor.make_request('getroutes')
        if not routesresp.ok():
            print(f'Routes failed to initialize')
            print(routesresp)
            sys.exit(1)
            # return False
        for route in routesresp.payload():
            # TODO: safe get, update occasionally
            if route['rt'] in self.routes:
                continue
            r = Route(route_id=route['rt'],
                      route_name=route['rtnm'],
                      color=route.get('rtclr'))
            r.save(force_insert=True)
            self.routes[route['rt']] = r
        return True

    def ok(self):
        return self.routes is not None

    def choose(self, scrape_interval):
        routes = (Route.select().where(Route.scrape_state == ScrapeState.ACTIVE)
                  .order_by(Route.last_scrape_attempt).limit(10))
        if not routes.exists():
            return None
        #print(type(routes))
        scrapetime = Util.utcnow()
        models = [r for r in routes]
        #print(type(models[-1]))
        #print(models[-1].last_scrape_attempt)
        #print(type(models[-1].last_scrape_attempt))
        if models[-1].last_scrape_attempt is not None:
            latest_scrape = Util.read_datetime(models[-1].last_scrape_attempt)
            if latest_scrape + scrape_interval > scrapetime:
                return None
        #for r in models:
        #    r.scrape_state = ScrapeState.PENDING
        #    r.save()
        return VehicleTask(models=routes)

    def choose_predictions(self, scrape_interval):
        # select up to 5 patterns without current prediction times
        scrapetime = Util.utcnow()
        thresh = scrapetime - scrape_interval
        patterns_to_scrape = (Stop.select().
                              where(Stop.scrape_state == ScrapeState.ACTIVE).
                              where((Stop.last_scrape_attempt < thresh) | (Stop.last_scrape_attempt.is_null())).
                              where(Stop.predicted_time.is_null()).order_by(Stop.last_scrape_attempt).
                              limit(5))
        # above is not necessarily disjoint
        models = [x for x in patterns_to_scrape]
        remain = 10 - len(models)
        patterns_to_scrape = ((Stop.select().where(Stop.scrape_state == ScrapeState.ACTIVE).
                               where((Stop.last_scrape_attempt < thresh) | (Stop.last_scrape_attempt.is_null())).
                               where(Stop.predicted_time.is_null(False)).
                               order_by(Stop.predicted_time).limit(remain)))
        models += [x for x in patterns_to_scrape]
        return models


class BusScraper(ScraperInterface):
    MAX_CONSECUTIVE_PATTERNS = 3

    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta,
                 api_key: str, debug=False, dry_run=False, scrape_predictions=False,
                 fetch_routes=False):
        super().__init__()
        self.start_time = Util.utcnow()
        self.dry_run = dry_run
        self.scrape_interval = scrape_interval
        self.night = False
        self.output_dir = output_dir
        tracker_env = os.getenv('TRACKERWRITE')
        if tracker_env == 's3':
            write_local = False
        elif tracker_env == 'local':
            write_local = True
        else:
            print(f'Unexpected value for TRACKERWRITE env var: {tracker_env}')
            sys.exit(1)
        self.requestor = Requestor(output_dir, output_dir, api_key, debug=debug, write_local=write_local)
        self.routes = Routes(self.requestor)
        self.count = 5
        self.scrape_predictions = scrape_predictions
        #self.routes.initialize(fetch_routes)
        self.consecutive_patterns = 0
        self.fetch_routes = fetch_routes
        self.seen_days: set[str] = set([])

        self.rt_queue = []
        self.metadata_queue = []
        self.last_scraped = None
        self.next_scrape = None
        self.subdir = 'unknown'
        logger.info(f'Starting scraper. Local environment: {os.environ}')

    def daily_action(self, new_day: str):
        logger.info(f'New day: {new_day}')
        self.subdir = new_day
        rawdatadir = self.output_dir / 'raw' / self.subdir
        rawdatadir.mkdir(parents=True, exist_ok=True)
        self.requestor.rawdatadir = rawdatadir

    def initialize(self):
        self.routes.initialize()

    def do_shutdown(self):
        self.requestor.bundler.output()

    def get_write_local(self):
        return self.requestor.write_local

    def scrape_one(self):
        scrapetime = Util.utcnow()
        datestr = scrapetime.strftime('%Y%m%d')
        if datestr not in self.seen_days:
            self.daily_action(datestr)
            self.seen_days.add(datestr)
        # unpause routes after 30 minutes
        thresh = scrapetime - datetime.timedelta(minutes=30)
        paused = Route.select().where((Route.scrape_state == ScrapeState.PAUSED)|(Route.scrape_state==ScrapeState.ATTEMPTED)).where(Route.last_scrape_attempt < thresh).order_by(Route.last_scrape_attempt)
        if paused.exists():
            for p in paused:
                p.scrape_state = ScrapeState.ACTIVE
                p.save()
        attempt_thresh = scrapetime - datetime.timedelta(minutes=2)
        paused = Route.select().where(Route.scrape_state == ScrapeState.ATTEMPTED).where(Route.last_scrape_attempt < attempt_thresh)
        if paused.exists():
            for p in paused:
                p.scrape_state = ScrapeState.ACTIVE
                p.save()
        pattern_paused = Stop.select().where(Stop.scrape_state == ScrapeState.ATTEMPTED).where(Stop.last_scrape_attempt < attempt_thresh)
        if pattern_paused.exists():
            for p in pattern_paused:
                p.scrape_state = ScrapeState.ACTIVE
                p.save()
        if self.consecutive_patterns >= self.MAX_CONSECUTIVE_PATTERNS:
            self.consecutive_patterns = 0
        else:
            patterns_to_scrape = Pattern.select().where(Pattern.scrape_state == ScrapeState.NEEDS_SCRAPING).limit(1)
            if patterns_to_scrape.exists():
                self.consecutive_patterns += 1
                scrapetask = PatternTask(patterns_to_scrape)
                scrapetask.scrape(self.requestor)
                return
        routes_to_scrape = self.routes.choose(self.scrape_interval)
        if routes_to_scrape is not None:
            # scrape predictions
            routes_to_scrape.scrape(self.requestor)
            return
        models = self.routes.choose_predictions(self.scrape_interval)
        if not models:
            # nothing to scrape right now
            #time.sleep(1)
            return
        scrapetask = PredictionTask(models)
        scrapetask.scrape(self.requestor)

    def freshen_debug(self):
        scrapetime = Util.utcnow()
        routes: Iterable[Route] = Route.select()
        for r in routes:
            r.last_scrape_attempt = scrapetime
            r.last_scrape_success = scrapetime
            r.save()

    def set_api_key(self, api_key):
        self.requestor.api_key = api_key

    def has_api_key(self):
        if not self.requestor.api_key:
            return False
        return True


class RunState(IntEnum):
    IDLE = 0
    RUNNING = 1
    SHUTDOWN_REQUESTED = 2
    SHUTDOWN = 3
    STOPPED = 4


# move rate limiting out of bowels of make_request and into scrape_one
# that should make async run easier
class Runner:
    def __init__(self, scraper: ScraperInterface):
        self.polling_task = None
        self.scraper = scraper
        self.state = RunState.STOPPED
        self.mutex = threading.Lock()
        self.initialized = False

    def done_callback(self, task: asyncio.Task):
        logging.info(f'Task {task} done')
        self.handle_shutdown()

    def handle_shutdown(self):
        logger.info(f'Gracefully handling shutdown.')
        #self.scraper.requestor.bundler.output()
        self.scraper.do_shutdown()

    def status(self):
        with self.mutex:
            state = self.state
        running = (state == RunState.RUNNING or state == RunState.IDLE)
        #write_local = self.scraper.requestor.write_local
        write_local = self.scraper.get_write_local()
        return {'running': running, 'state': state.name, 'write_local': write_local}

    def exithandler(self, *args):
        logging.info(f'Shutdown requested: {args}')
        #asyncio.run(self.stop())
        self.stop()

    def syncstart(self):
        with self.mutex:
            self.state = RunState.IDLE

    def syncstop(self):
        with self.mutex:
            self.state = RunState.STOPPED
        self.handle_shutdown()

    async def loop(self):
        if not self.initialized:
            self.scraper.initialize()
            self.initialized = True
        last_request = Util.utcnow() - datetime.timedelta(hours=1)
        while True:
            next_scrape = last_request + datetime.timedelta(seconds=4)
            scrape_time = Util.utcnow()
            while scrape_time < next_scrape:
                scrape_time = Util.utcnow()
                wait = next_scrape - scrape_time
                logging.debug(f'Request Last scrape {last_request} next_scrape {next_scrape} waiting {wait}')
                try:
                    await asyncio.sleep(min(wait.total_seconds(), 1))
                except asyncio.CancelledError:
                    logging.info(f'Polling cancelled 1!')
                    return
            scrape_time = Util.utcnow()
            last_request = scrape_time
            with self.mutex:
                if self.state != RunState.IDLE and self.state != RunState.RUNNING:
                    logging.info(f'Polling cancelled 3 {self.state}')
                    break
                self.state = RunState.RUNNING
            self.scraper.scrape_one()
            with self.mutex:
                if self.state != RunState.IDLE and self.state != RunState.RUNNING:
                    logging.info(f'Polling cancelled 2 {self.state}')
                    break
                self.state = RunState.IDLE
            #logging.info(f'Iteration done')
        #self.state = RunState.SHUTDOWN
        logging.info(f'Recorded shutdown')

    async def start(self):
        self.polling_task = asyncio.create_task(self.loop())
        self.polling_task.add_done_callback(self.done_callback)
        logger.info(f'Polling start wait')
        await self.polling_task
        logger.info(f'Polling start done')

    async def stop(self):
        logging.info(f'Stop')
        was_running = False
        with self.mutex:
            if self.state == RunState.RUNNING:
                was_running = True
            self.state = RunState.SHUTDOWN_REQUESTED
        if not was_running:
            self.polling_task.cancel()
        # if self.polling_task:
        #     #self.requestor.cancel()
        #     self.polling_task.cancel()
        # self.state = RunState.SHUTDOWN_REQUESTED
        # self.polling_task.cancel()

    async def run_until_done(self):
        async with asyncio.TaskGroup() as task_group:
            #await self.polling_task
            self.polling_task = task_group.create_task(self.loop())
        self.handle_shutdown()
        logging.info(f'Task group done')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--dry_run', action='store_true',
                        help='Simulate scraping.')
    parser.add_argument('--fetch_routes', action='store_true',
                        help='Fetch routes. By default, if routes are present there is no fetching.')
    parser.add_argument('--freshen_debug', action='store_true',
                        help='Bump last scraped for all active routes to present.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--scrape_predictions', action='store_true',
                        help='Print debug logging.')
    # parser.add_argument('--write_local_files', action='store_true', default=False,
    #                     help='Print debug logging.')
    parser.add_argument('--output_dir', type=str, nargs=1,
                        #default=['~/transit/scraping/bustracker'],
                        default=['/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Bus tracker API key.')
    args = parser.parse_args()
    if not args.api_key:
        print(f'API key required')
    db_initialize()
    outdir = Path(args.output_dir[0])
    outdir.mkdir(parents=True, exist_ok=True)
    datadir = outdir / 'raw_data'
    datadir.mkdir(parents=True, exist_ok=True)
    statedir = outdir / 'state'
    statedir.mkdir(parents=True, exist_ok=True)
    ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key=args.api_key[0], debug=args.debug,
                    dry_run=args.dry_run, scrape_predictions=args.scrape_predictions,
                    fetch_routes=args.fetch_routes)
    ts.initialize()
    logging.info(f'Initializing scraping to {outdir} every {ts.scrape_interval.total_seconds()} seconds.')
    if args.freshen_debug:
        logging.info(f'Artifical freshen debug')
        ts.freshen_debug()
    #asyncio.run(ts.loop())
    runner = Runner(ts)
    signal.signal(signal.SIGINT, runner.exithandler)
    signal.signal(signal.SIGTERM, runner.exithandler)
    #asyncio.run(runner.start())
    #asyncio.run(runner.block_until_done())
    asyncio.run(runner.run_until_done())
    logging.info(f'End of program')
