#!/usr/bin/env python3

import argparse
import os
import datetime
import logging
from pathlib import Path
from enum import Enum
import json
import time
import pytz
import sys
import signal
from typing import Iterable, Tuple
from abc import ABC, abstractmethod

import requests

from scrapemodels import Route, Pattern, Count, ErrorMessage

from util import Util

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

    def __init__(self, json_dict=None, error_code=None, error_list=None):
        self.json_dict = json_dict
        self.error_code = error_code
        self.error_list = error_list

    def __str__(self):
        jds = str(self.json_dict)[:300]
        return f'ResponseWrapper: dict {jds} code {self.error_code} list {self.error_list}'

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

    def get_error_code(self):
        return self.error_code

    def payload(self):
        return self.json_dict


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

    def __init__(self, output_dir: Path, api_key: str, debug=False):
        self.start_time = Util.utcnow()
        self.api_key = api_key
        self.output_dir = output_dir
        self.request_count = 0
        self.last_request = Util.utcnow()
        self.debug = debug
        self.shutdown = False
        self.initialize_logging()

    def cancel(self):
        self.shutdown = True

    def initialize_logging(self):
        logdir = self.output_dir / 'logs'
        logdir.mkdir(parents=True, exist_ok=True)
        datestr = self.start_time.strftime('%Y%m%d%H%M%Sz')
        logfile = logdir / f'bus-scraper-{datestr}.log'
        loglink = logdir / 'latest.log'
        loglink.unlink(missing_ok=True)
        loglink.symlink_to(logfile)
        level = logging.INFO
        if self.debug:
            level = logging.DEBUG
        logging.basicConfig(filename=logfile,
                            filemode='a',
                            format='%(asctime)s: %(message)s',
                            datefmt='%Y%m%d %H:%M:%S',
                            level=level)

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
            errordict = Requestor.parse_error(bustime_response)
            return ResponseWrapper(json_dict=bustime_response[expected], error_list=app_error)
        if 'error' in bustime_response:
            errordict = Requestor.parse_error(bustime_response)
            app_error = bustime_response['error']
            errorstr = str(app_error)
            logging.error(f'Application error: {trunc_response}')
            if 'transaction limit' in errorstr.lower():
                return ResponseWrapper.rate_limit_error()
            if 'API' in errorstr:
                return ResponseWrapper.permanent_error()
            if 'internal server error' in errorstr.lower():
                return ResponseWrapper.permanent_error()
            return ResponseWrapper.transient_error()
        # if app_error_code is None:
        #     logging.error(f'Could not parse error code from JSON response: {trunc_response}')
        #     return ResponseWrapper.permanent_error()
        # if not app_error_code.isdigit():
        #     logging.error(f'Unreadable application error code (non-numeric) {app_error_code}: {trunc_response}')
        #     return ResponseWrapper.permanent_error()
        # num_code = int(app_error_code)
        # if num_code == 0:
        logging.error(f'Unexpected response schema: {trunc_response}')
        return ResponseWrapper.permanent_error()
        # logging.warning(f'Application error {app_error_code}: {trunc_response}')
        # return ResponseWrapper(error_code=num_code)

    @staticmethod
    def parse_error(bustime_response: dict):
        error_list = bustime_response.get('error', [])
        rv = {'rt': [], 'stpid': []}
        if not isinstance(error_list, list):
            return None
        errortime = Util.utcnow()
        for e in error_list:
            msg = e.get('msg')
            model = ErrorMessage.get_or_none(ErrorMessage.text == msg)
            if model is None:
                model = ErrorMessage(text=msg, last_seen=errortime)
            model.count = model.count + 1
            model.save(force_insert=True)
            rt = e.get('rt')
            stpid = e.get('stpid')
            if rt:
                rv['rt'].append(rt)
            if stpid:
                rv['stpid'].append(stpid)
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
        diff = Util.utcnow() - self.last_request
        if diff < datetime.timedelta(seconds=4):
            wait = datetime.timedelta(seconds=4) - diff
            logging.debug(f'Last scrape {self.last_request} waiting {wait}')
            time.sleep(wait.total_seconds())
        self.last_request = Util.utcnow()
        params = kwargs
        params['key'] = self.api_key
        params['format'] = 'json'
        trunc_response = '(unavailable)'
        self.request_count += 1
        ac = kwargs.copy()
        del ac['key']
        logging.info(f'Request {self.request_count:6d}: cmd {command} args {ac}')
        try:
            req_time = Util.utcnow()
            response = requests.get(f'{self.BASE_URL}/{command}', params=params, timeout=10)
            trunc_response = response.text[:Requestor.LOG_PAYLOAD_LIMIT]
            result = self.parse_success(response, command)
            if result.ok():
                self.log(req_time, command, response.text)
            return result
        except requests.exceptions.Timeout:
            logging.warning(f'Request timed out.')
            return ResponseWrapper.transient_error()
        except requests.JSONDecodeError:
            logging.warning(f'Unable to decode JSON payload: {trunc_response}')
            return ResponseWrapper.permanent_error()

    def log(self, req_time, command, text_response):
        datestr = req_time.strftime('%Y%m%d%H%M%Sz')
        filename = self.output_dir / 'raw_data' / f'ttscrape-{command}-{datestr}.json'
        with open(filename, 'w') as ofh:
            ofh.write(text_response)


class Pattern2:
    def __init__(self, route_id: str, pattern_id: int, requestor: Requestor):
        self.route_id = route_id
        self.pattern_id = pattern_id
        self.requestor = requestor
        self.direction = None
        self.len_ft = None
        self.stops_and_waypoints = None
        self.timestamp = None
        self.approx_next_start = None
        # self.last_seen = None

    def get_origin(self):
        # sw = self.stops_and_waypoints[self.stops_and_waypoints.typ == 'S'].sort_values('seq')
        for s in self.stops_and_waypoints:
            if s['typ'] == 'S':
                return s['stpid']
        # if len(sw) > 1:
        #     return sw.iloc[0].stpid
        return None

    def initialize(self):
        filename = self.pattern_filename()
        found = False
        if filename.exists():
            with open(filename) as fh:
                try:
                    d = json.load(fh)
                    if not self.from_dict(d):
                        logging.warning(f'Unable to parse pattern from file {filename}')
                        found = False
                    else:
                        found = True
                except json.JSONDecodeError:
                    logging.warning(f'Unable to load pattern from file {filename}')
                    found = False
        if found:
            return True
        patternsresp = self.requestor.make_request('getpatterns', pid=self.pattern_id)
        logging.debug(patternsresp)
        if not patternsresp.ok():
            return False
        # TODO: safety
        result = self.from_dict(patternsresp.payload()[0])
        if result:
            self.serialize()
        return result

    def pattern_filename(self):
        statedir = self.requestor.output_dir / 'state'
        return statedir / f'pattern-{self.route_id}-{self.pattern_id}.json'

    def from_dict(self, pattern_dict: dict):
        if {'pid', 'ln', 'rtdir', 'pt'} - set(pattern_dict.keys()):
            logger.warning(f'Error parsing pattern: keys missing {pattern_dict.keys()}')
            return False
        if int(self.pattern_id) != int(pattern_dict['pid']):
            logger.warning(
                f'Error parsing pattern: pid mismatch. got {pattern_dict["pid"]}, expected {self.pattern_id}')
            return False
        self.direction = pattern_dict['rtdir']
        self.len_ft = pattern_dict['ln']
        pattern_list: list[dict] = pattern_dict['pt']
        # self.stops_and_waypoints = pd.DataFrame(pattern_list)
        pattern_list.sort(key=lambda d: d['seq'])
        self.stops_and_waypoints = pattern_list
        ts = pattern_dict.get('timestamp')
        if ts:
            self.timestamp = datetime.datetime.fromisoformat(ts)
        else:
            self.timestamp = Util.utcnow()
        return True


class RouteInfo:
    """
    Useful things to scrape:
    gettime()
    getvehicles(rt, tmres='s') # up to 10 comma-separated routes
    getroutes()
    getdirections(rt) # 1
    getstops(rt, dir)
    getpatterns(rt) # 1
     or getpatterns(pid) # up to 10

    Also: getpredictions()
    """

    def __init__(self, route: dict, requestor: Requestor, deserialize=False):
        self.directions = []
        self.patterns = {}
        self.origins = []
        self.patterns_last_seen = {}
        self.stops_to_patterns = {}
        self.requestor: Requestor = requestor
        if deserialize:
            self.parse_scraping_history(route)
            return
        self.route_id: str = route['rt']
        self.route_name: str = route['rtnm']
        self.color = route.get('rtclr')
        self.last_origin_predictions = None
        self.last_scrape_attempt = None
        self.last_scrape_successful = None
        self.last_successful_scrape = None

    def __lt__(self, other):
        return self.route_id < other.route_id

    def __eq__(self, other):
        return self.route_id == other.route_id

    def get_pattern_id_from_stop(self, stop_id: str):
        pid = self.stops_to_patterns.get(stop_id)
        if pid is not None:
            return pid
        for v in self.patterns:
            pid = v.pattern_id
            s = v.get_origin()
            if s is not None:
                self.stops_to_patterns[pid] = s
                if s == stop_id:
                    return pid
        return None

    def calc_next_scrape(self, interval: datetime.timedelta):
        if self.last_scrape_attempt is None:
            return Util.utcnow() - datetime.timedelta(minutes=1)
        if not self.last_scrape_successful:
            return self.last_scrape_attempt + datetime.timedelta(minutes=15)
        return self.last_scrape_attempt + interval

    def mark_attempt(self, scrapetime):
        self.last_scrape_attempt = scrapetime

    def get_pattern(self, pid: int):
        if pid is None:
            return None
        p = self.patterns.get(pid)
        if p:
            self.patterns_last_seen[pid] = Util.utcnow()
            return p
        p = Pattern(self.route_id, pid, self.requestor)
        if not p.initialize():
            logging.error(f'Could not initialize pattern {pid} on route {self.route_id}')
            return None
        self.patterns_last_seen[pid] = Util.utcnow()
        return p

    def prediction_scrapes(self):
        rv = []
        for pid, v in self.patterns_last_seen.items():
            pattern = self.get_pattern(pid)
            if not pattern:
                continue
            stop_id = pattern.get_origin()
            diff = Util.utcnow() - v
            if pattern.approx_next_start:
                rv.append((pattern.approx_next_start, self.route_id, pid, stop_id))
            elif diff < datetime.timedelta(minutes=5):
                rv.append((Util.utcnow(), self.route_id, pid, stop_id))
        return rv

    def mark_prediction_attempt(self, pid):
        pattern = self.get_pattern(pid)
        if not pattern:
            logger.warning(f'Unable to find pattern {pid}, {type(pid)}')
            return False
        pattern.approx_next_start = Util.utcnow() + datetime.timedelta(minutes=30)

    def get_origin_predictions(self):
        stops = []
        for p in self.patterns.values():
            origin = p.get_origin()
            if origin:
                stops.append(origin)
        if not stops:
            return
        stopids = ','.join(stops[:10])
        self.last_origin_predictions = Util.utcnow()
        res = self.requestor.make_request('getpredictions', stpid=stopids)
        if not res.ok():
            logging.warning(f'Error getting origin predictions for route {self.route_id}')

    def parse_vehicle_update(self, vehicles):
        for d in vehicles:
            if d['rt'] != self.route_id:
                continue
            self.last_scrape_successful = True
            self.last_successful_scrape = Util.utcnow()
            p = self.get_pattern(d['pid'])

    def parse_prediction_update(self, predictions, stop_to_pid):
        for prd in predictions:
            if prd['rt'] != self.route_id:
                continue
            if prd['typ'] != 'D':
                continue
            prediction = prd['prdctdn']
            predno = None
            if prediction == 'DUE':
                predno = 0
            elif prediction.isdigit():
                predno = int(prediction)
            # pid = self.get_pattern_id_from_stop(prd['stpid'])
            pid = stop_to_pid.get(prd['stpid'].strip())
            if pid is None:
                logging.warning(f'Unable to find pattern {pid} for stop {prd["stpid"]}')
                continue
            pattern = self.get_pattern(pid)
            if pattern is None:
                logging.warning(f'Unable to find pattern {pid} for stop {prd["stpid"]}')
                continue
            if predno is None:
                pattern.approx_next_start = Util.utcnow() + datetime.timedelta(minutes=30)
                continue
            pattern.approx_next_start = Util.utcnow() + datetime.timedelta(minutes=predno)


class ScrapeState(Enum):
    ACTIVE = 0
    PENDING = 1
    PAUSED = 2
    INACTIVE = 3
    NEEDS_SCRAPING = 4


class ScrapeTask(ABC):
    def __init__(self):
        self.model_dict = {}

    @abstractmethod
    def handle_response(self, response: list):
        pass

    @abstractmethod
    def handle_errors(self, error_dict: dict):
        pass

    @abstractmethod
    def scrape(self) -> Tuple[str, dict]:
        pass


class PatternTask(ScrapeTask):
    def __init__(self, model):
        super().__init__()
        self.model = model

    def scrape(self) -> Tuple[str, dict]:
        return 'getpatterns', {'pid': self.model.pattern_id}

    def handle_response(self, response: list):
        for pattern in response:
            pattern_id = pattern.get('pid')
            if pattern_id != self.model.pattern_id:
                logger.warning(f'Received unexpected pattern id in response: {pattern_id}')
                continue
            self.model.length = pattern.get('ln')
            self.model.direction = pattern.get('rtdir')
            min_seq = None
            stop_id = None
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
            self.model.first_stop = stop_id
            self.model.save()

    def handle_errors(self, error_dict: dict):
        pass


class VehicleTask(ScrapeTask):
    def __init__(self, models):
        super().__init__()
        route_ids = []
        for m in models:
            key = m.route_id
            route_ids.append(key)
            self.model_dict[key] = m
        self.route_ids = ','.join(sorted(route_ids))

    def scrape(self) -> Tuple[str, dict]:
        return 'getvehicles', {'rt': self.route_ids, 'tmres': 's'}

    def handle_response(self, response: list):
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
        for p in pattern_ids:
            rt, pid = p
            if not Pattern.select().where(Pattern.pattern_id == pid).exists():
                rtm = self.model_dict.get(rt)
                if rtm is None:
                    continue
                m = Pattern(pattern_id=pid,
                            route=rtm,
                            scrape_state=ScrapeState.NEEDS_SCRAPING)
                m.insert()

    def handle_errors(self, error_dict: dict):
        for r in error_dict.get('rt', []):
            m = self.model_dict.get(r)
            if m is None:
                continue
            m.scrape_state = ScrapeState.PAUSED
            m.save()


class PredictionTask(ScrapeTask):
    def __init__(self, models: Iterable[Pattern]):
        super().__init__()
        stops = []
        for m in models:
            key = m.first_stop
            stops.append(key)
            self.model_dict[key] = m
        self.stopids = ','.join(sorted(stops))

    def scrape(self) -> Tuple[str, dict]:
        return 'getpredictions', {'stpid': self.stopids}

    def handle_errors(self, error_dict: dict):
        for r in error_dict.get('stpid', []):
            m = self.model_dict.get(r)
            if m is None:
                continue
            m.scrape_state = ScrapeState.PAUSED
            m.save()

    def handle_response(self, prediction_list: list):
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
                m.predicted_time = t
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
                m.save()


class Routes:
    def __init__(self, requestor):
        self.requestor = requestor
        self.routes = {}

    def initialize(self):
        routes = Route.select()
        for r in routes:
            self.routes[r.route_id] = r
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

    def choose(self):
        routes = (Route.select().where(Route.scrape_state == ScrapeState.ACTIVE)
                  .order_by(Route.last_scrape_attempt).limit(10))
        for r in routes:
            r.scrape_state = ScrapeState.PENDING
            r.save()
        return VehicleTask(models=routes)

    def choose_predictions(self, interval):
        s = []
        for route in self.routes.values():
            s += route.prediction_scrapes()
        if not s:
            return []
        s.sort()
        first = s[0][0]
        diff = first - Util.utcnow()
        if diff > datetime.timedelta():
            return []
        return [x[1:] for x in s[:10]]


class RunState(Enum):
    RUNNING = 1
    SHUTDOWN_REQUESTED = 2
    SHUTDOWN = 3


class BusScraper:
    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta,
                 api_key: str, debug=False, dry_run=False, scrape_predictions=False):
        self.start_time = Util.utcnow()
        self.dry_run = dry_run
        self.scrape_interval = scrape_interval
        self.night = False
        self.requestor = Requestor(output_dir, api_key, debug=debug)
        self.routes = Routes(self.requestor)
        self.state = RunState.RUNNING
        self.count = 5
        self.scrape_predictions = scrape_predictions
        self.routes.initialize()

        self.rt_queue = []
        self.metadata_queue = []
        self.last_scraped = None
        self.next_scrape = None

    def prediction_scrape(self):
        # scrape predictions this time
        # rv.append((Util.utcnow(), self.route_id, pid, stop_id))
        pred_to_scrape = self.routes.choose_predictions(self.scrape_interval)
        if pred_to_scrape:
            stops = []
            stop_to_pid = {}
            for route_id, pid, stop in pred_to_scrape:
                stop_to_pid[stop] = pid
                logger.info(f'Scraping predition: rt {route_id}  pid {pid}  stop {stop}')
                self.routes.routes[route_id].mark_prediction_attempt(pid)
                stops.append(stop)
            stopstr = ','.join(stops)
            res = self.requestor.make_request('getpredictions', stpid=stopstr)
            if not res.ok():
                time.sleep(1)
                return False
            for route_id, pid, stop in pred_to_scrape:
                self.routes.routes[route_id].parse_prediction_update(res.payload(), stop_to_pid)
            return True

    def scrape_one(self):
        self.count -= 1
        if self.count <= 0:
            self.routes.serialize()
            self.count = 100
        if self.count % 7 == 0 and self.scrape_predictions:
            return self.prediction_scrape()
        # self.scrape_interval
        routes_to_scrape = self.routes.choose()
        routestr = ','.join([x.route_id for x in routes_to_scrape])
        scrapetime = Util.utcnow()
        for route in routes_to_scrape:
            route.mark_attempt(scrapetime)
        if self.dry_run:
            logger.info(f'Would scrape {routestr}')
            for route in routes_to_scrape:
                route.last_scrape_successful = True
                route.last_successful_scrape = Util.utcnow()
            return True
        res = self.requestor.make_request('getvehicles', rt=routestr)
        if not res.ok():
            time.sleep(1)
            return False
        for route in routes_to_scrape:
            route.parse_vehicle_update(res.payload())
        return True

    def loop(self):
        while self.state == RunState.RUNNING:
            self.scrape_one()
        self.state = RunState.SHUTDOWN
        logging.info(f'Recorded shutdown')

    def exithandler(self, *args):
        logging.info(f'Shutdown requested: {args}')
        self.routes.serialize()
        self.requestor.cancel()
        self.state = RunState.SHUTDOWN_REQUESTED


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--dry_run', action='store_true',
                        help='Simulate scraping.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--scrape_predictions', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Bus tracker API key.')
    args = parser.parse_args()
    if not args.api_key:
        print(f'API key required')
    outdir = Path(args.output_dir[0]).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)
    datadir = outdir / 'raw_data'
    datadir.mkdir(parents=True, exist_ok=True)
    statedir = outdir / 'state'
    statedir.mkdir(parents=True, exist_ok=True)
    ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key=args.api_key[0], debug=args.debug,
                    dry_run=args.dry_run, scrape_predictions=args.scrape_predictions)
    signal.signal(signal.SIGINT, ts.exithandler)
    signal.signal(signal.SIGTERM, ts.exithandler)
    logging.info(f'Initializing scraping to {outdir} every {ts.scrape_interval.total_seconds()} seconds.')
    ts.loop()
    logging.info(f'End of program')