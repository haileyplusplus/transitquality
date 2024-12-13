#!/usr/bin/env python3

import argparse
import os
import datetime
import logging
from pathlib import Path
import json
import time
import pytz
import sys

import requests
#import pandas as pd


logger = logging.getLogger(__file__)

CTA_TIMEZONE = pytz.timezone('America/Chicago')


class Util:
    @staticmethod
    def utcnow():
        #return datetime.datetime.now (tz=datetime.UTC)
        return datetime.datetime.utcnow()


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
        #return isinstance(self.json_dict, dict)
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
        #'getvehicles': ('vehicle', list[dict[str, str | int | bool]]),
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
        self.initialize_logging()

    def initialize_logging(self):
        logdir = self.output_dir / 'logs'
        logdir.mkdir(parents=True, exist_ok=True)
        datestr = self.start_time.strftime('%Y%m%d%H%M%Sz')
        logfile = logdir / f'train-scraper-{datestr}.log'
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
            return ResponseWrapper(json_dict=bustime_response[expected], error_list=app_error)
        if 'error' in bustime_response:
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
        #if num_code == 0:
        logging.error(f'Unexpected response schema: {trunc_response}')
        return ResponseWrapper.permanent_error()
        #logging.warning(f'Application error {app_error_code}: {trunc_response}')
        #return ResponseWrapper(error_code=num_code)

    def make_request(self, command, **kwargs) -> ResponseWrapper:
        """
        Makes a request by appending command to BASE_URL. Automatically adds api key and JSON format to arg dict.
        :param command:
        :param kwargs:
        :return: JSON response dict, or int if application or server error
        """
        diff = Util.utcnow() - self.last_request
        if diff < datetime.timedelta(seconds=5):
            wait = datetime.timedelta(seconds=5) - diff
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


class Pattern:
    def __init__(self, route_id: str, pattern_id: int, requestor: Requestor):
        self.route_id = route_id
        self.pattern_id = pattern_id
        self.requestor = requestor
        self.direction = None
        self.len_ft = None
        self.stops_and_waypoints = None
        self.timestamp = None

    def get_origin(self):
        #sw = self.stops_and_waypoints[self.stops_and_waypoints.typ == 'S'].sort_values('seq')
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
        if self.pattern_id != pattern_dict['pid']:
            logger.warning(f'Error parsing pattern: pid mismatch. got {pattern_dict["pid"]}, expected {self.pattern_id}')
            return False
        self.direction = pattern_dict['rtdir']
        self.len_ft = pattern_dict['ln']
        pattern_list: list[dict] = pattern_dict['pt']
        #self.stops_and_waypoints = pd.DataFrame(pattern_list)
        pattern_list.sort(key=lambda d: d['seq'])
        self.stops_and_waypoints = pattern_list
        ts = pattern_dict.get('timestamp')
        if ts:
            self.timestamp = datetime.datetime.fromisoformat(ts)
        else:
            self.timestamp = Util.utcnow()
        return True

    def to_dict(self):
        return {
            'route_id': self.route_id,
            'pid': self.pattern_id,
            'ln': self.len_ft,
            'rtdir': self.direction,
            'timestamp': self.timestamp.isoformat(),
            'pt': self.stops_and_waypoints,
        }

    def serialize(self):
        filename = self.pattern_filename()
        if filename.exists():
            return
        with open(filename, 'w') as ofh:
            json.dump(self.to_dict(), ofh)


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
    def __init__(self, route: dict, requestor: Requestor):
        self.route_id: str = route['rt']
        self.route_name: str = route['rtnm']
        self.color = route.get('rtclr')
        self.requestor: Requestor = requestor
        self.directions = []
        self.patterns = {}
        self.origins = []
        self.last_origin_predictions = None
        self.last_scrape_attempt = None
        self.last_scrape_successful = None
        self.last_successful_scrape = None

    def __lt__(self, other):
        return self.route_id < other.route_id

    def __eq__(self, other):
        return self.route_id == other.route_id

    def calc_next_scrape(self, interval: datetime.timedelta):
        if self.last_scrape_attempt is None:
            return Util.utcnow()
        if not self.last_scrape_successful:
            return self.last_scrape_attempt + datetime.timedelta(minutes=15)
        return self.last_scrape_attempt + interval

    def mark_attempt(self, scrapetime):
        self.last_scrape_attempt = scrapetime

    def get_pattern(self, pid: int):
        p = self.patterns.get(pid)
        if p:
            return p
        p = Pattern(self.route_id, pid, self.requestor)
        if not p.initialize():
            logging.error(f'Could not initialize pattern {pid} on route {self.route_id}')
            return None
        self.patterns[pid] = p
        return p

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

    def refresh(self):
        """
        {
	"bustime-response": {
		"directions": [
			{
				"dir": "Eastbound"
			},
			{
				"dir": "Westbound"
			}
		]
	}
}


{"bustime-response": {"error": [ {
    "msg": "Invalid API access key supplied"
} ] }}

"vehicle": [...

		"error": [
			{
				"rt": "137",
				"msg": "No data found for parameter"
			}
		]

Transaction limit for current
day has been exceeded.


{
	"bustime-response": {
		"error": [
			{
				"stpid": "6601",
				"msg": "No service scheduled"
			}
		]
	}
}

        :return:
        """
        patternsresp = self.requestor.make_request('patterns', rt=self.route_id)
        self.patterns = patternsresp.payload()

        # directionsresp = self.requestor.make_request('getdirections', rt=self.route_id)
        # if not directionsresp.ok():
        #     return False
        # directions = directionsresp.payload().get('directions', [])
        # if not directions:
        #     return False
        # for dirdict in directions:
        #     dir_ = dirdict.get('dir')
        #     if dir_:
        #         stopsresp = self.requestor.make_request('getstops', rt=self.route_id, dir=dir_)
        #         if not stopsresp.ok():
        #             break
        #         if 'stops' not in stopsresp:
        #             break
        return True


class Routes:
    def __init__(self, requestor):
        self.requestor = requestor
        self.routes = None

    def initialize(self):
        routesresp = self.requestor.make_request('getroutes')
        if not routesresp.ok():
            print(f'Routes failed to initialize')
            print(routesresp)
            sys.exit(1)
            #return False
        self.routes = {}
        for route in routesresp.payload():
            # TODO: safe get
            route_info = RouteInfo(route, self.requestor)
            self.routes[route['rt']] = route_info
        return True

    def ok(self):
        return self.routes is not None

    def choose(self, interval):
        s = []
        for route in self.routes.values():
            s.append((route.calc_next_scrape(interval), route))
        s.sort()
        first, _ = s[0]
        diff = first - Util.utcnow()
        if diff > datetime.timedelta():
            time.sleep(diff.total_seconds())
        routes = [x[1] for x in s[:10]]
        return routes


class BusScraper:
    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta, api_key: str, debug=False):
        self.start_time = Util.utcnow()
        self.scrape_interval = scrape_interval
        self.night = False
        self.requestor = Requestor(output_dir, api_key, debug=debug)
        self.routes = Routes(self.requestor)
        self.routes.initialize()

        self.rt_queue = []
        self.metadata_queue = []
        self.last_scraped = None
        self.next_scrape = None

    def scrape_one(self):
        routes_to_scrape = self.routes.choose(self.scrape_interval)
        routestr = ','.join([x.route_id for x in routes_to_scrape])
        scrapetime = Util.utcnow()
        for route in routes_to_scrape:
            route.mark_attempt(scrapetime)
        res = self.requestor.make_request('getvehicles', rt=routestr)
        if not res.ok():
            time.sleep(1)
            return False
        for route in routes_to_scrape:
            route.parse_vehicle_update(res.payload())
        return True

    def loop(self):
        while True:
            self.scrape_one()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Train tracker API key.')
    args = parser.parse_args()
    if not args.api_key:
        print(f'API key required')
    outdir = Path(args.output_dir[0]).expanduser()
    outdir.mkdir(parents=True, exist_ok=True)
    datadir = outdir / 'raw_data'
    datadir.mkdir(parents=True, exist_ok=True)
    statedir = outdir / 'state'
    statedir.mkdir(parents=True, exist_ok=True)
    ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key=args.api_key[0], debug=args.debug)
    logging.info(f'Initializing scraping to {outdir} every {ts.scrape_interval.total_seconds()} seconds.')
    ts.loop()
