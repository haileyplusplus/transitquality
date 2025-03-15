#!/usr/bin/env python3

import os
import datetime
import logging
from pathlib import Path
import sys
from typing import Iterable, Tuple
from abc import ABC, abstractmethod

import requests

from backend.scrapemodels import Route, Pattern, Count, ErrorMessage, db_initialize, Stop
from backend.util import Util
from backend.scraper_interface import ScraperInterface, ScrapeState, ResponseWrapper, ParserInterface
from backend.requestor import Requestor


logger = logging.getLogger(__file__)


class BusParser(ParserInterface):
    COMMAND_RESPONSE_SCHEMA = {
        'gettime': ('tm', str),
        'getvehicles': ('vehicle', list),
        'getroutes': ('routes', list),
        'getpatterns': ('ptr', list),
        'getstops': ('stops', list),
        'getdirections': ('directions', list),
        'getpredictions': ('prd', list),
    }

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
        expected, _ = BusParser.COMMAND_RESPONSE_SCHEMA.get(command, (None, None))
        if expected in bustime_response:
            # partial errors are possible
            app_error = bustime_response.get('error')
            error_dict = BusParser.parse_error(bustime_response)
            return ResponseWrapper(json_dict=bustime_response[expected],
                                   error_dict=error_dict)
        if 'error' in bustime_response:
            error_dict = BusParser.parse_error(bustime_response)
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
                model.last_seen=errortime
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
        # We don't request patterns until we know about them, so by the time we
        # get here they will always be in the database
        for pattern in response:
            pattern_id = pattern.get('pid')
            model: Pattern = self.model_dict.get(pattern_id)
            if model is None:
                logger.warning(f'Received unexpected pattern id in response: {pattern_id}')
                continue
            model.length = pattern.get('ln')
            model.direction = pattern.get('rtdir')
            model.timestamp = Util.utcnow()
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
            else:
                model.save()

    def handle_errors(self, error_dict: dict):
        pass


class VehicleTask(ScrapeTask):
    def __init__(self, models: Iterable[Route], callback):
        super().__init__(models)
        self.callback = callback

    def get_key(self, model: Route):
        return model.route_id

    def get_scrape_params(self) -> Tuple[str, dict]:
        return 'getvehicles', {'rt': self.ids, 'tmres': 's'}

    def handle_response(self, response: list):
        logger.debug(f'Handling vehicle response: {response}')
        route_ids = set([])
        resp_time = Util.utcnow()
        pattern_ids = set([])
        if self.callback:
            self.callback(response)
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
    def __init__(self, requestor, callback):
        self.requestor = requestor
        self.callback = callback
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
        scrapetime = Util.utcnow()
        models = [r for r in routes]
        if models[-1].last_scrape_attempt is not None:
            latest_scrape = Util.read_datetime(models[-1].last_scrape_attempt)
            if latest_scrape + scrape_interval > scrapetime:
                return None
        return VehicleTask(models=routes, callback=self.callback)

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
    BASE_URL = 'http://www.ctabustracker.com/bustime/api/v3'

    def __init__(self, output_dir: Path, scrape_interval: datetime.timedelta,
                 debug=False, dry_run=False, scrape_predictions=False,
                 fetch_routes=False, write_local=False, callback=None):
        super().__init__()
        self.start_time = Util.utcnow()
        self.dry_run = dry_run
        self.scrape_interval = scrape_interval
        self.night = False
        self.output_dir = output_dir
        self.requestor = Requestor(self.BASE_URL,
                                   output_dir, output_dir, BusParser(),
                                   debug=debug, write_local=write_local,
                                   callback=callback)
        self.routes = Routes(self.requestor, callback=None)
        self.count = 0
        self.scrape_predictions = scrape_predictions
        self.fetch_routes = fetch_routes
        self.seen_days: set[str] = set([])

        self.rt_queue = []
        self.metadata_queue = []
        self.last_scraped = None
        self.next_scrape = None
        self.subdir = 'unknown'
        logger.info(f'Starting scraper. Local environment: {os.environ}')

    def get_requestor(self):
        return self.requestor

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

    def get_name(self) -> str:
        return 'bus'

    def get_bundle_status(self) -> dict:
        d = self.requestor.bundler.status()
        d['last_scraped'] = self.last_scraped
        d['total_count'] = self.count
        return d

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
        self.count += 1
        if self.count % 20 == 0:
            pattern_thresh = Util.utcnow() - datetime.timedelta(days=3)
            patterns_to_scrape = (Pattern.select().
                                  where((Pattern.scrape_state == ScrapeState.NEEDS_SCRAPING) |
                                        (Pattern.timestamp < pattern_thresh)).
                                  limit(1))
            if patterns_to_scrape.exists():
                scrapetask = PatternTask(patterns_to_scrape)
                scrapetask.scrape(self.requestor)
                self.last_scraped = Util.utcnow()
                return
        routes_to_scrape = self.routes.choose(self.scrape_interval)
        if routes_to_scrape is not None:
            # scrape predictions
            routes_to_scrape.scrape(self.requestor)
            self.last_scraped = Util.utcnow()
            return
        models = self.routes.choose_predictions(self.scrape_interval)
        if not models:
            # nothing to scrape right now
            #time.sleep(1)
            return
        scrapetask = PredictionTask(models)
        scrapetask.scrape(self.requestor)
        self.last_scraped = Util.utcnow()

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

    @staticmethod
    def get_pattern_bundle():
        # oops, we already had this. keeping for posterity
        scrapetime = Util.utcnow()
        patterns: Iterable[Pattern] = Pattern.select()
        rv = []
        for p in patterns:
            rv.append({
                'pid': p.pattern_id,
                'route_id': p.route,
                'scraped': p.timestamp.isoformat(),
                'first_stop': p.first_stop,
                'direction': p.direction,
                'length': p.length,
                'last_seen': p.predicted_time,
            })
        return {'patterns': rv, 'timestamp': scrapetime}
