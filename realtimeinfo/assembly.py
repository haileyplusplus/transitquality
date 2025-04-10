import asyncio
import datetime
import json
import logging

import grequests

from interfaces import ureg
from interfaces.estimates import BusResponse, TrainEstimate, TrainResponse, TransitEstimate, StopEstimates, \
    StopEstimate, CombinedResponseType, TransitOutput, BusEstimate, Mode, PositionInfo
from realtimeinfo.queries import QueryManager, TrainQuery


logger = logging.getLogger(__file__)


class NearStopQuery:
    def __init__(self, qm: QueryManager, sa, lat: float, lon: float, do_conversion):
        self.qm = qm
        self.sa = sa
        self.lat = lat
        self.lon = lon
        self.do_conversion = do_conversion

    @staticmethod
    def td_round(x: datetime.timedelta):
        x = datetime.timedelta(seconds=round(x.total_seconds()))
        return x

    def route_coalesce(self, dirname, v):
        routes = {}
        for item in v:
            route = item.route
            routes.setdefault(route, []).append(item)
        results = []
        ureg.formatter.default_format = '.2fP'
        for k, v in sorted(routes.items()):
            routev = []
            for d in v:
                logger.debug(f'coalesce {dirname} {k}  vp {d.vehicle_position}  sp {d.stop_position}  le {d.low_estimate}')
                routev.append(d)
                d.age = self.td_round(d.age)
                d.distance_from_vehicle = d.distance_from_vehicle.to(ureg.miles)
                # logger.debug(json.dumps(d, indent=4))
                if d.low_estimate is None:
                    miles = d.distance_from_vehicle.to(ureg.miles)
                    if miles <= ureg.miles * 1:
                        d.display = True
                        if d.distance_from_vehicle <= (200 * ureg.meter):
                            d.displayed_estimate = 'Due'
                    else:
                        d.display = False
                    # consider better values
                    d.low_estimate = datetime.timedelta(minutes=1)
                    d.high_estimate = datetime.timedelta(minutes=5)
                    # routev.append(d)
                    continue
                # predicted = d.predicted_minutes
                # age = d.age
                d.low_estimate -= d.age
                d.high_estimate -= d.age
                if d.waiting_to_depart and d.predicted_minutes:
                    # el += predicted * 60
                    # eh += predicted * 60
                    d.low_estimate += d.predicted_minutes
                    d.high_estimate += d.predicted_minutes
                # el = round((d.low_estimate - age) / 60)
                # eh = round((d.high_estimate - age) / 60)
                # d.low_estimate = el
                # d.high_estimate = eh
                if d.walk_time is None:
                    logger.debug(f'  Missing walk time in {d.pattern}')
                    d.display = False
                    continue

                if d.walk_time > datetime.timedelta(0) and datetime.timedelta(0) <= d.high_estimate <= d.walk_time:
                    item = d
                    logger.debug(
                        f'  filtering out item due to walk time: item {item.pattern} vid ? vp {item.vehicle_position}  sp {item.stop_position}  le {item.low_estimate}  he {item.high_estimate}')
                    d.display = False
                    continue
                # age_minutes = round(d['age'] / 60)
                # d['age'] = round(d['age'])
                # d['old_estimate'] = f'{el}-{eh} min'
                # d['estimate'] = f'{el}-{eh} min'
                elm = round(d.low_estimate.total_seconds() / 60)
                ehm = round(d.high_estimate.total_seconds() / 60)
                d.displayed_estimate = f'{elm}-{ehm} min'
                # routev.append(d)
            routev.sort(key=lambda x: x.low_estimate)
            for item in routev:
                if isinstance(item, TrainEstimate):
                    vehicle = item.run
                else:
                    vehicle = item.vehicle
                logger.debug(
                    f'  item {item.pattern} vid {vehicle} vp {item.vehicle_position}  sp {item.stop_position}  le {item.low_estimate}  he {item.high_estimate}')
            displayed = 0
            for r in routev:
                if r.display:
                    displayed += 1
                if displayed > 2:
                    r.display = False
            results += routev
        if self.do_conversion:
            return [self.convert_output(x) for x in results]
        return results

    async def fetch_routing(self, results: list[TransitEstimate]):
        routing_queries = set([])
        urls = []
        reqs = []
        routing_responses = {}
        stop_to_pattern = {}
        for item in results:
            stop_to_pattern[item.stop_id] = item.pattern
            if item.stop_id in routing_queries:
                continue
            routing_queries.add(item.stop_id)
            routing_json = {"locations": [
                {"lat": self.lat,
                 "lon": self.lon,
                 "street": "Street1"},
                {"lat": item.stop_lat,
                 "lon": item.stop_lon,
                 "street": "Street2"}],
                "costing": "pedestrian",
                "units": "miles",
                "id": str(item.stop_id)}
            jp = json.dumps(routing_json)
            urls.append(f'{self.qm.config.get_server("valhalla")}/route?json={jp}')
        for u in urls:
            logger.debug(f'Requesting routing {u}')
            reqs.append(grequests.get(u))

        def handler(request, exception):
            logger.warning(f'Issue with {request}: {exception}')

        responses = grequests.map(reqs, exception_handler=handler)
        #logger.debug('index', index.keys())
        logger.debug(f'Sent {len(reqs)} requests and got {len(responses)} responses')
        for resp in responses:
            if resp is None:
                logger.warning(f'  Got null response')
                continue
            if resp.status_code not in {200, 201}:
                logger.warning(f'   Response status code to {resp.request.url}: {resp.staus_code}')
                continue

            jd = resp.json()
            summary = jd['trip']['summary']
            # logger.debug(jd)
            seconds = summary['time']
            miles = summary['length']
            stop_id = int(jd['id'])
            routing_queries.discard(stop_id)
            routing_responses[stop_id] = (datetime.timedelta(seconds=int(seconds)), miles * ureg.miles)
        return routing_responses

    async def estimate_vehicle_locations(self, results: list[TransitEstimate]) -> CombinedResponseType:
        directions = {'Northbound': [], 'Southbound': [], 'Eastbound': [], 'Westbound': []}
        # estimate_params: list[dict] = []
        index = {}
        ests = {}

        for item in results:
            estimate_key = (item.pattern, item.stop_position)
            if isinstance(item, BusEstimate):
                vehicle = item.vehicle
            else:
                vehicle = item.run
            ests.setdefault(estimate_key,
                            StopEstimate(
                                pattern_id=item.pattern,
                                stop_position=item.stop_position,
                                vehicle_positions=[],
                            )
                            ).vehicle_positions.append(
                PositionInfo(
                    vehicle_position=item.vehicle_position,
                    vehicle_id=vehicle,
                )
            )
            pattern_id = item.pattern
            vehicle_distance = round(item.vehicle_position.m)
            index.setdefault(pattern_id, {})[vehicle_distance] = item
        reqs = []
        estimates_query = StopEstimates(estimates=sorted(ests.values()))
        reqs.append(grequests.post('http://localhost:8500/estimates/', data=estimates_query.model_dump_json()))
        logger.debug(f'Requesting estimate http://localhost:8500/estimates/ post {estimates_query.model_dump_json(indent=4)}')
        #return qm.get_estimates(stop_estimates.estimates)

        #estimate_response: EstimateResponse = EstimateResponse.model_validate_json(resp.text)

        gathered = await asyncio.gather(
            self.qm.get_estimates(estimates_query),
            self.fetch_routing(results)
        )
        estimate_response, routing_responses = gathered

        for p in estimate_response.patterns:
            pattern_id = p.pattern_id
            for se in p.single_estimates:
                # vehicle_position = se.vehicle_position
                # TODO: fix this
                vehicle_position = round(se.vehicle_position.m)
                if vehicle_position in index[pattern_id]:
                    index[pattern_id][vehicle_position].low_estimate = se.low_estimate
                    index[pattern_id][vehicle_position].high_estimate = se.high_estimate
                    index[pattern_id][vehicle_position].trace_info = se.info
                else:
                    logger.warning(f'Warning: pattern {pattern_id} vehicle position missing {vehicle_position}')

        for item in results:
            rr = routing_responses.get(item.stop_id)
            if rr:
                item.walk_time, item.walk_distance = rr
            else:
                logger.warning(f'Warning: stop {item.stop_id} missing routing')
            # item.walk_time, item.walk
            directions.setdefault(item.direction, []).append(item)
        return directions

    async def nearest_buses(self) -> BusResponse:
        start = datetime.datetime.now()
        results = self.qm.nearest_stop_vehicles(self.lat, self.lon)
        end = datetime.datetime.now()
        latency = int((end - start).total_seconds())
        # return {'results': results, 'start': start.isoformat(), 'latency': latency,
        #         'lat': lat, 'lon': lon}
        return BusResponse(
            results=results,
            start=start,
            latency=latency,
            lat=self.lat,
            lon=self.lon
        )

    async def nearest_trains(self) -> TrainResponse:
        if self.sa is None:
            return TrainResponse(results=[])
        tq = TrainQuery(self.qm.engine, self.sa)
        return TrainResponse(results=tq.get_relevant_stops(self.lat, self.lon))

    async def run_query(self) -> CombinedResponseType:
        #backend = 'http://localhost:8500/nearest-estimates'
        #resp = requests.get(backend, params=request.args)
        #if resp.status_code != 200:
        #    return f'Error handling request'
        results: list[TransitEstimate] = []
        logger.debug(f'Running query')
        resp = await asyncio.gather(self.nearest_buses(), self.nearest_trains())
        logger.debug(f'Gathered')
        bus_response, train_response = resp
        logger.debug(f'resp done')
        #bus_response: BusResponse = BusResponse.model_validate_json(resp.text)
        results += bus_response.results
        #train_resp = requests.get('http://localhost:8500/nearest-trains', params=request.args)
        #if resp.status_code == 200:
        #    train_response: TrainResponse = TrainResponse.model_validate_json(train_resp.text)
        #    results += train_resp.json()['results']
        results += train_response.results
        directions = await self.estimate_vehicle_locations(results)
        directions2 = {}
        for k, v in directions.items():
            # directions.setdefault(item.direction, []).append(item)
            directions2[k] = self.route_coalesce(k, v)
        #raw = json.dumps(jsonable_encoder(directions2), indent=4)
        # raw = directions2.model_dump_json
        return directions2

    def convert_output(self, e: TransitEstimate) -> TransitOutput:
        if isinstance(e, BusEstimate):
            mode = Mode.BUS
            if e.vehicle is None:
                vehicle = 0
            else:
                vehicle = e.vehicle
        else:
            mode = Mode.TRAIN
            vehicle = e.run
        miles = lambda x: f"{x.to('mi').m:0.2f} mi" if x is not None else None
        minutes = lambda x: round(x.total_seconds() / 60) if x is not None else None
        if e.waiting_to_depart:
            adj = e.predicted_minutes - e.age
        else:
            adj = -1 * e.age
        return TransitOutput(
            pattern=e.pattern,
            vehicle=vehicle,
            route=e.route,
            mode=mode,
            direction=e.direction,
            stop_id=e.stop_id,
            stop_name=e.stop_name,
            stop_lat=e.stop_lat,
            stop_lon=e.stop_lon,
            stop_position=miles(e.stop_position),
            vehicle_position=miles(e.vehicle_position),
            distance_from_vehicle=miles(e.distance_from_vehicle),
            distance_to_stop=miles(e.distance_to_stop),
            last_update=e.last_update.isoformat(),
            age_seconds=round(e.age.total_seconds()),
            destination_stop_id=e.destination_stop_id,
            destination_stop_name=e.destination_stop_name,
            waiting_to_depart=e.waiting_to_depart,
            predicted_minutes=minutes(e.predicted_minutes),
            low_estimate_minutes=minutes(e.low_estimate),
            high_estimate_minutes=minutes(e.high_estimate),
            walk_time_minutes=minutes(e.walk_time),
            total_low_minutes=minutes(e.low_estimate + adj),
            total_high_minutes=minutes(e.high_estimate + adj),
            walk_distance=miles(e.walk_distance),
            display=e.display
        )
