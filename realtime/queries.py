import datetime
import cProfile
import heapq
import statistics
from typing import Iterable

import geoalchemy2
import sqlalchemy.exc
from sqlalchemy import text, select, func
from sqlalchemy.orm import Session
from geoalchemy2.shape import to_shape

import requests
import redis

import pandas as pd
import numpy as np


from realtime.rtmodel import db_init, BusPosition, CurrentVehicleState, Stop, TrainPosition, PatternStop
from backend.util import Util
from schedules.schedule_analyzer import ScheduleAnalyzer, ShapeManager
from interfaces.estimates import TrainEstimate, BusEstimate, StopEstimate, SingleEstimate, EstimateResponse, \
    PatternResponse
from interfaces import ureg, Q_


class EstimateFinder:
    def __init__(self, redis_client, estimate_request: StopEstimate):
        self.redis = redis_client
        self.estimate_request = estimate_request
        self.debug = estimate_request.debug

    def get_redis_keys(self, pid):
        if pid >= 308500000:
            redis_keys = self.redis.keys(pattern=f'trainposition:{pid}:*')
        else:
            redis_keys = self.redis.keys(pattern=f'busposition:{pid}:*')
        return redis_keys

    def get_latest_redis(self, pid, stop_position):
        #r = self.redis
        pipeline = self.redis.pipeline()
        ts = pipeline.ts()
        heap = []
        heapsize = 10
        redis_keys = self.get_redis_keys(pid)
        for item in redis_keys:
            ts.get(item)
        results = pipeline.execute()
        index = 0
        filtered = 0
        for item in redis_keys:
            value = results[index]
            index += 1
            if value[1] < stop_position.m:
                filtered += 1
                continue
            heapq.heappush(heap, (value[0], item))
            if len(heap) > heapsize:
                heapq.heappop(heap)

        if self.debug:
            print(f'    Filtered {filtered} trips for {pid} not reaching {stop_position.m}')

        heap.sort()
        return heap

    def get_closest(self, pipeline, redis_key, dist):
        ts = pipeline.ts()
        thresh = ureg.feet * 3000
        if redis_key.decode('utf-8').startswith('train'):
            thresh = 3000 * ureg.meter
        # name msec seems inaccurate - do they mean seconds?
        ts.range(redis_key, '-', '+', count=1, aggregation_type='max', bucket_size_msec=1,
                 filter_by_min_value=(dist-thresh).m, filter_by_max_value=dist.m)
        ts.range(redis_key, '-', '+', count=1, aggregation_type='min', bucket_size_msec=1,
                 filter_by_min_value=dist.m, filter_by_max_value=(dist+thresh).m)
        #if debug and pipeline.command_stack:
        #    for i, pipeline_item in enumerate(pipeline.command_stack[-2:]):
        #        print(f'    pipeline queue: {redis_key} / {i} / {pipeline_item}')

        def callback(left, right):
            if self.debug:
                print(f'    closest to {dist} in {redis_key}: {left}, {right}')
            if not left or not right:
                return None
            left_ts, left_dist = left[0]
            right_ts, right_dist = right[0]
            if abs(dist.m - left_dist) < abs(dist.m - right_dist):
                return left[0]
            return right[0]

        return redis_key, callback

    @staticmethod
    def printable_ts(ts: int):
        return datetime.datetime.fromtimestamp(ts).isoformat()

    def get_single_estimate(self):
        row = self.estimate_request
        pid = row.pattern_id
        stop_dist = row.stop_position
        for bus_dist in row.vehicle_positions:
            if self.debug:
                print(f'Getting estimate {pid} vehicle {bus_dist} stop {stop_dist}')
            if bus_dist >= stop_dist:
                #yield -1, -1, info
                print(f'  skipping')
                continue
            #def estimate_redis(self, pid, bus_dist, stop_dist, debug=False):
            trips = self.get_latest_redis(pid, stop_dist)
            if self.debug:
                print(f'  Found {len(trips)} total trips')
            info = {}
            pipeline = self.redis.pipeline()
            estimates = []
            pipeline_stack = []
            for ts, redis_key in trips:
                pipeline_stack.append(self.get_closest(pipeline, redis_key, bus_dist.to(ureg.meters)))
                pipeline_stack.append(self.get_closest(pipeline, redis_key, stop_dist.to(ureg.meters)))

            results = pipeline.execute()

            def process(closest_bus, closest_stop, rk1, rk2):
                if self.debug:
                    print(f'  inner process {closest_bus} st {closest_stop}')
                if not closest_bus or not closest_stop:
                    return None
                #print(f'Process {closest_bus} {closest_stop} T {rk1} {rk2}')
                bus_time_samp, bus_dist_samp = closest_bus
                stop_time_samp, stop_dist_samp = closest_stop
                travel_time = stop_time_samp - bus_time_samp
                travel_dist = stop_dist_samp - bus_dist_samp
                if self.debug:
                    print(f'    tt {travel_time} td {travel_dist}')
                if travel_dist <= 0 or travel_time <= 0:
                    return None
                travel_rate = travel_dist / travel_time
                # in meters
                actual_dist = (stop_dist - bus_dist).m
                key = datetime.datetime.fromtimestamp(bus_time_samp).isoformat()
                info[key] = {}
                d = info[key]
                d['redis_key'] = rk1
                if rk1 != rk2:
                    d['error'] = f'Redis key mismatch: {rk1} / {rk2}'
                d['from'] = bus_dist_samp
                d['to'] = stop_dist_samp
                d['travel_time'] = round(travel_time / 60, 1)
                d['travel_dist'] = travel_dist
                d['travel_rate'] = travel_rate
                # print(f'pid {pid} trip starting at {self.printable_ts(ts)}  bus {bus_dist} stop {stop_dist} redis key '
                #       f'{redis_key}: closest bus {closest_bus}  closest stop {closest_stop} '
                #       f'travel time {travel_time} travel dist {travel_dist} '
                #       f'travel rate {travel_rate} actual dist {actual_dist} '
                #       f'estimate {actual_dist / travel_rate}')
                computed = actual_dist / travel_rate
                d['raw_estimate'] = round(computed / 60, 1)
                #print(f'computed: {computed}')
                return computed

            while pipeline_stack:
                rk1, cb1 = pipeline_stack.pop(0)
                result1 = cb1(results.pop(0), results.pop(0))

                rk2, cb2 = pipeline_stack.pop(0)
                result2 = cb2(results.pop(0), results.pop(0))

                result = process(result1, result2, rk1, rk2)
                if self.debug:
                    print(f'  process {result1} {result2}  {rk1} {rk2} => {result}')

                if result:
                    estimates.append(result)

            if not estimates or len(estimates) < 2:
                #yield None, None, info
                continue

            # consider more sophisticated percentile stuff
            stdev = statistics.stdev(estimates)
            mean = statistics.mean(estimates)
            #print(estimates)
            considered = [x for x in estimates if abs(x - mean) < 2 * stdev]
            if not considered:
                continue
            info['stdev'] = stdev
            info['considered'] = considered
            info['bus_position'] = bus_dist.m
            #print(info)
            #yield min(considered), max(considered), info
            yield SingleEstimate(
                vehicle_position=bus_dist,
                low_estimate=datetime.timedelta(seconds=min(considered)),
                high_estimate=datetime.timedelta(seconds=max(considered)),
                info=info
            )


class QueryManager:
    def __init__(self, engine):
        self.engine = engine
        self.patterns = {}
        self.redis = redis.Redis(host='rttransit.guineafowl-cloud.ts.net')
        print(f'Initialize redis: {self.redis.ping()}')
        query = ('select p.pattern_id, pattern_stop.stop_id, stop.stop_name from pattern_stop inner join '
                 '(select pattern_id, max(sequence) as endseq from pattern_stop group by pattern_id) as p '
                 'on p.endseq = pattern_stop.sequence and p.pattern_id = pattern_stop.pattern_id inner join '
                 'stop on stop.id = pattern_stop.stop_id')
        self.last_stops = {}
        with Session(engine) as session:
            rows = session.execute(text(query))
            for row in rows:
                pid, stop_id, stop_name = row
                self.last_stops[pid] = (stop_id, stop_name)
        self.load_pattern_info()
        self.report()

    def report(self):
        print(f'Database stats at load time')
        with self.engine.connect() as conn:
            for table in [Stop, BusPosition]:
                count = conn.execute(select(func.count('*')).select_from(table)).all()
                print(f'table {table} has count {count}')

    def load_pattern_info(self):
        url = 'http://leonard.guineafowl-cloud.ts.net:8002/patterninfo'
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f'Error loading patterns: {resp.status_code}')
            return
        patterns = resp.json()['pattern_info']
        for p in patterns:
            self.patterns[p['pattern_id']] = p

    def get_estimates(self, rows: Iterable[StopEstimate]) -> EstimateResponse:
        rv = EstimateResponse(patterns=[])
        for row in rows:
            response = PatternResponse(
                pattern_id=row.pattern_id,
                stop_position=row.stop_position,
                single_estimates=[]
            )
            estimate_finder = EstimateFinder(self.redis, row)
            for single_estimate in estimate_finder.get_single_estimate():
                response.single_estimates.append(single_estimate)
            rv.patterns.append(response)
        return rv

    def nearest_stop_vehicles(self, lat, lon) -> list[BusEstimate]:
        query = """
        select current_vehicle_state.last_update, current_vehicle_state.distance,
            current_vehicle_state.id as vehicle_id, stop_pattern_distance, 
            pattern_id, x.rt, x.id as stop_id, stop_name, st_y(stop_geom) as stop_lat, st_x(stop_geom) as stop_lon, dist 
            from (
                 select DISTINCT ON (pattern_id) pattern_id, rt, id, stop_name, stop_geom, dist, stop_pattern_distance 
                 from (
                     select stop.id, pattern_stop.pattern_id, stop_name, stop.geom as stop_geom, 
                     pattern_stop.distance as stop_pattern_distance, pattern.rt, 
                     ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist 
                     from stop 
                        inner join pattern_stop on stop.id = pattern_stop.stop_id 
                        inner join pattern on pattern_stop.pattern_id = pattern.id ORDER BY dist
                 ) WHERE dist < :thresh ORDER BY pattern_id, dist
            ) as x 
              left join current_vehicle_state on current_vehicle_state.pid = pattern_id 
               
              order by dist, distance
        """
# where distance is null or distance < stop_pattern_distance

        predictions = """
            select pattern_id, stop_id, destination, route, timestamp, prediction from bus_prediction 
            inner join schedule_destinations
            on bus_prediction.stop_id = schedule_destinations.first_stop_id and bus_prediction.destination = schedule_destinations.destination_headsign
            and bus_prediction.route = schedule_destinations.route_id
            inner join pattern_destinations
            on bus_prediction.stop_id = pattern_destinations.origin_stop
            and bus_prediction.route = pattern_destinations.rt
            and bus_prediction.prediction_type = 'D'
            and schedule_destinations.last_stop_id = pattern_destinations.last_stop
            where timestamp + make_interval(mins => prediction) >= now() at time zone 'America/Chicago'
            and pattern_id in (
            
            select pattern_id from (
            select DISTINCT ON (pattern_id) pattern_id, rt, id, stop_name, stop_geom, dist, stop_pattern_distance 
                             from (
                                 select stop.id, pattern_stop.pattern_id, stop_name, stop.geom as stop_geom, 
                                 pattern_stop.distance as stop_pattern_distance, pattern.rt, 
                                 ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist 
                                 from stop 
                                    inner join pattern_stop on stop.id = pattern_stop.stop_id 
                                    inner join pattern on pattern_stop.pattern_id = pattern.id ORDER BY dist
                             ) WHERE dist < :thresh ORDER BY pattern_id, dist
            )                 
            
            )
            order by pattern_id
        """
        #routes = {}
        all_items = []
        with Session(self.engine) as session:
            result = session.execute(text(query), {"lat": float(lat), "lon": float(lon), "thresh": 1000})
            prediction_result = session.execute(text(predictions),
                                                {"lat": float(lat), "lon": float(lon), "thresh": 1000})
            startquery = Util.ctanow().replace(tzinfo=None)
            predictions = {}
            seen = set([])
            for p in prediction_result:
                key = p.pattern_id
                predictions[key] = p
            local_now = Util.ctanow()
            for row in result:
                row_distance = row.distance
                print(f'Looking for pattern {row.pattern_id}  distance {row_distance} stop distance {row.stop_pattern_distance}')
                last_stop_id, last_stop_name = self.last_stops.get(row.pattern_id, (None, None))
                if last_stop_id is None:
                    print(f'No last stop found for {row.pattern_id} - {row.stop_name} {row.rt}')
                    continue
                info = self.patterns.get(row.pattern_id, {})
                direction = info.get('direction')
                row_update = row.last_update
                if row_distance is None or row_distance >= row.stop_pattern_distance:
                    prediction = predictions.get(row.pattern_id)
                    if prediction is None:
                        print(f'No prediction found for {row.pattern_id} - {row.stop_name} {row.rt}')
                        continue
                    if row.pattern_id in seen:
                        continue
                    row_distance = 0
                    seen.add(row.pattern_id)
                    #prediction_first_stop = prediction.stop_id
                    row_update = prediction.timestamp
                    predicted_minutes = prediction.prediction

                    print(f'Prediction: {row.pattern_id} - {row.stop_name} {row.rt} / {row_update} mins {predicted_minutes}')
                    pts = prediction.timestamp.replace(tzinfo=Util.CTA_TIMEZONE)
                    age = (local_now - pts).total_seconds() / 60
                    predicted_minutes = round(predicted_minutes - age)
                    dxx = BusEstimate(
                        query_start=startquery,
                        pattern=row.pattern_id,
                        route=row.rt,
                        direction=direction,
                        stop_id=row.stop_id,
                        stop_name=row.stop_name,
                        stop_lat=row.stop_lat,
                        stop_lon=row.stop_lon,
                        stop_position=Q_(row.stop_pattern_distance, 'ft'),
                        vehicle_position=Q_(row_distance, 'ft'),
                        distance_from_vehicle=Q_(row.stop_pattern_distance, 'ft'),
                        last_update=row_update,
                        distance_to_stop=Q_(row.dist, 'm'),
                        age=datetime.timedelta(seconds=age),
                        destination_stop_id=last_stop_id,
                        destination_stop_name=last_stop_name,
                        waiting_to_depart=True,
                        predicted_minutes=datetime.timedelta(minutes=predicted_minutes),
                        vehicle=row.vehicle_id,
                    )
                    # TODO: avoid copy
                    #print(dxx)
                    key = (row.rt, last_stop_name)
                    #routes[key] = dxx
                    all_items.append(dxx)
                    continue
                if row_distance >= row.stop_pattern_distance:
                    #print(f'Skip ')
                    continue
                bus_distance = row.stop_pattern_distance - row_distance
                # split this out into its own thing
                #point = to_shape(row.stop_geom)
                #lat, lon = point.y, point.x

                age = (startquery - row_update).total_seconds()

                dxx = BusEstimate(
                    query_start=startquery,
                    pattern=row.pattern_id,
                    route=row.rt,
                    direction=direction,
                    stop_id=row.stop_id,
                    stop_name=row.stop_name,
                    stop_lat=row.stop_lat,
                    stop_lon=row.stop_lon,
                    stop_position=Q_(row.stop_pattern_distance, 'ft'),
                    vehicle_position=Q_(row_distance, 'ft'),
                    distance_from_vehicle=Q_(bus_distance, 'ft'),
                    last_update=row_update,
                    distance_to_stop=Q_(row.dist, 'm'),
                    age=datetime.timedelta(seconds=age),
                    destination_stop_id=last_stop_id,
                    destination_stop_name=last_stop_name,
                    waiting_to_depart=False,
                    vehicle=row.vehicle_id,
                )
                key = (row.rt, last_stop_name)
                #routes[key] = dxx
                all_items.append(dxx)
        return all_items

    def detail(self, pid: int, stop_dist):
        with Session(self.engine) as session:
            stmt = select(CurrentVehicleState).where(CurrentVehicleState.pid == pid).where(CurrentVehicleState.distance < stop_dist).order_by(CurrentVehicleState.distance)
            result = session.scalars(stmt)
            rt = None
            rv = []
            for row in result:
                bus_dist = row.distance
                mi_from_here = (stop_dist - bus_dist) / 5280.0
                timestamp = row.last_update
                vid = row.id
                rt = row.rt
                x1, x2, interp = self.estimate(pid, bus_dist, stop_dist)
                rv.append({
                    'bus_pattern_dist': bus_dist,
                    'mi_from_here': f'{mi_from_here:0.2f}mi',
                    'timestamp': timestamp.isoformat(),
                    'vid': vid,
                    'destination': row.destination,
                    'estimate': f'{x1}-{x2} min'
                })
            return {
                'rt': rt,
                'pid': pid,
                'stop_distance': stop_dist,
                'updates': rv
            }


class TrainQuery:
    DIRECTION_MAPPING = {
        ("blue", "Forest Park"): 5,
        ("blue", "Jefferson Park"): 1,
        ("blue", "O'Hare"): 1,
        ("blue", "Racine"): 5,
        ("blue", "UIC-Halsted"): 5,
        ("brn", "Kimball"): 1,
        ("brn", "Loop"): 5,
        ("g", ""): 5,
        ("g", "Ashland/63rd"): 5,
        ("g", "Cottage Grove"): 5,
        ("g", "Harlem/Lake"): 1,
        ("org", "Loop"): 1,
        ("org", "Midway"): 5,
        ("p", "Howard"): 5,
        ("p", "Linden"): 1,
        ("p", "Loop"): 5,
        ("pink", "54th/Cermak"): 5,
        ("pink", "Loop"): 1,
        ("red", "95th/Dan Ryan"): 5,
        ("red", "Howard"): 1,
        ("y", "Howard"): 5,
        ("y", "Skokie"): 1,
    }
    """
    Maintains state for train position queries. There are far fewer trains and the live updates don't have pattern info,
    so we gather the data from the database and join here
    """
    def __init__(self, engine, schedule_analyzer: ScheduleAnalyzer):
        self.engine = engine
        self.schedule_analyzer = schedule_analyzer

    def get_relevant_stops(self, lat, lon) -> list[TrainEstimate]:
        # TODO: handle rare trips better
        query = """
        select 
            stop_pattern_distance, pid, 
            x.rt as xrt, x.id as stop_id, stop_name, st_y(stop_geom) as stop_lat, st_x(stop_geom) as stop_lon, dist, last_stop.stop_id as last_stop_id, stop_headsign, direction_change
            from 
            (
             select DISTINCT ON (pattern_id, stop_headsign) pattern_id as pid, rt, id, stop_name, stop_geom, dist, stop_pattern_distance, stop_headsign, direction_change from (
        
                select stop.id, pattern_stop.pattern_id, stop_name, stop.geom as stop_geom, pattern_stop.distance as stop_pattern_distance, pattern.rt, 
                pattern_stop.stop_headsign, pattern_stop.direction_change,
                ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM('SRID=4326;POINT(:lon :lat)'\\:\\:geometry, 26916) as dist from 
                  stop 
                    inner join pattern_stop on stop.id = pattern_stop.stop_id inner join pattern on pattern_stop.pattern_id = pattern.id 
                    WHERE pattern.id > 300000000
                    ORDER BY dist
                ) 
        
              WHERE dist < :thresh ORDER BY pattern_id, stop_headsign, dist
                )
            as x 
            INNER JOIN last_stop on last_stop.pattern_id = x.pid
            WHERE pid not in ('308500040', '308500084', '308500128', '308500129', '308500022', '308500029', '308500038', '308500039')
            order by dist
        """
        startquery = Util.ctanow().replace(tzinfo=None)
        with Session(self.engine) as session:
            state_query = 'select * from current_train_state'
            trains = {}
            current_state = session.execute(text(state_query))
            for row in current_state:
                print(f'Found {row.dest_station}')
                key = (row.dest_station, row.direction)
                trains.setdefault(key, []).append(row)

            rv = []

            result = session.execute(text(query), {"lat": float(lat), "lon": float(lon),
                                                   "thresh": 1000})

            for row in result:
                print(f'Found {row}')
                shape_manager: ShapeManager = self.schedule_analyzer.managed_shapes.get(row.pid)
                direction = self.DIRECTION_MAPPING.get((row.xrt, row.stop_headsign))
                if not direction:
                    print(f'Unrecognized route / headsign combo: {row.xrt}, {row.stop_headsign}')
                    continue
                key = (row.last_stop_id, direction)
                if direction == 1:
                    dirname = "Northbound"
                else:
                    dirname = "Southbound"
                pattern_trains = trains.get(key)
                if not pattern_trains:
                    continue
                stop_id = row.stop_id
                rt = row.xrt
                for train in pattern_trains:
                    stmt = (select(PatternStop).where(PatternStop.pattern_id == int(row.pid)).
                            where(PatternStop.stop_id == int(train.next_stop)))
                    try:
                        pattern_stop = session.scalar(stmt)
                    except sqlalchemy.exc.NoResultFound as e:
                        pattern_stop = None
                    if pattern_stop is None:
                        print(f'Could not find pattern stop {row.pid} {rt} {train.id}')
                        continue
                    next_train_pattern_distance = pattern_stop.distance
                    #print(train.geom, type(train.geom))
                    # The ORM would do this for us automatically, but we have a manual query here
                    train_wkb = geoalchemy2.elements.WKBElement(train.geom)
                    train_point = to_shape(train_wkb)
                    #train_dist = shape_manager.get_distance_along_shape_dc(row.direction_change, train_point)
                    _, train_dist = shape_manager.get_distance_along_shape_anchor(next_train_pattern_distance, train_point, False)
                    if train_dist > row.stop_pattern_distance:
                        continue
                    dist_from_train = row.stop_pattern_distance - train_dist
                    age = (startquery - train.last_update).total_seconds()
                    result = TrainEstimate(
                        query_start=startquery,
                        pattern=row.pid,
                        route=rt,
                        direction=dirname,
                        destination=train.dest_station_name,
                        run=train.id,
                        stop_id=stop_id,
                        stop_name=row.stop_name,
                        stop_lat=row.stop_lat,
                        stop_lon=row.stop_lon,
                        stop_position=Q_(row.stop_pattern_distance, 'm'),
                        vehicle_position=Q_(train_dist, 'm'),
                        distance_from_vehicle=Q_(dist_from_train, 'm'),
                        distance_to_stop=Q_(row.dist, 'm'),
                        age=datetime.timedelta(seconds=age),
                        destination_stop_id=train.dest_station,
                        destination_stop_name=train.dest_station_name,
                        next_stop_position=Q_(next_train_pattern_distance, 'm'),
                        next_stop_id=train.next_stop,
                        waiting_to_depart=False,
                        last_update=train.last_update,
                    )
                    rv.append(result)
            return rv


def main():
    engine = db_init(local=True)
    qm = QueryManager(engine)
    lon = -87.610056
    lat = 41.822556
    # no longer includes estimates
    results = qm.nearest_stop_vehicles(lat, lon)
    return results


if __name__ == "__main__":
    cProfile.run('main()', sort='cumtime')
