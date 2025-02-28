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
import shapely
from pydantic import BaseModel



from realtime.rtmodel import db_init, BusPosition, CurrentVehicleState, Stop, TrainPosition, PatternStop
from backend.util import Util
from schedules.schedule_analyzer import ScheduleAnalyzer, ShapeManager


class StopEstimate(BaseModel):
    pattern_id: int
    bus_location: int
    stop_pattern_distance: int


class StopEstimates(BaseModel):
    estimates: list[StopEstimate]


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

    def get_single_estimate(self, row: StopEstimate):
        #print('row is', row, type(row))
        el, eh, info = self.estimate_redis(row.pattern_id, row.bus_location, row.stop_pattern_distance)
        #el, eh, _ = self.estimate(row.pattern_id, row.bus_location, row.stop_pattern_distance)
        #return f'{el}-{eh} min'
        return el, eh, info

    def get_estimates(self, rows: Iterable[StopEstimate]):
        rv = []
        #print(rows)
        for row in rows:
            el, eh, info = self.get_single_estimate(row)
            rv.append({
                'pattern': row.pattern_id,
                'bus_location': row.bus_location,
                'low': el,
                'high': eh,
                'info': info
            })
        return rv

    def nearest_stop_vehicles(self, lat, lon, include_estimate=False, include_all_items=False):
        query = ('select current_vehicle_state.last_update, current_vehicle_state.distance, stop_pattern_distance, '
                 'pattern_id, x.rt, x.id as stop_id, stop_name, st_y(stop_geom) as stop_lat, st_x(stop_geom) as stop_lon, dist from ('
                 'select DISTINCT ON (pattern_id) pattern_id, rt, id, stop_name, stop_geom, dist, stop_pattern_distance from '
                 '(select stop.id, pattern_stop.pattern_id, stop_name, stop.geom as stop_geom, pattern_stop.distance as stop_pattern_distance, '
                 'pattern.rt, ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist '
                 'from stop inner join pattern_stop on stop.id = pattern_stop.stop_id inner join pattern on '
                 'pattern_stop.pattern_id = pattern.id ORDER BY dist) WHERE dist < :thresh ORDER BY pattern_id, dist) as x inner join '
                 'current_vehicle_state on current_vehicle_state.pid = pattern_id where '
                 'distance < stop_pattern_distance order by dist, distance')
        routes = {}
        all_items = []
        with Session(self.engine) as session:
            result = session.execute(text(query), {"lat": float(lat), "lon": float(lon), "thresh": 1000})
            startquery = Util.ctanow().replace(tzinfo=None)
            for row in result:
                last_stop_id, last_stop_name = self.last_stops.get(row.pattern_id, (None, None))
                if last_stop_id is None:
                    continue
                info = self.patterns.get(row.pattern_id, {})
                direction = info.get('direction')
                bus_distance = row.stop_pattern_distance - row.distance
                # split this out into its own thing
                #point = to_shape(row.stop_geom)
                #lat, lon = point.y, point.x
                age = (startquery - row.last_update).total_seconds()

                estimate = '?'
                if include_estimate:
                    se = StopEstimate(pattern_id=row.pattern_id,
                                      bus_location=row.distance,
                                      stop_pattern_distance=row.stop_pattern_distance)
                    estimate = self.get_single_estimate(se)

                dxx = {'pattern': row.pattern_id,
                       'startquery': startquery.isoformat(),
                       'route': row.rt,
                       'direction': direction,
                       'stop_id': row.stop_id,
                       'stop_name': row.stop_name,
                       'stop_lat': row.stop_lat,
                       'stop_lon': row.stop_lon,
                       'stop_pattern_distance': row.stop_pattern_distance,
                       'bus_distance': bus_distance,
                       'dist': row.dist,
                       'last_update': row.last_update.isoformat(),
                       'age': age,
                       'vehicle_distance': row.distance,
                       'last_stop_id': last_stop_id,
                       'last_stop_name': last_stop_name,
                       'estimate': estimate,
                       }
                key = (row.rt, last_stop_name)
                routes[key] = dxx
                all_items.append(dxx)
        if include_all_items:
            return all_items
        return list(routes.values())


    def get_stop_latlon(self, stop_id):
        with Session(self.engine) as session:
            stop = session.get(Stop, stop_id)
            if stop is None:
                return 0, 0
            point = to_shape(stop.geom)
            return point.y, point.x

    def get_position_dataframe(self, pid):
        with Session(self.engine) as session:
            #thresh = datetime.datetime.now(tz=Util.CTA_TIMEZONE) - datetime.timedelta(hours=5)
            thresh = datetime.datetime.now() - datetime.timedelta(hours=11)
            # select timestamp, pdist, origtatripno from bus_position where pid = 5907 order by origtatripno, timestamp;
            query = select(BusPosition).where(BusPosition.pid == pid).where(
                BusPosition.timestamp > thresh).order_by(
                BusPosition.origtatripno, BusPosition.timestamp)
            print('bus position query: ', query, pid, type(pid), thresh, thresh.isoformat())
            dfrows = []
            for row in session.scalars(query):
                dr = row.__dict__
                del dr['_sa_instance_state']
                dr['epochstamp'] = int(row.timestamp.timestamp())
                dfrows.append(dr)
            print(f'Found {len(dfrows)} rows')
            df = pd.DataFrame(dfrows)
            # TODO: remove this later
            df.to_csv(f'/tmp/df-{pid}.csv', index=False)
            print(f'Got df {df}')
            return df

    def interpolate(self, pid, bus_dist, stop_dist):
        df = self.get_position_dataframe(int(pid))
        if df.empty:
            print(f'Could not find df for {pid}, {bus_dist}, {stop_dist}')
            return pd.DataFrame()
        rows = []
        for trip in df.origtatripno.unique():
            tdf = df[df.origtatripno == trip]
            if len(tdf) < 10:
                continue
            distseq = tdf.pdist.diff().dropna()
            if distseq.empty:
                continue
            if min(distseq) < -1500:
                continue
            if min(bus_dist, stop_dist) < tdf.pdist.min():
                continue
            if max(bus_dist, stop_dist) > tdf.pdist.max():
                continue
            bus_time = np.interp(x=bus_dist, xp=tdf.pdist, fp=tdf.epochstamp)
            stop_time = np.interp(x=stop_dist, xp=tdf.pdist, fp=tdf.epochstamp)
            travel = datetime.timedelta(seconds=int(stop_time - bus_time))
            bus_ts = datetime.datetime.fromtimestamp(int(bus_time))
            rows.append({'origtatripno': trip,
                         'travel': travel,
                         'bus_timestamp': bus_ts.isoformat()})
        return pd.DataFrame(rows)

    def estimate(self, pid, bus_dist, stop_dist):
        interp = self.interpolate(pid, bus_dist, stop_dist)
        if interp.empty:
            return -1, -1, -1
        x1 = round(interp[-10:].travel.quantile(0.05).total_seconds() / 60)
        x2 = round(interp[-10:].travel.quantile(0.95).total_seconds() / 60)
        return x1, x2, interp

    def get_redis_keys(self, pid):
        if pid >= 308500000:
            redis_keys = self.redis.keys(pattern=f'trainposition:{pid}:*')
        else:
            redis_keys = self.redis.keys(pattern=f'busposition:{pid}:*')
        return redis_keys

    def get_latest_redis(self, pid):
        #cursor = 0
        r = self.redis
        pipeline = self.redis.pipeline()
        ts = pipeline.ts()
        heap = []
        heapsize = 10
        redis_keys = self.get_redis_keys(pid)
        for item in redis_keys:
            ts.get(item)
        results = pipeline.execute()
        index = 0
        for item in redis_keys:
            value = results[index]
            index += 1
            heapq.heappush(heap, (value[0], item))
            if len(heap) > heapsize:
                heapq.heappop(heap)
        # while True:
        #     cursor, items = r.scan(cursor, match=f'busposition:{pid}:*')
        #     for item in items:
        #         value = ts.get(item)
        #         heapq.heappush(heap, (value[0], item))
        #         if len(heap) > heapsize:
        #             heapq.heappop(heap)
        #     if cursor == 0:
        #         break
        heap.sort()
        return heap

    def get_closest(self, pipeline, redis_key, dist):
        ts = pipeline.ts()
        thresh = 3000
        # name msec seems inaccurate - do they mean seconds?
        ts.range(redis_key, '-', '+', count=1, aggregation_type='max', bucket_size_msec=1,
                 filter_by_min_value=dist-thresh, filter_by_max_value=dist)
        ts.range(redis_key, '-', '+', count=1, aggregation_type='min', bucket_size_msec=1,
                 filter_by_min_value=dist, filter_by_max_value=dist+thresh)
        #  print(f'closest to {dist} in {redis_key}: {left}, {right}')

        def callback(left, right):
            if not left or not right:
                return None
            left_ts, left_dist = left[0]
            right_ts, right_dist = right[0]
            if abs(dist - left_dist) < abs(dist - right_dist):
                return left[0]
            return right[0]

        return redis_key, callback

    @staticmethod
    def printable_ts(ts: int):
        return datetime.datetime.fromtimestamp(ts).isoformat()

    def estimate_redis(self, pid, bus_dist, stop_dist):
        trips = self.get_latest_redis(pid)
        info = {}
        if bus_dist >= stop_dist:
            return -1, -1, info
        pipeline = self.redis.pipeline()
        estimates = []
        pipeline_stack = []
        for ts, redis_key in trips:
            #closest_bus = self.get_closest(pipeline, redis_key, bus_dist)
            pipeline_stack.append(self.get_closest(pipeline, redis_key, bus_dist))
            #closest_stop = self.get_closest(pipeline, redis_key, stop_dist)
            pipeline_stack.append(self.get_closest(pipeline, redis_key, stop_dist))

        results = pipeline.execute()

        def process(closest_bus, closest_stop, rk1, rk2):
            if not closest_bus or not closest_stop:
                return None
            bus_time_samp, bus_dist_samp = closest_bus
            stop_time_samp, stop_dist_samp = closest_stop
            travel_time = stop_time_samp - bus_time_samp
            travel_dist = stop_dist_samp - bus_dist_samp
            if travel_dist <= 0 or travel_time <= 0:
                return None
            travel_rate = travel_dist / travel_time
            actual_dist = stop_dist - bus_dist
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
            return computed

        while pipeline_stack:
            rk1, cb1 = pipeline_stack.pop(0)
            result1 = cb1(results.pop(0), results.pop(0))

            rk2, cb2 = pipeline_stack.pop(0)
            result2 = cb2(results.pop(0), results.pop(0))

            result = process(result1, result2, rk1, rk2)

            if result:
                estimates.append(result)

        if not estimates or len(estimates) < 2:
            return -1, -1, info

        # consider more sophisticated percentile stuff
        stdev = statistics.stdev(estimates)
        mean = statistics.mean(estimates)
        considered = [x for x in estimates if abs(x - mean) < 2 * stdev]
        info['stdev'] = stdev
        info['considered'] = considered
        return min(considered), max(considered), info

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

    # def distance_along_pattern(self, train_position: TrainPosition):
    #     shape_manager: ShapeManager = self.schedule_analyzer.managed_shapes.get(row.pid)
    #     train_wkb = geoalchemy2.elements.WKBElement(train_position.geom)
    #     train_point = to_shape(train_wkb)
    #     train_dist = shape_manager.get_distance_along_shape_dc(row.direction_change, train_point)
    #     dist_from_train = row.stop_pattern_distance - train_dist
    #     return dist_from_train

    def get_relevant_stops(self, lat, lon):
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
            # routes = set([])
            # result = [x for x in resultx]
            # #
            # for row in result:
            #     routes.add(row.xrt)

            #state_query = 'select *, ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist from current_train_state where rt in (:routes) order by dist;'
            #state_query = 'select *, ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist from current_train_state order by dist;'
            #routestr = ', '.join([f"'{rt}'" for rt in routes])
            # current_state = session.execute(text(state_query),
            #                                 {"lat": float(self.lat), "lon": float(self.lon)})
            #                                  #"routes": routestr})
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
                    result = {
                        "pattern": row.pid,
                        "startquery": startquery.isoformat(),
                        "route": rt,
                        "direction": dirname,
                        "destination": train.dest_station_name,
                        "run": train.id,
                        "stop_id": stop_id,
                        "stop_name": row.stop_name,
                        "stop_lat": row.stop_lat,
                        "stop_lon": row.stop_lon,
                        "stop_pattern_distance": row.stop_pattern_distance,
                        # needs to be renamed. this is the distance of the train from the station
                        "bus_distance": int(dist_from_train),
                        "dist": int(row.dist),
                        "last_update": train.last_update.isoformat(),
                        "age": int(age),
                        "vehicle_distance": int(train_dist),
                        "last_stop_id": train.dest_station,
                        "last_stop_name": train.dest_station_name,
                        "next_train_pattern_distance": next_train_pattern_distance,
                        "next_stop_id": train.next_stop,
                        "estimate": "?",
                        "mi": "2.01mi",
                        "walk_time": -1,
                        "walk_dist": "?"
                    }
                    rv.append(result)

            return {'results': rv}


def main():
    engine = db_init(local=True)
    qm = QueryManager(engine)
    # ,
    lon = -87.610056
    lat = 41.822556
    #lon = -87.632892
    #lat = 41.903914
    results = qm.nearest_stop_vehicles(lat, lon, include_estimate=True)
    return results


if __name__ == "__main__":
    #engine = db_init()
    #qm = QueryManager(engine)
    #lon = -87.632892
    #lat = 41.903914
    #results = qm.nearest_stop_vehicles(lat, lon)
    #cProfile.run('qm.nearest_stop_vehicles', 'lat', 'lon')
    cProfile.run('main()', sort='cumtime')
    # for row in results:
    #     print(row)
    # df = qm.get_position_dataframe(5907)
    # inter = qm.interpolate(5907, 32922, 38913)
