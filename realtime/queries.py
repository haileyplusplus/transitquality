import datetime
import cProfile

from sqlalchemy import text, select
from sqlalchemy.orm import Session
import requests

import pandas as pd
import numpy as np

from realtime.rtmodel import db_init, BusPosition, CurrentVehicleState


class QueryManager:
    def __init__(self, engine):
        self.engine = engine
        self.patterns = {}
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

    def load_pattern_info(self):
        url = 'http://leonard.guineafowl-cloud.ts.net:8002/patterninfo'
        resp = requests.get(url)
        if resp.status_code != 200:
            print(f'Error loading patterns: {resp.status_code}')
            return
        patterns = resp.json()['pattern_info']
        for p in patterns:
            self.patterns[p['pattern_id']] = p

    def nearest_stop_vehicles(self, lat, lon):
        query = ('select current_vehicle_state.last_update, current_vehicle_state.distance, stop_pattern_distance, '
                 'pattern_id, x.rt, x.id as stop_id, stop_name, dist from ('
                 'select DISTINCT ON (pattern_id) pattern_id, rt, id, stop_name, dist, stop_pattern_distance from '
                 '(select stop.id, pattern_stop.pattern_id, stop_name, pattern_stop.distance as stop_pattern_distance, '
                 'pattern.rt, ST_TRANSFORM(geom, 26916) <-> ST_TRANSFORM(\'SRID=4326;POINT(:lon :lat)\'\\:\\:geometry, 26916) as dist '
                 'from stop inner join pattern_stop on stop.id = pattern_stop.stop_id inner join pattern on '
                 'pattern_stop.pattern_id = pattern.id ORDER BY dist) WHERE dist < :thresh) as x inner join '
                 'current_vehicle_state on current_vehicle_state.pid = pattern_id where '
                 'distance < stop_pattern_distance order by dist, distance')
        patterns = {}
        with Session(self.engine) as session:
            result = session.execute(text(query), {"lat": float(lat), "lon": float(lon), "thresh": 1000})
            for row in result:
                last_stop_id, last_stop_name = self.last_stops.get(row.pattern_id, (None, None))
                if last_stop_id is None:
                    continue
                info = self.patterns.get(row.pattern_id, {})
                direction = info.get('direction')
                bus_distance = row.stop_pattern_distance - row.distance
                el, eh, _ = self.estimate(row.pattern_id, row.distance, row.stop_pattern_distance)

                dxx = {'pattern': row.pattern_id,
                       'route': row.rt,
                       'direction': direction,
                       'stop_id': row.stop_id,
                       'stop_name': row.stop_name,
                       'stop_pattern_distance': row.stop_pattern_distance,
                       'bus_distance': bus_distance,
                       'dist': row.dist,
                       'last_update': row.last_update.isoformat(),
                       'vehicle_distance': row.distance,
                       'last_stop_id': last_stop_id,
                       'last_stop_name': last_stop_name,
                       'estimate_min': el,
                       'estimate_max': eh
                       }
                patterns[row.pattern_id] = dxx
        return list(patterns.values())

    def get_position_dataframe(self, pid):
        with Session(self.engine) as session:
            thresh = datetime.datetime.now() - datetime.timedelta(hours=5)
            # select timestamp, pdist, origtatripno from bus_position where pid = 5907 order by origtatripno, timestamp;
            query = select(BusPosition).where(BusPosition.pid == pid).where(
                BusPosition.timestamp > thresh).order_by(
                BusPosition.origtatripno, BusPosition.timestamp)
            #rows = session.execute(query)
            dfrows = []
            #for row in rows:
            #    dfrows.append(dict(rows))
            #df = pd.DataFrame([row._mapping for row in rows])
            for row in session.scalars(query):
                dr = row.__dict__
                del dr['_sa_instance_state']
                dr['epochstamp'] = int(row.timestamp.timestamp())
                dfrows.append(dr)
            return pd.DataFrame(dfrows)

    def interpolate(self, pid, bus_dist, stop_dist):
        df = self.get_position_dataframe(pid)
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


def main():
    engine = db_init()
    qm = QueryManager(engine)
    lon = -87.632892
    lat = 41.903914
    results = qm.nearest_stop_vehicles(lat, lon)
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
