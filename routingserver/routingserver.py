import datetime

import pytz
import requests
import pandas as pd

from fastapi import FastAPI

app = FastAPI()

START_TIME = datetime.datetime.now(datetime.UTC)


class Routing:
    BASE_URL = 'http://abed:7001/api/v1/plan'

    def __init__(self):
        self.samples = []
        self.rawsamp = None

    def query(self, from_, to_, arrival_time: datetime.datetime):
        """
        http://localhost:7001/api/v1/plan?timetableView=false&fromPlace=chicago_10520
        &toPlace=chicago_10981&arriveBy=true&time=2025-01-01T13:00:00-06:00
        :return:
        """
        common = {
            'timetableView': False,
            'arriveBy': True,
            'fromPlace': f'{from_}',
            'toPlace': f'{to_}',
        }
        for day in range(1, 8):
            arrival = arrival_time - datetime.timedelta(days=day)
            common['time'] = arrival.isoformat()
            #print(f'trying {common}')
            # TODO: look at keeping the connection open
            resp = requests.get(self.BASE_URL, params=common)
            if resp.status_code != 200:
                print(f'Error fetching {common}')
                return False
            if self.rawsamp is None:
                self.rawsamp = resp.json()
            #print(str(resp.json()))
            parsed = self.parse_response(resp.json(), arrival)
            if parsed:
                self.samples.append(parsed)

    def parse_response(self, d: dict, arrival: datetime.datetime):
        itins = d.get('itineraries', [])
        if len(itins) < 1:
            print(f'No itins')
            return {}
        itin = itins[0]
        endwait = arrival - datetime.datetime.fromisoformat(itin['endTime'])
        routes = []
        for leg in itin['legs']:
            if leg['mode'] == 'BUS':
                routes.append(leg['routeShortName'])
        triptime = datetime.timedelta(seconds=itin['duration']) + endwait
        return {
            'day': arrival.strftime('%Y%m%d'),
            'total_time': triptime.total_seconds() / 60.0,
            'start': itin['startTime'],
            'path': ', '.join(routes)
        }

    def df(self):
        return pd.DataFrame(self.samples)

    def parse_search_results(self, items: list):
        rv = {}
        for item in items:
            include = False
            for a in item['areas']:
                if a['name'] == 'Chicago':
                    include = True
                    break
            if include:
                d = {
                    'type': item['type'],
                    'lat': item['lat'],
                    'lon': item['lon'],
                }
                if item['type'] == 'STOP':
                    d['stop'] = item['id']
                rv[item['name']] = d
        return rv

    def search(self, query):
        resp = requests.get('http://abed:7001/api/v1/geocode', params={'text': query})
        if resp.status_code != 200:
            print(f'Error searching for {query}')
            return False
        results = self.parse_search_results(resp.json())
        return {'results': results}


@app.get('/routebylatlon')
def routebylatlon(from_: str, to: str, arrival: str, include_raw=False):
    r = Routing()
    arrival_time = datetime.datetime.fromisoformat(arrival)
    r.query(from_, to, arrival_time)
    df = r.df()
    if df.empty:
        rv = {
            'times': [],
            'routes': 'no route found',
            'arrival': arrival,
        }
        if include_raw:
            rv['debug'] = r.rawsamp
        return rv
    routes = df['path'].unique()
    local_arrive = arrival_time.astimezone(pytz.timezone('America/Chicago'))
    x = []
    rv = {'times': x,
          'routes': ' / '.join(routes),
          'arrival': arrival}
    if include_raw:
        rv['debug'] = r.rawsamp
    for pct in [50, 95, 99]:
        start = local_arrive - datetime.timedelta(minutes=df.total_time.quantile(pct/100.0))
        x.append((f'{pct}%', start.isoformat()))
    return rv


@app.get('/routebystop')
def routebystop(from_: str, to: str, arrival: str, include_raw=False):
    return routebylatlon(f'chicago_{from_}', f'chicago_{to}', arrival, include_raw)


@app.get('/search')
def search(text: str):
    r = Routing()
    return r.search(text)
