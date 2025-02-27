import json

from flask import Flask, render_template, request

import grequests
import requests

from realtime.queries import StopEstimate

app = Flask(__name__)


"""
http://localhost:7001/api/v1/plan?directModes=WALK&fromPlace=41.903914,-87.632892,0&toPlace=chicago_2034
"""


@app.route('/')
def main():
    return render_template('main.html')


def route_coalesce(v):
    routes = {}
    for item in v:
        route = item['route']
        routes.setdefault(route, []).append(item)
    results = []
    for k, v in sorted(routes.items()):
        routev = []
        for d in v:
            print(f'coalesce')
            print(d)
            if 'el' not in d:
                if d['mi_numeric'] <= 1:
                    routev.append(d)
                continue
            if d['walk_time'] > 0 and -1 < d['eh'] <= d['walk_time']:
                continue
            age = d['age']
            # age_minutes = round(d['age'] / 60)
            d['age'] = round(d['age'])
            el = round((d['el'] - age) / 60)
            eh = round((d['eh'] - age) / 60)
            d['estimate'] = f'{el}-{eh} min'
            routev.append(d)
        routev.sort(key=lambda x: x['el'])
        results += routev[:2]
    return results


@app.route('/estimates')
def estimates():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    skip_estimates = request.args.get('skip')
    backend = 'http://localhost:8500/nearest-estimates'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    results = d['results']
    train_resp = requests.get('http://localhost:8500/nearest-trains', params=request.args)
    if resp.status_code == 200:
        results += train_resp.json()['results']
    directions = {'Northbound': [], 'Southbound': [], 'Eastbound': [], 'Westbound': []}
    urls = []
    estimate_params: list[dict] = []
    index = {}
    for item in results:
        if item['pattern'] >= 308500000:
            dist_mi = item['bus_distance'] / 1609.34
        else:
            dist_mi = item['bus_distance'] / 5280.0
        item['mi'] = f'{dist_mi:0.2f}mi'
        item['mi_numeric'] = dist_mi
        routing_json = {"locations": [
            {"lat": lat,
             "lon": lon,
             "street": "Street1"},
            {"lat": item['stop_lat'],
             "lon": item['stop_lon'],
             "street":"Street2"}],
            "costing":"pedestrian",
            "units":"miles",
            "id": str(item['pattern'])}
        jp = json.dumps(routing_json)
        urls.append(f'http://brie.guineafowl-cloud.ts.net:8902/route?json={jp}')
        estimate_params.append(
            {
                'pattern_id': item['pattern'],
                'bus_location': item['vehicle_distance'],
                'stop_pattern_distance': item['stop_pattern_distance']
            }
        )
        pattern_id = int(item['pattern'])
        vehicle_distance = item['vehicle_distance']
        index.setdefault(pattern_id, {})[vehicle_distance] = item
    reqs = []
    for u in urls:
        reqs.append(grequests.get(u))
    if not skip_estimates:
        reqs.append(grequests.post('http://localhost:8500/estimates/', json={'estimates': estimate_params}))
    #print(f'estimate params: ', estimate_params)
    #print(reqs)

    def handler(request, exception):
        print(f'Issue with {request}: {exception}')

    responses = grequests.map(reqs, exception_handler=handler)
    print('index', index.keys())
    for resp in responses:
        #print(resp)
        if resp is None:
            continue
        if resp.status_code not in {200, 201}:
            continue
        jd = resp.json()
        if 'estimates' in jd:
            print(jd)
            for e in jd['estimates']:
                pattern = e['pattern']
                vehicle_dist = e['bus_location']
                eh = e['high']
                el = e['low']
                eststr = f'{el}-{eh} min'
                index[pattern][vehicle_dist]['estimate'] = eststr
                index[pattern][vehicle_dist]['el'] = el
                index[pattern][vehicle_dist]['eh'] = eh
                index[pattern][vehicle_dist]['raw_estimate'] = e
        else:
            summary = jd['trip']['summary']
            #print(jd)
            seconds = summary['time']
            miles = summary['length']
            pattern = int(jd['id'])
            for vd in index[pattern].values():
                vd['walk_time'] = round(seconds / 60.0)
                vd['walk_dist'] = f'{miles:0.2f}'
    for item in results:
        directions.setdefault(item['direction'], []).append(item)
    directions2 = {}
    for k, v in directions.items():
        #directions.setdefault(item['direction'], []).append(item)
        directions2[k] = route_coalesce(v)
        #v.sort(key=lambda x: x['route'])
    raw = json.dumps(directions2, indent=4)
    return render_template('bus_status.html', results=directions2, raw=raw, lat=lat, lon=lon)


@app.route('/detail')
def deatail():
    backend = 'http://localhost:8500/detail'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    return render_template('detail.html', detail=d['detail'])
