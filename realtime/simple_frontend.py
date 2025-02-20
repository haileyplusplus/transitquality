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
        dist_mi = item['bus_distance'] / 5280.0
        item['mi'] = f'{dist_mi:0.2f}mi'
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
    for v in directions.values():
        v.sort(key=lambda x: x['route'])
    raw = json.dumps(d, indent=4)
    return render_template('bus_status.html', results=directions, raw=raw, lat=lat, lon=lon)


@app.route('/detail')
def deatail():
    backend = 'http://localhost:8500/detail'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    return render_template('detail.html', detail=d['detail'])