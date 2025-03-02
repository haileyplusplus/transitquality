import datetime
import json

from flask import Flask, render_template, request

import grequests
import requests

from fastapi.encoders import jsonable_encoder
from interfaces import Q_, ureg
from interfaces.estimates import BusResponse, TrainEstimate, TrainResponse, TransitEstimate, StopEstimates, StopEstimate

app = Flask(__name__)


"""
http://localhost:7001/api/v1/plan?directModes=WALK&fromPlace=41.903914,-87.632892,0&toPlace=chicago_2034
"""


@app.route('/')
def main():
    return render_template('main.html')


def td_round(x: datetime.timedelta):
    x = datetime.timedelta(seconds=round(x.total_seconds()))
    return x


def route_coalesce(v):
    routes = {}
    for item in v:
        route = item.route
        routes.setdefault(route, []).append(item)
    results = []
    ureg.formatter.default_format = '.2fP'
    for k, v in sorted(routes.items()):
        routev = []
        for d in v:
            print(f'coalesce {k}')
            d.age = td_round(d.age)
            d.distance_from_vehicle = d.distance_from_vehicle.to(ureg.miles)
            #print(json.dumps(d, indent=4))
            if d.low_estimate is None:
                miles = d.distance_from_vehicle.to(ureg.miles)
                if miles <= ureg.miles * 1:
                    routev.append(d)
                # consider better values
                d.low_estimate = datetime.timedelta(minutes=1)
                d.high_estimate = datetime.timedelta(minutes=5)
                continue
            #predicted = d.predicted_minutes
            # age = d.age
            # el = d.low_estimate
            # eh = d.high_estimate
            d.low_estimate -= d.age
            d.high_estimate -= d.age
            if d.waiting_to_depart and d.predicted_minutes:
                #el += predicted * 60
                #eh += predicted * 60
                d.low_estimate += d.predicted_minutes
                d.high_estimate += d.predicted_minutes
            # el = round((d.low_estimate - age) / 60)
            # eh = round((d.high_estimate - age) / 60)
            # d.low_estimate = el
            # d.high_estimate = eh


            if d.walk_time > datetime.timedelta(0) and datetime.timedelta(0) <= d.high_estimate <= d.walk_time:
                continue
            # age_minutes = round(d['age'] / 60)
            # d['age'] = round(d['age'])
            # d['old_estimate'] = f'{el}-{eh} min'
            # d['estimate'] = f'{el}-{eh} min'
            elm = round(d.low_estimate.total_seconds() / 60)
            ehm = round(d.high_estimate.total_seconds() / 60)
            d.displayed_estimate = f'{elm}-{ehm} min'
            routev.append(d)
        routev.sort(key=lambda x: x.low_estimate)
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
    #d = resp.json()
    #results = d['results']
    results: list[TransitEstimate] = []
    bus_response: BusResponse = BusResponse.model_validate_json(resp.text)
    results += bus_response.results
    train_resp = requests.get('http://localhost:8500/nearest-trains', params=request.args)
    if resp.status_code == 200:
        train_response: TrainResponse = TrainResponse.model_validate_json(train_resp.text)
        #results += train_resp.json()['results']
        results += train_response.results
    directions = {'Northbound': [], 'Southbound': [], 'Eastbound': [], 'Westbound': []}
    urls = []
    #estimate_params: list[dict] = []
    index = {}
    ests = []
    for item in results:
        # if item.pattern >= 308500000:
        #     dist_mi = item.bus_distance / 1609.34
        # else:
        #     dist_mi = item.bus_distance / 5280.0
        # item.mi = f'{dist_mi:0.2f}mi'
        # item.mi_numeric = dist_mi
        routing_json = {"locations": [
            {"lat": lat,
             "lon": lon,
             "street": "Street1"},
            {"lat": item.stop_lat,
             "lon": item.stop_lon,
             "street": "Street2"}],
            "costing": "pedestrian",
            "units": "miles",
            "id": str(item.pattern)}
        jp = json.dumps(routing_json)
        urls.append(f'http://brie.guineafowl-cloud.ts.net:8902/route?json={jp}')
        ests.append(
            StopEstimate(
                pattern_id=item.pattern,
                bus_location=item.vehicle_position,
                stop_pattern_distance=item.stop_position
            )
        )
        # estimate_params.append(
        #     {
        #         'pattern_id': item.pattern,
        #         'bus_location': item.vehicle_position,
        #         'stop_pattern_distance': item.stop_position
        #     }
        # )
        pattern_id = item.pattern
        vehicle_distance = round(item.vehicle_position.m)
        index.setdefault(pattern_id, {})[vehicle_distance] = item
    reqs = []
    for u in urls:
        reqs.append(grequests.get(u))
    estimates_query = StopEstimates(estimates=ests)
    if not skip_estimates:
        reqs.append(grequests.post('http://localhost:8500/estimates/', data=estimates_query.model_dump_json()))
    #print(f'estimate params: ', estimate_params)
    #print(reqs)

    def handler(request, exception):
        print(f'Issue with {request}: {exception}')

    responses = grequests.map(reqs, exception_handler=handler)
    print('index', index.keys())
    for resp in responses:
        if resp is None:
            continue
        if resp.status_code not in {200, 201}:
            continue

        jd = resp.json()
        if 'estimates' in jd:
            print(jd)
            for e in jd['estimates']:
                pattern = e['pattern']
                # TODO: fix this
                vehicle_dist = round(e['bus_location']['_magnitude'])
                eh = e['high']
                el = e['low']
                #eststr = f'{el}-{eh} min'
                #index[pattern][vehicle_dist].displayed_estimate = eststr
                if el and eh:
                    index[pattern][vehicle_dist].low_estimate = datetime.timedelta(seconds=el)
                    index[pattern][vehicle_dist].high_estimate = datetime.timedelta(seconds=eh)
                index[pattern][vehicle_dist].trace_info = e
        else:
            summary = jd['trip']['summary']
            #print(jd)
            seconds = summary['time']
            miles = summary['length']
            pattern = int(jd['id'])
            for vd in index[pattern].values():
                vd.walk_time = datetime.timedelta(seconds=int(seconds))
                vd.walk_distance = miles * ureg.miles
                print(vd.walk_distance)
    for item in results:
        directions.setdefault(item.direction, []).append(item)
    directions2 = {}
    for k, v in directions.items():
        #directions.setdefault(item.direction, []).append(item)
        directions2[k] = route_coalesce(v)
    raw = json.dumps(jsonable_encoder(directions2), indent=4)
    #raw = directions2.model_dump_json
    return render_template('bus_status.html', results=directions2, raw=raw, lat=lat, lon=lon)


@app.route('/detail')
def deatail():
    backend = 'http://localhost:8500/detail'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    return render_template('detail.html', detail=d['detail'])
