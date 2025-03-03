import datetime
import json

from flask import Flask, render_template, request

import grequests
import requests

from fastapi.encoders import jsonable_encoder
from interfaces import Q_, ureg
from interfaces.estimates import BusResponse, TrainEstimate, TrainResponse, TransitEstimate, StopEstimates, \
    StopEstimate, EstimateResponse

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


def route_coalesce(dirname, v):
    routes = {}
    for item in v:
        route = item.route
        routes.setdefault(route, []).append(item)
    results = []
    ureg.formatter.default_format = '.2fP'
    for k, v in sorted(routes.items()):
        routev = []
        for d in v:
            print(f'coalesce {dirname} {k}  vp {d.vehicle_position}  sp {d.stop_position}  le {d.low_estimate}')
            routev.append(d)
            d.age = td_round(d.age)
            d.distance_from_vehicle = d.distance_from_vehicle.to(ureg.miles)
            #print(json.dumps(d, indent=4))
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
                #routev.append(d)
                continue
            #predicted = d.predicted_minutes
            # age = d.age
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
                item = d
                print(f'  filtering out item due to walk time: item {item.pattern} vid ? vp {item.vehicle_position}  sp {item.stop_position}  le {item.low_estimate}  he {item.high_estimate}')
                d.display = False
                continue
            # age_minutes = round(d['age'] / 60)
            # d['age'] = round(d['age'])
            # d['old_estimate'] = f'{el}-{eh} min'
            # d['estimate'] = f'{el}-{eh} min'
            elm = round(d.low_estimate.total_seconds() / 60)
            ehm = round(d.high_estimate.total_seconds() / 60)
            d.displayed_estimate = f'{elm}-{ehm} min'
            #routev.append(d)
        routev.sort(key=lambda x: x.low_estimate)
        for item in routev:
            if isinstance(item, TrainEstimate):
                vehicle = item.run
            else:
                vehicle = item.vehicle
            print(f'  item {item.pattern} vid {vehicle} vp {item.vehicle_position}  sp {item.stop_position}  le {item.low_estimate}  he {item.high_estimate}')
        displayed = 0
        for r in routev:
            if r.display:
                displayed += 1
            if displayed > 2:
                r.display = False
        results += routev
    return results


@app.route('/estimates')
def estimates():
    lat = request.args.get('lat')
    lon = request.args.get('lon')
    print(f'#############  Start estimates query handing {lat} {lon}')
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
    ests = {}
    for item in results:
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
        estimate_key = (item.pattern, item.stop_position)
        ests.setdefault(estimate_key,
                        StopEstimate(
                            pattern_id=item.pattern,
                            stop_position=item.stop_position,
                            vehicle_positions = [],
                        )
                        ).vehicle_positions.append(item.vehicle_position)
        pattern_id = item.pattern
        vehicle_distance = round(item.vehicle_position.m)
        index.setdefault(pattern_id, {})[vehicle_distance] = item
    reqs = []
    for u in urls:
        reqs.append(grequests.get(u))
    estimates_query = StopEstimates(estimates=sorted(ests.values()))
    if not skip_estimates:
        reqs.append(grequests.post('http://localhost:8500/estimates/', data=estimates_query.model_dump_json()))
        print(f'Requesting estimate http://localhost:8500/estimates/ post {estimates_query.model_dump_json(indent=4)}')
    #print(f'estimate params: ', estimate_params)
    #print(reqs)

    def handler(request, exception):
        print(f'Issue with {request}: {exception}')

    responses = grequests.map(reqs, exception_handler=handler)
    print('index', index.keys())
    print(f'Sent {len(reqs)} requests and got {len(responses)} responses')
    for resp in responses:
        if resp is None:
            print(f'  Got null response')
            continue
        if resp.status_code not in {200, 201}:
            print(f'   Response status code to {resp.request.url}: {resp.staus_code}')
            continue

        if '/estimates' in resp.request.url:
            estimate_response: EstimateResponse = EstimateResponse.model_validate_json(resp.text)
            for p in estimate_response.patterns:
                pattern_id = p.pattern_id
                for se in p.single_estimates:
                    #vehicle_position = se.vehicle_position
                    # TODO: fix this
                    vehicle_position = round(se.vehicle_position.m)
                    if vehicle_position in index[pattern_id]:
                        index[pattern_id][vehicle_position].low_estimate = se.low_estimate
                        index[pattern_id][vehicle_position].high_estimate = se.high_estimate
                        index[pattern_id][vehicle_position].trace_info = se.info
                    else:
                        print(f'Warning: pattern {pattern_id} vehicle position missing {vehicle_position}')
        else:
            jd = resp.json()
            summary = jd['trip']['summary']
            #print(jd)
            seconds = summary['time']
            miles = summary['length']
            pattern = int(jd['id'])
            for vd in index[pattern].values():
                vd.walk_time = datetime.timedelta(seconds=int(seconds))
                vd.walk_distance = miles * ureg.miles
                #print(vd.walk_distance)
    for item in results:
        directions.setdefault(item.direction, []).append(item)
    directions2 = {}
    for k, v in directions.items():
        #directions.setdefault(item.direction, []).append(item)
        directions2[k] = route_coalesce(k, v)
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
