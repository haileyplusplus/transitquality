import datetime
import json

from flask import Flask, render_template, request

import grequests
import requests

from fastapi.encoders import jsonable_encoder
from interfaces import Q_, ureg
from interfaces.estimates import BusResponse, TrainEstimate, TrainResponse, TransitEstimate, StopEstimates, \
    StopEstimate, EstimateResponse, DetailRequest, CombinedResponseType, CombinedResponse
from realtime.assembly import NearStopQuery

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
    if not lat or not lon:
        print(f'Invalid estimates query')
        return f'Invalid estimates query {lat} {lon}'
    print(f'#############  Start estimates query handing {lat} {lon}')
    # q = NearStopQuery(lat=float(lat), lon=float(lon))
    backend = 'http://localhost:8500/combined-estimate'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    #directions2 = resp.json()
    #raw = json.dumps(jsonable_encoder(directions2), indent=4)
    ureg.formatter.default_format = '.2fP'
    directions2 = CombinedResponse.model_validate_json(resp.text)
    raw = directions2.model_dump_json(indent=4)
    return render_template('bus_status.html', results=directions2.response, raw=raw, lat=lat, lon=lon)


@app.route('/detail')
def detail():
    backend = 'http://localhost:8500/detail'
    detail_request = DetailRequest(
        pattern_id=request.args.get('pid'),
        stop_id=request.args.get('stop_id'),
        stop_position=request.args.get('stop_dist'),
        walk_time=request.args.get('walk_time')
    )
    resp = requests.post(backend, data=detail_request.model_dump_json())
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    if not d['detail']:
        return f'Not implemented'
    d['detail']['stop_name'] = request.args.get('stop_name')
    return render_template('detail.html', detail=d['detail'])
