import json

from flask import Flask, render_template, request


import requests

app = Flask(__name__)


@app.route('/')
def main():
    return render_template('main.html')


@app.route('/estimates')
def estimates():
    #lat = request.args.get('lat')
    #lon = request.args.get('lon')
    backend = 'http://localhost:8500/nearest-estimates'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    results = d['results']
    directions = {'Northbound': [], 'Southbound': [], 'Eastbound': [], 'Westbound': []}
    for item in results:
        dist_mi = item['bus_distance'] / 5280.0
        item['mi'] = f'{dist_mi:0.2f}mi'
        directions.setdefault(item['direction'], []).append(item)
    for v in directions.values():
        v.sort(key=lambda x: x['route'])
    raw = json.dumps(d, indent=4)
    return render_template('bus_status.html', results=directions, raw=raw)


@app.route('/detail')
def deatail():
    backend = 'http://localhost:8500/detail'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    d = resp.json()
    return render_template('detail.html', detail=d['detail'])