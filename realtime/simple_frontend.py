import json

from flask import Flask, render_template, request


import requests

app = Flask(__name__)


@app.route('/')
def main():
    return """
<p>Choose location</p>
<form method="get" action="/estimates">
Lat: <input type="text" name="lat" value="41.903914"/><br/>
Lon: <input type="text" name="lon" value="-87.632892"/><br/>
<input type="submit"/>
</form>    
    """


@app.route('/estimates')
def estimates():
    #lat = request.args.get('lat')
    #lon = request.args.get('lon')
    backend = 'http://localhost:8500/nearest-estimates'
    resp = requests.get(backend, params=request.args)
    if resp.status_code != 200:
        return f'Error handling request'
    results = resp.json()['results']
    directions = {'Northbound': [], 'Southbound': [], 'Eastbound': [], 'Westbound': []}
    for item in results:
        dist_mi = item['bus_distance'] / 5280.0
        item['mi'] = f'{dist_mi:0.2f}mi'
        directions.setdefault(item['direction'], []).append(item)
    raw = json.dumps(results, indent=4)
    return render_template('bus_status.html', results=directions, raw=raw)
