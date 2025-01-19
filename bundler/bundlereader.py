#!/usr/bin/env python3

import argparse
import json
import re
from enum import StrEnum
from pathlib import Path
from tarfile import TarFile

import pandas as pd
import peewee
from peewee import SqliteDatabase

from analysis.processor import PatternParser
from analysis.datamodels import Pattern, Direction, Stop, PatternStop, Waypoint, PatternIndex, database_proxy


class MemoryPatternManager:
    def __init__(self):
        Pattern.timestamp = peewee.DateTimeField(null=True)
        self.db = SqliteDatabase(':memory:')
        database_proxy.initialize(self.db)
        self.db.connect()
        self.db.create_tables([Direction, Pattern, Stop, PatternStop,
                               Waypoint, PatternIndex])
        self.parser = PatternParser(self.db, None, False)

    def parse(self, patterns: dict):
        for p in patterns.values():
            self.parser.parse_inner(p)

    def write_all_stops(self, writer):
        for stop in Stop.select():
            writer.write('stops', {
                'stop_id': stop.stop_id,
                'stop_name': stop.stop_name,
                'stop_lat': stop.lat,
                'stop_lon': stop.lon,
            })

    def write_routes(self, writer):
        for pattern in PatternIndex.select(PatternIndex.route).distinct():
            writer.writerow({
                'route_id': pattern.route,
                'route_short_name': pattern.route,
                'route_type': 3
            })


    def get_stops(self, pid: int):
        p = Pattern.get_or_none(Pattern.pattern_id == pid)
        if p is None:
            return []
        return p.stops


class Mode(StrEnum):
    BUS = 'bus'
    TRAIN = 'train'


class Route:
    def __init__(self, route: str, indexdict: dict):
        self.route = route
        self.vehicles = {}
        self.indexdict = indexdict

    def __hash__(self):
        return hash(self.route)

    def __eq__(self, other):
        return self.route == other.route

    def __lt__(self, other):
        return self.route < other.route

    def get_vehicle(self, vid: str):
        v = self.vehicles[vid]
        return pd.DataFrame(v)

    def process_file(self, filename, contents):
        print(f'Processing {filename}')
        indices = self.indexdict[filename]
        for i in indices:
            if filename.startswith(Mode.BUS):
                self.process_bus(contents, i)
            elif filename.startswith(Mode.TRAIN):
                self.process_train(contents, i)
            else:
                raise ValueError(f'Unexpected filename: {filename}')

    def process_bus(self, contents, i):
        brdict = contents['requests'][i]['response']['bustime-response']
        for v in brdict.get('vehicle', []):
            if v['rt'] == self.route:
                vehicle = v['vid']
                self.vehicles.setdefault(vehicle, []).append(v)

    def process_train(self, contents, i):
        trdict = contents['requests'][i]['response']['ctatt']
        if 'route' not in trdict:
            return
        train_positions = None
        for rd in trdict['route']:
            if rd['@name'] == self.route:
                train_positions = rd['train']
                break
        if train_positions is None:
            raise ValueError
        if isinstance(train_positions, dict):
            self.vehicles.setdefault(train_positions['rn'], []).append(train_positions)
        else:
            for v in train_positions:
                self.vehicles.setdefault(v['rn'], []).append(v)


class BundleReader:
    DAY_RE = re.compile(r'(20\d{6})')

    def __init__(self, bundle_file: Path, routes: list[str] | None):
        self.bundle_file = bundle_file
        self.routes_to_parse = routes
        self.routes = {}
        self.index = None
        self.day = self.DAY_RE.search(self.bundle_file.name).groups()[0]

    def routes_from_index(self):
        return self.index['index'].keys()

    def process_bundle_file(self, mode=Mode.BUS):
        with TarFile.open(self.bundle_file, 'r:xz') as archive:
            index_fh = archive.extractfile(f'index.json')
            self.index = json.load(index_fh)
            if self.routes_to_parse is None:
                self.routes_to_parse = self.routes_from_index()
            for r in self.routes_to_parse:
                self.routes.setdefault(r, Route(r, self.index['index'][r]))
            all_files = {}
            for route in self.routes.values():
                for k in route.indexdict.keys():
                    all_files.setdefault(k, set([])).add(route)
            for filename, routes in sorted(all_files.items()):
                fh = archive.extractfile(filename)
                contents = json.load(fh)
                for route in routes:
                    route.process_file(filename, contents)

    def generate_vehicles(self):
        for r in self.routes.values():
            for vid in r.vehicles.keys():
                yield r, r.get_vehicle(vid)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bundles')
    parser.add_argument('--bundle_file', type=str,
                        help='File with bus scrape data.')
    parser.add_argument('--routes', type=str,
                        help='Comma-separated list of routes.')
    args = parser.parse_args()
    bundle_file = Path(args.bundle_file).expanduser()
    print(f'Routes: {args.routes}')
    if not args.routes:
        routes = None
    else:
        routes = args.routes.split(',')
    rr = BundleReader(bundle_file, routes)
    rr.process_bundle_file()
    pdd = json.load((bundle_file.parent / 'patterns2025.json').open())
    mpm = MemoryPatternManager()
    mpm.parse(pdd['patterns'])
