#!/usr/bin/env python3

import argparse
import datetime
import json
from pathlib import Path


class Bundler:
    def __init__(self, data_dir: Path, day: str):
        self.data_dir = data_dir
        self.day = day.replace('-', '')
        self.first = None
        self.last = None
        self.prev = None
        self.max_interval = None
        self.index = 0
        self.route_index = {}
        self.requests_out = []

    def store_routes(self, routes, request_time: datetime.datetime):
        for rt in routes.split(','):
            val = (self.index, request_time.isoformat())
            self.route_index.setdefault(rt, []).append(val)
        self.index += 1

    def output(self):
        outfile = self.data_dir / f'bundle-{self.day}.json'
        if outfile.exists():
            print(f'Not overwriting existing file')
            return False
        with open(outfile, 'w') as jfh:
            outdict = {'v': '2.0',
                       'command': 'vehiclebundle',
                       'day': self.day,
                       'first': self.first.isoformat(),
                       'last': self.last.isoformat(),
                       'index': self.route_index,
                       'requests': self.requests_out}
            json.dump(outdict, jfh)

    def summary(self):
        print(f'Summary for {self.day}:')
        print()
        print(f'First update: {self.first.isoformat()}')
        print(f'Last update: {self.last.isoformat()}')
        print(f'Max interval: {self.max_interval.total_seconds()} seconds')
        print(f'Total requests: {len(self.requests_out)}')
        print()
        print(f'Requests by route:')
        for k, v in sorted(self.route_index.items()):
            print(f'  {k:4}: {len(v):5d}')

    def scan_file(self, file: Path):
        with open(file) as fh:
            d = json.load(fh)
            if d.get('command') != 'getvehicles':
                return False
            for r in d.get('requests', []):
                routes = r['request_args']['rt']
                request_time = datetime.datetime.fromisoformat(r['request_time'])
                self.store_routes(routes, request_time)
                if self.first is None:
                    self.first = request_time
                else:
                    self.last = request_time
                    interval = request_time - self.prev
                    if self.max_interval is None:
                        self.max_interval = interval
                    else:
                        self.max_interval = max(interval, self.max_interval)
                self.prev = request_time
                self.requests_out.append(r)
        return True

    def scan_day(self):
        dir_ = self.data_dir / 'getvehicles' / self.day
        if not dir_.exists():
            print(f'Directory not found')
            return False
        files = sorted(dir_.glob('t??????z.json'))
        for f in files:
            self.scan_file(f)
        self.summary()
        self.output()
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Package scraped data.')
    parser.add_argument('--show_stats', action='store_true',
                        help='Show bundling stats.')
    parser.add_argument('--day', type=str,
                        help='Bundle a single day.')
    parser.add_argument('--data_dir', type=str, default='~/transit/bustracker/raw',
                        help='Data directory with scraped files')
    args = parser.parse_args()
    data_dir = Path(args.data_dir).expanduser()
    day = args.day
    b = Bundler(data_dir, day)
    b.scan_day()

