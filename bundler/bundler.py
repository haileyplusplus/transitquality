#!/usr/bin/env python3

import argparse
import bz2
import datetime
import json
from pathlib import Path

from s3path import S3Path


class Bundler:
    THRESH = datetime.timedelta(minutes=10)

    def __init__(self, data_dir: Path | S3Path, day: str):
        self.data_dir = data_dir
        self.day = day.replace('-', '')
        self.first = None
        self.last = None
        self.prev = None
        self.max_interval = None
        self.index = 0
        self.total = 0
        self.processed = 0
        self.route_index = {}
        self.requests_out = []
        self.outfile = self.data_dir / f'bundle-{self.day}.json.bz2'
        self.done = False
        self.success = False

    def done(self):
        return self.done

    def status(self):
        return {'active': True, 'day': self.day,
                'processed': self.processed, 'total': self.total}

    def store_routes(self, routes, request_time: datetime.datetime):
        for rt in routes.split(','):
            val = (self.index, request_time.isoformat())
            self.route_index.setdefault(rt, []).append(val)
        self.index += 1

    def output(self):
        if not self.complete(thresh=self.THRESH):
            print(f'Not bundling incomplete day {self.day}')
            return False
        if self.outfile.exists():
            print(f'Not overwriting existing file')
            return False
        with bz2.open(self.outfile, 'wt', encoding='UTF-8') as jfh:
            outdict = {'v': '2.0',
                       'bundle_type': 'vehicle',
                       'day': self.day,
                       'first': self.first.isoformat(),
                       'last': self.last.isoformat(),
                       'max_interval_seconds': self.max_interval.total_seconds(),
                       'index': self.route_index,
                       'requests': self.requests_out}
            json.dump(outdict, jfh)

    def summary(self, by_route=False):
        print(f'Summary for {self.day}:')
        print()
        print(f'First update: {self.first.isoformat()}')
        print(f'Last update: {self.last.isoformat()}')
        print(f'Max interval: {self.max_interval.total_seconds()} seconds')
        print(f'Total requests: {len(self.requests_out)}')
        print()
        if by_route:
            print(f'Requests by route:')
            for k, v in sorted(self.route_index.items()):
                print(f'  {k:4}: {len(v):5d}')

    def complete(self, thresh: datetime.timedelta):
        day_start = datetime.datetime.strptime(self.day, '%Y%m%d').replace(tzinfo=datetime.UTC)
        day_end = day_start + datetime.timedelta(days=1)
        if self.max_interval > thresh:
            return False
        if self.first - day_start > thresh:
            return False
        if day_end - self.last > thresh:
            return False
        return True

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
        if self.outfile.exists():
            self.done = True
            return False
        dir_ = self.data_dir / 'getvehicles' / self.day
        if not dir_.exists():
            print(f'Directory not found')
            self.done = True
            return False
        files = sorted(dir_.glob('t??????z.json'))
        self.total = len(files)
        for f in files:
            self.scan_file(f)
            self.processed += 1
        self.summary()
        self.output()
        self.done = True
        self.success = True
        return True


def bundle_all(data_dir):
    vehicle_dir = data_dir / 'getvehicles'
    count = 0
    for day_ in vehicle_dir.glob('20??????'):
        bundler = Bundler(data_dir, day=day_.name)
        if bundler.scan_day():
            count += 1
    print(f'Bundled {count} days')


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
    if args.day:
        day = args.day
        b = Bundler(data_dir, day)
        b.scan_day()
    else:
        print(f'Bundling all files')
        bundle_all(data_dir)


