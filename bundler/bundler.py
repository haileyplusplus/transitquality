#!/usr/bin/env python3

import argparse
import dataclasses
import tempfile
import lzma
import datetime
import json
import pytz
import os
from pathlib import Path
from tarfile import TarFile

from s3path import S3Path

from backend.util import Util


@dataclasses.dataclass
class Stats:
    first = None
    last = None
    prev = None
    max_interval = None
    index = 0
    total = 0
    processed = 0
    request_count = 0

    def stats(self):
        return {'processed': self.processed, 'total': self.total}

    def index_stats(self):
        if not self.first:
            return {}
        return {'first': self.first.isoformat(),
                'last': self.last.isoformat(),
                'max_interval_seconds': self.max_interval.total_seconds()
                }


class Bundler:
    THRESH = datetime.timedelta(minutes=10)

    def __init__(self, data_dir: Path | S3Path, day: str):
        self.data_dir = data_dir
        self.day = day.replace('-', '')
        self.route_index = {}
        self.patterns = {}
        #self.requests_out = []
        self.bus_stats = Stats()
        self.train_stats = Stats()
        self.outfile = self.data_dir / f'bundle-{self.day}.tar.lz'
        self.done = False
        self.success = None
        self.start_time = datetime.datetime.now(tz=datetime.UTC)
        self.end_time = None
        #self.tempout = None
        self.tempdir = tempfile.TemporaryDirectory()
        self.traindir = (Path(self.tempdir.name) / 'train')
        self.traindir.mkdir()
        self.prepare_output()

    def __del__(self):
        self.tempdir.cleanup()

    def prepare_output(self):
        if self.outfile.exists():
            print(f'Not overwriting existing file')
            self.success = False
            self.done = True
            return
        #self.tempout = tempfile.NamedTemporaryFile('wb')
        #self.tarfile = tarfile.open(self.tempout.name, mode='w:xz')

    def is_done(self):
        return self.done

    def status(self):
        return {'active': True, 'day': self.day,
                'bus': self.bus_stats.stats(),
                'train': self.train_stats.stats(),
                'outfile': self.outfile.name,
                'done': self.done,
                'success': self.success,
                'start': self.start_time,
                'end': self.end_time}

    def store_routes(self, routes, request_time: datetime.datetime, file):
        for rt in routes.split(','):
            val = (self.bus_stats.index, file.parent.name, file.name, request_time.isoformat())
            self.route_index.setdefault(rt, []).append(val)
        self.bus_stats.index += 1

    def output(self):
        # fix this
        #if not self.complete(thresh=self.THRESH):
        #    print(f'Not bundling incomplete day {self.day}')
        #    return False
        with tempfile.NamedTemporaryFile('wb') as tempout:
            tempout.close()
            with TarFile.open(tempout.name, mode='w:xz') as tarfile:
                #with bz2.open(self.outfile, 'wt', encoding='UTF-8') as jfh:
                patterns = {}
                for k, v in self.patterns.items():
                    patterns[k] = list(v)
                outdict = {'v': '2.0',
                           'bundle_type': 'vehicle',
                           'day': self.day,
                           'bus': self.bus_stats.index_stats(),
                           'train': self.train_stats.index_stats(),
                           'index': self.route_index,
                           'patterns': patterns}
                td = Path(self.tempdir.name)
                for f in td.glob('*.json'):
                    tarfile.add(f, arcname=f'bus/{f.name}')
                for f in self.traindir.glob('*.json'):
                    tarfile.add(f, arcname=f'train/{f.name}')
                index = Path(self.tempdir.name) / 'index.json'
                with index.open('w') as wfh:
                    json.dump(outdict, wfh)
                tarfile.add(td / 'index.json', arcname='index.json')
            self.outfile.write_bytes(open(tempout.name, 'rb').read())

    # def summary(self, by_route=False):
    #     print(f'Summary for {self.day}:')
    #     print()
    #     print(f'First update: {self.first.isoformat()}')
    #     print(f'Last update: {self.last.isoformat()}')
    #     print(f'Max interval: {self.max_interval.total_seconds()} seconds')
    #     print(f'Total requests: {self.request_count}')
    #     print()
    #     if by_route:
    #         print(f'Requests by route:')
    #         for k, v in sorted(self.route_index.items()):
    #             print(f'  {k:4}: {len(v):5d}')

    # def complete(self, thresh: datetime.timedelta):
    #     day_start = datetime.datetime.strptime(self.day, '%Y%m%d').replace(tzinfo=datetime.UTC)
    #     day_end = day_start + datetime.timedelta(days=1)
    #     if self.max_interval > thresh:
    #         return False
    #     if self.first - day_start > thresh:
    #         return False
    #     if day_end - self.last > thresh:
    #         return False
    #     return True

    def process_patterns(self, r):
        updates = r.get('response', {}).get('bustime-response', {}).get('vehicle', [])
        for u in updates:
            route = u['rt']
            pid = u['pid']
            self.patterns.setdefault(route, set([])).add(pid)

    def scan_file(self, file: Path | S3Path, bus, stats, cmd):
        with file.open() as fh:
            d = json.load(fh)
            if d.get('command') != cmd:
                return False
            stats.index = 0
            for r in d.get('requests', []):
                routes = r['request_args']['rt']
                request_time = datetime.datetime.fromisoformat(r['request_time'])
                if bus:
                    self.store_routes(routes, request_time, file)
                    self.process_patterns(r)
                if stats.first is None:
                    stats.first = request_time
                else:
                    stats.last = request_time
                    interval = request_time - stats.prev
                    if stats.max_interval is None:
                        stats.max_interval = interval
                    else:
                        stats.max_interval = max(interval, stats.max_interval)
                stats.prev = request_time
                stats.request_count += 1
                #self.requests_out.append(r)
            filename = f'{file.parent.name}{file.name}'
            if bus:
                outfile = (Path(self.tempdir.name) / filename)
            else:
                outfile = self.traindir / filename
            with outfile.open('w') as wfh:
                json.dump(d, wfh)
        return True

    def scan_day_inner(self, bus=True):
        if bus:
            stats = self.bus_stats
            cmd = 'getvehicles'
        else:
            stats = self.train_stats
            cmd = 'ttpositions.aspx'
        if self.outfile.exists():
            self.done = True
            return False
        naive_day = datetime.datetime.strptime(self.day, '%Y%m%d')
        next_day = naive_day + datetime.timedelta(days=1)
        # start service day at 3am
        chicago_day_start = Util.CTA_TIMEZONE.localize(naive_day.replace(hour=3))
        chicago_day_end = Util.CTA_TIMEZONE.localize(next_day.replace(hour=3))
        files = []
        for dir_ in [self.data_dir / cmd / self.day,
                     self.data_dir / cmd / next_day.strftime('%Y%m%d')]:
            print(f'Processing files in {dir_.name}')
            if not dir_.exists():
                print(f'Directory not found')
                self.done = True
                return False
            files += sorted(dir_.glob('t??????z.json'))
        process = []
        print(f'Processing from {chicago_day_start.isoformat()} to {chicago_day_end.isoformat()}')
        for f in files:
            parent = f.parent.name
            name = f.name
            filedate = pytz.UTC.localize(
                datetime.datetime.strptime(f'{parent}{name}', '%Y%m%dt%H%M%Sz.json'))
            if filedate < chicago_day_start or filedate >= chicago_day_end:
                continue
            process.append(f)
        stats.total = len(process)
        for f in process:
            self.scan_file(f, bus, stats, cmd)
            stats.processed += 1
        #self.summary()

    def scan_day(self):
        if self.is_done():
            return False
        self.scan_day_inner(bus=True)
        self.scan_day_inner(bus=False)
        self.output()
        self.done = True
        self.success = True
        self.end_time = datetime.datetime.now(tz=datetime.UTC)
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


