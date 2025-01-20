import argparse
import json

from s3path import S3Path
from pathlib import Path
import boto3
import botocore.exceptions
from typing import Iterable

import gtfs_kit

from bundler.bundlereader import BundleReader, MemoryPatternManager
from bundler.schedule_writer import ScheduleWriter
from bundler.interpolate_bus import BusTripsHandler
from bundler.interpolate_train import TrainTripsHandler, TrainManager


class RouteInterpolate:
    BUCKET = S3Path('/transitquality2024/bustracker/raw')

    def __init__(self):
        #self.workdir = tempfile.TemporaryDirectory()
        self.workpath = Path('/transitworking')
        try:
            boto3.setup_default_session(profile_name='transitquality_boto')
        except botocore.exceptions.ProfileNotFound:
            print(f'Not using boto profile')
        #with pattern_file.open() as jfh:
        #    self.patterns = json.load(jfh)
        self.load_working()

    def load_working(self):
        # TODO: finer grained date parsing
        bundles: Iterable[S3Path] = self.BUCKET.glob('bundle-2025????.tar.lz')
        pattern_file = self.BUCKET / 'patterns2025.json'
        items = list(bundles)
        items.append(pattern_file)
        for b in items:
            existing = self.workpath / b.name
            if existing.exists():
                continue
            with (self.workpath / b.name).open('wb') as ofh:
                with b.open('rb') as fh:
                    ofh.write(fh.read())


class Interpolator:
    TRAIN_ROUTES = ['red', 'p', 'y', 'blue', 'pink', 'g', 'org', 'brn']

    def __init__(self, bundle_path: Path | S3Path, gtfs_file: Path | S3Path, daystr: str, routes=None):
        self.bundle_path = bundle_path
        self.daystr = daystr
        self.mpm = None
        self.reader = None
        self.gtfs_file = gtfs_file
        self.feed = None
        self.load_bundle(routes)
        self.load_feed()
        # for debugging
        self.current = None

    def load_feed(self):
        self.feed = gtfs_kit.read_feed(self.gtfs_file, dist_units='ft')

    def load_bundle(self, routes):
        #bundle_file = Path(args.bundle_file).expanduser()
        bundle_file = self.bundle_path / f'bundle-{self.daystr}.tar.lz'
        # print(f'Routes: {args.routes}')
        # if not args.routes:
        #     routes = None
        # else:
        #     routes = args.routes.split(',')
        self.reader = BundleReader(bundle_file, routes)
        self.reader.process_bundle_file()
        pdict = json.load((bundle_file.parent / 'patterns2025.json').open())
        self.mpm = MemoryPatternManager()
        self.mpm.parse(pdict['patterns'])
        # vsamp = r.routes['8'].get_vehicle('1310')

    def write_bundle(self, output_path: Path | S3Path):
        #writer = ScheduleWriter(Path('/tmp/take2'), r.day)
        writer = ScheduleWriter(output_path, self.daystr)
        self.mpm.write_all_stops(writer)
        writer.write('calendar_dates', {
            'service_id': self.daystr,
            'date': self.daystr,
            'exception_type': 1
        })
        for x in self.reader.routes_to_parse:
            writer.write('routes', {
                'route_id': x,
                'route_short_name': x,
                'route_type': 3
            })
        agency_file = writer.output_path / 'agency.txt'
        with agency_file.open('w') as afh:
            afh.write(
                'agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n0,Chicago Transit Authority,http://transitchicago.com,America/Chicago,en,1-888-YOURCTA,http://www.transitchicago.com/travel_information/fares/default.aspx\n')
            # mpm.write_routes(dw)
        train_manager = TrainManager(self.daystr, self.feed, writer)
        TrainTripsHandler.write_all_stops(self.feed, writer)
        for route, vsamp in self.reader.generate_vehicles():
            if route.route in self.TRAIN_ROUTES:
                th = TrainTripsHandler(train_manager, route, vsamp)
            else:
                th = BusTripsHandler(route, self.daystr, vsamp, self.mpm, writer)
            self.current = th
            th.process_all_trips()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bundles')
    parser.add_argument('--bundle_path', type=str,
                        help='Path with bus/train scrape data.')
    parser.add_argument('--bundle_day', type=str,
                        help='Day to scrape (YYYYmmdd).')
    parser.add_argument('--gtfs_file', type=str,
                        help='Applicable GTFS file.')
    parser.add_argument('--output_path', type=str,
                        help='Output path.')
    parser.add_argument('--routes', type=str,
                        help='Filter to this comma-separated list of routes.')
    args = parser.parse_args()
    if not args.routes:
        routes = None
    else:
        routes = args.routes.split(',')
    gtfs_file = Path(args.gtfs_file).expanduser()
    bundle_path = Path(args.bundle_path).expanduser()
    output_path = Path(args.output_path).expanduser()
    daystr = args.bundle_day
    interpolator = Interpolator(bundle_path, gtfs_file, daystr, routes)
    interpolator.write_bundle(output_path)
