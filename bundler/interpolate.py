import argparse
import datetime
import threading
import csv
from types import SimpleNamespace

from s3path import S3Path
from pathlib import Path
import boto3
import botocore.exceptions
import tempfile
import json
from typing import Iterable

import pandas as pd

from bundler.bundlereader import BundleReader, MemoryPatternManager, Route
from backend.util import Util


class StopWriter:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.st_fh = (self.output_path / 'stop_times.txt').open('w')
        st_fieldnames = ['trip_id',
                         'arrival_time',
                         'departure_time',
                         'stop_id',
                         'stop_sequence',
                         'shape_dist_traveled']
        self.writer_stop_time = csv.DictWriter(self.st_fh, st_fieldnames)
        self.writer_stop_time.writeheader()
        self.trips_fh = (self.output_path / 'trips.txt').open('w')
        self.writer_trips = csv.DictWriter(
            self.trips_fh,
            ['route_id', 'service_id', 'trip_id']
        )
        self.writer_trips.writeheader()

    def write_stop_time(self, row: dict):
        self.writer_stop_time.writerow(row)

    def __del__(self):
        self.st_fh.close()


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


class TripsHandler:
    def __init__(self, routex: Route,
                 day: str,
                 vehicle_df: pd.DataFrame, mpm: MemoryPatternManager,
                 writer: StopWriter):
        self.route = routex
        self.day = day
        naive_day = datetime.datetime.strptime(self.day, '%Y%m%d')
        self.next_day_thresh = Util.CTA_TIMEZONE.localize(naive_day + datetime.timedelta(days=1))
        self.vehicle_df = vehicle_df
        self.vehicle_df['tmstmp'] = self.vehicle_df.loc[:, 'tmstmp'].apply(lambda x: int(Util.CTA_TIMEZONE.localize(
            datetime.datetime.strptime(x, '%Y%m%d %H:%M:%S')).timestamp()))
        self.trip_ids = list(vehicle_df.origtatripno.unique())
        self.error = None
        self.mpm = mpm
        self.output_df = pd.DataFrame()
        self.writer = writer

    def record_error(self, trip_id, msg):
        self.error = f'{trip_id}: {msg}'
        print(self.error)

    def gtfs_time(self, ts: datetime.datetime):
        if ts >= self.next_day_thresh:
            hour = ts.hour + 24
            return ts.strftime(f'{hour:02d}:%M:%S')
        return ts.strftime('%H:%M:%S')

    def process_all_trips(self):
        for trip_id in self.trip_ids:
            self.writer.writer_trips.writerow({
                'route_id': self.route.route,
                'service_id': self.day,
                'trip_id': trip_id,
            })
            self.process_trip(trip_id)

    def process_trip(self, trip_id: str):
        stops = []
        stop_index = {}
        df = self.vehicle_df[self.vehicle_df.origtatripno == trip_id]
        # TODO: rename
        vehicles_df = df[['tmstmp', 'pdist']]
        pids = df['pid'].unique()
        if len(pids) != 1:
            self.record_error(trip_id, f'Wrong number of pattern ids: {pids}')
            return
        pid = pids[0]
        for ps in self.mpm.get_stops(pid):
            stop_index[ps.stop.stop_id] = ps
            stops.append({
                'stpid': ps.stop.stop_id,
                'seq': ps.sequence_no,
                'pdist': ps.pattern_distance,
            })
        if not stops:
            self.record_error(trip_id=trip_id, msg='Missing stops')
            return False
        stops_df = pd.DataFrame(stops)
        #vehicles_df = pd.DataFrame([{'pdist': x.pattern_distance,
        #                             'tmstmp': int(x.timestamp.timestamp())} for x in positions]).sort_values('tmstmp')
        minval = vehicles_df.pdist.min()
        maxval = vehicles_df.pdist.max()
        beginnings = vehicles_df[vehicles_df.pdist == minval]
        endings = vehicles_df[vehicles_df.pdist == maxval]
        begin_drop = len(beginnings) - 1
        end_drop = len(endings) - 1
        filtered = vehicles_df
        if end_drop > 0:
            filtered = vehicles_df.drop(vehicles_df.tail(end_drop).index)
        if begin_drop > 0:
            filtered = filtered.drop(filtered.head(begin_drop).index)
        if filtered.empty:
            self.record_error(trip_id=trip_id, msg='Interpolated vehicle error')
            return False
        pattern_template = pd.DataFrame(index=stops_df.pdist, columns={'tmstmp': float('NaN')})
        combined = pd.concat([pattern_template, filtered.set_index('pdist')]).sort_index().tmstmp.astype('float').interpolate(
            method='index', limit_direction='both')
        combined = combined.groupby(combined.index).last()
        df = stops_df.set_index('pdist').assign(tmstmp=combined.apply(
            lambda x: Util.CTA_TIMEZONE.localize(datetime.datetime.fromtimestamp(int(x)))
        ))
        #stop_interpolation = []
        for _, row in df.iterrows():
            pattern_stop = stop_index[row.stpid]
            interpolated_timestamp = self.gtfs_time(row.tmstmp)
            self.writer.write_stop_time({
                'trip_id': f'{self.day}.{trip_id}',
                'arrival_time': interpolated_timestamp,
                'departure_time': interpolated_timestamp,
                'stop_id': pattern_stop.stop_id,
                'stop_sequence': pattern_stop.sequence_no,
                'shape_dist_traveled': pattern_stop.pattern_distance,
            })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bundles')
    parser.add_argument('--bundle_file', type=str,
                        help='File with bus scrape data.')
    parser.add_argument('--routes', type=str,
                        help='Comma-separated list of routes.')
    args = parser.parse_args()
    bundle_file = Path(args.bundle_file).expanduser()
    routes = args.routes.split(',')
    r = BundleReader(bundle_file, routes)
    r.process_bundle_file()
    pdict = json.load((bundle_file.parent / 'patterns2025.json').open())
    mpm = MemoryPatternManager()
    mpm.parse(pdict['patterns'])
    #vsamp = r.routes['8'].get_vehicle('1310')
    writer = StopWriter(Path('/tmp'))
    stops_file = writer.output_path / 'stops.txt'
    with stops_file.open('w') as sfh:
        dw = csv.DictWriter(sfh,
                            ['stop_id', 'stop_name', 'stop_lat', 'stop_lon'])
        dw.writeheader()
        mpm.write_all_stops(dw)
    cal_file = writer.output_path / 'calendar_dates.txt'
    with cal_file.open('w') as calfh:
        calfh.write(f'service_id,date,exception_type\n{r.day},{r.day},1\n')
    agency_file = writer.output_path / 'agency.txt'
    with agency_file.open('w') as afh:
        afh.write('agency_name,agency_url,agency_timezone,agency_lang,agency_phone,agency_fare_url\n0,Chicago Transit Authority,http://transitchicago.com,America/Chicago,en,1-888-YOURCTA,http://www.transitchicago.com/travel_information/fares/default.aspx\n')
    for route, vsamp in r.generate_vehicles():
        th = TripsHandler(route, r.day, vsamp, mpm, writer)
        th.process_all_trips()
