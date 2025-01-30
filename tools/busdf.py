#!/usr/bin/env python3

import argparse

import pandas as pd

from bundler.bundlereader import BundleReader
from pathlib import Path


class DfManager:
    TRAIN_ROUTES = ['red', 'p', 'y', 'blue', 'pink', 'g', 'org', 'brn']
    EXPECTED_UNIQUE = ['vid', 'pid', 'rt', 'des', 'tatripid', 'stst', 'stsd']

    def __init__(self, bundle_file: Path):
        reader = BundleReader(bundle_file, routes=None)
        reader.process_bundle_file()
        self.df = pd.DataFrame()
        self.trips_with_errors = set([])
        for route, vid, vdf in reader.generate_vehicles():
            if route.route in self.TRAIN_ROUTES:
                continue
            self.df = pd.concat([self.df, vdf])

    def record_error(self, trip, msg):
        self.trips_with_errors.add(trip)
        print(f'Error in {trip}: {msg}')

    def validate(self, origtatripno: str):
        tdf = self.df[self.df.origtatripno == origtatripno]
        for field in self.EXPECTED_UNIQUE:
            if tdf[field].nunique() != 1:
                self.record_error(origtatripno, f'Non-unique field {field}')
        if not tdf.tmstmp.is_monotonic_increasing:
            self.record_error(origtatripno, 'tmstmp not monotonically increasing')
        if not tdf.pdist.is_monotonic_increasing:
            self.record_error(origtatripno, 'pdist not monotonically increasing')

    def validate_all(self):
        trips = self.df.origtatripno.unique()
        for t in trips:
            self.validate(t)
        print(f'Found {len(self.trips_with_errors)} trips with errors')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bus updates from a bundle into a large dataframe.')
    parser.add_argument('--bundle_file', type=str,
                        help='Bundle filename')
    args = parser.parse_args()
    manager = DfManager(Path(args.bundle_file).expanduser())
    df = manager.df
    manager.validate_all()
