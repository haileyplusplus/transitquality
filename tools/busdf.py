#!/usr/bin/env python3

import argparse

import pandas as pd

from bundler.bundlereader import BundleReader
from pathlib import Path

TRAIN_ROUTES = ['red', 'p', 'y', 'blue', 'pink', 'g', 'org', 'brn']


def read(bundle_file: Path):
    reader = BundleReader(bundle_file, routes=None)
    reader.process_bundle_file()
    df = pd.DataFrame()
    for route, vid, vdf in reader.generate_vehicles():
        if route.route in TRAIN_ROUTES:
            continue
        df = pd.concat([df, vdf])
    return df


def validate(df):
    pass



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bus updates from a bundle into a large dataframe.')
    parser.add_argument('--bundle_file', type=str,
                        help='Bundle filename')
    args = parser.parse_args()
    df = read(Path(args.bundle_file).expanduser())
