#!/usr/bin/env python3

import sys
import argparse
import datetime
from pathlib import Path
import json

import gtfs_kit
import pendulum
import pandas as pd
import tqdm


class PatternManager:
    def __init__(self, statedir: Path):
        self.statedir = statedir
        self.summary_df = None
        self.pattern_df = None
        self.parse()

    def parse(self):
        summary_df = pd.DataFrame()
        pattern_df = pd.DataFrame()
        for f in self.statedir.glob('ttscrape-getpatterns-*.json'):
            #print(f'Reading {f}')
            with open(f) as fh:
                try:
                    p = json.load(fh)
                except json.JSONDecodeError:
                    continue
                top = p['bustime-response']['ptr'][0]
                df = pd.DataFrame(top['pt'])
                df['pid'] = top['pid']
                del top['pt']
                summary_df = pd.concat([summary_df, pd.DataFrame([top])], ignore_index=True)
                pattern_df = pd.concat([pattern_df, df], ignore_index=True)
        self.summary_df = summary_df
        self.pattern_df = pattern_df


class VehicleManager:
    def __init__(self, outdir: Path, prefix: str):
        self.outdir = outdir
        self.df = pd.DataFrame()
        self.parse(prefix)

    def parse(self, prefix: str):
        total = pd.DataFrame()
        for f in self.outdir.glob(f'ttscrape-getvehicles-{prefix}*.json'):
            df0 = pd.read_json(f)
            df = pd.DataFrame.from_records(df0['bustime-response']['vehicle'])
            total = pd.concat([total, df], ignore_index=True)
        self.df = total


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--schedule_dir', type=str, nargs=1, default=['~/datasets/transit'],
                        help='Directory containing schedule files.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    args = parser.parse_args()
    outdir = Path(args.output_dir[0]).expanduser()
    datadir = outdir / 'raw_data'
    vm = VehicleManager(datadir, '202412151')
    pm = PatternManager(datadir)
