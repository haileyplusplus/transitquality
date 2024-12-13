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
        for f in self.statedir.glob('pattern-*.json'):
            #print(f'Reading {f}')
            with open(f) as fh:
                try:
                    p = json.load(fh)
                except json.JSONDecodeError:
                    continue
                df = pd.DataFrame(p['pt'])
                df['pid'] = p['pid']
                del p['pt']
                summary_df = pd.concat([summary_df, pd.DataFrame([p])], ignore_index=True)
                pattern_df = pd.concat([pattern_df, df], ignore_index=True)
        self.summary_df = summary_df
        self.pattern_df = pattern_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--schedule_dir', type=str, nargs=1, default=['~/datasets/transit'],
                        help='Directory containing schedule files.')
    parser.add_argument('--output_dir', type=str, nargs=1, default=['~/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Bus tracker API key.')
    args = parser.parse_args()
    outdir = Path(args.output_dir[0]).expanduser()
    datadir = outdir / 'raw_data'
    statedir = outdir / 'state'
    pm = PatternManager(statedir)
