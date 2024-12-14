#!/usr/bin/env python3

import argparse
from pathlib import Path
import json

import pandas as pd
from tqdm import tqdm


class Combiner:
    def __init__(self, scrape_dir: Path):
        self.scrape_dir = scrape_dir
        self.lines = {}

    def parse_file(self, filename: Path):
        with open(filename) as fh:
            z = json.load(fh)
            routes = z['ctatt']['route']
            for route in routes:
                if 'train' not in route:
                    continue
                line_name = route['@name']
                rt = route['train']
                if isinstance(rt, dict):
                    rt = [rt]
                df = pd.DataFrame(rt)
                if line_name in self.lines:
                    self.lines[line_name] = pd.concat([self.lines[line_name], df], ignore_index=True)
                else:
                    self.lines[line_name] = df

    def parse(self):
        files = self.scrape_dir.glob('ttscrape-*.json')
        for f in tqdm(files):
            self.parse_file(f)

    def output(self):
        parsed = self.scrape_dir / 'parsed'
        parsed.mkdir(exist_ok=True)
        for k, v in self.lines.items():
            fn = parsed / f'{k}.json'
            v.to_json(fn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--dry_run', action='store_true',
                        help='Simulate scraping.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--scrape_dir', type=str, nargs=1, default=['~/transit/traintracker'],
                        help='Output directory for generated files.')
    args = parser.parse_args()
    scrapedir = Path(args.scrape_dir[0]).expanduser()
    c = Combiner(scrapedir)
    c.parse()
    c.output()
