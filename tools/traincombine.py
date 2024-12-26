#!/usr/bin/env python3

import argparse
import datetime
import json
from pathlib import Path

from backend.util import Util


class Batcher:
    def __init__(self, output_path: Path):
        self.items: list[tuple[Path, datetime.datetime]] = []
        self.date = None
        self.output_path = output_path

    def add(self, item: Path):
        """
        Adds the item iff it belongs in the batch.
        :param item:
        :return: Whether the item was added.
        """
        timestamp = datetime.datetime.strptime(item.name, 'ttscrape-%Y%m%d%H%M%S.json')
        d = timestamp.date()
        if self.date is None:
            self.date = d
        if self.date != d:
            return False
        self.items.append((item, timestamp))
        return True

    def consume(self, items: list):
        while items and self.add(items[0]):
            items.pop(0)

    def process(self):
        out = []
        daystr = self.date.strftime('%Y%m%d')
        new_fn = self.output_path / f'ttcombined-{daystr}.json'
        if new_fn.exists():
            with open(new_fn) as jfh:
                existing = json.load(jfh)
                if sorted(list(existing.keys())) != ['ctatts', 'day']:
                    raise ValueError
                out = existing['ctatts']
        for item, timestamp in self.items:
            with open(item) as jfh:
                d = json.load(jfh)
                if list(d.keys()) != ['ctatt']:
                    raise ValueError(f'Unexpected JSON file format: {item}')
                out.append({'scraped': timestamp.astimezone().astimezone(Util.CTA_TIMEZONE).isoformat(),
                            'ctatt': d['ctatt']})
        with open(new_fn, 'w') as ofh:
            json.dump({'day': daystr, 'ctatts': out}, ofh)
        for item, _ in self.items:
            item.unlink()
        print(f'Processed {daystr}')


class Combiner:
    def __init__(self, input_path: Path, output_path: Path, dry_run=False):
        if not input_path.exists():
            print(f'Input path {input_path} does not exist.')
            return
        if input_path == output_path:
            print(f'Paths must be different.')
            return
        output_path.mkdir(parents=True, exist_ok=True)
        self.input_path = input_path
        self.output_path = output_path
        self.dry_run = dry_run
        self.items = sorted(input_path.glob('ttscrape-20*.json'))

    def make_batches(self):
        while self.items:
            b = Batcher(self.output_path)
            b.consume(self.items)
            b.process()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--input_dir', type=str, default='~/transit/ttarch',
                        help='Input directory for files')
    parser.add_argument('--output_dir', type=str, default='~/transit/traincombined',
                        help='Output directory for files')
    args = parser.parse_args()
    c = Combiner(Path(args.input_dir).expanduser(),
                 Path(args.output_dir).expanduser())
    c.make_batches()
