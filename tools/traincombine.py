#!/usr/bin/env python3

import argparse
import copy
import datetime
import json
from pathlib import Path

from backend.util import Util


class Batcher:
    OUTER_TEMPLATE = {
        "v": "2.0",
        "command": "ttpositions.aspx",
        "requests": []
    }

    REQUEST_TEMPLATE = {
            "request_args": {
                "rt": "Red,Blue,Brn,G,Org,P,Pink,Y",
                "outputType": "JSON"
            },
            "request_time": None,
            "latency_ms": 0
    }

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
        d = (timestamp.date(), timestamp.hour)
        if self.date is None:
            self.date = d
        if self.date != d:
            return False
        self.items.append((item, timestamp))
        return True

    def consume(self, items: list):
        self.items = []
        while items and self.add(items[0]):
            items.pop(0)

    def process(self):
        out = {
            "v": "2.0",
            "command": "ttpositions.aspx",
            "requests": []
        }
        reqlist = out['requests']
        daystr = self.date[0].strftime('%Y%m%d')
        np = self.output_path / 'ttpositions.aspx' / f'{daystr}'
        np.mkdir(parents=True, exist_ok=True)
        ts = self.items[0][1]
        new_fn = np / ts.strftime('t%H%M%Sz.json')
        if new_fn.exists():
            raise ValueError('incremental only')
        for item, timestamp in self.items:
            outreq = copy.copy(self.REQUEST_TEMPLATE)
            with open(item) as jfh:
                d = json.load(jfh)
                if list(d.keys()) != ['ctatt']:
                    raise ValueError(f'Unexpected JSON file format: {item}')
                reqtime = timestamp.astimezone().astimezone(Util.CTA_TIMEZONE).isoformat()
                outreq['request_time'] = reqtime
                outreq['response'] = d['ctatt']
            reqlist.append(outreq)
        with open(new_fn, 'w') as ofh:
            json.dump(out, ofh)
        #for item, _ in self.items:
        #    item.unlink()
        print(f'Processed {daystr}, {ts.hour}')


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
