#!/usr/bin/env python3
import datetime
from pathlib import Path
import json


class PatternHistory:
    def __init__(self, input_dir: Path):
        # input dir is root with daily files under
        self.input_dir = input_dir
        self.patterns = {}
        self.errors = 0

    def traverse(self):
        for p in self.input_dir.glob('20??????'):
            for f in p.glob('t??????z.json'):
                self.read_file(f)

    def read_file(self, file: Path):
        with file.open() as fh:
            jd = json.load(fh)
            requests = jd.get('requests', [])
            if not requests or jd.get('command') != 'getpatterns':
                self.errors += 1
                return
            for req in requests:
                pid = int(req.get('request_args', {}).get('pid'))
                time = datetime.datetime.fromisoformat(req.get('request_time'))
                self.patterns.setdefault(pid, {})[time] = json.dumps(req['response'])

    def stats(self):
        for k, v in sorted(self.patterns.items()):
            print(k)
            for ts, raw in sorted(v.items()):
                print(f'  {ts}: {hash(raw)}')


if __name__ == "__main__":
    ph = PatternHistory(Path('~/transit/s3/getpatterns').expanduser())
    ph.traverse()
    ph.stats()
