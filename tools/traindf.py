from pathlib import Path
import sys
import json
import glob

import pandas as pd


def read_file(filename: Path, dx):
    with open(filename) as fh:
        d = json.load(fh)
        reqs = d['requests']
        for req in reqs:
            routes = req['response']['ctatt']['route']
            for r in routes:
                key = r['@name']
                wdf = dx.setdefault(key, pd.DataFrame())
                if 'train' in r:
                    if isinstance(r['train'], dict):
                        ndf = pd.DataFrame([r['train']])
                    else:
                        ndf = pd.DataFrame(r['train'])
                    dx[key] = pd.concat([wdf, ndf], ignore_index=True)


if __name__ == "__main__":
    d = {}
    #for f in sys.argv[1:]:
    for f in glob.glob('*.json'):
        filename = Path(f)
        read_file(filename, d)
