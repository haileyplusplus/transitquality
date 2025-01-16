from pathlib import Path
import sys
import json

import pandas as pd


def read_file(filename: Path):
    d = {}
    with open(filename) as fh:
        d = json.load(fh)
        reqs = d['requests']
        for req in reqs:
            routes = req['response']['ctatt']['route']
            for r in routes:
                key = r['@name']
                wdf = d.setdefault(key, pd.DataFrame())
                if 'train' in r:
                    if isinstance(r['train'], dict):
                        ndf = pd.DataFrame([r['train']])
                    else:
                        ndf = pd.DataFrame(r['train'])
                    d[key] = pd.concat([wdf, ndf], ignore_index=True)
    return d


if __name__ == "__main__":
    filename = Path(sys.argv[1])
    df = read_file(filename)
