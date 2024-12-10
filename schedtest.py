import sys
from pathlib import Path

import gtfs_kit


if __name__ == "__main__":
    files = Path('.').glob('cta_rt*.zip')
    for f in files:
        feed = gtfs_kit.read_feed(f, dist_units='mi')
        break
