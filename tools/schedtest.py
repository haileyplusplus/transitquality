import sys
from pathlib import Path

import gtfs_kit
import transit_service_analyst as tsa


if __name__ == "__main__":
    files = Path('~/datasets/transit').expanduser().glob('cta_*.zip')
    for f in files:
        feed = gtfs_kit.read_feed(f, dist_units='mi')
        ds = feed.calendar_dates.iloc[0].date
        print(f'First date: {ds}')
        #analysis = tsa.load_gtfs('gtfs', ds)
        break
