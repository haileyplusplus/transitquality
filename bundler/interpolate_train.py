#!/usr/bin/env python3

import argparse
from pathlib import Path


from bundler.bundlereader import BundleReader


class TrainManager:
    pass


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Read bundles')
    parser.add_argument('--bundle_file', type=str,
                        help='File with bus scrape data.')
    parser.add_argument('--routes', type=str,
                        help='Comma-separated list of routes.')
    args = parser.parse_args()
    bundle_file = Path(args.bundle_file).expanduser()
    print(f'Routes: {args.routes}')
    if not args.routes:
        routes = None
    else:
        routes = args.routes.split(',')
    r = BundleReader(bundle_file, routes)
    r.process_bundle_file()
    for route, vsamp in r.generate_vehicles():
        print(route)
        print(vsamp)
        break
