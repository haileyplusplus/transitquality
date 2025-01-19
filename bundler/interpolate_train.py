#!/usr/bin/env python3

import argparse


class TrainManager:
    pass



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Package scraped data.')
    parser.add_argument('--show_stats', action='store_true',
                        help='Show bundling stats.')
    parser.add_argument('--day', type=str,
                        help='Bundle a single day.')
    parser.add_argument('--data_dir', type=str, default='~/transit/bustracker/raw',
                        help='Data directory with scraped files')
    args = parser.parse_args()
    data_dir = Path(args.data_dir).expanduser()
    if args.day:
        day = args.day
        b = Bundler(data_dir, day)
        b.scan_day()
    else:
        print(f'Bundling all files')
        bundle_all(data_dir)


