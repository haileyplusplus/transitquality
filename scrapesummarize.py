#!/usr/bin/env python3

from pathlib import Path
import argparse


class Summarizer:
    def __init__(self, root: Path, day: str):
        self.root = root



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--schedule_dir', type=str, nargs=1, default=['~/datasets/transit'],
                        help='Directory containing schedule files.')
    parser.add_argument('--root_dir', type=str, nargs=1, default=['~/transit/bustracker/raw'],
                        help='Root directory with raw files.')
    args = parser.parse_args()
    root = Path(args.output_dir[0]).expanduser()
    s = Summarizer(root)
