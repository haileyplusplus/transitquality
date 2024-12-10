#!/usr/bin/env python3

import sys
import json
import datetime
import argparse
from pathlib import Path

import gtfs_kit


class ConfigManager:
    def __init__(self, config_file: Path):
        self.config_file = config_file
        self.runs = {}
        self.error = None

    def load(self):
        with open(self.config_file) as cfh:
            raw = json.load(cfh)
            if not isinstance(raw, list):
                self.error = f'Malformed config'
                return False
            for configitem in raw:
                if not isinstance(configitem, dict):
                    self.error = f'Malformed config (expected dict)'
                    return False
                key = configitem.get('analyzertype')
                if key is None:
                    self.error = f'Malformed config (missing analyzertype key)'
                    return False
                self.runs.setdefault(key, []).append(configitem)


class Analyzer:
    def __init__(self, feed: gtfs_kit.feed.Feed, config: ConfigManager):
        config.load()
        self.runs = config.runs
        self.feed = feed

    def get_dates(self, ndays):
        return list(self.feed.calendar_dates[self.feed.calendar_dates.exception_type == 1].date[:ndays])

    def run_twostopfixedtime_all(self):
        count = 0
        for item in self.runs.get('twostopfixedtime', []):
            results = self.run_twostopfixedtime_single(item.get('start_tofd'),
                                                       item.get('start_stop'),
                                                       item.get('end_stop'),
                                                       item.get('ndays', 7))
            if results is not None:
                count += 1
                self.print_twostopfixedtime_report(results)
        return count

    def get_stop_by_desc(self, stop_desc):
        filtered = self.feed.stops[self.feed.stops.stop_desc == stop_desc]
        if len(filtered) != 1:
            return None
        return filtered.iloc[0]

    @staticmethod
    def apply_datetime(row):
        date = datetime.datetime.strptime(row.date, '%Y%m%d')
        h, m, s = [int(x) for x in row.departure_time.split(':')]
        if h >= 24:
            date += datetime.timedelta(days=1)
            h -= 24
        tofd = datetime.time(h, m, s)
        return datetime.datetime.combine(date, tofd)

    @staticmethod
    def to_seconds(rowval):
        tok = [int(x) for x in rowval.split(':')]
        if len(tok) == 1:
            print(tok)
        s = 0
        if len(tok) == 2:
            h, m = tok
        else:
            h, m, s = tok
        return (h * 60 + m) * 60 + s

    def run_twostopfixedtime_single(self, tofd, start_stop, end_stop, ndays):
        #t = datetime.time.fromisoformat(tofd)
        start = self.get_stop_by_desc(start_stop)
        end = self.get_stop_by_desc(end_stop)
        if start is None or end is None:
            print(f'Missing stop: {start_stop} , {end_stop}')
            return None
        dates = self.get_dates(ndays)
        s1 = self.feed.build_stop_timetable(start.stop_id, dates)
        s2 = self.feed.build_stop_timetable(end.stop_id, dates)
        for df in [s1, s2]:
            df['datetime'] = df.apply(self.apply_datetime, axis=1)
            df['departure_seconds'] = df.departure_time.map(self.to_seconds)
            df.set_index('trip_id', inplace=True)
        joined = s1.join(s2, lsuffix='_start', rsuffix='_end')
        joined = joined[joined.stop_sequence_start < joined.stop_sequence_end]
        if joined.empty:
            return None
        joined['duration'] = joined['datetime_end'] - joined['datetime_start']
        tofd_seconds = self.to_seconds(tofd)
        daily_trips = joined[joined.departure_seconds_start >= tofd_seconds].groupby(['date_start']).first()
        daily_trips['wait'] = daily_trips.apply(
            lambda x: datetime.timedelta(seconds=x.departure_seconds_start - tofd_seconds), axis=1)
        daily_trips['total'] = daily_trips['duration'] + daily_trips['wait']
        return daily_trips[['route_id_start', 'datetime_start', 'datetime_end', 'duration', 'wait', 'total']]

    def print_twostopfixedtime_report(self, df):
        print(f'Wait and travel times between stops')
        def calc(field, summary):
            if summary == 0:
                return int(df[field].min().total_seconds() // 60)
            elif summary == 1:
                return int(df[field].max().total_seconds() // 60)
            return int(df[field].quantile(summary).total_seconds() // 60)

        def sumstr(summary):
            return f'Wait: {calc("wait", summary):3d}m  Travel: {calc("duration", summary):3d}  Total: {calc("total", summary):3d}'

        print(f'Best:   {sumstr(0)}')
        print(f'Median: {sumstr(0.5)}')
        print(f'95%ile: {sumstr(0.95)}')
        print(f'Worst:  {sumstr(1)}')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Analyze GTFS feeds created from realtime feeds.')
    parser.add_argument('--feed_dir', type=str, nargs=1, default=['~/tmp/transit'],
                        help='Output directory for generated files.')
    parser.add_argument('--num_days', type=int, nargs=1,
                        default=[7],
                        #, default=[10918],
                        help='Number of days to analyze.')
    args = parser.parse_args()
    config_file = Path(__file__).parent.resolve() / 'analysis_configs' / 'analysis.json'
    if not config_file.exists():
        print(f'Missing config file {config_file}')
        sys.exit(1)
    files = Path(args.feed_dir[0]).expanduser().glob('cta_rt*12*.zip')
    feed = None
    if files:
        feed = gtfs_kit.read_feed([x for x in files][-1], dist_units='mi')
    if feed is None:
        print(f'Missing GTFS feed')
        sys.exit(1)
    print(feed.calendar_dates)
    manager = ConfigManager(config_file)
    print(manager.error)
    analyzer = Analyzer(feed, manager)
    analyzer.run_twostopfixedtime_all()
