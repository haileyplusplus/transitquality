#!/usr/bin/env python3

import sys
import argparse
import datetime
from pathlib import Path

import gtfs_kit
import pendulum
import pandas as pd
import tqdm


"""
Matching schedules to directions: 0 is south and west, 1 is north and east
"""

class DateWrap:
    def __init__(self, datestr: str):
        """

        :param datestr: YYYYmmdd date string
        """
        self.date = pendulum.date(int(datestr[0:4]),
                                  int(datestr[4:6]),
                                  int(datestr[6:8]))
        self.datestr = datestr

    def __str__(self):
        return self.datestr

    def __lt__(self, other):
        return self.date < other.date

    def get(self):
        return self.date

    def tomorrow(self):
        return DateWrap(self.date.add(days=1).strftime('%Y%m%d'))

    def in_past(self):
        return self.date < pendulum.now().date()


class TofdWrap:
    CUTOVER_HOUR = 3

    def __init__(self, refdate: DateWrap, tofdstr: str):
        if isinstance(tofdstr, TofdWrap):
            self.dt = tofdstr.dt
            return
        h, m, s = [int(x) for x in tofdstr.split(':')]
        if h >= 24:
            refdate = refdate.tomorrow()
            h -= 24
        tofd = pendulum.time(h, m, s)
        self.dt = pendulum.instance(datetime.datetime.combine(refdate.get(), tofd))

    def output(self, refdate):
        d = self.dt.date()
        tofd = self.dt.time()
        if refdate.get() != d:
            if (d - refdate.get()).days != 1:
                return None
            h = tofd.hour + 24
            return tofd.strftime(f'{h}:%M:%S')
        return tofd.strftime('%H:%M:%S')

    def filter(self, refdate: DateWrap):
        if self.dt.date() == refdate.get():
            # same day
            return self.dt.hour >= self.CUTOVER_HOUR
        if self.dt.date() == refdate.tomorrow().get():
            return self.dt.hour < self.CUTOVER_HOUR
        return False


class ScheduleTools:
    def __init__(self, schedule_dir: Path):
        self.schedule_dir = schedule_dir
        sched = sorted([x for x in schedule_dir.glob('cta_20??????.zip')])[-1]
        self.feed = gtfs_kit.read_feed(sched, dist_units='mi')
        self.cache = {}

    def get_daily_stats(self, date: DateWrap):
        result = self.cache.get(date.get())
        if result is not None:
            return result
        routes = self.feed.get_routes(str(date))
        if routes.empty:
            return None
        route_ids = list(routes.route_id)
        #route_ids = ['8', '72', '74', '37']
        cached = self.feed.compute_trip_stats(route_ids)
        self.cache[date.get()] = cached
        return cached

    def correct_route_stats(self, date: DateWrap):
        today = self.get_daily_stats(date)
        if today is None:
            return None
        tomorrow = self.get_daily_stats(date.tomorrow())
        if tomorrow is None:
            return None
        for col in {'start_time', 'end_time'}:
            today[col] = today.apply(lambda x: TofdWrap(date, x[col]), axis=1)
            tomorrow[col] = tomorrow.apply(lambda x: TofdWrap(date.tomorrow(), x[col]), axis=1)
        combined = pd.concat([today, tomorrow])
        #print(combined)
        #combined = combined[lambda x: x['start_time'].filter(date)]
        combined = combined[combined.apply(lambda x: x.start_time.filter(date), axis=1)]
        for col in {'start_time', 'end_time'}:
            combined[col] = combined.apply(lambda x: x[col].output(date), axis=1)
        return combined

    def route_stats(self, date: DateWrap, nodrop=False):
        crs = self.correct_route_stats(date)
        if crs is None:
            return None
        stats = self.feed.compute_route_stats(crs, [str(date)], split_directions=True)
        if nodrop:
            return stats
        return stats.drop(columns=(['route_short_name', 'num_trip_starts', 'num_trip_ends', 'num_stop_patterns', 'is_loop', 'route_type']))

    def generate(self):
        today = pendulum.now().date().strftime('%Y%m%d')
        dates = self.feed.get_dates()
        #dates = ['20241229', '20241230', '20241231']
        sched_dates = [DateWrap(x) for x in dates if not DateWrap(x).in_past()]
        df = pd.DataFrame()
        for d in tqdm.tqdm(sorted(sched_dates)):
            r = self.route_stats(d)
            if r is not None:
                df = pd.concat([df, r], ignore_index=True)
        df.to_json(self.schedule_dir / f'route-summary-{today}.json')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--generate', action='store_true',
                        help='Generate schedules.')
    parser.add_argument('--schedule_dir', type=str, nargs=1, default=['~/datasets/transit'],
                        help='Directory containing schedule files.')
    args = parser.parse_args()
    sched_dir = Path(args.schedule_dir[0]).expanduser()
    tools = ScheduleTools(sched_dir)
    if args.generate:
        tools.generate()
