#!/usr/bin/env python3

import sys
import datetime
from pathlib import Path

import pandas as pd
import geopandas as gpd

import gtfs_kit


class FeedWrapper:
    def __init__(self, feed: gtfs_kit.Feed, datestr: str):
        self.feed = feed
        self.datestr = datestr
        self.cache = {}

    def get_timetable(self, route):
        rv = self.cache.get(route)
        if rv:
            return rv
        rv = self.feed.build_route_timetable(route, self.datestr)
        self.cache[route] = rv
        return rv

    def get_stop_patterns(self, route: str):
        stats = self.feed.compute_trip_stats([route])
        stop_patterns = stats.groupby(['stop_pattern_name'])
        # g.iloc[(g['distance'] - key).abs().argsort()][:1]
        return stop_patterns.first()

    def get_closest_pattern(self, route, dist):
        patterns = self.get_stop_patterns(route)
        return patterns.iloc[(patterns['distance'] - dist).abs().argsort()][:2]

    def get_trip(self, trip_id):
        return self.feed.trips[self.feed.trips.trip_id == trip_id]

    def get_trip_stops(self, trip_id):
        return self.feed.stop_times[self.feed.stop_times.trip_id == trip_id]


class RealtimeConverter:
    def __init__(self, rt_path: Path, fw: FeedWrapper, start: datetime.date, end: datetime.date):
        self.rt_path = rt_path
        self.fw = fw
        self.start = start
        self.end = end
        self.days = {}

    def process(self):
        rtfile = self.rt_path / self.start.strftime('%Y-%m-%d.csv')
        rtdf = pd.read_csv(rtfile, low_memory=False)
        self.days[self.start] = rtdf

    def process_trip(self, date, route, pid, sched_stops, single_rt_trip):
        print(f'  -- process trip -- ')
        # this interpolation isn't quite right: maybe need to set the index and use that
        # fixed with below
        # pd.concat([spf, s3], ignore_index=True).sort_values(['pdist']).interpolate(method='linear')[1:].astype(int)[:50]
        # sp['unixts'] = sp.apply(lambda x: int(datetime.datetime.strptime(x.tmstmp, '%Y%m%d %H:%M').timestamp()), axis=1)
        # intermediate2 = pd.concat([spf, s3], ignore_index=True).sort_values(['pdist']).set_index('pdist').interpolate(method='index')[1:].astype(int)
        # trip_stops from gtfs
        def timefn(x):
            # TODO: figure out 24-hour cutover
            rawdate = datetime.datetime.strptime(x, '%Y%m%d %H:%M')
            if rawdate.hour < 4:
                rawdate += datetime.timedelta(days=1)
            return int(rawdate.timestamp())
        times = single_rt_trip['tmstmp'].apply(timefn)
        single_rt1 = pd.concat([times, single_rt_trip['pdist']], axis=1)
        deltas = single_rt1.iloc[1] - single_rt1.iloc[0]
        if single_rt1.iloc[0].pdist == 0:
            single_rt = single_rt1
        else:
            v = deltas.pdist / deltas.tmstmp
            nt = int(single_rt1.iloc[0].tmstmp - single_rt1.iloc[0].pdist / v)
            fakerow = pd.DataFrame([{'tmstmp': nt, 'pdist': 0}])
            single_rt = pd.concat([fakerow, single_rt1]).sort_values(['pdist'], ignore_index=True)
        single_rt['stop_id'] = -1
        single_sched = pd.concat([sched_stops['shape_dist_traveled'].rename('pdist'), sched_stops['stop_id']],
                                 axis=1).reset_index().drop(columns=['index'])
        print(single_rt)
        print(single_sched)
        print(' ====== ')
        combined = pd.concat([single_rt, single_sched],
                             ignore_index=True).sort_values(
            ['pdist']).set_index('pdist')
        print(combined)
        #combined = combined.infer_objects(copy=True)
        combined['stop_id'] = combined['stop_id'].astype(int)
        interpolated = combined.interpolate(method='index')[1:].astype(int)
        return interpolated

    def process_pattern(self, date, route, pid):
        df: pd.DataFrame = self.days.get(date)
        if df.empty:
            return False
        pdf = df.query(f'rt == "{route}" and pid == {pid}')
        approx_len = pdf.pdist.max()
        schedule_patterns = self.fw.get_closest_pattern(route, approx_len)
        representative_trip = schedule_patterns.iloc[0].trip_id
        sched_stops = self.fw.get_trip_stops(representative_trip)
        rt_trips = pdf.tatripid.unique()
        for t in rt_trips:
            print(f' ==== T ====')
            single_rt_trip = pdf[pdf.tatripid == t]
            r = self.process_trip(date, route, pid, sched_stops, single_rt_trip)
            print(r)


if __name__ == "__main__":
    print(f'Starting')
    sched_path = Path('~/datasets/transit').expanduser()
    feed = gtfs_kit.read_feed(sched_path / 'google_transit_2024-05-18.zip', 'mi')
    fw = FeedWrapper(feed, '20240509')
    start = datetime.date(2024, 5, 9)
    rt = RealtimeConverter(Path('~/tmp/transit').expanduser(),
                           fw,
                           start,
                           start)
    rt.process()
    rt.process_pattern(start, '72', 10918)
