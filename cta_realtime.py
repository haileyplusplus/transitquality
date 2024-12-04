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

    def get_stop_patterns(self, route):
        stats = self.feed.compute_trip_stats(route)
        stop_patterns = stats.groupby(['stop_pattern_name'])
        # g.iloc[(g['distance'] - key).abs().argsort()][:1]
        return stop_patterns

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

    def process_pattern(self, date, route, pid):
        df = self.days.get(date)
        if not df:
            return False
        pdf = df.query(f'route == "{route}" and pid == {pid}')
        approx_len = pdf.pdist.max()
        schedule_patterns = self.fw.get_closest_pattern(route, approx_len)
        representative_trip = schedule_patterns.iloc[0].trip_id
        stops = self.fw.get_trip_stops(representative_trip)
        rt_trips = pdf.tatripid.unique()
        for t in rt_trips:
            pass
        # this interpolation isn't quite right: maybe need to set the index and use that
        # pd.concat([spf, s3], ignore_index=True).sort_values(['pdist']).interpolate(method='linear')[1:].astype(int)[:50]
        # sp['unixts'] = sp.apply(lambda x: int(datetime.datetime.strptime(x.tmstmp, '%Y%m%d %H:%M').timestamp()), axis=1)





if __name__ == "__main__":
    pass