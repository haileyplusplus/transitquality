#!/usr/bin/env python3
from pathlib import Path

import gtfs_kit
import pyproj
import shapely

from shapely.ops import split
from sqlalchemy import select, or_
from sqlalchemy.orm import Session
from geoalchemy2.shape import to_shape, from_shape

from realtime.rtmodel import *

from schedules.schedule_analyzer import ShapeManager

class PatternAnalyzer:
    def __init__(self, dev=True):
        self.engine = db_init(dev=dev)

    def pattern_stats(self, pattern_id: int):
        with Session(self.engine) as session:
            detail = session.get(TrainPatternDetail, pattern_id)
            if not detail:
                print(f'Pattern {pattern_id} not found')
                return
            shape = to_shape(detail.geom)
            origin_point = shape.interpolate(0, normalized=1)
            x = 0
            farthest = (0, origin_point, 0)
            while x < shape.length:
                point_along_line = shape.interpolate(x)
                dist = origin_point.distance(point_along_line)
                closest_again = shape.line_locate_point(shape.interpolate(x))
                if dist > farthest[0]:
                    farthest = (dist, point_along_line, x)
                rc = shape.length - closest_again
                print(f'{x:5}: {int(dist):5}  ca {int(closest_again):5}  rc {int(rc):5}')
                x += 100
            farthest_point = farthest[2]
            print(f'Farthest at {farthest_point}')
            splitpoint = farthest[1]
            splitsnap = shapely.snap(shape, splitpoint, tolerance=1)
            segments = split(splitsnap, splitpoint)
            if len(segments.geoms) != 2:
                print(f'Not splittable here')
                return
            front, back = segments.geoms
            x = 0
            while x < shape.length:
                # if x <= front.length:
                #     point_along_line = front.interpolate(x)
                #     this_line = front
                #     other_line = back
                #     adj = front.length
                # else:
                #     point_along_line = back.interpolate(x - front.length)
                #     this_line = back
                #     other_line = front
                #     adj = 0
                #dist = origin_point.distance(point_along_line)
                # closest_other = other_line.line_locate_point(point_along_line) + adj
                # dist_to_other = other_line.distance(point_along_line)
                point_along_line = shape.interpolate(x)
                c = shape.length - x
                md = abs(x - c)
                complement = shape.interpolate(c)
                dist_to_other = point_along_line.distance(complement)
                z = 0
                print(f'{x:5}: D {int(dist_to_other):5}  CP {int(z):5}  C {int(c):5}  MD {int(md):5}')
                x += 100

    def pattern_stats2(self):
        """
        Algorithm:
          within 2000 of midpoint: don't correct
          before: lower
          after: higher
        :return:
        """
        with Session(self.engine) as session:
            pattern_id = 308500017
            q = select(TrainPosition).where(TrainPosition.run == 401).where(
                or_(TrainPosition.synthetic_trip_id == 3, TrainPosition.synthetic_trip_id == 4)
            ).order_by(TrainPosition.timestamp)
            detail = session.get(TrainPatternDetail, pattern_id)
            if not detail:
                print(f'Pattern {pattern_id} not found')
                return
            shape = to_shape(detail.geom)
            midpoint = shape.length / 2
            prev = 0
            for pos in session.scalars(q):
                raw_point = to_shape(pos.geom)
                coord_point = ShapeManager.transform(raw_point)
                x = shape.line_locate_point(coord_point)
                complement = shape.length - x
                midpoint_distance = abs(midpoint - x)
                if midpoint_distance < 2000:
                    # don't correct
                    corrected = x
                elif prev < midpoint:
                    corrected = min(x, complement)
                else:
                    corrected = max(x, complement)
                prev = corrected
                print(f'{pos.dest_name} {pos.pattern_distance} {raw_point} {int(corrected):5}: MD {int(midpoint_distance):5}')


if __name__ == "__main__":
    pa = PatternAnalyzer()
    # print('brown')
    # print(pa.pattern_stats(308500017))
    # print('purple')
    # print(pa.pattern_stats(308500102))
    # print('green')
    # print(pa.pattern_stats(308500012))
    pa.pattern_stats2()
