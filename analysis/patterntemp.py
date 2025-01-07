"""
Temporarily export most recent patterns from database
"""

#!/usr/bin/env python3
import csv
import dataclasses
import itertools
from functools import partial
import sys
import argparse
import datetime
from pathlib import Path
import json
from types import SimpleNamespace
from typing import Iterable
import logging
import pandas as pd

import peewee
from playhouse.shortcuts import model_to_dict

from backend.util import Util
from analysis.datamodels import db_initialize, Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip, VehiclePosition, StopInterpolation, File, FileParse, Error, TimetableView, PatternIndex


logger = logging.getLogger(__file__)


PROJROOT = Path(__file__).parent.parent


class Exporter:
    def __init__(self):
        self.d = {}
        self.create_bundle()

    def create_bundle(self):
        patterns: Iterable[PatternIndex] = PatternIndex.select().order_by(PatternIndex.timestamp.desc())
        self.d['patterns'] = {}
        out = self.d['patterns']
        for p in patterns:
            if p.pattern_id in out:
                continue
            out[p.pattern_id] = json.loads(p.raw)

    def serve(self):
        return self.d