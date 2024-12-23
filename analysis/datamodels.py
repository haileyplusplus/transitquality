import peewee
from peewee import CharField, BooleanField, DateField, ForeignKeyField, IntegerField, Model, SqliteDatabase, \
    DateTimeField, PostgresqlDatabase, DatabaseProxy, AutoField
from playhouse.postgres_ext import DateTimeTZField

from pathlib import Path
from backend.util import Util
import shutil
import socket
import sys
import os


database_proxy = DatabaseProxy()

"""
Relations as derived from scraping:

getroutes lists all routes (route id)
getvehicles links route, pattern, and vehicle, with a check on pattern distance. It also lists a destination which is 
maybe implicit in the directional pattern (stop name is close but doesn't have an exact match).
getpatterns lists pattern metadata with stops and waypoints. It's a superset of getstops.


"""


class BaseModel(Model):
    class Meta:
        database = database_proxy


class Route(BaseModel):
    route_id = CharField(primary_key=True)
    route_name = CharField(null=True)
    route_color = CharField(null=True)
    route_display = CharField(null=True)
    timestamp = DateTimeTZField()
    active = BooleanField()


class Direction(BaseModel):
    direction_id = CharField(primary_key=True)
    direction_name = CharField(null=True)


class Pattern(BaseModel):
    pattern_id = IntegerField(primary_key=True)
    route = ForeignKeyField(Route, null=True, backref='patterns')
    direction = ForeignKeyField(Direction, null=True)
    timestamp = DateTimeTZField(null=True)
    length = IntegerField(null=True)


class Stop(BaseModel):
    stop_id = CharField(primary_key=True)
    stop_name = CharField()
    lat = CharField()
    lon = CharField()


class PatternStop(BaseModel):
    """
    In the API response, lat and lon can be a check on stop id consistency
    """
    pattern_stop_id = AutoField()
    pattern = ForeignKeyField(Pattern, backref='stops')
    stop = ForeignKeyField(Stop, backref='route_patterns')
    sequence_no = IntegerField()
    pattern_distance = IntegerField()


class Waypoint(BaseModel):
    waypoint_id = AutoField()
    sequence_no = IntegerField()
    pattern = ForeignKeyField(Pattern, backref='waypoints')
    lat = CharField()
    lon = CharField()
    distance = IntegerField()


class Trip(Model):
    trip_id = AutoField()
    schedule_local_day = CharField()
    origtatripno = CharField()
    vehicle_id = CharField()
    route = ForeignKeyField(Route, backref='trips')
    pattern = ForeignKeyField(Pattern, backref='patterns')
    destination = CharField(null=True)
    ta_block_id = CharField(null=True)
    ta_trip_id = CharField(null=True)
    zone = CharField()
    mode = IntegerField(null=True)
    passenger_load = CharField(null=True)
    schedule_time = DateTimeTZField(null=True)
    has_interpolation = BooleanField(null=True)

    class Meta:
        database = database_proxy
        indexes = (
            (('schedule_local_day', 'origtatripno'), True),
        )


class VehiclePosition(Model):
    position_id = AutoField()
    trip = ForeignKeyField(Trip, backref='positions')
    lat = CharField()
    lon = CharField()
    heading = IntegerField()
    timestamp = DateTimeTZField()
    pattern_distance = IntegerField()
    delay = BooleanField()

    class Meta:
        database = database_proxy
        indexes = (
            (('trip', 'timestamp'), True),
        )


class StopInterpolation(BaseModel):
    interpolation_id = AutoField()
    trip = ForeignKeyField(Trip, backref='interpolated_stop_times')
    pattern_stop = ForeignKeyField(PatternStop)
    interpolated_timestamp = DateTimeTZField()


class File(BaseModel):
    file_id = AutoField()
    relative_path = CharField()
    filename = CharField()
    command = CharField()
    start_time = DateTimeTZField()
    end_time = DateTimeTZField(null=True)


class FileParse(BaseModel):
    parse_id = AutoField()
    file_id = ForeignKeyField(File, backref='parse_attempts')
    parse_time = DateTimeTZField()
    parse_stage = CharField()
    parse_iteration = IntegerField(null=True)
    parse_success = BooleanField(null=True)


class Error(BaseModel):
    error_id = AutoField()
    parse_attempt = ForeignKeyField(FileParse, backref='errors')
    data_timestamp = DateTimeTZField(null=True)
    error_class = CharField(null=True)
    error_key = CharField(null=True)
    error_message = CharField(null=True)
    error_content = CharField(null=True)


class TimetableView(BaseModel):
    SQL = ('select trip.trip_id, schedule_time, trip.route_id, direction_id, destination, interpolated_timestamp, '
           'sequence_no, pattern_distance, stop_name, patternstop.stop_id from trip '
           'inner join stopinterpolation on trip.trip_id = stopinterpolation.trip_id '
           'inner join patternstop on stopinterpolation.pattern_stop_id = patternstop.pattern_stop_id '
           'inner join stop on patternstop.stop_id = stop.stop_id '
           'inner join pattern on patternstop.pattern_id = pattern.pattern_id '
           'order by route_id, schedule_time, sequence_no;')

    trip_id = IntegerField()
    schedule_time = DateTimeTZField()
    route_id = CharField()
    direction_id = CharField()
    destination = CharField()
    interpolated_timestamp = DateTimeTZField()
    sequence_no = IntegerField()
    pattern_distance = IntegerField()
    stop_name = CharField()
    stop_id = CharField()

    class Meta:
        db_table = 'timetable'


VIEWS = {'timetable': TimetableView}


def create_views(db: peewee.Database):
    existing_views = [x.name for x in db.get_views()]
    print(existing_views)
    for k, v in VIEWS.items():
        if k not in existing_views:
            db.execute_sql(f'CREATE VIEW {k} AS {v.SQL}', commit=True)


def db_initialize():
    dbhost = os.getenv('POSTGRES_SERVER')
    if dbhost:
        db = PostgresqlDatabase('busdata', user='postgres', password='mypostgrespassword',
                                host=dbhost, port=5432)
    else:
        print(f'Warning! No db host')
        db = None
    database_proxy.initialize(db)
    db.connect()
    db.create_tables([
        Route, Direction, Pattern, Stop, PatternStop, Waypoint, Trip,
        VehiclePosition, StopInterpolation, File, FileParse, Error
    ])
    create_views(db)
    #print(f'Initialized {db} in {self.catalog.name}')
    return db
