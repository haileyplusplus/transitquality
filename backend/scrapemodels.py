from peewee import CharField, BooleanField, DateField, ForeignKeyField, IntegerField, Model, SqliteDatabase, DateTimeField, PostgresqlDatabase
from playhouse.postgres_ext import DateTimeTZField

from backend.util import Util
import datetime
import os

DateTimeType = DateTimeField


dbhost = os.getenv('POSTGRES_SERVER')
DateTimeType = DateTimeTZField
if dbhost:
    db = PostgresqlDatabase('busscrapestate', user='postgres', password='mypostgrespassword',
                            host=dbhost, port=5432)
else:
    db = None


class BaseModel(Model):
    class Meta:
        database = db


class Route(BaseModel):
    route_id = CharField(primary_key=True)
    route_name = CharField(null=True)
    route_color = CharField(null=True)
    last_scrape_attempt = DateTimeType(default=datetime.datetime.fromtimestamp(0))
    last_scrape_success = DateTimeType(null=True)
    scrape_state = IntegerField(default=0)


class Pattern(BaseModel):
    pattern_id = IntegerField(primary_key=True)
    route = ForeignKeyField(Route, backref='patterns')
    timestamp = DateTimeType(default=Util.utcnow())
    first_stop = CharField(null=True)
    direction = CharField(null=True)
    length = IntegerField(null=True)
    last_scrape_attempt = DateTimeType(null=True)
    last_scrape_success = DateTimeType(null=True)
    minutes_predicted = IntegerField(null=True)
    # overload: this is now last seen
    predicted_time = DateTimeType(null=True)
    scrape_state = IntegerField(null=True)


class Stop(BaseModel):
    stop_id = CharField(primary_key=True)
    timestamp = DateTimeType(default=Util.utcnow())
    stop_name = CharField(null=True)
    last_scrape_attempt = DateTimeType(null=True)
    last_scrape_success = DateTimeType(null=True)
    # For stops with multiple routes, this is the closest prediction
    minutes_predicted = IntegerField(null=True)
    predicted_time = DateTimeType(null=True)
    scrape_state = IntegerField(default=0)


class Count(BaseModel):
    day = DateField()
    command = CharField()
    requests = IntegerField(default=0)
    errors = IntegerField(default=0)
    app_errors = IntegerField(default=0)
    partial_errors = IntegerField(default=0)


class ErrorMessage(BaseModel):
    text = CharField()
    count = IntegerField(default=0)
    last_seen = DateTimeType()


def db_initialize():
    if db is None:
        return
    db.connect()
    db.create_tables([Route, Pattern, Count, ErrorMessage, Stop])
