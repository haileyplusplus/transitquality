from peewee import CharField, BooleanField, DateField, ForeignKeyField, IntegerField, Model, SqliteDatabase, DateTimeField

from pathlib import Path
from util import Util

dbpath = Path(__file__).parent / 'data'

#TODO: make this configurable
db = SqliteDatabase(dbpath / 'scrapestate.sqlite3')


class BaseModel(Model):
    class Meta:
        database = db


class Route(BaseModel):
    route_id = CharField(primary_key=True)
    route_name = CharField(null=True)
    route_color = CharField(null=True)
    last_scrape_attempt = DateTimeField(null=True)
    last_scrape_success = DateTimeField(null=True)
    scrape_state = IntegerField(default=0)


class Pattern(BaseModel):
    pattern_id = IntegerField(primary_key=True)
    route = ForeignKeyField(Route, backref='patterns')
    timestamp = DateTimeField(default=Util.utcnow())
    first_stop = CharField(null=True)
    direction = CharField(null=True)
    length = IntegerField(null=True)
    last_scrape_attempt = DateTimeField(null=True)
    last_scrape_success = DateTimeField(null=True)
    minutes_predicted = IntegerField(null=True)
    predicted_time = DateTimeField(null=True)
    scrape_state = IntegerField(null=True)


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
    last_seen = DateTimeField()


def db_initialize():
    db.connect()
    db.create_tables([Route, Pattern, Count, ErrorMessage])
