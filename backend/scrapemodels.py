from peewee import CharField, BooleanField, DateField, ForeignKeyField, IntegerField, Model, SqliteDatabase, DateTimeField, PostgresqlDatabase
from playhouse.postgres_ext import DateTimeTZField

from pathlib import Path
from backend.util import Util
import shutil
import socket
import sys
import os

DateTimeType = DateTimeField


if socket.gethostname() == 'campari':
    dbpath = Path(__file__).parent / 'data'
    campari_dbpath = Path('~/transit/scraping/bustracker/').expanduser()
    dbname = 'scrapestate.sqlite3'
    greendale_dbname = 'scrapestate-greendale.sqlite3'
    campari_db = campari_dbpath / dbname
    if not campari_db.exists():
        sys.exit(1)
    db = SqliteDatabase(campari_db)
elif socket.gethostname() == 'greendale':
    dbpath = Path(__file__).parent / 'data'
    campari_dbpath = Path('~/transit/scraping/bustracker/').expanduser()
    dbname = 'scrapestate.sqlite3'
    greendale_dbname = 'scrapestate-greendale.sqlite3'
    campari_db = campari_dbpath / dbname
    if not campari_db.exists():
        sys.exit(1)
    greendale_db = campari_dbpath / greendale_dbname
    greendale_db.unlink(missing_ok=True)
    shutil.copy(campari_db, greendale_db)
    db = SqliteDatabase(greendale_db)
else:
    #dbpath = Path('/var/transit')
    #dbname = 'scrapestate.sqlite3'
    dbhost = os.getenv('POSTGRES_SERVER')
    #assert dbhost
    DateTimeType = DateTimeTZField
    #db = SqliteDatabase(dbpath / dbname)
    db = PostgresqlDatabase('busscrapestate', user='postgres', password='mypostgrespassword',
                            host=dbhost, port=5432)


class BaseModel(Model):
    class Meta:
        database = db


class Route(BaseModel):
    route_id = CharField(primary_key=True)
    route_name = CharField(null=True)
    route_color = CharField(null=True)
    last_scrape_attempt = DateTimeType(null=True)
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
    db.connect()
    db.create_tables([Route, Pattern, Count, ErrorMessage, Stop])
