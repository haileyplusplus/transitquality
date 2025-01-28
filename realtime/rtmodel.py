import datetime
from typing import List
from typing import Optional

from sqlalchemy import create_engine, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase



"""
            row = {
                'trip_id': f'{self.day}.{self.vehicle_id}.{trip_id}',
                'arrival_time': interpolated_timestamp,
                'departure_time': interpolated_timestamp,
                'stop_id': pattern_stop.stop_id,
                'stop_sequence': pattern_stop.sequence_no,
                'shape_dist_traveled': pattern_stop.pattern_distance,
            }

            trip_row = {
                'route_id': self.route.route,
                'service_id': self.day,
                'trip_id': f'{self.day}.{self.vehicle_id}.{trip_id}',
            }

"""


class Base(DeclarativeBase):
    pass


class Route(Base):
    __tablename__ = "route"

    id: Mapped[str] = mapped_column(String(8), primary_key=True)
    name: Mapped[str]

    patterns: Mapped[List["Pattern"]] = relationship(back_populates="route")
    active_trips: Mapped[List["ActiveTrip"]] = relationship(back_populates="route")
    trips: Mapped[List["Trip"]] = relationship(back_populates="route")


class Stop(Base):
    __tablename__ = "stop"

    id: Mapped[int] = mapped_column(primary_key=True)
    stop_name: Mapped[str]
    lat: Mapped[float]
    lon: Mapped[float]

    pattern_stops: Mapped[List["PatternStop"]] = relationship(back_populates="stop")


class Pattern(Base):
    __tablename__ = "pattern"

    id: Mapped[int] = mapped_column(primary_key=True)
    updated: Mapped[datetime.datetime]
    rt = mapped_column(ForeignKey("route.id"))

    route: Mapped[Route] = relationship(back_populates="patterns")
    pattern_stops: Mapped[List["PatternStop"]] = relationship(back_populates="pattern")


class PatternStop(Base):
    __tablename__ = "pattern_stop"

    id: Mapped[int] = mapped_column(primary_key=True)
    pattern_id = mapped_column(ForeignKey("pattern.id"))
    stop_id = mapped_column(ForeignKey("stop.id"))
    sequence: Mapped[int]
    distance: Mapped[int]

    pattern: Mapped[Pattern] = relationship(back_populates="pattern_stops")
    stop: Mapped[Stop] = relationship(back_populates="pattern_stops")


class Trip(Base):
    __tablename__ = "trip"

    id: Mapped[str] = mapped_column(String(20), primary_key=True)
    service_id: Mapped[str] = mapped_column(String(8))
    rt = mapped_column(ForeignKey("route.id"))

    route: Mapped[Route] = relationship(back_populates="trips")


class Vehicle(Base):
    __tablename__ = "vehicle"

    id: Mapped[int] = mapped_column(primary_key=True)
    last_update: Mapped[datetime.datetime]


class ActiveTrip(Base):
    __tablename__ = "active_trip"

    vid: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    lat: Mapped[float]
    lon: Mapped[float]
    pid: Mapped[int]
    rt = mapped_column(ForeignKey("route.id"))
    pdist: Mapped[int]
    origtatripno: Mapped[str]

    route: Mapped[Route] = relationship(back_populates="active_trips")



"""
                        {
                            "vid": "1106",
                            "tmstmp": "20250107 18:09:32",
                            "lat": "41.85788345336914",
                            "lon": "-87.66141510009766",
                            "hdg": "92",
                            "pid": 3916,
                            "rt": "18",
                            "des": "Michigan Avenue",
                            "pdist": 25545,
                            "dly": false,
                            "tatripid": "87",
                            "origtatripno": "259615897",
                            "tablockid": "18 -205",
                            "zone": "",
                            "mode": 1,
                            "psgld": "N/A",
                            "stst": 63630,
                            "stsd": "2025-01-07"
                        },
"""

def db_init(echo=False):
    engine = create_engine("sqlite+pysqlite:////tmp/rt.db", echo=echo)
    Base.metadata.create_all(engine)
    return engine


if __name__ == "__main__":
    db_init()
