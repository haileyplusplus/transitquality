import datetime
from typing import List
from typing import Optional

from sqlalchemy import create_engine, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase
from geoalchemy2 import Geometry


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
    bus_positions: Mapped[List["BusPosition"]] = relationship(back_populates="route")
    trips: Mapped[List["Trip"]] = relationship(back_populates="route")
    train_positions: Mapped[List["TrainPosition"]] = relationship(back_populates="route")
    current_vehicles: Mapped[List["CurrentVehicleState"]] = relationship(back_populates="route")
    current_trains: Mapped[List["CurrentTrainState"]] = relationship(back_populates="route")


class Stop(Base):
    __tablename__ = "stop"

    id: Mapped[int] = mapped_column(primary_key=True)
    stop_name: Mapped[str]
    #lat: Mapped[float]
    #lon: Mapped[float]
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))

    pattern_stops: Mapped[List["PatternStop"]] = relationship(back_populates="stop")


class Pattern(Base):
    __tablename__ = "pattern"

    id: Mapped[int] = mapped_column(primary_key=True)
    updated: Mapped[datetime.datetime]
    rt = mapped_column(ForeignKey("route.id"), nullable=True)
    length: Mapped[int]

    route: Mapped[Route] = relationship(back_populates="patterns")
    pattern_stops: Mapped[List["PatternStop"]] = relationship(back_populates="pattern")
    current_vehicles: Mapped[List["CurrentVehicleState"]] = relationship(back_populates="pattern")


class PatternStop(Base):
    __tablename__ = "pattern_stop"

    id: Mapped[int] = mapped_column(primary_key=True)
    pattern_id = mapped_column(ForeignKey("pattern.id"))  # manual index
    stop_id = mapped_column(ForeignKey("stop.id"))  # manual indes
    sequence: Mapped[int]
    distance: Mapped[int]
    # only used for train lines
    direction_change: Mapped[int] = mapped_column(nullable=True)
    stop_headsign: Mapped[str] = mapped_column(nullable=True)

    pattern: Mapped[Pattern] = relationship(back_populates="pattern_stops")
    stop: Mapped[Stop] = relationship(back_populates="pattern_stops")


class Trip(Base):
    __tablename__ = "trip"

    id: Mapped[str] = mapped_column(String(30), primary_key=True)
    #service_id: Mapped[str] = mapped_column(String(8))
    rt = mapped_column(ForeignKey("route.id"))
    pid = mapped_column(ForeignKey("pattern.id"))

    route: Mapped[Route] = relationship(back_populates="trips")
    trip_updates: Mapped[List["TripUpdate"]] = relationship(back_populates="trip")


class TripUpdate(Base):
    __tablename__ = "trip_update"

    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    trip_id = mapped_column(ForeignKey("trip.id"), primary_key=True)
    distance: Mapped[int]

    trip: Mapped[Trip] = relationship(back_populates="trip_updates")
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))


class CurrentVehicleState(Base):
    __tablename__ = "current_vehicle_state"

    id: Mapped[int] = mapped_column(primary_key=True)
    last_update: Mapped[datetime.datetime]
    #lat: Mapped[float]
    #lon: Mapped[float]
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))
    pid = mapped_column(ForeignKey("pattern.id"))  # index manually added
    rt = mapped_column(ForeignKey("route.id"))
    distance: Mapped[int]
    origtatripno: Mapped[str]
    destination: Mapped[str]

    route: Mapped[Route] = relationship(back_populates="current_vehicles")
    pattern: Mapped[Pattern] = relationship(back_populates="current_vehicles")


class BusPosition(Base):
    __tablename__ = "bus_position"

    vid: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    #lat: Mapped[float]
    #lon: Mapped[float]
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))
    pid: Mapped[int]
    rt = mapped_column(ForeignKey("route.id"))
    pdist: Mapped[int]
    tatripid: Mapped[str]
    origtatripno: Mapped[str]
    tablockid: Mapped[str]
    destination: Mapped[str]
    completed: Mapped[bool]

    route: Mapped[Route] = relationship(back_populates="bus_positions")


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


class TrainPosition(Base):
    __tablename__ = "train_position"

    run: Mapped[int] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime] = mapped_column(primary_key=True)
    rt = mapped_column(ForeignKey("route.id"))
    dest_station: Mapped[int]
    dest_name: Mapped[str] = mapped_column(String(20))
    direction: Mapped[int]
    next_station: Mapped[int]
    next_stop: Mapped[int]
    arrival: Mapped[datetime.datetime]
    approaching: Mapped[bool]
    delayed: Mapped[bool]
    #lat: Mapped[float]
    #lon: Mapped[float]
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))
    heading: Mapped[int]
    completed: Mapped[bool]
    pattern: Mapped[int] = mapped_column(nullable=True)
    synthetic_trip_id: Mapped[int] = mapped_column(nullable=True)
    pattern_distance: Mapped[int] = mapped_column(nullable=True)

    route: Mapped[Route] = relationship(back_populates="train_positions")


"""
                            "@name": "red",
                            "train": [
                                {
                                    "rn": "825",
                                    "destSt": "30173",
                                    "destNm": "Howard",
                                    "trDr": "1",
                                    "nextStaId": "40240",
                                    "nextStpId": "30046",
                                    "nextStaNm": "79th",
                                    "prdt": "2025-01-13T21:57:34",
                                    "arrT": "2025-01-13T21:58:34",
                                    "isApp": "1",
                                    "isDly": "0",
                                    "flags": null,
                                    "lat": "41.74593",
                                    "lon": "-87.62504",
                                    "heading": "358"
                                },
"""


class CurrentTrainState(Base):
    __tablename__ = "current_train_state"

    id: Mapped[int] = mapped_column(primary_key=True)
    last_update: Mapped[datetime.datetime]
    geom = mapped_column(Geometry(geometry_type='POINT', srid=4326))
    rt = mapped_column(ForeignKey("route.id"))
    dest_station: Mapped[int]
    dest_station_name: Mapped[str]
    direction: Mapped[int]
    next_station: Mapped[int]
    next_stop: Mapped[int]
    next_arrival: Mapped[datetime.datetime]
    approaching: Mapped[bool]
    delayed: Mapped[bool]
    heading: Mapped[int]
    # For simplicity and nullability there is no foreign key
    current_pattern: Mapped[int] = mapped_column(nullable=True)
    synthetic_trip_id: Mapped[int] = mapped_column(nullable=True)
    pattern_distance: Mapped[int] = mapped_column(nullable=True)
    update_count: Mapped[int] = mapped_column(nullable=True)

    route: Mapped[Route] = relationship(back_populates="current_trains")


class BusPrediction(Base):
    __tablename__ = "bus_prediction"

    stop_id: Mapped[int] = mapped_column(primary_key=True)
    destination: Mapped[str] = mapped_column(primary_key=True)
    route: Mapped[str] = mapped_column(primary_key=True)
    timestamp: Mapped[datetime.datetime]
    origtatripno: Mapped[str]
    prediction: Mapped[int]


class TrainPrediction(Base):
    __tablename__ = "train_prediction"

    station_id: Mapped[int] = mapped_column(primary_key=True)
    route: Mapped[str] = mapped_column(primary_key=True)
    destination: Mapped[str] = mapped_column(primary_key=True)
    destination_stop_id: Mapped[int]
    stop_id: Mapped[int]
    stop_description: Mapped[str]
    run: Mapped[int]
    timestamp: Mapped[datetime.datetime]
    predicted_time: Mapped[datetime.datetime]


def db_init(echo=False, dev=False, local=False):
    #engine = create_engine("sqlite+pysqlite:////tmp/rt.db", echo=echo)
    # for local development
    if dev:
        conn_str = "postgresql://postgres:rttransit@rttransit-1.guineafowl-cloud.ts.net/rttransitstate"
    elif local:
        conn_str = "postgresql://postgres:rttransit@localhost/rttransitstate"
    else:
        conn_str = "postgresql://postgres:rttransit@rttransit.guineafowl-cloud.ts.net/rttransitstate"
    print(f'Connecting to {conn_str}')
    engine = create_engine(conn_str, echo=echo)
    Base.metadata.create_all(engine)
    return engine


"""
Figure out how to handle views:

create view last_stop as select distinct on (pattern_id) pattern_id, sequence, distance, stop_id from pattern_stop order by pattern_id, sequence desc;
"""

if __name__ == "__main__":
    db_init()
