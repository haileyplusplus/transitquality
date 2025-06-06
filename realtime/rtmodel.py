import datetime
from typing import List

from sqlalchemy import create_engine, String, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.orm import DeclarativeBase
from geoalchemy2 import Geometry

from backend.util import Config


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
    next_stop_distance: Mapped[int] = mapped_column(nullable=True)
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
    prediction_type: Mapped[str] = mapped_column(nullable=True)
    origin: Mapped[str] = mapped_column(nullable=True)
    vehicle_id: Mapped[int] = mapped_column(nullable=True)
    direction: Mapped[str] = mapped_column(nullable=True)
    block_id: Mapped[str] = mapped_column(nullable=True)
    delay: Mapped[bool] = mapped_column(nullable=True)


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


class TrainPatternDetail(Base):
    __tablename__ = "train_pattern_detail"

    pattern_id: Mapped[int] = mapped_column(primary_key=True)
    route_id: Mapped[str]
    pattern_length_meters: Mapped[int]
    service_id: Mapped[int]
    direction_id: Mapped[int]
    direction: Mapped[str]
    schedule_instance_count: Mapped[int]
    stop_count: Mapped[int]
    first_stop_name: Mapped[str]
    last_stop_name: Mapped[str]
    first_stop_id: Mapped[int]
    last_stop_id: Mapped[int]
    geom = mapped_column(Geometry(geometry_type='LINESTRING', srid=26916))


class ScheduleDestinations(Base):
    __tablename__ = "schedule_destinations"

    trip_id: Mapped[str] = mapped_column(primary_key=True)
    first_stop_id: Mapped[int]
    last_stop_id: Mapped[int]
    destination_headsign: Mapped[str]
    distance: Mapped[int]
    route_id: Mapped[str]
    service_id: Mapped[int]
    shape_id: Mapped[int]
    direction: Mapped[str]


def db_init(config, echo=False):
    conn_str = f'postgresql://postgres:rttransit@{config.get_server("vehicle-db")}/rttransitstate'
    print(f'Connecting to {conn_str}')
    engine = create_engine(conn_str, echo=echo)
    Base.metadata.create_all(engine)
    return engine


"""
Views:

- not used in queries

create view train_position_summary as select run, timestamp, rt, dest_name, direction, next_stop, stop.stop_name as next_station_name, arrival, pattern, 
synthetic_trip_id, pattern_distance from train_position inner join stop on train_position.next_stop = stop.id
order by run, timestamp;

- used in queries

TODO: actually initialize these automatically

create view last_stop as select distinct on (pattern_id) pattern_id, sequence, distance, stop_id from pattern_stop order by pattern_id, sequence desc;


create view pattern_destinations as select pattern_stop.pattern_id, pattern_stop.stop_id as origin_stop, length, rt, last_stop, stop_name as last_stop_name from pattern_stop 
inner join pattern on pattern.id = pattern_stop.pattern_id
inner join (select distinct on (pattern_id) pattern_id, sequence, stop_id as last_stop from pattern_stop order by pattern_id, sequence desc)
as last_stop_table on last_stop_table.pattern_id = pattern.id
inner join stop on last_stop_table.last_stop = stop.id
where pattern_stop.sequence = 1;

"""

if __name__ == "__main__":
    db_init(Config('prod'))
