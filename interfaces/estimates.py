import datetime
from typing import Optional, Annotated
#from enum import IntEnum

from pydantic import BaseModel
from pydantic_pint import PydanticPintQuantity
from pint import Quantity

from . import ureg

# class DistanceUnit(IntEnum):
#     FEET = 1
#     METERS = 2
#     MILES = 3
#
#
# class Distance(BaseModel):
#     distance: float
#     unit: DistanceUnit
#
#     def meters(self):
#         if self.unit == DistanceUnit.METERS:
#             return self.distance
#         if self.unit == DistanceUnit.FEET:
#             return self.distance *


class TransitEstimate(BaseModel):
    """
    "pattern": 2170,
"startquery": "2025-03-01T15:07:06.892402",
"route": "73",
"direction": "Eastbound",
"stop_id": 1410,
"stop_name": "Lasalle & Eugenie",
"stop_lat": 41.912578000001,
"stop_lon": -87.633340000002,
"predicted_minutes": -4,
"stop_pattern_distance": 36396,
"bus_distance": 36396,
"dist": 450.1714418220115,
"last_update": "2025-03-01T15:06:00",
"age": 67,
"vehicle_distance": 0,
"last_stop_id": 15417,
"last_stop_name": "Clark & North",
"estimate": "-5--5 min",
"mi": "6.89mi",
"mi_numeric": 6.8931818181818185,
"walk_time": 6,
"walk_dist": "0.31",
"el": -5,
"eh": -5,
    """
    query_start: datetime.datetime
    pattern: int
    route: str
    direction: str
    stop_id: int
    stop_name: str
    stop_lat: float
    stop_lon: float
    stop_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    vehicle_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    distance_from_vehicle: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    distance_to_stop: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    last_update: datetime.datetime
    age: datetime.timedelta
    destination_stop_id: int
    destination_stop_name: str
    waiting_to_depart: bool
    predicted_minutes: Optional[datetime.timedelta] = None
    displayed_estimate: Optional[str] = None
    low_estimate: Optional[datetime.timedelta] = None
    high_estimate: Optional[datetime.timedelta] = None
    walk_time: Optional[datetime.timedelta] = None
    walk_distance: Optional[Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]] = None
    display: bool = True
    trace_info: Optional[dict] = None


class TrainEstimate(TransitEstimate):
    run: int
    next_stop_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    next_stop_id: int


class BusEstimate(TransitEstimate):
    vehicle: Optional[int] = None


class BusResponse(BaseModel):
    results: list[BusEstimate]
    start: datetime.datetime
    latency: float
    lat: float
    lon: float


class TrainResponse(BaseModel):
    results: list[TrainEstimate]


class StopEstimate(BaseModel):
    pattern_id: int
    bus_location: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]
    stop_pattern_distance: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]


class StopEstimates(BaseModel):
    estimates: list[StopEstimate]


class EstimateResponse(BaseModel):
    pass