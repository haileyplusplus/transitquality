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
Meters = Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]



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
    stop_position: Meters | str
    vehicle_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
    distance_from_vehicle: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
    distance_to_stop: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
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
    walk_distance: Optional[Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str] = None
    display: bool = True
    trace_info: Optional[dict] = None


class TrainEstimate(TransitEstimate):
    run: int
    next_stop_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
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
    stop_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
    vehicle_positions: list[Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]] | list[str]
    debug: bool = False

    def __lt__(self, other):
        if self.pattern_id == other.pattern_id:
            return self.stop_position < other.stop_position
        return self.pattern_id < other.pattern_id


class StopEstimates(BaseModel):
    estimates: list[StopEstimate]


class SingleEstimate(BaseModel):
    vehicle_position: Meters | str
    low_estimate: datetime.timedelta
    high_estimate: datetime.timedelta
    info: dict


class PatternResponse(BaseModel):
    pattern_id: int
    stop_position: Meters | str
    single_estimates: list[SingleEstimate]


class EstimateResponse(BaseModel):
    """
                    'pattern': row.pattern_id,
                    'bus_location': info['bus_position'],
                    'low': el,
                    'high': eh,
                    'info': info
    """
    patterns: list[PatternResponse]
