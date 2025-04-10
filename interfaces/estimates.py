import datetime
from enum import StrEnum
from typing import Optional, Annotated

from pydantic import BaseModel
from pydantic_pint import PydanticPintQuantity
from pint import Quantity

from . import ureg

Meters = Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)]


class Mode(StrEnum):
    BUS = 'bus'
    TRAIN = 'train'


class TransitEstimate(BaseModel):
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


class TransitOutput(BaseModel):
    pattern: int
    vehicle: int
    route: str
    mode: Mode
    direction: str
    stop_id: int
    stop_name: str
    stop_lat: float
    stop_lon: float
    stop_position: str
    vehicle_position: str
    distance_from_vehicle: str
    distance_to_stop: str
    last_update: str
    age_seconds: int
    destination_stop_id: int
    destination_stop_name: str
    waiting_to_depart: bool
    predicted_minutes: Optional[int] = None
    low_estimate_minutes: Optional[int] = None
    high_estimate_minutes: Optional[int] = None
    walk_time_minutes: Optional[int] = None
    total_low_minutes: Optional[int] = None
    total_high_minutes: Optional[int] = None
    walk_distance: Optional[str] = None
    display: bool = True


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


class PositionInfo(BaseModel):
    vehicle_position: Meters | str
    vehicle_id: Optional[int]


class StopEstimate(BaseModel):
    pattern_id: int
    stop_position: Annotated[Quantity, PydanticPintQuantity('m', ureg=ureg)] | str
    vehicle_positions: list[PositionInfo]
    debug: bool = False

    def __lt__(self, other):
        if self.pattern_id == other.pattern_id:
            return self.stop_position < other.stop_position
        return self.pattern_id < other.pattern_id


class StopEstimates(BaseModel):
    estimates: list[StopEstimate]
    recalculate_positions: bool = False


class SingleEstimate(BaseModel):
    vehicle_position: Meters | str
    distance_to_vehicle_mi: str
    timestamp: Optional[datetime.datetime]
    vehicle_id: Optional[int]
    low_estimate: datetime.timedelta
    high_estimate: datetime.timedelta
    low_mins: int
    high_mins: int
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


class DetailRequest(BaseModel):
    pattern_id: int
    stop_id: int
    stop_position: Meters | str
    walk_time: datetime.timedelta


class CombinedEstimateRequest(BaseModel):
    lat: float
    lon: float


CombinedResponseType = dict[str, list[TransitEstimate]]


class CombinedResponse(BaseModel):
    response: CombinedResponseType


class CombinedOutput(BaseModel):
    response: dict[str, list[TransitOutput]]


