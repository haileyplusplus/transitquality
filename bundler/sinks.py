import abc
from abc import ABC


class SinkInterface(ABC):
    @abc.abstractmethod
    def trip_update(self, trip_row: dict):
        pass

    @abc.abstractmethod
    def stop_time_update(self, row: dict):
        pass


