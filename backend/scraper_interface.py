from enum import IntEnum
from abc import ABC, abstractmethod


class ScrapeState(IntEnum):
    ACTIVE = 0
    PENDING = 1
    ATTEMPTED = 2
    PAUSED = 3
    INACTIVE = 4
    NEEDS_SCRAPING = 5


class ScraperInterface(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def do_shutdown(self):
        pass

    @abstractmethod
    def get_write_local(self):
        pass

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def scrape_one(self):
        pass
