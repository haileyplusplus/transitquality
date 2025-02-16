from enum import IntEnum
from abc import ABC, abstractmethod
import requests


class ResponseWrapper:
    """
    Simple wrapper to encapsulate response return values
    """
    TRANSIENT_ERROR = -1
    PERMANENT_ERROR = -2
    RATE_LIMIT_ERROR = -3
    PARTIAL_ERROR = -4

    def __init__(self, json_dict=None, error_code=None, error_dict=None):
        self.json_dict = json_dict
        self.error_code = error_code
        self.error_dict = error_dict

    def __str__(self):
        jds = str(self.json_dict)[:300]
        return f'ResponseWrapper: dict {jds} code {self.error_code} dict {self.error_dict}'

    @classmethod
    def transient_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.TRANSIENT_ERROR)

    @classmethod
    def permanent_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.PERMANENT_ERROR)

    @classmethod
    def rate_limit_error(cls):
        return ResponseWrapper(error_code=ResponseWrapper.RATE_LIMIT_ERROR)

    def ok(self):
        # return isinstance(self.json_dict, dict)
        return self.json_dict is not None

    def get_error_dict(self):
        return self.error_dict

    def get_error_code(self):
        return self.error_code

    def payload(self):
        return self.json_dict


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

    @abstractmethod
    def get_name(self) -> str:
        pass

    @abstractmethod
    def get_bundle_status(self) -> dict:
        pass

    @abstractmethod
    def get_requestor(self):
        pass

    def get_bundle(self):
        requestor = self.get_requestor()
        return requestor.bundler.bundles


class ParserInterface(ABC):
    @staticmethod
    @abstractmethod
    def parse_success(response: requests.Response, command: str) -> ResponseWrapper:
        pass

    @staticmethod
    @abstractmethod
    def parse_error(bustime_response: dict):
        pass
