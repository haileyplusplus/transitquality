#!/usr/bin/env python3

import requests
import json

from backend.util import Config


class ValhallaHelper:
    """
    Helper for querying Vahalla
    """
    def __init__(self):
        self.config = Config('prod')

    def query(self, cmd, jd):
        jp = json.dumps(jd)
        resp = requests.get(f'{self.config.get_server("valhalla")}/{cmd}?json={jp}')
        print(f'Response: {resp.status_code}')
        print(json.dumps(resp.json(), indent=4))

    @staticmethod
    def make_list(*items):
        rv = []
        for item in items:
            rv.append({'lat': item[0], 'lon': item[1]})
        return rv


if __name__ == "__main__":
    vh = ValhallaHelper()
