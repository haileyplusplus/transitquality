#!/usr/bin/env python3

import requests
import json


class ValhallaHelper:
    """
    Helper for querying Vahalla
    """
    BASE_URL = 'http://rttransit:8002/'

    def query(self, cmd, jd):
        jp = json.dumps(jd)
        resp = requests.get(f'{self.BASE_URL}/{cmd}?json={jp}')
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
