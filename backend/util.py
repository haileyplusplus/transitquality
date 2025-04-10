import datetime
import json
import pytz
from pathlib import Path


CONFIG_DIR = Path(__file__).parent.parent / 'config'


class Util:
    CTA_TIMEZONE = pytz.timezone('America/Chicago')

    @staticmethod
    def utcnow():
        return datetime.datetime.now(pytz.UTC)

    @staticmethod
    def ctanow():
        return datetime.datetime.now(Util.CTA_TIMEZONE)

    @staticmethod
    def read_datetime(obj):
        if isinstance(obj, datetime.datetime):
            return obj
        return datetime.datetime.fromisoformat(obj)


class Config:
    def __init__(self, current_environment):
        self.servers = {}
        self.current_environment = current_environment
        with (CONFIG_DIR / 'connections.json').open() as jfh:
            config_dict = json.load(jfh)
            connections = config_dict['connections']
            self.hierarchy = connections['environment_hierarchy']
            servers = connections['servers']
            for server in servers:
                self.servers[server['name']] = server
            self.allowed_origins = connections['allowed_origins']
            self.allowed_hosts = connections['allowed_hosts']

    def get_server(self, name):
        server = self.servers[name]
        if self.current_environment in server:
            return server[self.current_environment]
        found = False
        for env in self.hierarchy:
            if not found:
                if self.current_environment == env:
                    found = True
                    continue
            elif env in server:
                return server[env]
        raise ValueError(f'Compatible environment {self.current_environment} not found for {name}')
