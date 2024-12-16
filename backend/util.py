import datetime
import pytz


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
