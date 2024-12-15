import datetime
import pytz


class Util:
    CTA_TIMEZONE = pytz.timezone('America/Chicago')

    @staticmethod
    def utcnow():
        return datetime.datetime.now(pytz.UTC)

    @staticmethod
    def localnow():
        return datetime.datetime.now(CTA_TIMEZONE)
