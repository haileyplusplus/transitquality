import argparse

from bundler.interpolate import ScheduleWriter
import datetime
from pathlib import Path
import zipfile


class Combiner:
    def __init__(self, target_file: Path, source_dir: Path,
                 start_date: datetime.date, days: int):
        """

        :param target_file: Target zip file with combined schedule
        """
        self.target_file = target_file
        self.source_dir = source_dir
        self.start_date = start_date
        self.day_count = days

    def combine(self):
        with zipfile.ZipFile(self.target_file, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            with zf.open('agency.txt', 'w') as afh:
                afh.write(
                    'agency_name,agency_url,agency_timezone\nChicago Transit Authority,http://transitchicago.com,America/Chicago\n'.encode('utf-8'))
            for k, v in ScheduleWriter.FEEDS_AND_FIELDS.items():
                with zf.open(k, 'w') as ofh:
                    ofh.write(','.join(v).encode('utf-8'))
                    ofh.write('\n'.encode('utf-8'))
                    for d in range(self.day_count):
                        datestr = (self.start_date + datetime.timedelta(days=d)).strftime('%Y%m%d')
                        source = self.source_dir / datestr / k
                        with (source.open('rb')) as sourcefile:
                            ofh.write(sourcefile.read())
                        if k in {'stops.txt', 'routes.txt'}:
                            # TODO: find a more elegant way of doing this
                            break


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Combine bundles')
    parser.add_argument('--output_file', type=str,
                        help='GTFS zip file to output.')
    parser.add_argument('--source_dir', type=str,
                        help='Root of directory with daily interpolated dumps.')
    parser.add_argument('--start_day', type=str,
                        help='Start day (YYYYmmdd).')
    parser.add_argument('--days', type=int, default=7,
                        help='Number of days to parse.')
    args = parser.parse_args()
    target_file = Path(args.output_file).expanduser()
    source_dir = Path(args.source_dir).expanduser()
    start = datetime.datetime.strptime(args.start_day, '%Y%m%d')
    c = Combiner(target_file, source_dir, start, args.days)
    c.combine()
    print(f'Done')
