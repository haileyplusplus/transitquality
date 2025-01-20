from pathlib import Path
import csv


class ScheduleWriter:
    FEEDS_AND_FIELDS = {
        'stop_times.txt': ['trip_id',
                           'arrival_time',
                           'departure_time',
                           'stop_id',
                           'stop_sequence',
                           'shape_dist_traveled'],
        'trips.txt': ['route_id', 'service_id', 'trip_id'],
        'stops.txt': ['stop_id', 'stop_name', 'stop_lat', 'stop_lon',
                      'location_type', 'parent_station', 'wheelchair_boarding'],
        'routes.txt': ['route_id', 'route_short_name', 'route_type'],
        'calendar_dates.txt': ['service_id', 'date' , 'exception_type'],
    }

    def __init__(self, output_path: Path, day: str):
        self.output_path = output_path
        self.file_handlers = []
        self.writers = {}
        basedir = self.output_path / day
        basedir.mkdir(exist_ok=True)
        for k, v in self.FEEDS_AND_FIELDS.items():
            table = k.removesuffix('.txt')
            self.file_handlers.append(
                (basedir / k).open('w')
            )
            # omit writing headers until merge
            self.writers[table] = csv.DictWriter(self.file_handlers[-1], v)

    def write(self, table: str, row: dict):
        self.writers[table].writerow(row)

    def __del__(self):
        for fh in self.file_handlers:
            fh.close()

