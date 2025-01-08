- Robustness
  - More error and summary reporting
  - Better appending to existing state
  - Maybe store in database
  - Notion of trips being complete or incomplete
  - Validation of assumptions: trip id uniqueness, vid uniqueness, etc
  - Normalize all fields on loading from csv (type mismatches)
  - stop_info join needs to be on day in addition to trip no (filter other df)

  - db as work queue


- Matching days in ghost bus data
  - some trips span midnight boundary; nontrival to put them together for now so return to it later


- Use multiple Docker containers
- Route start times missing: start scraping based on scheduled departure

- Train tracker scrape fix:
  - generalize runner to do arbitrary scraping
  - create a scraper interface

New way to start server:
 - TRACKERWRITE=local docker compose up --build
 - or s3

Schedule scraper
 - only get new schedules

Ordering
https://github.com/coleifer/peewee/issues/600

Seeing actual discrepancies such as trips 22936, 20448 vid 1923
Need to validate input better

Cookies for favorite stops

Service counts:

https://stackoverflow.com/questions/7299342/what-is-the-fastest-way-to-truncate-timestamps-to-5-minutes-in-postgres
select date_trunc('hour', timestamp) at time zone 'America/Chicago' + date_part('minute', timestamp)::int / 5 * interval '5 min' as ts, count(vehicle_id) from vehicles group by ts order by ts;

Aggregating headways:
weekdays
- 6:30 - 10
- 10 - 2
- 2 - 7
- 7 - 10

Combine ridership stats with service provided to get riders per bus

set bundle interval in app.py

        fieldnames = ['trip_id',
                      'arrival_time',
                      'departure_time',
                      'stop_id',
                      'stop_sequence',
                      'shape_dist_traveled']
