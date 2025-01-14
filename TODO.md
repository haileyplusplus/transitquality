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

Known issues
- with daily bundle, trips around 3am boundary will be incomplete
- fast bus turns: need vehicle-level interpolation; pandas interpolation in one direction is not as smart.
```
87  1956  20250105 06:36:40   41.90176773071289  -87.77480105696053  335  2291  70             Austin  40765  False  1032453    259605803   70 -802          1   N/A  21660  2025-01-05
88  1956  20250105 06:37:40   41.90176773071289  -87.77480105696053  335  2291  70             Austin  40765  False  1032453    259605803   70 -802          1   N/A  21660  2025-01-05
89  1956  20250105 06:39:03   41.90203857421875   -87.7713394165039   88  2289  70  Walton & Dearborn   1017  False  1032457    259605733   70 -802          1   N/A  23880  2025-01-05
90  1956  20250105 06:39:49   41.90211868286133  -87.76608276367188   89  2289  70  Walton & Dearborn   2408  False  1032457    259605733   70 -802          1   N/A  23880  2025-01-05
91  1956  20250105 06:40:59  41.902157318897736  -87.76383267916165   87  2289  70  Walton & Dearborn   3024  False  1032457    259605733   70 -802          1   N/A  23880  2025-01-05
```

too-fast interpolations: weed out


operational:
 better health checks
 postmortem: citycollege crashed due to excess cpu or memory usage while bundling. leonard was running but bus scraping wasn't working due to api key errors after code update. The status page gave no indication that bus scraping was broken.
