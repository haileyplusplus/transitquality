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

