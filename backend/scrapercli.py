import argparse
import datetime
import logging
from pathlib import Path
import signal
import asyncio

from backend.scrapemodels import db_initialize
from backend.runner import Runner
from backend.busscraper2 import BusScraper


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape CTA Bus Tracker locations and other data.')
    parser.add_argument('--dry_run', action='store_true',
                        help='Simulate scraping.')
    parser.add_argument('--fetch_routes', action='store_true',
                        help='Fetch routes. By default, if routes are present there is no fetching.')
    parser.add_argument('--freshen_debug', action='store_true',
                        help='Bump last scraped for all active routes to present.')
    parser.add_argument('--debug', action='store_true',
                        help='Print debug logging.')
    parser.add_argument('--scrape_predictions', action='store_true',
                        help='Print debug logging.')
    # parser.add_argument('--write_local_files', action='store_true', default=False,
    #                     help='Print debug logging.')
    parser.add_argument('--output_dir', type=str, nargs=1,
                        #default=['~/transit/scraping/bustracker'],
                        default=['/transit/scraping/bustracker'],
                        help='Output directory for generated files.')
    parser.add_argument('--api_key', type=str, nargs=1,
                        help='Bus tracker API key.')
    args = parser.parse_args()
    if not args.api_key:
        print(f'API key required')
    db_initialize()
    outdir = Path(args.output_dir[0])
    outdir.mkdir(parents=True, exist_ok=True)
    datadir = outdir / 'raw_data'
    datadir.mkdir(parents=True, exist_ok=True)
    statedir = outdir / 'state'
    statedir.mkdir(parents=True, exist_ok=True)
    ts = BusScraper(outdir, datetime.timedelta(seconds=60), api_key=args.api_key[0], debug=args.debug,
                    dry_run=args.dry_run, scrape_predictions=args.scrape_predictions,
                    fetch_routes=args.fetch_routes)
    ts.initialize()
    logging.info(f'Initializing scraping to {outdir} every {ts.scrape_interval.total_seconds()} seconds.')
    if args.freshen_debug:
        logging.info(f'Artifical freshen debug')
        ts.freshen_debug()
    #asyncio.run(ts.loop())
    runner = Runner(ts)
    signal.signal(signal.SIGINT, runner.exithandler)
    signal.signal(signal.SIGTERM, runner.exithandler)
    #asyncio.run(runner.start())
    #asyncio.run(runner.block_until_done())
    asyncio.run(runner.run_until_done())
    logging.info(f'End of program')
