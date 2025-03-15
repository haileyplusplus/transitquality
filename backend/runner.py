import datetime
import logging
from enum import IntEnum
import asyncio
import threading

from backend.scraper_interface import ScraperInterface
from backend.util import Util


logger = logging.getLogger(__file__)


class RunState(IntEnum):
    IDLE = 0
    RUNNING = 1
    SHUTDOWN_REQUESTED = 2
    SHUTDOWN = 3
    STOPPED = 4


class Runner:
    def __init__(self, scraper: ScraperInterface):
        self.polling_task = None
        self.scraper = scraper
        self.state = RunState.STOPPED
        self.mutex = threading.Lock()
        self.initialized = False

    def done_callback(self, task: asyncio.Task):
        logging.info(f'Task {task} done')
        self.handle_shutdown()

    def handle_shutdown(self):
        logger.info(f'Gracefully handling shutdown.')
        self.scraper.do_shutdown()

    def status(self):
        with self.mutex:
            state = self.state
        running = (state == RunState.RUNNING or state == RunState.IDLE)
        write_local = self.scraper.get_write_local()
        return {'running': running, 'state': state.name, 'write_local': write_local,
                'name': self.scraper.get_name(),
                'bundles': self.scraper.get_bundle_status()}

    def exithandler(self, *args):
        logging.info(f'Shutdown requested: {args}')
        self.stop()

    def syncstart(self):
        with self.mutex:
            self.state = RunState.IDLE

    def syncstop(self):
        with self.mutex:
            self.state = RunState.STOPPED
        self.handle_shutdown()

    async def loop(self):
        logger.info(f'Loop: {self.scraper.get_name()}')
        if not self.initialized:
            self.scraper.initialize()
            self.initialized = True
        last_request = Util.utcnow() - datetime.timedelta(hours=1)
        while True:
            next_scrape = last_request + datetime.timedelta(seconds=4)
            scrape_time = Util.utcnow()
            while scrape_time < next_scrape:
                scrape_time = Util.utcnow()
                wait = next_scrape - scrape_time
                logging.debug(f'Request Last scrape {last_request} next_scrape {next_scrape} waiting {wait}')
                try:
                    await asyncio.sleep(min(wait.total_seconds(), 1))
                except asyncio.CancelledError:
                    logging.info(f'Polling cancelled 1!')
                    return
            scrape_time = Util.utcnow()
            last_request = scrape_time
            with self.mutex:
                if self.state != RunState.IDLE and self.state != RunState.RUNNING:
                    logging.info(f'Polling cancelled 3 {self.state}')
                    break
                self.state = RunState.RUNNING
            self.scraper.scrape_one()
            with self.mutex:
                if self.state != RunState.IDLE and self.state != RunState.RUNNING:
                    logging.info(f'Polling cancelled 2 {self.state}')
                    break
                self.state = RunState.IDLE
        logging.info(f'Recorded shutdown')

    async def start(self):
        self.polling_task = asyncio.create_task(self.loop())
        self.polling_task.add_done_callback(self.done_callback)
        logger.info(f'Polling start wait')
        await self.polling_task
        logger.info(f'Polling start done')

    async def stop(self):
        logging.info(f'Stop')
        was_running = False
        with self.mutex:
            if self.state == RunState.RUNNING:
                was_running = True
            self.state = RunState.SHUTDOWN_REQUESTED
        if not was_running:
            self.polling_task.cancel()

    async def run_until_done(self):
        async with asyncio.TaskGroup() as task_group:
            self.polling_task = task_group.create_task(self.loop())
        self.handle_shutdown()
        logging.info(f'Task group done')
