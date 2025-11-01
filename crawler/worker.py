from threading import Thread, Lock
from urllib.parse import urlparse

from inspect import getsource
from utils.download import download
from utils import get_logger
from utils.statistics import StatisticsCollector
import scraper
import time


class Worker(Thread):
    _domain_times = {}
    _domain_lock = Lock()

    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.stats_collector = StatisticsCollector()
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    def _wait_for_domain_politeness(self, url):
        parsed = urlparse(url)
        domain = parsed.netloc
        with Worker._domain_lock:
            current_time = time.time()
            if domain in Worker._domain_times:
                last_time = Worker._domain_times[domain]
                elapsed = current_time - last_time
                if elapsed < self.config.time_delay:
                    sleep_time = self.config.time_delay - elapsed
                    time.sleep(sleep_time)
            Worker._domain_times[domain] = time.time()
        
    def run(self):
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                self.logger.info("Frontier is empty. Stopping Crawler.")
                break
            self._wait_for_domain_politeness(tbd_url)
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            if resp.status == 200:
                self.stats_collector.save_page_stats(tbd_url, resp)
            scraped_urls = scraper.scraper(tbd_url, resp)
            for scraped_url in scraped_urls:
                self.frontier.add_url(scraped_url)
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
