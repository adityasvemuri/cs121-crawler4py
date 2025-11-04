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
    MIN_WORD_COUNT = 10 
    MAX_CONTENT_SIZE = 10 * 1024 * 1024  # 10MB max

    def __init__(self, worker_id, config, frontier):
        self.logger = get_logger(f"Worker-{worker_id}", "Worker")
        self.config = config
        self.frontier = frontier
        self.stats_collector = StatisticsCollector()
        assert {getsource(scraper).find(req) for req in {"from requests import", "import requests"}} == {-1}, "Do not use requests in scraper.py"
        assert {getsource(scraper).find(req) for req in {"from urllib.request import", "import urllib.request"}} == {-1}, "Do not use urllib.request in scraper.py"
        super().__init__(daemon=True)
    
    def _is_dead_url(self, resp):
        """Detect dead URLs that return 200 but have no meaningful content."""
        if resp.status != 200:
            return False
        if not resp.raw_response or not resp.raw_response.content:
            return True
        
        # Check if content is too small (likely a dead page)
        content_size = len(resp.raw_response.content)
        if content_size < 100:  # Very small content, likely dead
            return True
        
        return False
    
    def _is_large_low_value(self, resp):
        """Detect very large files with potentially low information value."""
        if not resp.raw_response or not resp.raw_response.content:
            return False
        
        content_size = len(resp.raw_response.content)
        if content_size > Worker.MAX_CONTENT_SIZE:
            self.logger.warning(f"Large file detected: {content_size} bytes")
            return True
        
        return False
    
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
        consecutive_empty = 0
        max_consecutive_empty = 5
        
        while True:
            tbd_url = self.frontier.get_tbd_url()
            if not tbd_url:
                consecutive_empty += 1
                if consecutive_empty >= max_consecutive_empty:
                    self.logger.info("Frontier is empty. Stopping Crawler.")
                    break
                time.sleep(self.config.time_delay)
                continue
            
            consecutive_empty = 0
            
            self._wait_for_domain_politeness(tbd_url)
            resp = download(tbd_url, self.config, self.logger)
            self.logger.info(
                f"Downloaded {tbd_url}, status <{resp.status}>, "
                f"using cache {self.config.cache_server}.")
            
            # Check for dead URLs or large low-value files
            if self._is_dead_url(resp):
                self.logger.warning(f"Dead URL detected (no meaningful content): {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            
            if self._is_large_low_value(resp):
                self.logger.warning(f"Large low-value file detected, skipping: {tbd_url}")
                self.frontier.mark_url_complete(tbd_url)
                time.sleep(self.config.time_delay)
                continue
            
            # Only process successful downloads
            if resp.status == 200:
                if resp.raw_response and resp.raw_response.content:
                    self.stats_collector.save_page_stats(tbd_url, resp)
                scraped_urls = scraper.scraper(tbd_url, resp)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            else:
                scraped_urls = scraper.scraper(tbd_url, resp)
                # Still check for links in non-200 responses (redirects, etc.)
                for scraped_url in scraped_urls:
                    self.frontier.add_url(scraped_url)
            
            self.frontier.mark_url_complete(tbd_url)
            time.sleep(self.config.time_delay)
