import os
import shelve

from utils import get_logger, get_urlhash, normalize
from scraper import is_valid

class Frontier(object):
    def __init__(self, config, restart):
        self.logger = get_logger("FRONTIER")
        self.config = config
        self.to_be_downloaded = []
        
        if not os.path.exists(self.config.save_file) and not restart:
            self.logger.info(
                f"Did not find save file {self.config.save_file}, "
                f"starting from seed.")
        elif os.path.exists(self.config.save_file) and restart:
            self.logger.info(
                f"Found save file {self.config.save_file}, deleting it.")
            os.remove(self.config.save_file)
        
        save = shelve.open(self.config.save_file)
        if restart:
            for url in self.config.seed_urls:
                urlhash = get_urlhash(normalize(url))
                save[urlhash] = (normalize(url), False)
                self.to_be_downloaded.append(normalize(url))
        else:
            total_count = len(save)
            tbd_count = 0
            for url, completed in save.values():
                if not completed and is_valid(url):
                    self.to_be_downloaded.append(url)
                    tbd_count += 1
            if tbd_count == 0 and total_count == 0:
                for url in self.config.seed_urls:
                    urlhash = get_urlhash(normalize(url))
                    save[urlhash] = (normalize(url), False)
                    self.to_be_downloaded.append(normalize(url))
            else:
                self.logger.info(
                    f"Found {tbd_count} urls to be downloaded from {total_count} "
                    f"total urls discovered.")
        save.close()

    def get_tbd_url(self):
        if self.to_be_downloaded:
            return self.to_be_downloaded.pop(0)
        return None

    def add_url(self, url):
        url = normalize(url)
        urlhash = get_urlhash(url)
        save = shelve.open(self.config.save_file)
        if urlhash not in save:
            save[urlhash] = (url, False)
            save.sync()
            save.close()
            self.to_be_downloaded.append(url)
        else:
            save.close()
    
    def mark_url_complete(self, url):
        urlhash = get_urlhash(url)
        save = shelve.open(self.config.save_file)
        if urlhash not in save:
            self.logger.error(
                f"Completed url {url}, but have not seen it before.")
        save[urlhash] = (url, True)
        save.sync()
        save.close()
