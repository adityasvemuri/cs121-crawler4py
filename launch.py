from configparser import ConfigParser
from argparse import ArgumentParser
import signal
import sys

from utils.server_registration import get_cache_server
from utils.config import Config
from crawler import Crawler


def main(config_file, restart):
    cparser = ConfigParser()
    cparser.read(config_file)
    config = Config(cparser)
    config.cache_server = get_cache_server(config, restart)
    crawler = Crawler(config, restart)
    
    def signal_handler(sig, frame):
        print("\n\nKeyboard interrupt received. Saving progress...")
        crawler.logger.info("Keyboard interrupt received. Workers will finish current operations...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        crawler.start()
    except KeyboardInterrupt:
        print("\n\nKeyboard interrupt received. Saving progress...")
        crawler.logger.info("Keyboard interrupt received. Shutting down gracefully...")
        sys.exit(0)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--restart", action="store_true", default=False)
    parser.add_argument("--config_file", type=str, default="config.ini")
    args = parser.parse_args()
    main(args.config_file, args.restart)
