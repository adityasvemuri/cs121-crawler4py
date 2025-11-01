import shelve
import time
from urllib.parse import urlparse

def monitor_crawl(save_file='frontier.shelve', interval=10):    
    try:
        while True:
            save = shelve.open(save_file)
            try:
                total_urls = len(save)
                completed = sum(1 for url, status in save.values() if status)
                pending = total_urls - completed
                
                subdomains = {}
                for url, status in save.values():
                    if status:
                        parsed = urlparse(url)
                        domain = parsed.netloc
                        subdomains[domain] = subdomains.get(domain, 0) + 1
                
                print(f"\n{'='*60}")
                print(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'='*60}")
                print(f"Total URLs discovered: {total_urls}")
                print(f"Completed: {completed}")
                print(f"Pending: {pending}")
                print(f"\nUnique subdomains found: {len(subdomains)}")
                if subdomains:
                    print("\nTop 10 subdomains by page count:")
                    sorted_subs = sorted(subdomains.items(), key=lambda x: x[1], reverse=True)[:10]
                    for subdomain, count in sorted_subs:
                        print(f"  {subdomain:40s} : {count:6d} pages")
                print(f"{'='*60}\n")
            finally:
                save.close()
            
            time.sleep(interval)
    except KeyboardInterrupt:
        print("\nMonitoring stopped.")

if __name__ == "__main__":
    monitor_crawl()

