import shelve
from html.parser import HTMLParser
import time
import os
import dbm
import PartA
from utils import get_urlhash, normalize

class TextExtractor(HTMLParser):
    def __init__(self):
        super().__init__()
        self.text = []
        self.in_script = False
        self.in_style = False
    
    def handle_starttag(self, tag, attrs):
        if tag in ['script', 'style']:
            self.in_script = tag == 'script'
            self.in_style = tag == 'style'
    
    def handle_endtag(self, tag):
        if tag in ['script', 'style']:
            self.in_script = False
            self.in_style = False
    
    def handle_data(self, data):
        if not self.in_script and not self.in_style:
            self.text.append(data)
    
    def get_text(self):
        return '\n'.join(self.text)

class StatisticsCollector:
    def __init__(self, stats_file='crawl_stats.shelve'):
        self.stats_file = stats_file
    
    def extract_text_from_html(self, html_content):
        parser = TextExtractor()
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8', errors='ignore')

        parser.feed(html_content)
        return parser.get_text()
    
    def count_words(self, text):
        return PartA.tokenize_text(text)
    
    def save_page_stats(self, url, resp):
        """Save page statistics.
        
        Args:
            url: URL of the page
            resp: Response object
        """
        if not resp.raw_response or not resp.raw_response.content:
            return
        
        html_content = resp.raw_response.content
        text = self.extract_text_from_html(html_content)
        word_counts = self.count_words(text)
        
        total_word_count = sum(word_counts.values())
        
        normalized_url = normalize(url)
        urlhash = get_urlhash(normalized_url)
        
        # Retry logic for shelve file access (handles concurrent access issues)
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Use flag='c' to create if doesn't exist, open if exists
                stats = shelve.open(self.stats_file, flag='c')
                stats[urlhash] = {
                    'url': normalized_url,
                    'word_count': total_word_count,
                    'words': word_counts
                }
                stats.sync()
                stats.close()
                break  # Success, exit retry loop
            except (dbm.error, OSError) as e:
                # Handle concurrent access errors with retryyyy
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))  # Exponential backoff
                    continue
                else:
                    # On final failure, silently skip saving stats for this page
                    # The page will still be crawled and processed normally
                    pass
            except Exception:
                # For any other unexpected error, skip saving stats
                break

