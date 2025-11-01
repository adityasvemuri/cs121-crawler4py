import shelve
import re
from html.parser import HTMLParser
import threading
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'Assignment1'))
from Assignment1 import PartA

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
        self.lock = threading.RLock()
    
    def extract_text_from_html(self, html_content):
        parser = TextExtractor()
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8', errors='ignore')

        parser.feed(html_content)
        return parser.get_text()
    
    def count_words(self, text):
        return PartA.tokenize_text(text)
    
    def save_page_stats(self, url, resp):
        if not resp.raw_response or not resp.raw_response.content:
            return
        
        html_content = resp.raw_response.content
        text = self.extract_text_from_html(html_content)
        word_counts = self.count_words(text)
        
        total_word_count = sum(word_counts.values())
        
        with self.lock:
            stats = shelve.open(self.stats_file)
            stats[url] = {
                'word_count': total_word_count,
                'words': word_counts
            }
            stats.sync()
            stats.close()

