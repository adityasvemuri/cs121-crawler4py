import shelve
from html.parser import HTMLParser
import threading
import time
import os
import dbm
import hashlib
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

def compute_simhash(text, hash_bits=64):
    """Compute SimHash for text content.
    
    Args:
        text: Text content to hash
        hash_bits: Number of bits in the hash (default 64)
    
    Returns:
        Integer representing the SimHash
    """
    if not text:
        return 0
    
    # Tokenize the text
    tokens = PartA.text_parser(text)
    
    # Initialize hash vector
    hash_vector = [0] * hash_bits
    
    # Process each token
    for token in tokens:
        if not token:
            continue
        
        # Hash the token using MD5
        token_hash = hashlib.md5(token.encode('utf-8', errors='ignore')).digest()
        
        # Convert to integer and process each bit
        # Use first 8 bytes (64 bits) from MD5 hash
        for i in range(min(hash_bits, len(token_hash) * 8)):
            byte_idx = i // 8
            bit_idx = i % 8
            bit = (token_hash[byte_idx] >> bit_idx) & 1
            
            if bit == 1:
                hash_vector[i] += 1
            else:
                hash_vector[i] -= 1
    
    # Convert vector to simhash
    simhash = 0
    for i in range(hash_bits):
        if hash_vector[i] > 0:
            simhash |= (1 << i)
    
    return simhash

def hamming_distance(hash1, hash2):
    """Calculate Hamming distance between two hashes.
    
    Args:
        hash1: First hash (integer)
        hash2: Second hash (integer)
    
    Returns:
        Number of differing bits
    """
    xor_result = hash1 ^ hash2
    return bin(xor_result).count('1')

class StatisticsCollector:
    def __init__(self, stats_file='crawl_stats.shelve', simhash_threshold=3):
        self.stats_file = stats_file
        self.lock = threading.RLock()
        self.simhash_threshold = simhash_threshold  # Hamming distance threshold for near-duplicates
    
    def extract_text_from_html(self, html_content):
        parser = TextExtractor()
        if isinstance(html_content, bytes):
            html_content = html_content.decode('utf-8', errors='ignore')

        parser.feed(html_content)
        return parser.get_text()
    
    def count_words(self, text):
        return PartA.tokenize_text(text)
    
    def compute_simhash(self, text):
        """Compute SimHash for text."""
        return compute_simhash(text)
    
    def is_near_duplicate(self, simhash):
        """Check if simhash is near-duplicate of any stored page.
        
        Args:
            simhash: SimHash to check
        
        Returns:
            True if near-duplicate found, False otherwise
        """
        if simhash == 0:
            return False
        
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                with self.lock:
                    stats = shelve.open(self.stats_file, flag='r')
                    try:
                        for urlhash, data in stats.items():
                            if isinstance(data, dict) and 'simhash' in data:
                                stored_simhash = data['simhash']
                                if stored_simhash and hamming_distance(simhash, stored_simhash) <= self.simhash_threshold:
                                    stats.close()
                                    return True
                    finally:
                        stats.close()
                break
            except (dbm.error, OSError):
                if attempt < max_retries - 1:
                    time.sleep(retry_delay * (attempt + 1))
                    continue
                else:
                    return False
            except Exception:
                return False
        
        return False
    
    def save_page_stats(self, url, resp, simhash=None):
        """Save page statistics including simhash.
        
        Args:
            url: URL of the page
            resp: Response object
            simhash: Optional pre-computed simhash (if None, will compute from text)
        """
        if not resp.raw_response or not resp.raw_response.content:
            return
        
        html_content = resp.raw_response.content
        text = self.extract_text_from_html(html_content)
        word_counts = self.count_words(text)
        
        total_word_count = sum(word_counts.values())
        
        normalized_url = normalize(url)
        urlhash = get_urlhash(normalized_url)
        
        # Compute simhash if not provided
        if simhash is None:
            page_simhash = self.compute_simhash(text)
        else:
            page_simhash = simhash
        
        # Retry logic for shelve file access (handles concurrent access issues)
        max_retries = 3
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                with self.lock:
                    # Use flag='c' to create if doesn't exist, open if exists
                    stats = shelve.open(self.stats_file, flag='c')
                    stats[urlhash] = {
                        'url': normalized_url,
                        'word_count': total_word_count,
                        'words': word_counts,
                        'simhash': page_simhash
                    }
                    stats.sync()
                    stats.close()
                break  # Success, exit retry loop
            except (dbm.error, OSError) as e:
                # Handle concurrent access errors with retry
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

