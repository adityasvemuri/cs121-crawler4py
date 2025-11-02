import shelve
import re
from urllib.parse import urlparse
from collections import defaultdict

STOP_WORDS = {
    'a', 'an', 'and', 'are', 'as', 'at', 'be', 'been', 'by', 'for', 'from',
    'has', 'he', 'in', 'is', 'it', 'its', 'of', 'on', 'that', 'the', 'to',
    'was', 'were', 'will', 'with', 'the', 'this', 'but', 'they', 'have',
    'had', 'what', 'said', 'each', 'which', 'their', 'if', 'up', 'out',
    'many', 'then', 'them', 'these', 'so', 'some', 'her', 'would', 'make',
    'like', 'into', 'him', 'has', 'two', 'more', 'go', 'no', 'way', 'could',
    'my', 'than', 'first', 'been', 'call', 'who', 'oil', 'its', 'now',
    'find', 'down', 'day', 'did', 'get', 'come', 'made', 'may', 'part'
}

def analyze_crawl_data(save_file='frontier.shelve', stats_file='crawl_stats.shelve'):
    print("Analyzing crawl data...")
    
    unique_urls = set()
    subdomain_counts = defaultdict(set)
    
    save = shelve.open(save_file)
    try:
        for url, completed in save.values():
            if completed:
                parsed = urlparse(url)
                unique_urls.add(url)
                subdomain = parsed.netloc
                subdomain_counts[subdomain].add(url)
    finally:
        save.close()
    
    print(f"\n{'='*60}")
    print(f"1. UNIQUE PAGES: {len(unique_urls)}")
    print(f"{'='*60}")
    
    word_counts = {}
    page_word_counts = {}
    
    try:
        stats = shelve.open(stats_file)
        try:
            all_words = {}
            for urlhash, data in stats.items():
                if isinstance(data, dict) and 'word_count' in data:
                    url_key = data.get('url', urlhash)
                    page_word_counts[url_key] = data['word_count']
                    if 'words' in data and isinstance(data['words'], dict):
                        for word, count in data['words'].items():
                            if word in all_words:
                                all_words[word] += count
                            else:
                                all_words[word] = count
            word_counts = all_words
        finally:
            stats.close()
    except FileNotFoundError:
        print(f"Warning: Statistics file {stats_file} not found. Run the crawler first.")
        return
    
    # Filter out stop words and words with 2 or fewer characters (only 3+ character words)
    filtered_word_counts = {word: count for word, count in word_counts.items() 
                           if word not in STOP_WORDS and len(word) > 2}
    top_50 = sorted(filtered_word_counts.items(), key=lambda x: (-x[1], x[0]))[:50]
    
    longest_page_url = max(page_word_counts.items(), key=lambda x: x[1]) if page_word_counts else ("N/A", 0)
    
    print(f"\n{'='*60}")
    print(f"2. LONGEST PAGE:")
    print(f"   URL: {longest_page_url[0]}")
    print(f"   Word Count: {longest_page_url[1]}")
    print(f"{'='*60}")
    
    print(f"\n{'='*60}")
    print(f"3. TOP 50 WORDS:")
    print(f"{'='*60}")
    for i, (word, count) in enumerate(top_50, 1):
        print(f"   {i:2d}. {word:20s} : {count:8d}")
    
    print(f"\n{'='*60}")
    print(f"4. SUBDOMAINS FOUND:")
    print(f"{'='*60}")
    sorted_subdomains = sorted(subdomain_counts.items())
    for subdomain, urls in sorted_subdomains:
        print(f"   {subdomain}, {len(urls)}")
    
    print(f"\n{'='*60}")
    
    return {
        'unique_pages': len(unique_urls),
        'longest_page': longest_page_url,
        'top_50_words': top_50,
        'subdomains': {subdomain: len(urls) for subdomain, urls in sorted_subdomains}
    }

if __name__ == "__main__":
    analyze_crawl_data()

