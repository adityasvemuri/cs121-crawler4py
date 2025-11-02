import re
from html.parser import HTMLParser
from urllib.parse import urlparse, urljoin, urlunparse

class LinkExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.links = []
        self.base_url = base_url

    def handle_starttag(self, tag, attrs):
        if tag == 'a':
            for attr, value in attrs:
                if attr == 'href' and value:
                    absolute_url = urljoin(self.base_url, value)
                    parsed = urlparse(absolute_url)
                    # Remove fragment part (defragment URLs)
                    clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ''))
                    self.links.append(clean_url)

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    links = []
    if resp.status != 200:
        return links
    if not resp.raw_response or not resp.raw_response.content:
        return links
    try:
        content = resp.raw_response.content
        if isinstance(content, bytes):
            html_content = content.decode('utf-8', errors='ignore')
        else:
            html_content = str(content)
        parser = LinkExtractor(resp.url if resp.url else url)
        parser.feed(html_content)
        links = parser.links
    except Exception:
        pass
    return links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        
        # Check for all required domains: *.ics.uci.edu, *.cs.uci.edu, *.informatics.uci.edu, *.stat.uci.edu
        netloc = parsed.netloc.lower()
        valid_domains = [".ics.uci.edu", ".cs.uci.edu", ".informatics.uci.edu", ".stat.uci.edu"]
        if not any(netloc.endswith(domain) for domain in valid_domains):
            return False
        
        # Check for fragment (should be removed, but validate it's not in the URL)
        if parsed.fragment:
            return False
        
        # Check for invalid file extensions using regex
        # Remove query parameters for checking extensions
        path_lower = parsed.path.lower()
        if '?' in path_lower:
            path_part = path_lower.split('?')[0]
        else:
            path_part = path_lower
        
        return not re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz|xml|rss|json"
            + r"|txt|py|java|cpp|c|h|hpp|cc|svg|woff"
            + r"|woff2|ttf|eot|otf)$", path_part)
    except TypeError:
        print("TypeError for ", parsed)
        raise
    except (AttributeError, Exception):
        return False
