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
                    clean_url = urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, parsed.query, ''))
                    self.links.append(clean_url)

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]

def extract_next_links(url, resp):
    links = []
    if resp.status != 200:
        return links
    if not resp.raw_response or not resp.raw_response.content:
        return links
    try:
        content = resp.raw_response.content
        if isinstance(content, bytes):
            try:
                html_content = content.decode('utf-8', errors='ignore')
            except:
                html_content = content.decode('latin-1', errors='ignore')
        else:
            html_content = str(content)
        parser = LinkExtractor(resp.url if resp.url else url)
        parser.feed(html_content)
        links = parser.links
    except Exception:
        pass
    return links

def is_valid(url):
    try:
        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            return False
        if not parsed.netloc.endswith(".ics.uci.edu"):
            return False
        if parsed.fragment:
            return False
        invalid_extensions = [
            "css", "js", "bmp", "gif", "jpg", "jpeg", "ico", "png", "tiff", "tif",
            "mid", "mp2", "mp3", "mp4", "wav", "avi", "mov", "mpeg", "mpg", "ram",
            "m4v", "mkv", "ogg", "ogv", "pdf", "ps", "eps", "tex", "ppt", "pptx",
            "doc", "docx", "xls", "xlsx", "names", "data", "dat", "exe", "bz2",
            "tar", "msi", "bin", "7z", "psd", "dmg", "iso", "epub", "dll", "cnf",
            "tgz", "sha1", "thmx", "mso", "arff", "rtf", "jar", "csv", "rm",
            "smil", "wmv", "swf", "wma", "zip", "rar", "gz", "xml", "rss", "json",
            "txt", "py", "java", "cpp", "c", "h", "hpp", "cc", "svg", "woff",
            "woff2", "ttf", "eot", "otf"
        ]
        path_lower = parsed.path.lower()
        for ext in invalid_extensions:
            if path_lower.endswith('.' + ext):
                return False
        if '?' in path_lower:
            path_part = path_lower.split('?')[0]
            for ext in invalid_extensions:
                if path_part.endswith('.' + ext):
                    return False
        return True
    except (TypeError, AttributeError):
        return False
