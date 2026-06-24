import requests
import xml.etree.ElementTree as ET
import re

def normalize_title(t):
    return "".join(c for c in t.lower() if c.isalnum())

def is_similar(found, target):
    norm_f = normalize_title(found)
    norm_t = normalize_title(target)
    if norm_f == norm_t or norm_t in norm_f or norm_f in norm_t:
        return True
    import difflib
    return difflib.SequenceMatcher(None, norm_f, norm_t).ratio() > 0.90

from urllib.parse import quote
def query_api(title):
    clean_t = re.sub(r'\$.*?\$', '', title)
    clean_t = re.sub(r'[\{\}\[\]\\]', '', clean_t)
    clean_t = clean_t.replace('"', '').strip()
    api_query_title = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
    api_query_title = re.sub(r'\s+', ' ', api_query_title).strip()
    url = f"https://export.arxiv.org/api/query?search_query=ti:%22{quote(api_query_title)}%22&max_results=3"
    print(url)
    response = requests.get(url)
    root = ET.fromstring(response.content)
    namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
    entries = root.findall('.//atom:entry', namespaces)
    for entry in entries:
        found_title_elem = entry.find('atom:title', namespaces)
        if found_title_elem is not None and found_title_elem.text:
            found_title = found_title_elem.text.replace('\n', ' ')
            if is_similar(found_title, title):
                id_url = entry.find('atom:id', namespaces).text
                raw_id = id_url.split('/abs/')[-1]
                arxiv_id = re.sub(r'v\d+$', '', raw_id).strip()
                print(f"   [API Search] Found ArXiv ID for '{title[:30]}...': {arxiv_id}")
                return {"id": arxiv_id}
    print("Not found")

query_api("PSTNet: Point Spatio-Temporal Convolution on Point Cloud Sequences")
