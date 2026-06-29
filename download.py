import argparse
import os
import tarfile
import gzip
import requests
import openreview
import time
import re
import json
from lxml import etree as ET
from urllib.parse import urlencode, quote
from dotenv import load_dotenv, set_key
import shutil
import difflib
from concurrent.futures import ThreadPoolExecutor
import socket
from stem import Signal
from stem.control import Controller

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

# --- CONFIGURATION ---
CONFERENCE_ID = ["NeurIPS.cc", "ICLR.cc", "ICML.cc", "ACL", "EMNLP", "CVPR", "ECCV", "SIGIR", "KDD", "WSDM", "ICDM", "SIGMOD", "ICDE"]
TARGET_YEARS = [2025, 2024, 2023, 2022, 2021]
DOWNLOAD_DIR = "./downloaded_tex"
CHECKPOINT_FILE = "download_progress.json"
GDRIVE_FOLDER_NAME = 'FFT_DataInconsistency/Data'  # Now handles nested creation correctly

SCOPES = ['https://www.googleapis.com/auth/drive.file']

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
ARXIV_API_BLOCKED = False
SEMANTIC_SCHOLAR_BLOCKED_UNTIL = 0.0  # Unix timestamp: skip S2 calls until this time
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

DIRECT_SESSION = requests.Session()
DIRECT_SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
})

# --- TOR CONFIGURATION ---
TOR_PROXY = "socks5h://127.0.0.1:9050"
TOR_CONTROL_PORT = 9051

def rotate_ip():
    """Requests a new IP address from the local Tor daemon."""
    try:
        with Controller.from_port(port=TOR_CONTROL_PORT) as controller:
            controller.authenticate()  # Tries various authentication methods including cookie auth
            controller.signal(Signal.NEWNYM)
            print("   🔄 [Tor] Successfully requested new IP identity. Waiting 10s for circuit...")
            # Wait for Tor to establish a new stable circuit
            time.sleep(10)
    except Exception as e:
        error_msg = str(e)
        if "Permission denied" in error_msg and "control.authcookie" in error_msg:
            print(f"   ⚠️ [Tor] Authentication failed due to strict permissions on the Tor cookie.")
            print("   ⚠️ To fix this, run these commands in your terminal and restart the script:")
            print("      sudo usermod -aG debian-tor $USER")
            print("      sudo chmod 644 /run/tor/control.authcookie")
        else:
            print(f"   ⚠️ [Tor] Failed to rotate IP: {e}")
            print("   ⚠️ Ensure Tor is installed (`sudo apt install tor`) and ControlPort 9051 is enabled in /etc/tor/torrc.")

def configure_tor(session):
    """Checks if Tor is running locally and configures the session to use it."""
    try:
        with socket.create_connection(("127.0.0.1", 9050), timeout=1):
            session.proxies = {
                'http': TOR_PROXY,
                'https': TOR_PROXY
            }
            print("✅ Tor proxy detected. Routing arXiv requests through Tor to prevent rate limits.")
            return True
    except (ConnectionRefusedError, socket.timeout, OSError):
        print("⚠️ Tor proxy not found at 127.0.0.1:9050.")
        print("⚠️ To enable IP rotation to bypass arXiv limits, please install Tor:")
        print("   sudo apt update && sudo apt install tor -y")
        print("   And append these lines to /etc/tor/torrc:")
        print("   ControlPort 9051")
        print("   CookieAuthentication 1")
        print("   Then restart tor: sudo systemctl restart tor")
        return False

configure_tor(SESSION)

# --- GOOGLE DRIVE HELPERS ---
def get_gdrive_service():
    """Authenticates using JSON data stored in environment variables."""
    creds = None
    
    token_env = os.environ.get('GDRIVE_TOKEN_JSON')
    if token_env:
        try:
            token_info = json.loads(token_env)
            creds = Credentials.from_authorized_user_info(token_info)
            print("Loaded authentication token from environment.")
        except Exception as e:
            print(f"⚠️ Could not parse GDRIVE_TOKEN_JSON environment variable: {e}")

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            print("Token expired. Refreshing...")
            creds.refresh(Request())
            # Save the refreshed token to the environment and .env file
            token_json = creds.to_json()
            os.environ['GDRIVE_TOKEN_JSON'] = token_json
            env_path = os.path.join(os.path.dirname(__file__), '.env')
            set_key(env_path, 'GDRIVE_TOKEN_JSON', token_json)
            print("Refreshed token saved to .env")
        else:
            creds_env = os.environ.get('GDRIVE_CREDENTIALS_JSON')
            if not creds_env:
                raise ValueError("Missing both GDRIVE_TOKEN_JSON and GDRIVE_CREDENTIALS_JSON environment variables.")
            
            print("No valid token found. Initiating browser authentication flow...")
            client_config = json.loads(creds_env)
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)
            
            print("\n" + "="*50)
            print("👉 NEW REFRESH TOKEN GENERATED 👈")
            print("To run headlessly next time, copy the entire JSON line below")
            print("and save it as your GDRIVE_TOKEN_JSON environment variable:\n")
            print(creds.to_json())
            print("="*50 + "\n")
            
    return build('drive', 'v3', credentials=creds)

def create_gdrive_folder(service, folder_name, parent_id=None):
    """Finds an existing folder or creates a new one on Google Drive."""
    query = f"name = '{folder_name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        query += " and 'root' in parents"
    
    results = service.files().list(q=query, fields="files(id)").execute()
    items = results.get('files', [])
    if items:
        return items[0]['id']
        
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder'
    }
    if parent_id:
        file_metadata['parents'] = [parent_id]
        
    folder = service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def get_all_gdrive_files(service, folder_id):
    """Retrieves a set of all file names present in a specific Google Drive folder."""
    file_names = set()
    page_token = None
    query = f"'{folder_id}' in parents and trashed = false"
    print("   [Google Drive] Fetching list of already uploaded files for dynamic skipping...")
    
    while True:
        try:
            results = service.files().list(q=query, spaces='drive', fields='nextPageToken, files(name)', pageToken=page_token).execute()
            items = results.get('files', [])
            for item in items:
                file_names.add(item.get('name'))
            page_token = results.get('nextPageToken', None)
            if page_token is None:
                break
        except Exception as e:
            print(f"   ⚠️ Error fetching files from Google Drive: {e}")
            break
            
    print(f"   [Google Drive] Found {len(file_names)} files in the destination folder.")
    return file_names

def get_or_create_gdrive_path(service, path_string):
    """Resolves a nested path string like 'FolderA/FolderB' into a final target folder ID."""
    parts = [p.strip() for p in path_string.split('/') if p.strip()]
    parent_id = None
    for part in parts:
        parent_id = create_gdrive_folder(service, part, parent_id)
    return parent_id

def upload_single_file(service, item_path, item, drive_parent_id):
    """Worker function to upload a single file."""
    file_metadata = {'name': item, 'parents': [drive_parent_id]}
    file_size = os.path.getsize(item_path)
    # Turn off resumable for files under 5MB to skip session setup overhead
    is_resumable = file_size > (5 * 1024 * 1024) 
    
    media = MediaFileUpload(item_path, resumable=is_resumable)
    service.files().create(body=file_metadata, media_body=media, fields='id').execute()

def upload_to_gdrive(service, local_dir, drive_parent_id):
    """Uploads local folders using parallel threads to bypass network latency."""
    files_to_upload = []
    folders_to_process = []
    
    # Separate files from directories first
    for item in os.listdir(local_dir):
        item_path = os.path.join(local_dir, item)
        if os.path.isdir(item_path):
            folders_to_process.append((item, item_path))
        else:
            files_to_upload.append((item_path, item))
            
    # Fire off up to 3 concurrent file uploads at once
    if files_to_upload:
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [
                executor.submit(upload_single_file, service, path, name, drive_parent_id)
                for path, name in files_to_upload
            ]
            # Ensure they all finish before moving on
            for future in futures:
                future.result()
                
    # Recursively handle subfolders sequentially
    for name, path in folders_to_process:
        sub_folder_id = create_gdrive_folder(service, name, drive_parent_id)
        upload_to_gdrive(service, path, sub_folder_id)
            
# --- ORIGINAL SCRAPER UTILITIES ---
def normalize_title(t):
    return "".join(c for c in t.lower() if c.isalnum())

def is_similar(found, target):
    norm_f = normalize_title(found)
    norm_t = normalize_title(target)
    if norm_f == norm_t or norm_t in norm_f or norm_f in norm_t:
        return True
    return difflib.SequenceMatcher(None, norm_f, norm_t).ratio() > 0.90

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {
                    "processed_titles": data.get("processed_titles", []),
                    "not_found_titles": data.get("not_found_titles", []),
                    "success_count": data.get("success_count", 0)
                }
        except Exception:
            pass
    return {"processed_titles": [], "not_found_titles": [], "success_count": 0}

def save_checkpoint(processed_titles, not_found_titles, success_count):
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({"processed_titles": processed_titles, "not_found_titles": not_found_titles, "success_count": success_count}, f, ensure_ascii=False, indent=4)

def get_openreview_papers_fixed(conference_id, years):
    titles = []
    for year in years:
        venue_id = f"{conference_id}/{year}/Conference"
        print(f"Đang quét bài được chấp nhận tại {venue_id}...")
        
        if year <= 2022:
            try:
                client_v1 = openreview.Client(baseurl='https://api.openreview.net')
                submissions = client_v1.get_all_notes(invitation=f"{venue_id}/-/Blind_Submission")
                if not submissions:
                    submissions = client_v1.get_all_notes(invitation=f"{venue_id}/-/Submission")
                
                year_titles_count = 0
                for note in submissions:
                    content = note.content
                    venue_status = content.get('venue', '')
                    bibtex_status = content.get('_bibtex', '')
                    
                    is_accepted = any(
                        keyword in str(venue_status).lower() or keyword in str(bibtex_status).lower()
                        for keyword in ['accept', 'poster', 'oral', 'spotlight']
                    )
                    
                    if is_accepted and 'workshop' not in str(venue_status).lower():
                        title = content.get('title')
                        if title:
                            titles.append(title.strip())
                            year_titles_count += 1
                print(f"-> [Thành công V1] Lọc được {year_titles_count} bài ACCEPTED cho năm {year}")
            except Exception as e:
                print(f"❌ Lỗi lọc bài hệ thống cũ năm {year}: {e}")
        else:
            try:
                client_v2 = openreview.api.OpenReviewClient(baseurl='https://api2.openreview.net')
                submissions = client_v2.get_all_notes(content={'venueid': venue_id})
                for note in submissions:
                    content = note.content
                    venue_name = content.get('venue', {}).get('value', '') if isinstance(content.get('venue'), dict) else content.get('venue', '')
                    if 'workshop' in str(venue_name).lower():
                        continue
                    title_obj = content.get('title', {})
                    title = title_obj.get('value') if isinstance(title_obj, dict) else title_obj
                    if title: 
                        titles.append(title.strip())
                print(f"-> [Thành công V2] Tìm thấy {len(submissions)} bài ACCEPTED cho năm {year}")
            except Exception as e:
                print(f"❌ Lỗi lọc bài hệ thống mới năm {year}: {e}")
        time.sleep(1)
    return list(set(titles))

def prefetch_all_dblp_papers(conferences, years, xml_path="dblp.xml"):
    """
    Scans the massive dblp.xml file iteratively and extracts all papers 
    for the requested conferences and years in a single pass.
    Returns a dictionary: { "CVPR": [title1, title2], ... }
    """
    print(f"🔄 Scanning {xml_path} for {len(conferences)} conferences over years {years}...")
    results = {conf: set() for conf in conferences}
    target_years_str = {str(y) for y in years}
    target_conf_lower = {conf: conf.lower() for conf in conferences}
    
    if not os.path.exists(xml_path):
        print(f"❌ {xml_path} not found. Cannot parse local DBLP data.")
        return {conf: [] for conf in conferences}
        
    try:
        context = ET.iterparse(xml_path, events=("end",), load_dtd=True, resolve_entities=True)
        for event, elem in context:
            if elem.tag in ("inproceedings", "article"):
                year_elem = elem.find("year")
                if year_elem is not None and year_elem.text in target_years_str:
                    
                    venue = ""
                    booktitle = elem.find("booktitle")
                    if booktitle is not None and booktitle.text:
                        venue = booktitle.text.lower()
                    else:
                        journal = elem.find("journal")
                        if journal is not None and journal.text:
                            venue = journal.text.lower()
                    
                    if venue:
                        # Check if this venue matches any of our targets
                        for conf, conf_lower in target_conf_lower.items():
                            if conf_lower in venue:
                                title_elem = elem.find("title")
                                if title_elem is not None and title_elem.text:
                                    title = title_elem.text.strip()
                                    if title.endswith('.'):
                                        title = title[:-1]
                                    results[conf].add(title)
                                    break
                                    
                # Aggressively clear memory to prevent OOM on 5GB XML
                elem.clear()
                while elem.getprevious() is not None:
                    del elem.getparent()[0]
                    
    except Exception as e:
        print(f"❌ Error while parsing {xml_path}: {e}")
        
    final_results = {conf: list(titles) for conf, titles in results.items()}
    for conf, titles in final_results.items():
        print(f"-> [Thành công] Found {len(titles)} papers from DBLP for {conf}")
        
    return final_results
class ArxivRateLimiter:
    def __init__(self, max_requests=4, window=1.0):
        self.max_requests = max_requests
        self.window = window
        self.requests = []

    def wait(self):
        now = time.time()
        # Clean up timestamps older than the sliding window
        self.requests = [t for t in self.requests if now - t < self.window]
        
        if len(self.requests) >= self.max_requests:
            # Calculate how long to sleep until the oldest request falls out of the window
            sleep_time = self.window - (now - self.requests[0])
            if sleep_time > 0:
                print(f"   [Rate Limiter] Rate limit ({self.max_requests} req/{self.window}s) reached. Sleeping for {sleep_time:.2f}s...")
                time.sleep(sleep_time)
            
            # Update timestamps after sleeping
            now = time.time()
            self.requests = [t for t in self.requests if now - t < self.window]
            
        self.requests.append(time.time())

arxiv_rate_limiter = ArxivRateLimiter(max_requests=4, window=1.0)

def make_arxiv_request(url, headers=None, stream=False, timeout=45, max_retries=7, use_direct_fallback=False):
    """Makes a rate-limited request to arXiv with retry logic for rate limits and server errors.
    If use_direct_fallback is True, it will bypass Tor and use DIRECT_SESSION for the final 2 retries.
    """
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    backoff = 2.0
    for attempt in range(max_retries):
        arxiv_rate_limiter.wait()
        
        session_to_use = SESSION
        if use_direct_fallback and attempt >= max_retries - 2:
            session_to_use = DIRECT_SESSION
            if attempt == max_retries - 2:
                print(f"   ⚠️ Tor failed repeatedly. Falling back to DIRECT connection...")
                
        try:
            response = session_to_use.get(url, headers=headers, stream=stream, timeout=timeout)
            
            # Handle rate limit (429) or temporary server error (403, 500, 502, 503, 504)
            if response.status_code in (403, 429, 500, 502, 503, 504):
                if attempt + 1 < max_retries:
                    print(f"   ⚠️ arXiv returned status code {response.status_code}. Retrying in {backoff}s... (Attempt {attempt+1}/{max_retries})")
                    if session_to_use == SESSION and 'socks' in SESSION.proxies.get('http', ''):
                        rotate_ip()
                    else:
                        time.sleep(backoff)
                    backoff *= 1.5
                    continue
                else:
                    break
                
            return response
        except (requests.exceptions.RequestException, ConnectionError) as e:
            if attempt + 1 < max_retries:
                print(f"   ⚠️ Connection error: {e}. Retrying in {backoff}s... (Attempt {attempt+1}/{max_retries})")
                if session_to_use == SESSION and 'socks' in SESSION.proxies.get('http', ''):
                    rotate_ip()
                else:
                    time.sleep(backoff)
                backoff *= 1.5
            else:
                break
            
    print(f"   ❌ Max retries reached for URL: {url}")
    return None

def query_arxiv_via_oai_pmh_batch(titles):
    """
    Queries arXiv API for a list of titles at once using OR operator.
    Returns a dictionary mapping normalized_title to {"id": arxiv_id}.
    """
    global ARXIV_API_BLOCKED
    if ARXIV_API_BLOCKED:
        return {}
        
    cleaned_queries = []
    for title in titles:
        # Clean special latex math/brackets from title
        clean_t = re.sub(r'\$.*?\$', '', title)
        clean_t = re.sub(r'[\{\}\[\]\\]', '', clean_t)
        # Remove double quotes from search query to avoid syntax issues
        clean_t = clean_t.replace('"', '').strip()
        # Keep only alphanumeric and space for the API query to prevent syntax issues
        api_query_title = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
        api_query_title = re.sub(r'\s+', ' ', api_query_title).strip()
        
        if api_query_title:
            cleaned_queries.append(f'ti:"{api_query_title}"')

    if not cleaned_queries:
        return {}

    search_query = " OR ".join(cleaned_queries)
    params = {
        "search_query": search_query,
        "max_results": len(titles) * 2
    }
    
    base_api_url = "https://export.arxiv.org/api/query"
    search_url = f"{base_api_url}?{urlencode(params)}"
    
    results = {}
    try:
        response = make_arxiv_request(search_url, timeout=45, use_direct_fallback=True)
        if response is None or response.status_code != 200:
            print("   ⚠️ arXiv API batch query failed. Setting ARXIV_API_BLOCKED = True to skip API and use HTML fallback.")
            ARXIV_API_BLOCKED = True
            return {}
            
        if b"<html" in response.content.lower()[:100]:
            print("   ⚠️ Máy chủ trả về giao diện HTML thay vì dữ liệu XML. Setting ARXIV_API_BLOCKED = True.")
            ARXIV_API_BLOCKED = True
            return {}
        
        root = ET.fromstring(response.content)
        namespaces = {'atom': 'http://www.w3.org/2005/Atom'}
        entries = root.findall('.//atom:entry', namespaces)
        
        for entry in entries:
            found_title_elem = entry.find('atom:title', namespaces)
            if found_title_elem is None or not found_title_elem.text:
                continue
            found_title = found_title_elem.text
            
            id_url_elem = entry.find('atom:id', namespaces)
            if id_url_elem is None or not id_url_elem.text:
                continue
                
            id_url = id_url_elem.text
            raw_id = id_url.split('/abs/')[-1]
            arxiv_id = re.sub(r'v\d+$', '', raw_id).strip()
            
            results[normalize_title(found_title)] = {"id": arxiv_id}
            
    except Exception as e:
        print(f"⚠️ Lỗi xử lý cổng API lô bài báo: {e}")
        
    return results

def query_arxiv_via_semantic_scholar(title):
    """
    Queries Semantic Scholar API as a fallback to resolve paper title to arXiv ID.
    Uses a timed block: after a 429, skips all S2 calls for 60s to avoid hammering
    a rate-limited API and wasting time on every single paper.
    """
    global SEMANTIC_SCHOLAR_BLOCKED_UNTIL
    
    # If we were recently blocked, check if the cooldown has passed
    now = time.time()
    if now < SEMANTIC_SCHOLAR_BLOCKED_UNTIL:
        remaining = int(SEMANTIC_SCHOLAR_BLOCKED_UNTIL - now)
        print(f"   [Semantic Scholar] Skipping (rate-limited, cooldown {remaining}s remaining)")
        return None
    
    # Clean query title
    clean_t = re.sub(r'\$.*?\$', '', title)
    clean_t = re.sub(r'[\{\}\[\]\\]', '', clean_t)
    query_str = clean_t.replace('"', '').strip()
    
    url = f"https://api.semanticscholar.org/graph/v1/paper/search?query={quote(query_str)}&fields=title,externalIds&limit=1"
    
    # Respect their 1 req/sec guideline
    time.sleep(1.0)
    
    try:
        response = DIRECT_SESSION.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                paper = data["data"][0]
                found_title = paper.get("title", "")
                
                if is_similar(found_title, title):
                    
                    external_ids = paper.get("externalIds", {})
                    arxiv_id = external_ids.get("ArXiv")
                    if arxiv_id:
                        print(f"   [Semantic Scholar] Found ArXiv ID for '{title[:30]}...': {arxiv_id}")
                        return {"id": arxiv_id}
                        
        elif response.status_code == 429:
            # Block S2 for 60 seconds to stop hammering the rate-limited endpoint
            SEMANTIC_SCHOLAR_BLOCKED_UNTIL = time.time() + 60.0
            print(f"   [Semantic Scholar] Rate limited (429). Blocking S2 for 60s...")
            
    except Exception as e:
        print(f"   [Semantic Scholar] Error resolving '{title[:30]}...': {e}")
        
    return None

def query_arxiv_via_html_search(title):
    """
    Queries the user-facing arXiv HTML search page as a fallback to resolve paper title to arXiv ID.
    This bypasses API rate limits and tarpits.
    """
    # Clean query title
    clean_t = re.sub(r'\$.*?\$', '', title)
    clean_t = re.sub(r'[\{\}\[\]\\]', '', clean_t)
    query_str = clean_t.replace('"', '').strip()
    
    url = f"https://arxiv.org/search/?query={quote(query_str)}&searchtype=title"
    
    # Respect general browsing rate limit
    time.sleep(1.0)
    
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        response = make_arxiv_request(url, headers=headers, timeout=45, max_retries=7, use_direct_fallback=True)
        
        if response and response.status_code == 200:
            html = response.text
            chunks = html.split('<li class="arxiv-result">')[1:]
            
            def clean_html(text):
                clean = re.sub(r'<[^>]+>', '', text)
                return " ".join(clean.split())
            
            for chunk in chunks[:10]: # Check top 10 results
                id_match = re.search(r'href="https://arxiv\.org/abs/([^"]+)"', chunk)
                title_match = re.search(r'<p class="title is-5 mathjax">\s*(.*?)\s*</p>', chunk, re.DOTALL)
                
                if id_match and title_match:
                    raw_id = id_match.group(1)
                    arxiv_id = re.sub(r'v\d+$', '', raw_id).strip()
                    found_title = clean_html(title_match.group(1))
                    
                    if is_similar(found_title, title):
                        print(f"   [HTML Search] Found ArXiv ID for '{title[:30]}...': {arxiv_id}")
                        return {"id": arxiv_id}
                        
    except Exception as e:
        print(f"   [HTML Search] Error resolving '{title[:30]}...': {e}")
        
    return None

def query_arxiv_via_api_single(title):
    """Queries export.arxiv.org/api/query for a single title, which often avoids strict HTML firewalls."""
    clean_t = re.sub(r'\$.*?\$', '', title)
    clean_t = re.sub(r'[\{\}\[\]\\]', '', clean_t)
    clean_t = clean_t.replace('"', '').strip()
    api_query_title = re.sub(r'[^a-zA-Z0-9\s]', ' ', clean_t)
    api_query_title = re.sub(r'\s+', ' ', api_query_title).strip()
    
    if not api_query_title:
        return None
        
    url = f"https://export.arxiv.org/api/query?search_query=ti:%22{quote(api_query_title)}%22&max_results=3"
    
    # Respect rate limit
    time.sleep(1.0)
    
    try:
        response = make_arxiv_request(url, timeout=45, max_retries=7, use_direct_fallback=True)
        if response and response.status_code == 200:
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
    except Exception as e:
        print(f"   [API Search] Error resolving '{title[:30]}...': {e}")
        
    return None

def resolve_arxiv_id(title):
    """
    Tries multiple methods to resolve a paper title to its arXiv ID.
    Method 1: export.arxiv API (handles Tor well, fast).
    Method 2: arXiv HTML search (fallback if API misses).
    Method 3: Semantic Scholar API (only if not currently blocked by a 429 cooldown).
    """
    # Method 1: API Single Search
    result = query_arxiv_via_api_single(title)
    if result:
        return result

    # Method 2: HTML Search Page (highly reliable, no API rate limits)
    result = query_arxiv_via_html_search(title)
    if result:
        return result
    
    # Method 3: Semantic Scholar (only call if not in a cooldown period)
    if time.time() >= SEMANTIC_SCHOLAR_BLOCKED_UNTIL:
        result = query_arxiv_via_semantic_scholar(title)
        if result:
            return result
    
    return None

def download_source_from_harvester(arxiv_id, title):
    """Downloads the source package and keeps it safely compressed."""
    # arxiv.org/src/ is primary because export.arxiv.org forces 1MB stream drops.
    src_url = f"https://arxiv.org/src/{quote(arxiv_id)}"
    
    def try_download(url, max_retries):
        safe_title = "".join([c if c.isalnum() else "_" for c in title[:50]])
        paper_dir = os.path.join(DOWNLOAD_DIR, safe_title)
        os.makedirs(paper_dir, exist_ok=True)
        tar_path = os.path.join(paper_dir, f"{safe_title}.tar.gz")

        for attempt in range(max_retries):
            headers = {}
            mode = 'wb'
            initial_size = 0
            
            if os.path.exists(tar_path):
                initial_size = os.path.getsize(tar_path)
                if initial_size > 0:
                    headers['Range'] = f'bytes={initial_size}-'
                    mode = 'ab'
                    
            response = make_arxiv_request(url, headers=headers, stream=True, timeout=45, max_retries=7, use_direct_fallback=True)
            if response is None:
                continue
                
            if response.status_code == 416:
                # Requested Range Not Satisfiable typically means we already have the full file
                return tar_path
                
            if response.status_code not in (200, 206): # 206 Partial Content
                if attempt + 1 < max_retries:
                    continue
                return None
            
            try:
                with open(tar_path, mode) as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                
                # Check if we got the full file
                expected_length = response.headers.get('content-length')
                if expected_length:
                    total_size = os.path.getsize(tar_path)
                    expected_total = initial_size + int(expected_length)
                    if total_size < expected_total:
                        raise Exception(f"Incomplete download: {total_size}/{expected_total}")
                
                return tar_path
            except Exception as e:
                print(f"   ⚠️ Stream connection broken during download of {url}: {e}")
                if attempt + 1 < max_retries:
                    current_size = os.path.getsize(tar_path) if os.path.exists(tar_path) else 0
                    print(f"   🔄 Resuming broken download stream from {current_size} bytes... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(2)
        return None

    # Try downloading from arxiv.org first
    # We use a high max_retries (20) so the resumable downloader can stitch 
    # together large files even if the stream drops every 1MB or 3MB.
    result_path = try_download(src_url, max_retries=20)
    
    # If it fails, fallback to export.arxiv.org
    if not result_path:
        fallback_url = f"https://export.arxiv.org/e-print/{quote(arxiv_id)}"
        print(f"   ⚠️ arxiv.org failed. Falling back to export.arxiv.org...")
        result_path = try_download(fallback_url, max_retries=20)
    return result_path is not None

def main():
    parser = argparse.ArgumentParser(description="Download LaTeX sources from arXiv.")
    parser.add_argument("--use_ckpt", action="store_true", help="Skip Google Drive file checking and use local checkpoint.")
    args = parser.parse_args()

    # FIXED: Removed duplicate 'flow.run_local_server' lines that forced double authentication.
    checkpoint = load_checkpoint()
    processed_titles = set(checkpoint["processed_titles"])
    not_found_titles = set(checkpoint["not_found_titles"])
    success_count = checkpoint["success_count"]
    
    print("Connecting to Google Drive API...")
    try:
        gdrive_service = get_gdrive_service()
        # FIXED: Resolves true folder hierarchy paths instead of making a single folder containing raw slashes
        main_folder_id = get_or_create_gdrive_path(gdrive_service, GDRIVE_FOLDER_NAME)
        print(f"Connected successfully. Target Google Drive Folder ID: {main_folder_id}")
        
        if args.use_ckpt:
            print("   [Checkpoint] --use_ckpt flag enabled. Skipping Google Drive file check.")
            existing_gdrive_files = set()
        else:
            # DYNAMIC CHECK: Fetch all files currently on GDrive to avoid relying purely on local checkpoint
            existing_gdrive_files = get_all_gdrive_files(gdrive_service, main_folder_id)
        
    except Exception as e:
        print(f"❌ Failed to connect to Google Drive: {e}")
        return

    non_openreview_confs = [c for c in CONFERENCE_ID if c not in ["NeurIPS.cc", "ICLR.cc", "ICML.cc"]]
    DBLP_CACHE_FILE = "dblp_cache.json"
    
    if os.path.exists(DBLP_CACHE_FILE):
        print(f"🔄 Loading pre-computed DBLP papers from {DBLP_CACHE_FILE}...")
        with open(DBLP_CACHE_FILE, "r") as f:
            dblp_papers_cache = json.load(f)
    else:
        dblp_papers_cache = prefetch_all_dblp_papers(non_openreview_confs, TARGET_YEARS, "dblp.xml")
        with open(DBLP_CACHE_FILE, "w") as f:
            json.dump(dblp_papers_cache, f)
        print(f"✅ Saved DBLP papers to {DBLP_CACHE_FILE}")

    for conf in CONFERENCE_ID:
        if conf in ["NeurIPS.cc", "ICLR.cc", "ICML.cc"]:
            paper_titles = get_openreview_papers_fixed(conf, TARGET_YEARS)
        else:
            paper_titles = dblp_papers_cache.get(conf, [])
        
        # Filter out already processed AND files that actually exist on Google Drive
        unprocessed_titles = []
        for t in paper_titles:
            safe_title = "".join([c if c.isalnum() else "_" for c in t[:50]])
            tar_file_name = f"{safe_title}.tar.gz"
            
            if args.use_ckpt:
                if t not in processed_titles and t not in not_found_titles:
                    unprocessed_titles.append(t)
            else:
                if tar_file_name not in existing_gdrive_files:
                    if t not in not_found_titles:
                        unprocessed_titles.append(t)
                elif t not in processed_titles:
                    # File is on GDrive but missing from local checkpoint. Heal the local checkpoint.
                    processed_titles.add(t)
                    if t in not_found_titles:
                        not_found_titles.remove(t)
                    success_count += 1
                
        # Sync the checkpoint
        save_checkpoint(list(processed_titles), list(not_found_titles), success_count)
        
        total_unprocessed = len(unprocessed_titles)
        print(f"\nTổng số bài báo ACCEPTED chưa xử lý: {total_unprocessed}")
        
        # Process in batches of 30
        batch_size = 30
        for start_idx in range(0, total_unprocessed, batch_size):
            batch_titles = unprocessed_titles[start_idx:start_idx + batch_size]
            print(f"\n[{start_idx + 1}-{start_idx + len(batch_titles)}/{total_unprocessed}] Đang tìm kiếm mã arXiv qua HTML search...")
            
            # Process each paper in the batch
            for title in batch_titles:
                print(f"   => Đang tìm kiếm: '{title[:50]}...'")
                paper_info = resolve_arxiv_id(title)
                            
                is_success = False
                if paper_info:
                    if download_source_from_harvester(paper_info["id"], title):
                        safe_title = "".join([c if c.isalnum() else "_" for c in title[:50]])
                        paper_dir = os.path.join(DOWNLOAD_DIR, safe_title)
                        
                        if os.path.exists(paper_dir):
                            try:
                                print(f"   => Pushing compressed archive to Google Drive...")
                                tar_file_name = f"{safe_title}.tar.gz"
                                local_tar_path = os.path.join(paper_dir, tar_file_name)
                                
                                file_metadata = {'name': tar_file_name, 'parents': [main_folder_id]}
                                media = MediaFileUpload(local_tar_path, resumable=True)
                                
                                max_retries = 3
                                for attempt in range(max_retries):
                                    try:
                                        gdrive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                                        break
                                    except Exception as e:
                                        if attempt + 1 < max_retries:
                                            print(f"   ⚠️ Lỗi mạng khi tải lên (Attempt {attempt+1}/{max_retries}): {e}. Đang thử lại...")
                                            time.sleep(3)
                                        else:
                                            raise e
                                
                                shutil.rmtree(paper_dir) # Clear local temp cache
                                success_count += 1
                                processed_titles.add(title)
                                is_success = True
                            except Exception as e:
                                print(f"   ❌ Lỗi đồng bộ lên Google Drive cho bài '{title[:30]}...': {e}")
                        else:
                            print(f"   ❌ Thư mục tải xuống không tồn tại: {paper_dir}")
                
                if not is_success:
                    not_found_titles.add(title)
                    if title in processed_titles:
                        processed_titles.remove(title)
                
                save_checkpoint(list(processed_titles), list(not_found_titles), success_count)
            
    print(f"\nHoàn thành! Đã tải thành công nguồn .tex của {success_count} bài báo lên Google Drive.")

if __name__ == "__main__":
    main()