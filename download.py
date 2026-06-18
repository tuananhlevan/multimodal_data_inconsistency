import os
import tarfile
import gzip
import requests
import openreview
import time
import re
import json
import xml.etree.ElementTree as ET
from urllib.parse import urlencode, quote
from dotenv import load_dotenv, set_key
import shutil
from concurrent.futures import ThreadPoolExecutor

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

load_dotenv()

# --- CONFIGURATION ---
CONFERENCE_ID = ["NeurIPS.cc", "ICLR.cc", "ICML.cc",]
TARGET_YEARS = [2025]
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
def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            pass
    return {"processed_titles": [], "success_count": 0}

def save_checkpoint(processed_titles, success_count):
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({"processed_titles": processed_titles, "success_count": success_count}, f, ensure_ascii=False, indent=4)

def get_openreview_papers_fixed(conference_id, years):
    titles = []
    for year in years:
        venue_id = f"{conference_id}/{year}/Conference"
        print(f"Đang quét bài được chấp nhận tại {venue_id}...")
        
        if year <= 2023:
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

def make_arxiv_request(url, headers=None, stream=False, timeout=30, max_retries=5):
    """Makes a rate-limited request to arXiv with retry logic for rate limits and server errors."""
    if headers is None:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
    
    backoff = 2.0
    for attempt in range(max_retries):
        arxiv_rate_limiter.wait()
        try:
            response = SESSION.get(url, headers=headers, stream=stream, timeout=timeout)
            
            # Handle rate limit (429) or temporary server error (500, 502, 503, 504)
            if response.status_code in (429, 500, 502, 503, 504):
                if attempt + 1 < max_retries:
                    print(f"   ⚠️ arXiv returned status code {response.status_code}. Retrying in {backoff}s... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(backoff)
                    backoff *= 2.0
                    continue
                else:
                    break
                
            return response
        except (requests.exceptions.RequestException, ConnectionError) as e:
            if attempt + 1 < max_retries:
                print(f"   ⚠️ Connection error: {e}. Retrying in {backoff}s... (Attempt {attempt+1}/{max_retries})")
                time.sleep(backoff)
                backoff *= 2.0
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
        response = make_arxiv_request(search_url, timeout=30)
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
        
        def normalize_title(t):
            return "".join(c for c in t.lower() if c.isalnum())
            
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
        response = SESSION.get(url, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            if "data" in data and len(data["data"]) > 0:
                paper = data["data"][0]
                found_title = paper.get("title", "")
                
                def normalize_title(t):
                    return "".join(c for c in t.lower() if c.isalnum())
                    
                if normalize_title(found_title) == normalize_title(title) or \
                   normalize_title(title) in normalize_title(found_title) or \
                   normalize_title(found_title) in normalize_title(title):
                    
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
        response = SESSION.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            html = response.text
            chunks = html.split('<li class="arxiv-result">')[1:]
            
            def clean_html(text):
                clean = re.sub(r'<[^>]+>', '', text)
                return " ".join(clean.split())
                
            def normalize_title(t):
                return "".join(c for c in t.lower() if c.isalnum())
                
            norm_target_title = normalize_title(title)
            
            for chunk in chunks[:10]: # Check top 10 results
                id_match = re.search(r'href="https://arxiv\.org/abs/([^"]+)"', chunk)
                title_match = re.search(r'<p class="title is-5 mathjax">\s*(.*?)\s*</p>', chunk, re.DOTALL)
                
                if id_match and title_match:
                    raw_id = id_match.group(1)
                    arxiv_id = re.sub(r'v\d+$', '', raw_id).strip()
                    found_title = clean_html(title_match.group(1))
                    norm_found_title = normalize_title(found_title)
                    
                    if norm_found_title == norm_target_title or \
                       norm_target_title in norm_found_title or \
                       norm_found_title in norm_target_title:
                        print(f"   [HTML Search] Found ArXiv ID for '{title[:30]}...': {arxiv_id}")
                        return {"id": arxiv_id}
                        
    except Exception as e:
        print(f"   [HTML Search] Error resolving '{title[:30]}...': {e}")
        
    return None

def resolve_arxiv_id(title):
    """
    Tries multiple methods to resolve a paper title to its arXiv ID.
    Method 1: arXiv HTML search (free, fast, no rate limit).
    Method 2: Semantic Scholar API (only if not currently blocked by a 429 cooldown).
    """
    # Method 1: HTML Search Page (highly reliable, no API rate limits)
    result = query_arxiv_via_html_search(title)
    if result:
        return result
    
    # Method 2: Semantic Scholar (only call if not in a cooldown period)
    if time.time() >= SEMANTIC_SCHOLAR_BLOCKED_UNTIL:
        result = query_arxiv_via_semantic_scholar(title)
        if result:
            return result
    
    return None

def download_source_from_harvester(arxiv_id, title):
    """Downloads the source package and keeps it safely compressed."""
    src_url = f"https://arxiv.org/src/{quote(arxiv_id)}"
    
    def try_download(url, max_retries):
        response = make_arxiv_request(url, stream=True, timeout=30, max_retries=max_retries)
        if response is None or response.status_code != 200:
            return None
            
        safe_title = "".join([c if c.isalnum() else "_" for c in title[:50]])
        paper_dir = os.path.join(DOWNLOAD_DIR, safe_title)
        os.makedirs(paper_dir, exist_ok=True)
        tar_path = os.path.join(paper_dir, f"{safe_title}.tar.gz")
        
        try:
            with open(tar_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            return tar_path
        except Exception as e:
            print(f"   ⚠️ Stream connection broken during download of {url}: {e}")
            # Clean up partial/corrupted file
            if os.path.exists(tar_path):
                try:
                    os.remove(tar_path)
                except Exception:
                    pass
            return None

    # Try downloading from arxiv.org first (as it supports > 1MB downloads)
    result_path = try_download(src_url, max_retries=3)
    
    # If it fails, fallback to export.arxiv.org
    if not result_path:
        fallback_url = f"https://export.arxiv.org/src/{quote(arxiv_id)}"
        print(f"   ⚠️ arxiv.org failed. Falling back to export.arxiv.org (Note: limited to <1MB)...")
        result_path = try_download(fallback_url, max_retries=1)
        
    return result_path is not None

def main():
    # FIXED: Removed duplicate 'flow.run_local_server' lines that forced double authentication.
    checkpoint = load_checkpoint()
    processed_titles = set(checkpoint["processed_titles"])
    success_count = checkpoint["success_count"]
    
    print("Connecting to Google Drive API...")
    try:
        gdrive_service = get_gdrive_service()
        # FIXED: Resolves true folder hierarchy paths instead of making a single folder containing raw slashes
        main_folder_id = get_or_create_gdrive_path(gdrive_service, GDRIVE_FOLDER_NAME)
        print(f"Connected successfully. Target Google Drive Folder ID: {main_folder_id}")
    except Exception as e:
        print(f"❌ Failed to connect to Google Drive: {e}")
        return

    for conf in CONFERENCE_ID:
        paper_titles = get_openreview_papers_fixed(conf, TARGET_YEARS)
        # Filter out already processed
        unprocessed_titles = [t for t in paper_titles if t not in processed_titles]
        total_unprocessed = len(unprocessed_titles)
        print(f"\nTổng số bài báo ACCEPTED chưa xử lý: {total_unprocessed}")
        
        # Process in batches of 30
        batch_size = 30
        for start_idx in range(0, total_unprocessed, batch_size):
            batch_titles = unprocessed_titles[start_idx:start_idx + batch_size]
            print(f"\n[{start_idx + 1}-{start_idx + len(batch_titles)}/{total_unprocessed}] Đang truy vấn metadata arXiv theo lô...")
            
            # Query batch
            api_results = query_arxiv_via_oai_pmh_batch(batch_titles)
            
            # Process each paper in the batch
            def normalize_title(t):
                return "".join(c for c in t.lower() if c.isalnum())
                
            for title in batch_titles:
                norm_title = normalize_title(title)
                paper_info = api_results.get(norm_title)
                
                # Substring match fallback for slight difference in symbols/colons
                if not paper_info:
                    for k, v in api_results.items():
                        if norm_title in k or k in norm_title:
                            paper_info = v
                            break
                            
                # If still not found, query our fallbacks (HTML search -> Semantic Scholar)
                if not paper_info:
                    print(f"   ⚠️ Title '{title[:30]}...' not found in arXiv batch. Trying fallbacks...")
                    paper_info = resolve_arxiv_id(title)
                            
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
                                media = MediaFileUpload(local_tar_path, resumable=False)
                                gdrive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                                
                                shutil.rmtree(paper_dir) # Clear local temp cache
                                success_count += 1
                            except Exception as e:
                                print(f"   ❌ Lỗi đồng bộ lên Google Drive cho bài '{title[:30]}...': {e}")
                        else:
                            print(f"   ❌ Thư mục tải xuống không tồn tại: {paper_dir}")
                
                processed_titles.add(title)
                save_checkpoint(list(processed_titles), success_count)
            
    print(f"\nHoàn thành! Đã tải thành công nguồn .tex của {success_count} bài báo lên Google Drive.")

if __name__ == "__main__":
    main()