import os
import re
import urllib.request
import tarfile
import tempfile
import logging
import urllib.parse
from urllib.error import URLError
from playwright.sync_api import sync_playwright
from download import get_gdrive_service, get_or_create_gdrive_path, upload_single_file
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

CHECKPOINT_FILE = "nature_pipeline_checkpoint.json"

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        except Exception as e:
            logging.warning(f"Could not load checkpoint: {e}")
    return set()

def save_checkpoint(processed_ids):
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump(list(processed_ids), f, indent=4)

def sanitize_filename(filename):
    """Sanitize the paper title to be a valid file name."""
    sanitized = re.sub(r'[\\/*?:"<>|]', "", filename)
    # truncate if it's too long
    sanitized = sanitized.strip()[:150]
    return sanitized if sanitized else "Untitled_Paper"

def download_file(url, output_path):
    """Download a file from a URL to the given path using urllib."""
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
    try:
        with urllib.request.urlopen(req) as response, open(output_path, 'wb') as out_file:
            out_file.write(response.read())
        return True
    except URLError as e:
        logging.error(f"Failed to download {url}: {e}")
        return False

def scrape_nature_comp_sci(target_years):
    """
    Scrapes the Nature Computational Science journal for the specified years.
    Finds the 'Source data' section of each article, downloads the files,
    and zips them together with the file name as the paper's title.
    """
    year_to_volume = {
        2021: 1, 2022: 2, 2023: 3, 2024: 4, 2025: 5
    }
    
    base_url = "https://www.nature.com"
    journal_path = "/natcomputsci"
    output_dir = "nature_source_data"
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize Google Drive service
    logging.info("Connecting to Google Drive API...")
    try:
        gdrive_service = get_gdrive_service()
        drive_folder_id = get_or_create_gdrive_path(gdrive_service, "FFT_DataInconsistency/Nature_Data")
        logging.info(f"Connected to Google Drive. Folder ID: {drive_folder_id}")
    except Exception as e:
        logging.error(f"Failed to connect to Google Drive: {e}")
        return
        
    processed_articles = load_checkpoint()
    logging.info(f"Loaded checkpoint with {len(processed_articles)} processed articles.")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        page = context.new_page()
        
        for year in target_years:
            if year not in year_to_volume:
                logging.warning(f"Year {year} is out of bounds for Nature Computational Science. Skipping.")
                continue
                
            volume = year_to_volume[year]
            logging.info(f"Scraping Volume {volume} (Year {year})...")
            
            for issue in range(1, 13):
                issue_url = f"{base_url}{journal_path}/volumes/{volume}/issues/{issue}"
                logging.info(f"Visiting Issue {issue}: {issue_url}")
                
                response = page.goto(issue_url, wait_until="domcontentloaded")
                if response and response.status == 404:
                    logging.info(f"Issue {issue} not found (might be future issue). Moving to next year.")
                    break
                    
                page.wait_for_timeout(2000)
                
                try:
                    article_links = page.eval_on_selector_all(
                        "a[href^='/articles/s']", 
                        "elements => elements.map(e => e.getAttribute('href'))"
                    )
                except Exception as e:
                    logging.warning(f"Could not extract links from issue {issue}: {e}")
                    continue
                
                article_urls = list(dict.fromkeys([f"{base_url}{link}" for link in article_links]))
                logging.info(f"Found {len(article_urls)} articles in Issue {issue}.")
                
                for url in article_urls:
                    article_id = url.split('/')[-1]
                    
                    if article_id in processed_articles:
                        logging.info(f"Article {article_id} already processed (checkpoint). Skipping.")
                        continue
                        
                    logging.info(f"Checking article: {article_id} ({url})")
                    
                    try:
                        page.goto(url, wait_until="domcontentloaded")
                        page.wait_for_timeout(2000)
                        
                        title_text = page.title().replace(' | Nature Computational Science', '').strip()
                        safe_title = sanitize_filename(title_text)
                        archive_path = os.path.join(output_dir, f"{safe_title}.tar.gz")
                        
                        if os.path.exists(archive_path):
                            logging.info(f"Source data for '{safe_title}' already downloaded. Skipping.")
                            continue
                            
                        # Find all sections and look for 'Source Data'
                        sections = page.query_selector_all("section")
                        source_data_links = []
                        
                        for sec in sections:
                            h2 = sec.query_selector("h2")
                            if h2 and "source data" in h2.inner_text().strip().lower():
                                links = sec.query_selector_all("a")
                                for a in links:
                                    href = a.get_attribute("href")
                                    if href and ("MediaObjects" in href or "static-content" in href or "download" in href):
                                        if href.startswith('/'):
                                            href = base_url + href
                                        source_data_links.append((a.inner_text().strip() or "data", href))
                        
                        if not source_data_links:
                            logging.info(f"No Source Data section found for {article_id}.")
                            processed_articles.add(article_id)
                            save_checkpoint(processed_articles)
                            continue
                            
                        logging.info(f"Found {len(source_data_links)} source data files for '{safe_title}'. Downloading...")
                        
                        # Create a temporary directory to store downloaded files before zipping
                        with tempfile.TemporaryDirectory() as temp_dir:
                            downloaded_files = []
                            for i, (link_text, link_url) in enumerate(source_data_links):
                                # Attempt to extract a sensible filename from URL or link text
                                filename = link_url.split('/')[-1]
                                if '?' in filename:
                                    filename = filename.split('?')[0]
                                if not filename or len(filename) > 50:
                                    filename = f"source_data_{i+1}.file"
                                    
                                file_path = os.path.join(temp_dir, filename)
                                logging.info(f"  -> Downloading {filename} ...")
                                
                                if download_file(link_url, file_path):
                                    downloaded_files.append((filename, file_path))
                                
                            if downloaded_files:
                                with tarfile.open(archive_path, 'w:gz') as tarf:
                                    for fname, fpath in downloaded_files:
                                        tarf.add(fpath, arcname=fname)
                                logging.info(f"Successfully archived source data to {archive_path}")
                                
                                # Upload to Google Drive
                                logging.info(f"Uploading {safe_title}.tar.gz to Google Drive...")
                                upload_single_file(gdrive_service, archive_path, f"{safe_title}.tar.gz", drive_folder_id)
                                logging.info(f"Successfully uploaded to Google Drive.")
                                
                                # Delete local file
                                os.remove(archive_path)
                                logging.info(f"Deleted local file {archive_path}")
                                
                                processed_articles.add(article_id)
                                save_checkpoint(processed_articles)
                            else:
                                logging.warning(f"Failed to download any source data files for {article_id}")
                                
                    except Exception as e:
                        logging.error(f"Failed to process {url}: {e}")
                        
        browser.close()

if __name__ == "__main__":
    scrape_nature_comp_sci([2024])
