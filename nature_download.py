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

PROGRESS_DIR = "checkpoint"
CHECKPOINT_FILE = os.path.join(PROGRESS_DIR, "nature_pipeline_checkpoint.json")

def load_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    return set(data), 0, 0
                return set(data.get("processed_ids", [])), data.get("skipped_count", 0), data.get("success_count", 0)
        except Exception as e:
            logging.warning(f"Could not load checkpoint: {e}")
    return set(), 0, 0

def save_checkpoint(processed_ids, skipped_count, success_count):
    os.makedirs(PROGRESS_DIR, exist_ok=True)
    with open(CHECKPOINT_FILE, 'w', encoding='utf-8') as f:
        json.dump({
            "processed_ids": list(processed_ids),
            "skipped_count": skipped_count,
            "success_count": success_count
        }, f, indent=4)

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
    
    base_url = "https://www.nature.com"
    journal_paths = ["/natcomputsci", "/nathumbehav", "/mp", "/dpn", "/nm", "/npjdigitalmed", "/emm", "/nclimate", "/npjclimataction", "/npjclimatsci", "/natfood", "/npjscifood", "/nutd", "/ejcn"]
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
        
    processed_articles, skipped_count, success_count = load_checkpoint()
    logging.info(f"Loaded checkpoint with {len(processed_articles)} processed, {skipped_count} skipped, {success_count} succeeded.")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080}
        )
        
        cookies_path = os.path.join(PROGRESS_DIR, "cookies.json")
        if os.path.exists(cookies_path):
            try:
                with open(cookies_path, "r") as f:
                    cookies = json.load(f)
                context.add_cookies(cookies)
                logging.info(f"Loaded session cookies from {cookies_path} for institutional access.")
            except Exception as e:
                logging.warning(f"Failed to load {cookies_path}: {e}")
                
        page = context.new_page()
        
        for journal_path in journal_paths:
            logging.info(f"Fetching volumes map for journal: {journal_path}")
            vol_url = f"{base_url}{journal_path}/volumes"
            
            try:
                page.goto(vol_url, wait_until="domcontentloaded")
                page.wait_for_timeout(1500)
                
                volumes_data = page.eval_on_selector_all(
                    "a[href*='/volumes/']",
                    "elements => elements.map(e => ({href: e.getAttribute('href'), text: e.parentElement.innerText}))"
                )
            except Exception as e:
                logging.warning(f"Could not extract volumes for {journal_path}: {e}")
                continue
                
            journal_year_to_volume = {}
            for item in volumes_data:
                if f"{journal_path}/volumes/" in item['href']:
                    text = item['text']
                    year_match = re.search(r'\b(19\d\d|20\d\d)\b', text)
                    if year_match:
                        year_val = int(year_match.group(1))
                        vol_num_str = item['href'].split('/')[-1]
                        if vol_num_str.isdigit():
                            journal_year_to_volume[year_val] = int(vol_num_str)
                            
            if not journal_year_to_volume:
                logging.warning(f"Could not build year-to-volume map for {journal_path}. Skipping.")
                continue
                
            for year in target_years:
                if year not in journal_year_to_volume:
                    logging.warning(f"Year {year} is out of bounds for {journal_path}. Skipping.")
                    continue
                    
                volume = journal_year_to_volume[year]
                logging.info(f"Scraping {journal_path} Volume {volume} (Year {year})...")
                
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
                            
                            # Check if the article is Open Access
                            is_open_access = page.query_selector("[data-test='open-access']") is not None
                            if not is_open_access:
                                logging.info(f"Article {article_id} is not Open Access. Skipping.")
                                processed_articles.add(article_id)
                                skipped_count += 1
                                save_checkpoint(processed_articles, skipped_count, success_count)
                                continue
                            
                            title_text = page.title().replace(' | Nature Computational Science', '').strip()
                            safe_title = sanitize_filename(title_text)
                            archive_path = os.path.join(output_dir, f"{safe_title}.tar.gz")
                            
                            if os.path.exists(archive_path):
                                logging.info(f"Source data for '{safe_title}' already downloaded. Skipping.")
                                processed_articles.add(article_id)
                                skipped_count += 1
                                save_checkpoint(processed_articles, skipped_count, success_count)
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
                                logging.info(f"No Source Data section found for {article_id}. Continuing to extract text and images...")
                                
                            logging.info(f"Found {len(source_data_links)} source data files for '{safe_title}'. Downloading...")
                            
                            # Create a temporary directory to store downloaded files before zipping
                            with tempfile.TemporaryDirectory() as temp_dir:
                                downloaded_files = []
                                
                                fig_dir = os.path.join(temp_dir, "fig")
                                os.makedirs(fig_dir, exist_ok=True)
                                
                                source_data_dir = os.path.join(temp_dir, "source_data")
                                os.makedirs(source_data_dir, exist_ok=True)
                                
                                # 1. EXTRACT TEXT SECTIONS
                                article_data = {
                                    "id": article_id,
                                    "url": url,
                                    "title": title_text,
                                    "sections": [],
                                    "figures": []
                                }
                                
                                ignore_sections = [
                                    "explore content", "about the journal", "publish with us", 
                                    "search", "associated content", "rights and permissions",
                                    "about this article", "this article is cited by", "nature.com footer links",
                                    "source data", "data availability", "code availability"
                                ]
                                
                                for sec in sections:
                                    h2 = sec.query_selector("h2")
                                    if h2:
                                        section_title = h2.inner_text().strip()
                                        if section_title.lower() in ignore_sections:
                                            continue
                                        
                                        section_text = sec.inner_text().strip()
                                        article_data["sections"].append({
                                            "title": section_title,
                                            "text": section_text
                                        })
                                
                                # 2. EXTRACT FIGURES
                                figures = page.query_selector_all("figure")
                                for idx, fig in enumerate(figures):
                                    img = fig.query_selector("img")
                                    if img:
                                        src = img.get_attribute("src") or img.get_attribute("data-src")
                                        if src:
                                            if src.startswith("//"):
                                                src = "https:" + src
                                            elif src.startswith("/"):
                                                src = base_url + src
                                                
                                            # Extract figure caption if available
                                            caption_element = fig.query_selector("figcaption")
                                            caption = caption_element.inner_text().strip() if caption_element else f"Figure {idx+1}"
                                            
                                            # Extract figure context (the detailed description below the image)
                                            desc_element = fig.query_selector("div[data-test='bottom-caption'], div.c-article-section__figure-description")
                                            context_text = desc_element.inner_text().strip() if desc_element else ""
                                            
                                            fig_basename = f"figure_{idx+1}.png"
                                            fig_rel_path = f"fig/{fig_basename}"
                                            fig_path = os.path.join(fig_dir, fig_basename)
                                            
                                            logging.info(f"  -> Downloading image {fig_basename} ...")
                                            if download_file(src, fig_path):
                                                downloaded_files.append((fig_rel_path, fig_path))
                                                article_data["figures"].append({
                                                    "filename": fig_rel_path,
                                                    "caption": caption,
                                                    "context": context_text,
                                                    "original_url": src
                                                })
                                
                                # Save JSON text
                                json_path = os.path.join(temp_dir, "article_text.json")
                                with open(json_path, "w", encoding="utf-8") as f:
                                    json.dump(article_data, f, ensure_ascii=False, indent=4)
                                downloaded_files.append(("article_text.json", json_path))
                                
                                # 3. DOWNLOAD SOURCE DATA FILES
                                for i, (link_text, link_url) in enumerate(source_data_links):
                                    # Attempt to extract a sensible filename from URL or link text
                                    filename = link_url.split('/')[-1]
                                    if '?' in filename:
                                        filename = filename.split('?')[0]
                                    if not filename or len(filename) > 50:
                                        filename = f"source_data_{i+1}.file"
                                        
                                    file_path = os.path.join(source_data_dir, filename)
                                    arcname = f"source_data/{filename}"
                                    logging.info(f"  -> Downloading {filename} ...")
                                    
                                    if download_file(link_url, file_path):
                                        downloaded_files.append((arcname, file_path))
                                    
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
                                    success_count += 1
                                    save_checkpoint(processed_articles, skipped_count, success_count)
                                else:
                                    logging.warning(f"Failed to download any source data files for {article_id}")
                                    
                        except Exception as e:
                            logging.error(f"Failed to process {url}: {e}")
                        
        browser.close()

if __name__ == "__main__":
    scrape_nature_comp_sci([2021, 2022, 2023, 2024, 2025])
