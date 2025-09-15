import requests
import json
import pandas as pd
from urllib.parse import urlparse
from warcio.archiveiterator import ArchiveIterator
from bs4 import BeautifulSoup
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob
import os 

# -------------------
# CONFIG
# -------------------
CC_INDEX_BASE = "http://index.commoncrawl.org/CC-MAIN-2025-13-index"
OUTPUT_CSV = "au_domains_march2025.csv"
MAX_RECORDS = 1000000   # limit for testing; increase/remove for full run
MAX_WORKERS = 30  # tune based on bandwidth & CPU
# -------------------



CC_INDEX_BASE = "https://index.commoncrawl.org/CC-MAIN-2024-38-index" # Example, you should use the latest index.

def fetch_all_index_records(url_pattern: str, start_page: int = 0):
    """
    Fetches all records for a given URL pattern from the Common Crawl
    index server, handling pagination automatically and saving each page
    to a separate CSV file. Allows starting the fetch from a specific page.
    """
    
    # Create the directory to save the CSV files
    output_dir = "common_crawl_pages"
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    # --- 1. Get the total number of pages to determine the scope of the fetch ---
    initial_params = {
        'url': url_pattern,
        'output': 'json',
        'showNumPages': 'true'
    }
    
    print(f"Fetching page count for {url_pattern}...")
    try:
        response = requests.get(CC_INDEX_BASE, params=initial_params, stream=True)
        response.raise_for_status()
        
        # The first response is a single line with page count info
        page_info = json.loads(next(response.iter_lines()).decode('utf-8'))
        total_pages = page_info.get('pages', 1)
        print(f"Total pages to retrieve: {total_pages}\n")

        # Limit to 100 pages to avoid overwhelming the server for testing purposes
        total_pages = min(100, total_pages)
    except (requests.exceptions.RequestException, json.JSONDecodeError) as e:
        print(f"Error fetching page count: {e}. Assuming single page.")
        total_pages = 1

    # --- 2. Iterate through each page and save records as CSV ---
    # The loop now starts from the 'start_page' provided by the user
    for page_num in range(start_page, total_pages):
        print(f"Fetching page {page_num + 1} of {total_pages}...")
        
        # Parameters for the actual data retrieval
        params = {
            'url': url_pattern,
            'output': 'json',
            'page': page_num
        }
        
        try:
            resp = requests.get(CC_INDEX_BASE, params=params, stream=True)
            resp.raise_for_status()

            records_list = [json.loads(line) for line in resp.iter_lines() if line]
            
            if records_list:
                # Get the fieldnames from the keys of the first record
                fieldnames = records_list[0].keys()
                
                # Define the CSV file path for the current page
                csv_filename = os.path.join(output_dir, f"page_{page_num + 1}.csv")
                
                with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(records_list)
                print(f"Page {page_num + 1} saved to {csv_filename}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_num}: {e}")
            continue

# Example Usage
# Start fetching from page 5 for the given URL pattern
# fetch_all_index_records("*.nasa.gov/*", start_page=4)




def fetch_warc_record(filename, offset, length=1024*1024):
    """Fetch a WARC record by byte range from Common Crawl."""
    url = f"https://data.commoncrawl.org/{filename}"
    headers = {"Range": f"bytes={offset}-{offset+length}"}
    resp = requests.get(url, headers=headers, stream=True, timeout=60)
    resp.raise_for_status()
    return resp.raw

import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Public S3 client (no creds needed)
s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

def fetch_warc_record_s3(filename, offset, length=1024*1024):
    """Fetch a WARC record by byte range from Common Crawl S3."""
    resp = s3.get_object(
        Bucket="commoncrawl",
        Key=filename,
        Range=f"bytes={offset}-{offset+length-1}"
    )
    return resp["Body"].read()

def extract_title_description(filename, offset):
    """Extract <title> and <meta description> from a WARC record."""
    try:
        stream = fetch_warc_record(filename, int(offset))
        for record in ArchiveIterator(stream):
            if record.rec_type == "response":
                html = record.content_stream().read()
                soup = BeautifulSoup(html, "html.parser")
                title = soup.title.string.strip() if soup.title else None
                desc_tag = soup.find("meta", attrs={"name": "description"})
                description = desc_tag["content"].strip() if desc_tag and desc_tag.get("content") else None
                return title, description
    except Exception:
        return None, None
    return None, None



def extract_page_metadata(filename, offset):
    """Extract common metadata fields and social media links from a WARC record."""
    # Initialize defaults
    metadata = {
        "title": None,
        "description": None,
        "keywords": None,
        "og_title": None,
        "og_description": None,
        "og_site_name": None,
        "twitter_title": None,
        "twitter_description": None,
        "canonical": None,
        "h1": None,
        "language": None,
        "linkedin": None,
        "facebook": None,
        "twitter": None,
        "instagram": None,
        "youtube": None,
    }

    try:
        stream = fetch_warc_record(filename, int(offset))
        for record in ArchiveIterator(stream):
            if record.rec_type != "response":
                continue

            html = record.content_stream().read()
            soup = BeautifulSoup(html, "html.parser")

            # --- Basic Metadata ---
            if soup.title and soup.title.string:
                metadata["title"] = soup.title.string.strip()

            desc_tag = soup.find("meta", attrs={"name": "description"})
            if desc_tag and desc_tag.get("content"):
                metadata["description"] = desc_tag["content"].strip()

            kw_tag = soup.find("meta", attrs={"name": "keywords"})
            if kw_tag and kw_tag.get("content"):
                metadata["keywords"] = kw_tag["content"].strip()

            # OpenGraph
            metadata["og_title"] = (soup.find("meta", property="og:title") or {}).get("content")
            metadata["og_description"] = (soup.find("meta", property="og:description") or {}).get("content")
            metadata["og_site_name"] = (soup.find("meta", property="og:site_name") or {}).get("content")

            # Twitter
            metadata["twitter_title"] = (soup.find("meta", attrs={"name": "twitter:title"}) or {}).get("content")
            metadata["twitter_description"] = (soup.find("meta", attrs={"name": "twitter:description"}) or {}).get("content")

            # Canonical
            canon = soup.find("link", rel="canonical")
            if canon and canon.get("href"):
                metadata["canonical"] = canon["href"]

            # H1
            h1 = soup.find("h1")
            if h1:
                metadata["h1"] = h1.get_text(strip=True)

            # Language
            if soup.html and soup.html.has_attr("lang"):
                metadata["language"] = soup.html["lang"]

            # --- Social Media Links ---
            for a in soup.find_all("a", href=True):
                href = a["href"].lower()
                if "linkedin.com" in href and not metadata["linkedin"]:
                    metadata["linkedin"] = href
                elif "facebook.com" in href and not metadata["facebook"]:
                    metadata["facebook"] = href
                elif ("twitter.com" in href or "x.com" in href) and not metadata["twitter"]:
                    metadata["twitter"] = href
                elif "instagram.com" in href and not metadata["instagram"]:
                    metadata["instagram"] = href
                elif "youtube.com" in href and not metadata["youtube"]:
                    metadata["youtube"] = href

            # Structured data (JSON-LD -> sameAs)
            for script in soup.find_all("script", type="application/ld+json"):
                if not script.string:
                    continue
                try:
                    data = json.loads(script.string)
                    if isinstance(data, dict) and "sameAs" in data:
                        same_as = data["sameAs"]
                        if isinstance(same_as, str):
                            same_as = [same_as]
                        for link in same_as:
                            link_l = link.lower()
                            if "linkedin.com" in link_l:
                                metadata["linkedin"] = link
                            elif "facebook.com" in link_l:
                                metadata["facebook"] = link
                            elif "twitter.com" in link_l or "x.com" in link_l:
                                metadata["twitter"] = link
                            elif "instagram.com" in link_l:
                                metadata["instagram"] = link
                            elif "youtube.com" in link_l:
                                metadata["youtube"] = link
                except Exception:
                    continue

            break  # only process the first "response" record

    except Exception as e:
        print(f"Error extracting metadata: {e}")

    return metadata



def process_record(rec):
    """Process a single index record and extract metadata."""
    filename = rec.get("filename")
    offset = rec.get("offset")
    page_meta = extract_page_metadata(filename, offset)
    return {
        "domain": rec.get("domain"),
        "url": rec.get("url"),
        "status": rec.get("status"),
        "mime": rec.get("mime"),
        "length": rec.get("length"),
        "filename": filename,
        "offset": offset,
        "digest": rec.get("digest"),
        "meta": page_meta,
    }





def main():
    folder = "folder/of/the/stored/csvs"
    all_files = glob.glob(os.path.join(folder, "*.csv"))

    if not all_files:
        raise FileNotFoundError(f"No CSV files found in {folder}")

    dfs = [pd.read_csv(f) for f in all_files]
    full_df = pd.concat(dfs, ignore_index=True)

    # full_df.to_csv('all_pages_combine.csv', index=False)


    # Extract domain vectorized
    full_df["domain"] = (
        full_df["url"]
        .dropna()
        .map(lambda x: urlparse(str(x)).netloc.lower() if pd.notna(x) else None)
    )

    # Keep only .au domains
    au_df = full_df[full_df["domain"].str.endswith(".au", na=False)]

    # Deduplicate: keep only first occurrence per domain
    filtered_df = au_df.drop_duplicates(subset="domain", keep="first")


    results = []
    filtered = filtered_df.to_dict("records")[:100]
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_record, rec): rec for rec in filtered}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Extracting from WARC"):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                print(f"Error processing record: {e}")

    df = pd.DataFrame(results)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"Saved results to {OUTPUT_CSV}")

    return df

if __name__ == "__main__":
    df = main()
