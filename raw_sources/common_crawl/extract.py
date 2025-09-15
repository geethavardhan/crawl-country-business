import requests
import json
import csv
import os
import pandas as pd

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
        total_pages = min(500, total_pages)
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

            # Create a DataFrame directly from the list of dictionaries
            records_df = pd.DataFrame([json.loads(line) for line in resp.iter_lines() if line])

            if not records_df.empty:
                # Define the CSV file path for the current page
                csv_filename = os.path.join(output_dir, f"page_{page_num + 1}.csv")
                
                # Use pandas to save the DataFrame to a CSV
                records_df.to_csv(csv_filename, index=False)
                print(f"Page {page_num + 1} saved to {csv_filename}")

        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page_num}: {e}")
            continue


fetch_all_index_records("*.au/", start_page=479)