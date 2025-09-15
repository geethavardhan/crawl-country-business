import requests
import os

def download_file(url, dest_folder="."):
    """Download a file from URL and save it to dest_folder"""
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)

    local_filename = os.path.join(dest_folder, url.split("/")[-1])
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
    return local_filename


if __name__ == "__main__":
    urls = [
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/0ae4d427-6fa8-4d40-8e76-c6909b5a071b/download/public_split_1_10.zip",
        "https://data.gov.au/data/dataset/5bd7fcab-e315-42cb-8daf-50b7efc2027e/resource/635fcb95-7864-4509-9fa7-a62a6e32b62d/download/public_split_11_20.zip"
    ]

    for url in urls:
        path = download_file(url, dest_folder="/path/to/folder")
        print(f"Downloaded: {path}")
