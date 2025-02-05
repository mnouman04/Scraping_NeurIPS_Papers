import os
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time

# Constants
THREAD_COUNT = 50  # Number of concurrent threads
MAX_RETRIES = 3    # Maximum retries for failed connections
TIMEOUT = 60       # Timeout in seconds
BASE_URL = "https://papers.nips.cc"
OUTPUT_DIR = "E:/programing/Data Science/scrapping python/output"
YEAR = 0 
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_page(url):
    """Fetches a web page with retries."""
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=TIMEOUT)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            print(f"Failed to fetch {url} (Attempt {attempt + 1}): {e}")
            time.sleep(2)
    print(f"Giving up on {url}")
    return None

def process_year_links():
    """Extracts and processes year links from the main page."""
    main_page_html = fetch_page(BASE_URL)
    if not main_page_html:
        return

    soup = BeautifulSoup(main_page_html, "html.parser")
    for year_link in soup.select("a[href^='/paper_files/paper/']"):
        year_url = BASE_URL + year_link["href"]
        global YEAR
        YEAR = year_url.split("/")[-1]
        print(f"year is {YEAR}")
        print(f"Processing year: {year_url}")
        process_paper_links(year_url)

def process_paper_links(year_url):
    """Extracts and processes paper links from a year's page."""
    year_page_html = fetch_page(year_url)
    if not year_page_html:
        return

    soup = BeautifulSoup(year_page_html, "html.parser")
    paper_links = [BASE_URL + a["href"] for a in soup.select("body > div.container-fluid > div > ul li a")]
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        executor.map(process_paper, paper_links)

def process_paper(paper_url):
    """Processes a single paper page and downloads the PDF."""
    paper_html = fetch_page(paper_url)
    if not paper_html:
        return

    soup = BeautifulSoup(paper_html, "html.parser")
    paper_title = sanitize_filename(soup.title.string if soup.title else "paper")
    pdf_link = soup.select_one("body > div.container-fluid > div > div a:contains('Paper')")
    
    if pdf_link:
        pdf_url = BASE_URL + pdf_link["href"]
        print(f"Downloading PDF: {pdf_url}")
        download_pdf(pdf_url, paper_title)

def download_pdf(pdf_url, file_name):
    """Downloads a PDF file."""
    os.makedirs(f"{OUTPUT_DIR}/{YEAR}", exist_ok=True)
    file_path = os.path.join(f"{OUTPUT_DIR}/{YEAR}", file_name + ".pdf")
    try:
        response = requests.get(pdf_url, timeout=TIMEOUT, stream=True)
        response.raise_for_status()
        
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Saved PDF: {file_path}")
    except requests.RequestException as e:
        with open("failed.txt", "a") as file:
            file.write(f"failed downloading {pdf_url}\n")
        print(f"Failed to download PDF {pdf_url}: {e}")

def sanitize_filename(filename):
    """Sanitizes a filename by removing special characters."""
    return "".join(c for c in filename if c.isalnum() or c in (" ", "-", "_")).strip().replace(" ", "_")

if __name__ == "__main__":
    process_year_links()
