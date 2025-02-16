import os
import csv
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
import time
import tkinter as tk
from tkinter import ttk, scrolledtext, filedialog, messagebox
import threading
import queue

class ScraperGUI:
    def __init__(self, master):
        self.master = master
        master.title("NIPS Paper Scraper")
        master.configure(background='#e6f7ff')  # Overall background color

        # Configure ttk styles for a modern look
        style = ttk.Style()
        style.theme_use("clam")
        style.configure('TFrame', background='#e6f7ff')
        style.configure('TLabel', background='#e6f7ff', font=('Helvetica', 10))
        style.configure('TButton', background='#cceeff', font=('Helvetica', 10, 'bold'))
        style.configure('TEntry', fieldbackground='#ffffff')
        
        # Counters for downloads
        self.total_downloads = 0
        self.yearly_downloads = {}
        
        self.create_widgets()
        self.stop_event = None
        self.log_queue = queue.Queue()
        self.scraping_thread = None
        self.metadata_queue = None
        self.stop_metadata_writer = None

    def create_widgets(self):
        # Create a main frame that splits into two columns (left for inputs/logs, right for table)
        main_frame = ttk.Frame(self.master, padding="10")
        main_frame.grid(row=0, column=0, sticky="nsew")
        self.master.columnconfigure(0, weight=1)
        self.master.rowconfigure(0, weight=1)

        # Left frame for inputs, log area, and summary
        self.left_frame = ttk.Frame(main_frame)
        self.left_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        # Right frame for table view
        self.right_frame = ttk.Frame(main_frame)
        self.right_frame.grid(row=0, column=1, sticky="nsew")
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=1)
        main_frame.rowconfigure(0, weight=1)

        # --- Left Frame Widgets ---
        row = 0
        ttk.Label(self.left_frame, text="Base URL:").grid(row=row, column=0, sticky=tk.W)
        self.base_url = ttk.Entry(self.left_frame, width=50)
        self.base_url.grid(row=row, column=1, columnspan=2, sticky=tk.EW)
        self.base_url.insert(0, "https://papers.nips.cc")
        row += 1

        ttk.Label(self.left_frame, text="Output Directory:").grid(row=row, column=0, sticky=tk.W)
        self.output_dir = ttk.Entry(self.left_frame, width=50)
        self.output_dir.grid(row=row, column=1, sticky=tk.EW)
        self.output_dir.insert(0, os.getcwd())
        ttk.Button(self.left_frame, text="Browse", command=self.browse_directory).grid(row=row, column=2, sticky=tk.W)
        row += 1

        ttk.Label(self.left_frame, text="Start Year:").grid(row=row, column=0, sticky=tk.W)
        self.start_year = ttk.Entry(self.left_frame, width=10)
        self.start_year.grid(row=row, column=1, sticky=tk.W)
        row += 1

        ttk.Label(self.left_frame, text="End Year:").grid(row=row, column=0, sticky=tk.W)
        self.end_year = ttk.Entry(self.left_frame, width=10)
        self.end_year.grid(row=row, column=1, sticky=tk.W)
        row += 1

        ttk.Label(self.left_frame, text="Scrape Type:").grid(row=row, column=0, sticky=tk.W)
        self.scrape_type = ttk.Combobox(self.left_frame, values=["PDFs", "Metadata", "Both"], state="readonly")
        self.scrape_type.current(0)
        self.scrape_type.grid(row=row, column=1, sticky=tk.W)
        row += 1

        ttk.Label(self.left_frame, text="Thread Count:").grid(row=row, column=0, sticky=tk.W)
        self.thread_count = ttk.Entry(self.left_frame)
        self.thread_count.grid(row=row, column=1, sticky=tk.W)
        self.thread_count.insert(0, "50")
        row += 1

        ttk.Label(self.left_frame, text="Max Retries:").grid(row=row, column=0, sticky=tk.W)
        self.max_retries = ttk.Entry(self.left_frame)
        self.max_retries.grid(row=row, column=1, sticky=tk.W)
        self.max_retries.insert(0, "3")
        row += 1

        ttk.Label(self.left_frame, text="Timeout (sec):").grid(row=row, column=0, sticky=tk.W)
        self.timeout = ttk.Entry(self.left_frame)
        self.timeout.grid(row=row, column=1, sticky=tk.W)
        self.timeout.insert(0, "60")
        row += 1

        # Log area with colored tags
        self.log_area = scrolledtext.ScrolledText(self.left_frame, width=80, height=15, background='#f9f9f9')
        self.log_area.grid(row=row, column=0, columnspan=3, sticky=tk.EW)
        self.log_area.tag_config("success", foreground="green")
        self.log_area.tag_config("error", foreground="red")
        row += 1

        # Summary Label for download counts
        self.summary_label = ttk.Label(self.left_frame, text="Total Downloads: 0")
        self.summary_label.grid(row=row, column=0, columnspan=3, sticky=tk.W, pady=(5,0))
        row += 1

        # Buttons for starting and stopping
        self.start_btn = ttk.Button(self.left_frame, text="Start", command=self.start_scraping)
        self.start_btn.grid(row=row, column=0, pady=5)
        self.stop_btn = ttk.Button(self.left_frame, text="Stop", command=self.stop_scraping, state=tk.DISABLED)
        self.stop_btn.grid(row=row, column=1, pady=5)

        self.left_frame.columnconfigure(1, weight=1)

        # --- Right Frame Widgets (Table) ---
        ttk.Label(self.right_frame, text="Downloaded Items", font=('Helvetica', 12, 'bold'),
                  background='#e6f7ff').grid(row=0, column=0, sticky=tk.W)
        self.table = ttk.Treeview(self.right_frame, columns=("Year", "Type", "Name"), show="headings", height=20)
        self.table.heading("Year", text="Year")
        self.table.heading("Type", text="Type")
        self.table.heading("Name", text="Name")
        self.table.column("Year", width=60, anchor="center")
        self.table.column("Type", width=80, anchor="center")
        self.table.column("Name", width=200, anchor="w")
        self.table.grid(row=1, column=0, sticky="nsew")
        scrollbar = ttk.Scrollbar(self.right_frame, orient="vertical", command=self.table.yview)
        self.table.configure(yscroll=scrollbar.set)
        scrollbar.grid(row=1, column=1, sticky="ns")
        self.right_frame.columnconfigure(0, weight=1)
        self.right_frame.rowconfigure(1, weight=1)

    def browse_directory(self):
        directory = filedialog.askdirectory()
        if directory:
            self.output_dir.delete(0, tk.END)
            self.output_dir.insert(0, directory)

    def log_message(self, message, tag=None):
        self.log_area.insert(tk.END, message + "\n", tag)
        self.log_area.see(tk.END)

    def update_summary(self):
        summary_text = f"Total Downloads: {self.total_downloads} | " + \
                       " | ".join([f"{year}: {count}" for year, count in sorted(self.yearly_downloads.items())])
        self.master.after(0, lambda: self.summary_label.config(text=summary_text))

    def update_download_count(self, year):
        self.total_downloads += 1
        self.yearly_downloads[year] = self.yearly_downloads.get(year, 0) + 1
        self.update_summary()

    def add_table_entry(self, year, file_type, name):
        # Use after() to safely update the Treeview from worker threads
        self.master.after(0, lambda: self.table.insert("", "end", values=(year, file_type, name)))

    def start_scraping(self):
        params = {
            "base_url": self.base_url.get(),
            "output_dir": self.output_dir.get(),
            "thread_count": self.thread_count.get(),
            "max_retries": self.max_retries.get(),
            "timeout": self.timeout.get(),
            "start_year": self.start_year.get(),
            "end_year": self.end_year.get(),
            "scrape_type": self.scrape_type.get()
        }
        try:
            params["thread_count"] = int(params["thread_count"])
            params["max_retries"] = int(params["max_retries"])
            params["timeout"] = int(params["timeout"])
            params["start_year"] = int(params["start_year"]) if params["start_year"] else None
            params["end_year"] = int(params["end_year"]) if params["end_year"] else None
            if params["start_year"] and params["end_year"] and params["start_year"] > params["end_year"]:
                raise ValueError("Start year must be <= end year")
        except ValueError as e:
            messagebox.showerror("Error", f"Invalid input: {str(e)}")
            return

        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.stop_event = threading.Event()
        self.log_queue = queue.Queue()
        self.metadata_queue = queue.Queue()
        self.stop_metadata_writer = threading.Event()

        # Reset counters
        self.total_downloads = 0
        self.yearly_downloads = {}
        self.update_summary()

        update_count_callback = self.update_download_count
        update_table_callback = self.add_table_entry

        self.scraping_thread = threading.Thread(
            target=self.run_scraping,
            args=(params, update_count_callback, update_table_callback),
            daemon=True
        )
        self.scraping_thread.start()
        self.master.after(100, self.check_log_queue)

    def stop_scraping(self):
        if self.stop_event:
            self.stop_event.set()
        if self.stop_metadata_writer:
            self.stop_metadata_writer.set()
        self.start_btn.config(state=tk.NORMAL)
        self.stop_btn.config(state=tk.DISABLED)

    def check_log_queue(self):
        while True:
            try:
                msg, tag = self.log_queue.get_nowait()
                self.log_message(msg, tag)
            except queue.Empty:
                break
        if self.scraping_thread.is_alive():
            self.master.after(100, self.check_log_queue)

    def run_scraping(self, params, update_count, update_table):
        def log(msg):
            tag = "success" if any(x in msg for x in ["Downloaded:", "Metadata saved:", "Collected metadata for:"]) else \
                  "error" if "Failed" in msg or "Error:" in msg else None
            self.log_queue.put((msg, tag))

        def metadata_writer():
            while not self.stop_metadata_writer.is_set() or not self.metadata_queue.empty():
                try:
                    metadata = self.metadata_queue.get(timeout=1)
                    year = metadata['year']
                    save_dir = os.path.join(params["output_dir"], str(year))
                    os.makedirs(save_dir, exist_ok=True)
                    csv_path = os.path.join(save_dir, "metadata.csv")
                    file_exists = os.path.isfile(csv_path)
                    try:
                        with open(csv_path, "a", newline="", encoding="utf-8") as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=metadata.keys())
                            if not file_exists:
                                writer.writeheader()
                            writer.writerow(metadata)
                        log(f"Metadata saved: {metadata['title']}")
                    except Exception as e:
                        error_msg = f"Failed to save metadata for {metadata['title']}: {str(e)}"
                        log(error_msg)
                        failed_dir = os.path.join(params["output_dir"], "failed_metadata")
                        os.makedirs(failed_dir, exist_ok=True)
                        with open(os.path.join(failed_dir, "failed_metadata.txt"), "a", encoding="utf-8") as f:
                            f.write(error_msg + "\n")
                except queue.Empty:
                    continue

        writer_thread = threading.Thread(target=metadata_writer, daemon=True)
        if params["scrape_type"] in ["Metadata", "Both"]:
            writer_thread.start()

        try:
            process_year_links(
                base_url=params["base_url"],
                output_dir=params["output_dir"],
                thread_count=params["thread_count"],
                max_retries=params["max_retries"],
                timeout=params["timeout"],
                start_year=params["start_year"],
                end_year=params["end_year"],
                scrape_type=params["scrape_type"],
                stop_event=self.stop_event,
                log=log,
                metadata_queue=self.metadata_queue,
                update_count=update_count,
                update_table=update_table
            )
            log("Scraping completed successfully!")
        except Exception as e:
            log(f"Error: {str(e)}")
        finally:
            self.stop_metadata_writer.set()
            self.stop_scraping()

def process_year_links(base_url, output_dir, thread_count, max_retries, timeout, start_year, end_year,
                         scrape_type, stop_event, log, metadata_queue, update_count, update_table):
    log("Fetching main page...")
    main_page_html = fetch_page(base_url, max_retries, timeout, stop_event, log)
    if not main_page_html:
        return

    soup = BeautifulSoup(main_page_html, "html.parser")
    year_links = soup.select("a[href^='/paper_files/paper/']")

    for link in year_links:
        if stop_event.is_set():
            log("Process stopped by user")
            return
        year_url = base_url + link["href"]
        year = year_url.split("/")[-1]
        if not year.isdigit():
            continue
        year_int = int(year)
        if (start_year and year_int < start_year) or (end_year and year_int > end_year):
            log(f"Skipping year {year}")
            continue

        log(f"\nProcessing year: {year}")
        process_paper_links(year_url, year, base_url, output_dir, thread_count, max_retries,
                            timeout, scrape_type, stop_event, log, metadata_queue, update_count, update_table)

def process_paper_links(year_url, year, base_url, output_dir, thread_count, max_retries, timeout,
                        scrape_type, stop_event, log, metadata_queue, update_count, update_table):
    log(f"Fetching year page: {year_url}")
    year_page_html = fetch_page(year_url, max_retries, timeout, stop_event, log)
    if not year_page_html:
        return

    soup = BeautifulSoup(year_page_html, "html.parser")
    paper_links = [base_url + a["href"] for a in soup.select("body > div.container-fluid > div > ul li a")]

    with ThreadPoolExecutor(max_workers=thread_count) as executor:
        futures = []
        for url in paper_links:
            futures.append(executor.submit(
                process_paper,
                url, year, base_url, output_dir,
                max_retries, timeout, scrape_type,
                stop_event, log, metadata_queue, update_count, update_table
            ))
        for future in futures:
            future.result()

def process_paper(paper_url, year, base_url, output_dir, max_retries, timeout,
                  scrape_type, stop_event, log, metadata_queue, update_count, update_table):
    if stop_event.is_set():
        return

    log(f"Processing paper: {paper_url}")
    paper_html = fetch_page(paper_url, max_retries, timeout, stop_event, log)
    if not paper_html:
        return

    soup = BeautifulSoup(paper_html, "html.parser")

    try:
        title = soup.find('h4').get_text(strip=True)
    except AttributeError:
        title = "Untitled"
    
    # New logic for authors: find h4 containing "Authors" and then get its next sibling's text.
    authors_element = soup.find("h4", string=lambda t: t and "Authors" in t)
    if authors_element:
        authors_sibling = authors_element.find_next_sibling()
        authors = authors_sibling.get_text(strip=True) if authors_sibling else "Unknown"
    else:
        authors = "Unknown"

    # New logic for abstract: find h4 containing "Abstract" and then get its next sibling's text.
    abstract_element = soup.find("h4", string=lambda t: t and "Abstract" in t)
    if abstract_element:
        abstract_sibling = abstract_element.find_next_sibling()
        abstractText = abstract_sibling.get_text(strip=True) if abstract_sibling else "No abstract available"
    else:
        abstractText = "No abstract available"

    pdf_link = soup.find('a', string='Paper')
    pdf_url = base_url + pdf_link['href'] if pdf_link else None

    if scrape_type in ["PDFs", "Both"] and pdf_url:
        sanitized_title = sanitize_filename(title)
        download_pdf(pdf_url, sanitized_title, year, output_dir, timeout, stop_event, log, update_count, update_table)

    if scrape_type in ["Metadata", "Both"]:
        metadata = {
            "title": title,
            "authors": authors,
            "abstract": abstractText,
            "pdf_url": pdf_url or "",
            "paper_url": paper_url,
            "year": year
        }
        metadata_queue.put(metadata)
        log(f"Collected metadata for: {title}")
        update_table(year, "Metadata", title)

def download_pdf(url, filename, year, output_dir, timeout, stop_event, log, update_count, update_table):
    if stop_event.is_set():
        return
    save_dir = os.path.join(output_dir, str(year))
    os.makedirs(save_dir, exist_ok=True)
    path = os.path.join(save_dir, f"{filename}.pdf")

    try:
        response = requests.get(url, stream=True, timeout=timeout)
        response.raise_for_status()
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if stop_event.is_set():
                    log(f"Stopped downloading: {filename}")
                    return
                f.write(chunk)
        log(f"Downloaded: {filename}")
        update_count(year)
        update_table(year, "PDF", filename)
    except Exception as e:
        error_msg = f"Failed to download {url}: {str(e)}"
        log(error_msg)
        failed_dir = os.path.join(output_dir, "failed_pdfs")
        os.makedirs(failed_dir, exist_ok=True)
        with open(os.path.join(failed_dir, "failed_pdfs.txt"), "a", encoding="utf-8") as f:
            f.write(error_msg + "\n")

def fetch_page(url, max_retries, timeout, stop_event, log):
    for attempt in range(max_retries):
        if stop_event.is_set():
            return None
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            log(f"Attempt {attempt+1} failed for {url}: {str(e)}")
            time.sleep(2)
    log(f"Max retries reached for {url}")
    return None

def sanitize_filename(filename):
    return "".join(c for c in filename if c.isalnum() or c in (" ", "-", "_")).strip().replace(" ", "_")

if __name__ == "__main__":
    root = tk.Tk()
    app = ScraperGUI(root)
    root.mainloop()
