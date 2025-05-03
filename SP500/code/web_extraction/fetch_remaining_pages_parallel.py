import concurrent.futures
from . import fetch_and_save_page

def fetch_remaining_pages_parallel(start_page: int, total_pages: int, base_url: str, output_dir: str, max_workers: int):
    """
    Fetches pages from start_page up to total_pages in parallel using threads.

    Args:
        start_page (int): The first page number to fetch (usually 2).
        total_pages (int): The total number of pages.
        base_url (str): The base URL template.
        output_dir (str): The directory to save HTML files.
        max_workers (int): The maximum number of concurrent threads.

    Raises:
        RuntimeError: If any thread fails during fetching.
    """
    if total_pages < start_page:
        # print(f"No remaining pages to fetch (total: {total_pages}, start: {start_page}).") # Logging removed
        return # Nothing to do

    pages_to_fetch = range(start_page, total_pages + 1)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit fetch tasks for each remaining page
        future_to_page = {
            executor.submit(fetch_and_save_page, page_num, base_url, output_dir): page_num
            for page_num in pages_to_fetch
        }

        # Process results (and exceptions) as they complete
        for future in concurrent.futures.as_completed(future_to_page):
            page_num = future_to_page[future]
            try:
                # Call result() to raise any exception that occurred in the thread
                future.result()
                # print(f"Page {page_num} processing complete.") # Logging removed
            except Exception as exc:
                # An error occurred in one of the worker threads
                # Re-raise the exception to fail the Dagster asset run
                raise RuntimeError(f"Error fetching page {page_num} in parallel: {exc}") from exc