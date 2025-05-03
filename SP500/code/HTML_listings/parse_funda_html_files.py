# parse_html.py
import os
import json
import re
import datetime
from typing import Dict, Tuple, Optional
from bs4 import BeautifulSoup

def extract_listing_id(url: str) -> int | None:
    """
    Extracts the numeric listing ID from a Funda detail URL.
    Example: https://www.funda.nl/detail/koop/stadskanaal/.../43995838/ -> 43995838

    Args:
        url (str): The Funda detail URL.

    Returns:
        int | None: The extracted listing ID as an integer, or None if not found/invalid.
    """
    if not url:
        return None
    match = re.search(r'/(\d+)/?$', url)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None

# --- Main Parsing Function ---
def parse_funda_html_files(
    html_input_dir: str,
    scrape_date: datetime.date
) -> Dict[int, Tuple[str, datetime.date]]:
    """
    Parses all HTML files in the specified directory to extract Funda listing URLs and IDs.

    Args:
        html_input_dir: The directory containing the HTML files for a specific date.
        scrape_date: The date associated with these scraped files.

    Returns:
        A dictionary mapping listing_id (int) to a tuple of (url (str), scrape_date (date)).
        Returns an empty dictionary if the input directory doesn't exist or contains no valid data.
    """
    extracted_data: Dict[int, Tuple[str, datetime.date]] = {}
    files_processed = 0
    listings_found_in_run = 0

    # Check if input directory exists before proceeding
    if not os.path.isdir(html_input_dir):
        print(f"Warning: Input directory for parsing not found: {html_input_dir}. Returning empty data.")
        return {} # Return empty dict if source dir is missing

    print(f"Parsing HTML files from: {html_input_dir}")

    try:
        html_files = [f for f in os.listdir(html_input_dir) if f.endswith(".html")]
        if not html_files:
            print(f"Warning: No HTML files found in {html_input_dir}. No data to parse.")
            return {} # Return empty dict if no files

        for filename in html_files:
            filepath = os.path.join(html_input_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    page_content = f.read()

                soup = BeautifulSoup(page_content, 'html.parser')
                script_tag = soup.find('script', type='application/ld+json', attrs={'data-hid': 'result-list-metadata'})

                if script_tag and script_tag.string:
                    try:
                        json_data = json.loads(script_tag.string)
                        if isinstance(json_data, dict) and \
                           ('ItemList' in json_data.get('@type', [])) and \
                           ('itemListElement' in json_data):

                            for item in json_data['itemListElement']:
                                if isinstance(item, dict) and 'url' in item:
                                    link = item['url']
                                    listing_id = extract_listing_id(link)

                                    if listing_id is not None:
                                        if listing_id not in extracted_data:
                                            listings_found_in_run += 1
                                        # Use the passed scrape_date
                                        extracted_data[listing_id] = (link, scrape_date)
                                    else:
                                        print(f"Warning: Could not extract listing ID from URL: {link} in file {filename}")
                        else:
                             print(f"Warning: JSON-LD in {filename} does not seem to be a valid ItemList.")

                    except json.JSONDecodeError:
                        print(f"Warning: Could not decode JSON in {filename}. Skipping.")
                        continue
                    except Exception as parse_err:
                        print(f"Warning: Error parsing JSON data in {filename}: {parse_err}. Skipping.")
                        continue
                # else: # Optional: Reduce noise, only warn if specifically needed
                #    if page_content: # Don't warn for empty files
                #         print(f"Warning: JSON-LD script tag not found or empty in {filename}.")

                files_processed += 1

            except FileNotFoundError:
                 print(f"Warning: File disappeared during processing: {filepath}. Skipping.")
                 continue
            except Exception as file_err:
                print(f"Warning: Could not read or process file {filename}: {file_err}. Skipping.")
                continue # Process next file

        print(f"Finished parsing {files_processed} HTML files.")
        print(f"Found {listings_found_in_run} new unique listings in this run (Total unique listings collected: {len(extracted_data)}).")

    except Exception as e:
        print(f"ERROR: Failed during HTML file processing loop in directory {html_input_dir}: {e}")
        # Reraise as a runtime error to signal failure
        raise RuntimeError(f"Failed during HTML file processing in {html_input_dir}: {e}") from e

    return extracted_data