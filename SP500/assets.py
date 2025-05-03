import os
from dagster import asset
import pyarrow as pa
import pyarrow.parquet as pq
from bs4 import BeautifulSoup
import json
import datetime
from .code.web_extraction import fetch_remaining_pages_parallel, fetch_initial_page, parse_total_pages, fetch_and_save_house_page
from .code.HTML_listings import parse_funda_html_files, store_links_to_duckdb
import duckdb
import re

import concurrent
# --- funda_page_source ---
@asset(
    name="funda_page_source",
    description="The raw HTML page source of today's Funda listings fetched in parallel using Selenium.",
    group_name="funda_scraping",
)
def funda_page_source_parallel() -> None:
    """
    Dagster asset that fetches Funda listing pages in parallel.
    1. Fetches page 1.
    2. Parses page 1 to find the total number of pages.
    3. Fetches remaining pages (2 to total) concurrently.
    """
    base_url = "https://www.funda.nl/zoeken/koop?publication_date=%221%22&search_result={page}"
    today = datetime.date.today().strftime("%Y%m%d")
    output_dir = f"data/funda_listings_HTML/{today}"
    os.makedirs(output_dir, exist_ok=True)
    max_workers = 5
    page1_load_timeout = 45

    try:
        # --- Fetch and save the initial page (page 1) ---
        first_page_url = base_url.format(page=1)
        filepath1 = os.path.join(output_dir, "funda_1.html")
        page1_source = fetch_initial_page(first_page_url, filepath1, page1_load_timeout)

        # --- Parse the initial page source to get total pages ---
        total_pages = parse_total_pages(page1_source)

        if total_pages == 0:
            return

        # --- Fetch remaining pages (if any) in parallel ---
        if total_pages > 1:
            fetch_remaining_pages_parallel(
                start_page=2,
                total_pages=total_pages,
                base_url=base_url,
                output_dir=output_dir,
                max_workers=max_workers
            )

    except Exception as e:
        raise RuntimeError(f"Asset execution failed. Last error: {e}") from e


# --- funda_listing_links ---
@asset(
    name="funda_listing_links",
    description="Extracts individual Funda listing URLs from saved HTML pages and stores them in DuckDB.",
    group_name="funda_scraping",
    deps=[funda_page_source_parallel]
)
def funda_listing_links() -> None:
    """
    Dagster asset that orchestrates parsing Funda HTML and storing links in DuckDB.
    """
    today = datetime.date.today()
    today_str = today.strftime("%Y%m%d")

    # --- Configuration ---
    base_data_dir = "data"
    html_input_dir = os.path.join(base_data_dir, "funda_listings_HTML", today_str)
    db_dir = os.path.join(base_data_dir, "duckdb")
    db_path = os.path.join(db_dir, "funda_listings.duckdb")
    table_name = "funda_links"

    print(f"Starting asset 'funda_listing_links' for date {today_str}")

    if not os.path.isdir(html_input_dir):
        error_msg = f"Input directory not found: {html_input_dir}. Ensure the upstream asset 'funda_page_source_parallel' ran successfully."
        print(f"ERROR: {error_msg}")
        raise FileNotFoundError(error_msg)

    try:
        # --- Parse HTML files ---
        extracted_data = parse_funda_html_files(
            html_input_dir=html_input_dir,
            scrape_date=today
        )

        # --- Store data in DuckDB ---
        store_links_to_duckdb(
            extracted_data=extracted_data,
            db_path=db_path,
            table_name=table_name
        )

        print(f"Asset 'funda_listing_links' completed successfully for {today_str}.")

    except Exception as e:
        print(f"ERROR: Asset 'funda_listing_links' failed during execution: {e}")
        raise e



# --- New Parallel House Source Asset ---
@asset(
    name="funda_house_source", 
    description="Fetches the raw HTML page source for multiple Funda listings in parallel using Selenium.",
    group_name="funda_scraping",
    deps=[funda_listing_links]
)
def funda_house_source_parallel() -> None:
    """
    Dagster asset that reads listing URLs from the DuckDB database,
    fetches the HTML source for each listing detail page in parallel using Selenium,
    and saves them to dated folders.
    """
    # --- Configuration ---
    today = datetime.date.today()
    today_str = today.strftime("%Y%m%d")
    output_dir = f"data/funda_house_HTML/{today_str}"
    db_dir = "data/duckdb"
    db_path = os.path.join(db_dir, "funda_listings.duckdb")
    table_name = "funda_links"
    max_workers = 15

    os.makedirs(output_dir, exist_ok=True)

    # --- Read URLs from DuckDB ---
    urls_to_fetch = []
    conn = None
    print(f"Reading listing URLs from DuckDB: {db_path}")
    try:
        conn = duckdb.connect(database=db_path, read_only=True) 
        sql_query = f"SELECT listing_id, url FROM {table_name} WHERE scraped_date = ?"
        urls_to_fetch = conn.execute(sql_query, (today,)).fetchall()
    except duckdb.Error as db_err:
        raise RuntimeError(f"Failed to read from DuckDB: {db_err}") from db_err
    except Exception as e:
        raise RuntimeError(f"Failed during database read operation: {e}") from e
    finally:
        if conn:
            conn.close()
            
    if not urls_to_fetch:
        print("No listing URLs found in the database to fetch.") 
        return

    print(f"Found {len(urls_to_fetch)} listing URLs to fetch.")

    # --- Fetch pages in parallel ---
    print(f"Fetching {len(urls_to_fetch)} listing pages in parallel (max_workers={max_workers})...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {
            executor.submit(fetch_and_save_house_page, listing_id, url, output_dir): url
            for listing_id, url in urls_to_fetch
        }

        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                future.result()
                print(f"Successfully fetched: {url}") 
            except Exception as exc:
                print(f"Error fetching URL {url}: {exc}")
                raise RuntimeError(f"Error fetching URL {url} in parallel: {exc}") from exc

    print(f"Successfully fetched and saved HTML for {len(urls_to_fetch)} listings.")

@asset(
    name="funda_listing_details",
    description="Extracts housing features from saved HTML pages and saves this in DuckDB.",
    group_name="funda_scraping",
    deps=[funda_house_source_parallel]
)
def funda_listing_details() -> None:
    """
    Reads saved HTML files for listings scraped today, parses key features
    (from 'Kenmerken' section) and general details (address, price, description, etc.),
    and saves all extracted information into a Parquet file located at
    /data/parquet/YYYYMMDD.parquet.
    """
    # --- Configuration ---
    today = datetime.date.today()
    today_str = today.strftime("%Y%m%d")
    html_input_dir = f"data/funda_house_HTML/{today_str}"
    db_path_read = os.path.join("data/duckdb", "funda_listings.duckdb")
    links_table_name = "funda_links"

    output_dir = "data/parquet" 
    parquet_filename = f"{today_str}.parquet"
    parquet_output_path = os.path.join(output_dir, parquet_filename)

    # --- Helper function for cleaning text ---
    def clean_text(text):
        if text:
            return re.sub(r'\s+', ' ', text).strip()
        return None

    # Check if input directory exists
    if not os.path.isdir(html_input_dir):
        print(f"Input directory not found: {html_input_dir}. Skipping asset.")
        raise FileNotFoundError(f"Input directory not found: {html_input_dir}.")
        return

    conn_read = None
    listings_to_process = []
    all_extracted_features = [] 

    try:
        # --- Retrieve listing IDs and URLs from the links table ---
        print(f"Connecting to DuckDB database for reading: {db_path_read}")
        if not os.path.exists(db_path_read):
             print(f"DuckDB database file not found at {db_path_read}. Skipping asset.")
             return

        conn_read = duckdb.connect(database=db_path_read, read_only=True)
        query = f"SELECT listing_id, url FROM {links_table_name} WHERE scraped_date = ?"
        listings_to_process = conn_read.execute(query, (today,)).fetchall()

    except duckdb.Error as db_read_err:
        raise RuntimeError(f"DuckDB error reading links table: {db_read_err}") from db_read_err
    except Exception as e:
        raise RuntimeError(f"Error retrieving listings from DB: {e}") from e
    finally:
        if conn_read:
            conn_read.close()
            print("Closed read connection to DuckDB.")

    if not listings_to_process:
        print(f"No listings found in '{links_table_name}' for date {today}. Asset run completed.")
        return

    print(f"{len(listings_to_process)} listings found to process for: {today}.")

    # --- Iterate through listings, read HTML and parse features ---
    processed_count = 0
    general_features_category = "Algemeen"

    for listing_id, url in listings_to_process:
        if listing_id is None:
            print(f"Warning: Listing ID is None for URL {url}, skipping.")
            continue

        detail_filename = f"{listing_id}.html"
        detail_filepath = os.path.join(html_input_dir, detail_filename)

        if not os.path.exists(detail_filepath):
            print(f"Warning: HTML file not found for {listing_id}: {detail_filepath}. Skipping.")
            continue

        try:
            with open(detail_filepath, "r", encoding="utf-8") as f:
                page_content = f.read()

            soup = BeautifulSoup(page_content, 'html.parser')
            processed_count += 1

            # 1. --- Extract General Features ("algemeen") ---
            try:
                h1_tag = soup.find('h1', attrs={'data-global-id': True})
                if h1_tag:
                    spans = h1_tag.find_all('span', recursive=False)
                    if len(spans) >= 1 and spans[0].get_text(strip=True):
                        address = clean_text(spans[0].get_text())
                        all_extracted_features.append((listing_id, general_features_category, "Adres", address, today))
                    if len(spans) >= 2 and spans[1].get_text(strip=True):
                        postcode_city_text = clean_text(spans[1].get_text())
                        pc_city_parts = postcode_city_text.split(maxsplit=2)
                        
                        if len(pc_city_parts) == 3:

                            if pc_city_parts[-1] == "België":
                                all_extracted_features.append((listing_id, general_features_category, "Postcode", pc_city_parts[0], today))
                                all_extracted_features.append((listing_id, general_features_category, "Plaats", pc_city_parts[1][:-1], today))
                                all_extracted_features.append((listing_id, general_features_category, "Land", "België", today))
                            else:
                                all_extracted_features.append((listing_id, general_features_category, "Postcode", pc_city_parts[0] + " " + pc_city_parts[1], today))
                                all_extracted_features.append((listing_id, general_features_category, "Plaats", pc_city_parts[2], today))
                                all_extracted_features.append((listing_id, general_features_category, "Land", "Nederland", today))
                        else:
                             all_extracted_features.append((listing_id, general_features_category, "PostcodePlaats", postcode_city_text, today)) # Fallback

            except Exception as e:
                print(f"Warning: Could not parse address/city for {listing_id}: {e}")

            try:
                 # Description
                 desc_section = soup.find('section', {'data-v-ad93a001': True, 'class': 'mt-6'})
                 if desc_section:
                     desc_div = desc_section.find('div', id=lambda x: x and x.startswith('headlessui-disclosure-panel-'))
                     if desc_div:
                         description = clean_text(desc_div.get_text(separator=' ', strip=True))
                         if description:
                             all_extracted_features.append((listing_id, general_features_category, "Omschrijving", description, today))
            except Exception as e:
                 print(f"Warning: Could not parse description for {listing_id}: {e}")

            try:
                # Price
                price_span = soup.find('span', string=lambda t: t and '€' in t and ('k.k.' in t or 'v.o.n.' in t) )
                if price_span:
                     price = clean_text(price_span.get_text())
                     if price:
                        all_extracted_features.append((listing_id, general_features_category, "Vraagprijs", price, today))

            except Exception as e:
                print(f"Warning: Could not parse price for {listing_id}: {e}")

            try:
                ul_stats = soup.find('ul', class_=lambda x: x and 'mt-2' in x and 'flex-wrap' in x)
                if ul_stats:
                    list_items = ul_stats.find_all('li', class_='flex', recursive=False)
                    for item in list_items:
                        value_span = item.find('span', class_='md:font-bold')
                        label_span = item.find('span', class_='hidden', string=True)

                        if value_span and label_span:
                            value = clean_text(value_span.get_text())
                            label_text = clean_text(label_span.get_text())
                            feature_name = None
                            if 'wonen' in label_text:
                                feature_name = 'Woonoppervlakte'
                            elif 'perceel' in label_text:
                                feature_name = 'Perceeloppervlakte'
                            elif 'slaapkamer' in label_text:
                                feature_name = 'Aantal slaapkamers'

                            if feature_name and value:
                                all_extracted_features.append((listing_id, general_features_category, feature_name, value, today))

            except Exception as e:
                print(f"Warning: Could not parse living area/plot/bedrooms for {listing_id}: {e}")


            # 2. --- Extract Specific Features (from "Kenmerken" section) ---
            features_section = soup.find('section', id='features')
            if features_section:
                categories = features_section.find_all('h3', class_='font-bold')

                for category_tag in categories:
                    category_name = clean_text(category_tag.get_text())
                    dl_tag = category_tag.find_next_sibling('dl')

                    if dl_tag and category_name:
                        dt_tags = dl_tag.find_all('dt', recursive=False)
                        dd_tags = dl_tag.find_all('dd', recursive=False)

                        for i in range(min(len(dt_tags), len(dd_tags))):
                            feature_name = clean_text(dt_tags[i].get_text())
                            feature_value_raw = dd_tags[i].get_text(separator=' ', strip=True)
                            feature_value = clean_text(feature_value_raw)

                            if feature_name and feature_value:
                                all_extracted_features.append(
                                    (listing_id, category_name, feature_name, feature_value, today)
                                )
            else:
                 print(f"  - No 'Kenmerken' section found in: {detail_filename}")


        except Exception as parse_err:
             print(f"Error parsing file {detail_filename} for listing {listing_id}: {parse_err}")
             continue

    # --- End of loop ---

    print(f"Processed {processed_count} HTML files.")

    if not all_extracted_features:
        print("No features (general or specific) were extracted from any HTML files. Parquet file will not be created.")
        return

    print(f"Total {len(all_extracted_features)} features extracted (including general details).")

    # --- Save ALL features combined into a single Parquet file ---
    try:
        print(f"Preparing to save data to Parquet file using PyArrow: {parquet_output_path}")

        column_names = [
            'listing_id',       
            'feature_category', 
            'feature_name',     
            'feature_value',    
            'scraped_date'      
        ]

        data_dict = {col_name: [row[i] for row in all_extracted_features]
                     for i, col_name in enumerate(column_names)}

        table = pa.Table.from_pydict(data_dict)

        # Ensure the output directory exists
        os.makedirs(output_dir, exist_ok=True)
        print(f"Ensured directory exists: {output_dir}")

        # Write the PyArrow Table to a Parquet file
        pq.write_table(table, parquet_output_path, compression='snappy')

        print(f"Successfully saved {table.num_rows} records to {parquet_output_path} using PyArrow")

    except ImportError:
        # This specific error might be less likely if you successfully imported pa and pq
        print("Error: pyarrow library is required. Please install it (`pip install pyarrow`).")
        raise ImportError("Pyarrow not found. Required for Parquet output.")
    except Exception as e:
        print(f"Error saving data to Parquet file {parquet_output_path} using PyArrow: {e}")
        raise RuntimeError(f"Error saving data to Parquet file {parquet_output_path}: {e}") from e
