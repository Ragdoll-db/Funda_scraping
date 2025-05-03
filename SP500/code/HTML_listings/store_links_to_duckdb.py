# store_to_duckdb.py
import os
import duckdb
import datetime
from typing import Dict, Tuple

def store_links_to_duckdb(
    extracted_data: Dict[int, Tuple[str, datetime.date]],
    db_path: str,
    table_name: str
) -> None:
    """
    Stores the extracted listing data into a DuckDB database.

    Creates the table if it doesn't exist and uses 'INSERT OR IGNORE'
    to handle potential duplicate listing IDs.

    Args:
        extracted_data: Dictionary mapping listing_id to (url, scrape_date).
        db_path: The full path to the DuckDB database file.
        table_name: The name of the table to insert data into.
    """
    if not extracted_data:
        print("No data was extracted, skipping database insertion.")
        return

    # Ensure the directory for the database file exists
    db_dir = os.path.dirname(db_path)
    if db_dir: # Avoid error if db_path is just filename in current dir
        os.makedirs(db_dir, exist_ok=True)

    print(f"Connecting to DuckDB database: {db_path}")
    conn = None
    try:
        conn = duckdb.connect(database=db_path, read_only=False)

        # Create table if it doesn't exist
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            listing_id BIGINT PRIMARY KEY,
            url TEXT,
            scraped_date DATE
        );
        """
        print(f"Executing: CREATE TABLE IF NOT EXISTS {table_name}...")
        conn.execute(create_table_sql)

        # Prepare data for insertion
        data_to_insert = [
            (listing_id, url_date_tuple[0], url_date_tuple[1])
            for listing_id, url_date_tuple in extracted_data.items()
        ]

        # Use executemany for efficient bulk insertion with INSERT OR IGNORE
        insert_sql = f"INSERT OR IGNORE INTO {table_name} (listing_id, url, scraped_date) VALUES (?, ?, ?)"
        print(f"Attempting to insert/ignore {len(data_to_insert)} records into table '{table_name}'...")
        conn.executemany(insert_sql, data_to_insert)
        conn.commit() # Commit the transaction

        print(f"Database operation complete for {len(data_to_insert)} records.")

    except duckdb.Error as db_err:
        print(f"ERROR: DuckDB error during database operation: {db_err}")
        raise RuntimeError(f"DuckDB error: {db_err}") from db_err
    except Exception as e:
        print(f"ERROR: Generic error during database operation: {e}")
        raise RuntimeError(f"Failed during database operation: {e}") from e
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")