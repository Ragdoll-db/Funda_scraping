import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def fetch_and_save_house_page(listing_id: int, url: str, output_dir: str):
    """
    Fetches and saves the HTML source for a single Funda listing detail page.
    Designed to be run in a separate thread. Creates its own driver instance.

    Args:
        listing_id (int): The Funda listing ID (used for filename).
        url (str): The URL of the listing detail page.
        output_dir (str): The directory to save the HTML file in.

    Raises:
        RuntimeError: If any Selenium or unexpected error occurs during fetching/saving.
        ValueError: If the fetched page source seems invalid.
    """
    filename = f"{listing_id}.html"
    filepath = os.path.join(output_dir, filename)

    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    driver = None
    service = None
    try:
        service = ChromeService()
        driver = webdriver.Chrome(service=service, options=chrome_options)

        driver.get(url)

        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        page_source = driver.page_source

        if "<body" not in page_source.lower():
             raise ValueError(f"Listing page {listing_id} did not load correctly (no body tag). URL: {url}")

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(page_source)

        return None

    except TimeoutException:
        raise RuntimeError(f"Timeout loading listing page {listing_id}. URL: {url}")
    except WebDriverException as e:
        raise RuntimeError(f"Selenium WebDriver failed for listing page {listing_id}: {e}. URL: {url}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error processing listing page {listing_id}: {e}. URL: {url}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                 pass 