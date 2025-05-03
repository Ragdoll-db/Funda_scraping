import os

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.common.exceptions import WebDriverException, TimeoutException

def fetch_and_save_page(page_num, base_url, output_dir):
    """
    Fetches and saves the HTML source for a single Funda page using Selenium.
    Designed to be run in a separate thread. Creates its own driver instance.

    Args:
        page_num (int): The page number to fetch.
        base_url (str): The base URL template with a placeholder for the page number.
        output_dir (str): The directory to save the HTML file in.

    Raises:
        RuntimeError: If any Selenium or unexpected error occurs during fetching/saving.
        ValueError: If the fetched page source seems invalid (e.g., missing body tag).
    """
    url = base_url.format(page=page_num)
    filename = f"funda_{page_num}.html"
    filepath = os.path.join(output_dir, filename)

    # Configure Chrome options for headless browsing
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
            EC.presence_of_element_located((By.CSS_SELECTOR, '[data-test-id="search-result-item"], .search-content-placeholder__title, nav[data-testid="pagination"]'))
        )

        # Get the page source
        page_source = driver.page_source

        # Basic validation of the fetched content
        if "<body" not in page_source.lower():
             raise ValueError(f"Page {page_num} did not load correctly (no body tag). URL: {url}")

        # Save the page source to a file
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(page_source)

        # Return None indicates success (no specific data needs to be returned)
        return None

    except TimeoutException:
        raise RuntimeError(f"Timeout loading page {page_num}. URL: {url}")
    except WebDriverException as e:
        raise RuntimeError(f"Selenium WebDriver failed for page {page_num}: {e}. URL: {url}")
    except Exception as e:
        raise RuntimeError(f"Unexpected error processing page {page_num}: {e}. URL: {url}")
    finally:
        # Ensure the WebDriver instance for this thread is always closed
        if driver:
            try:
                driver.quit()
            except Exception as quit_exc:
                 pass
