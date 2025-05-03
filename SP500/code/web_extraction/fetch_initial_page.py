from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException, TimeoutException
from selenium.webdriver.common.by import By

def fetch_initial_page(url: str, output_path: str, timeout: int) -> str:
    """
    Fetches the first page, saves its source, and returns the source code.

    Args:
        url (str): The URL of the first page.
        output_path (str): The full file path to save the page source.
        timeout (int): Seconds to wait for elements to load.

    Returns:
        str: The HTML source code of the first page.

    Raises:
        RuntimeError: If fetching, waiting, or saving fails.
    """
    # Configure Chrome options
    chrome_options_init = ChromeOptions()
    chrome_options_init.add_argument("--headless")
    chrome_options_init.add_argument("--no-sandbox")
    chrome_options_init.add_argument("--disable-dev-shm-usage")
    chrome_options_init.add_argument("--disable-gpu")
    chrome_options_init.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    )

    service_init = None
    driver_init = None
    page_source = None

    try:
        # Start the WebDriver instance
        service_init = ChromeService()
        driver_init = webdriver.Chrome(service=service_init, options=chrome_options_init)

        # Fetch the page
        driver_init.get(url)

        # Wait for key elements indicating load completion
        WebDriverWait(driver_init, timeout).until(
             EC.presence_of_element_located((By.CSS_SELECTOR, '[data-test-id="search-result-item"], .search-content-placeholder__title, nav[data-testid="pagination"]'))
        )

        # Get the page source
        page_source = driver_init.page_source

        # Save the page source
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(page_source)

        return page_source # Return the source for parsing

    except TimeoutException as e_timeout:
         error_msg = f"Timeout ({timeout}s) waiting for elements on initial page. Check selectors/load. URL: {url}"
         raise RuntimeError(error_msg) from e_timeout
    except (WebDriverException, OSError, Exception) as e: # Catch file errors too
         raise RuntimeError(f"Failed during initial page fetch/save. URL: {url}. Original error: {type(e).__name__} - {e}") from e
    finally:
        # Robustly quit the driver
        if driver_init:
            try:
                driver_init.quit()
            except Exception:
                pass # Suppress quit errors