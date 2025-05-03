from bs4 import BeautifulSoup

def parse_total_pages(page_source: str) -> int:
    """
    Parses the HTML source of the first page to determine the total number of result pages.

    Args:
        page_source (str): The HTML source code of the first page.

    Returns:
        int: The total number of pages found (0 if no results or parsing fails).
    """
    try:
        soup = BeautifulSoup(page_source, 'html.parser')
        page_numbers = []
        total_pages = 0

        # Find the pagination navigation element
        pagination_nav = soup.select_one('nav[data-testid="pagination"]')

        if pagination_nav:
            # Find all links within pagination
            page_links = pagination_nav.select('a')
            for link in page_links:
                link_text = link.get_text(strip=True)
                if link_text.isdigit():
                    try:
                        page_numbers.append(int(link_text))
                    except ValueError:
                        continue

            if page_numbers:
                total_pages = max(page_numbers)
            else:
                 # Pagination exists, but no numbers? Check for results.
                 search_results = soup.select('[data-test-id="search-result-item"]')
                 total_pages = 1 if search_results else 0
        else:
            # No pagination nav? Check for results.
            search_results = soup.select('[data-test-id="search-result-item"]')
            total_pages = 1 if search_results else 0

        return total_pages

    except Exception as e:
        # If parsing fails for any reason, log it and return 0 pages
        # print(f"Warning: Failed to parse total pages from page source: {e}") # Logging removed
        return 0 # Indicate failure or no pages