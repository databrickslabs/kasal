import time
import logging
import os
import json
from typing import Any, Optional, Type, Dict, ClassVar, List

from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from seleniumbase import Driver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
import random

# Configure logging
logger = logging.getLogger(__name__)

class CROScrapingToolSchema(BaseModel):
    """Input schema for CROScrapingTool."""
    search_term: str = Field(
        ...,
        description="The company name or number to search for on core.cro.ie",
    )

class CROScrapingTool(BaseTool):
    """
    A tool specialized for searching and scraping company information from core.cro.ie (Irish CRO).
    This tool automates the process of:
    1. Navigating to core.cro.ie
    2. Entering a search term (company name or number)
    3. Clicking the search button
    4. Extracting the resulting company information
    Uses SeleniumBase for reliable web interactions.
    """
    name: str = "CRO Company Search Tool"
    description: str = "Search for Irish company information using company name or number on core.cro.ie"
    args_schema: Type[BaseModel] = CROScrapingToolSchema

    # Website configuration
    base_url: str = "https://core.cro.ie/"
    
    # Search fields and selectors
    search_input_id: str = "registeredNumber"
    
    # Selenium configuration
    wait_time: int = 10
    headless: bool = False  # Default to non-headless for debugging
    output_dir: str = "/tmp"
    user_agents: ClassVar[list[str]] = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/109.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
    ]

    def __init__(
        self,
        wait_time: Optional[int] = None,
        headless: bool = False,
        output_dir: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if wait_time is not None:
            self.wait_time = wait_time
        if output_dir is not None:
            self.output_dir = output_dir
        self.headless = headless
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def _run(
        self,
        search_term: str,
        **kwargs: Any,
    ) -> str:
        """Execute the CRO search and return the results"""
        logger.info(f"Starting search for: {search_term}")
        
        try:
            driver = self._create_driver()
            results = {}
            
            try:
                # STEP 1: Navigate to the website
                logger.info(f"Opening {self.base_url}")
                try:
                    driver.get(self.base_url)
                except Exception as e:
                    logger.warning(f"Initial page load issue: {e}, trying reconnect method")
                    driver.uc_open_with_reconnect(self.base_url, reconnect_time=10)
                
                # Wait for page to load
                logger.info("Waiting for page to load...")
                time.sleep(5)
                
                # Take screenshot of initial page
                initial_screenshot = os.path.join(self.output_dir, f"cro_initial_page.png")
                driver.save_screenshot(initial_screenshot)
                
                # STEP 2: Click on "here" link (usually needed for search access)
                logger.info("Looking for 'here' link...")
                here_link = self._find_element_with_fallbacks(
                    driver,
                    [
                        (By.LINK_TEXT, "here"),
                        (By.PARTIAL_LINK_TEXT, "here"),
                        (By.XPATH, "//a[contains(text(), 'here')]"),
                    ],
                    "here link"
                )
                
                if here_link:
                    here_link.click()
                    time.sleep(2)
                
                # STEP 3: Find and click on the search input field
                logger.info(f"Looking for search input field...")
                input_field = self._find_element_with_fallbacks(
                    driver,
                    [
                        (By.ID, self.search_input_id),
                        (By.NAME, self.search_input_id),
                        (By.XPATH, f"//input[@id='{self.search_input_id}']"),
                        (By.XPATH, "//input[contains(@placeholder, 'number')]"),
                        (By.XPATH, "//input[contains(@placeholder, 'search')]"),
                    ],
                    "search input"
                )
                
                if not input_field:
                    # If specific field not found, try the first input field
                    try:
                        input_fields = driver.find_elements(By.TAG_NAME, "input")
                        if input_fields:
                            input_field = input_fields[0]
                            logger.info("Using first available input field")
                    except Exception as e:
                        logger.warning(f"Could not find any input fields: {e}")
                        return f"Error: Could not locate search input field on the CRO website. Technical details: {e}"
                
                # Click to focus the input field
                input_field.click()
                time.sleep(0.5)
                input_field.click()  # Double click to ensure focus
                
                # STEP 4: Type the search term
                logger.info(f"Entering search term: {search_term}")
                input_field.clear()
                # Type with small delays to mimic human typing
                for char in search_term:
                    input_field.send_keys(char)
                    time.sleep(random.uniform(0.05, 0.1))
                
                time.sleep(1)
                
                # Take screenshot after typing
                typing_screenshot = os.path.join(self.output_dir, "cro_after_typing.png")
                driver.save_screenshot(typing_screenshot)
                
                # STEP 5: Submit the search
                logger.info("Submitting search...")
                # First try to find and click the search button
                search_button = self._find_element_with_fallbacks(
                    driver,
                    [
                        (By.XPATH, "//button[contains(text(), 'Search')]"),
                        (By.XPATH, "//regsys-reactive-button[2]/button"),
                        (By.XPATH, "//button[@type='submit']"),
                        (By.CSS_SELECTOR, "button[type='submit']"),
                    ],
                    "search button"
                )
                
                if search_button:
                    search_button.click()
                    logger.info("Clicked search button")
                else:
                    # If button not found, press Enter
                    logger.info("Search button not found, pressing Enter key")
                    input_field.send_keys(Keys.ENTER)
                
                # Wait for results to load
                logger.info(f"Waiting {self.wait_time} seconds for results to load")
                time.sleep(self.wait_time)
                
                # Take screenshot of search results
                results_screenshot = os.path.join(self.output_dir, "cro_search_results.png")
                driver.save_screenshot(results_screenshot)
                
                # Save page source for debugging
                page_source_path = os.path.join(self.output_dir, "cro_results_page.html")
                with open(page_source_path, "w", encoding="utf-8") as f:
                    f.write(driver.page_source)
                
                # STEP 6: Check for search results and extract information
                body_text = driver.find_element(By.TAG_NAME, "body").text
                
                if "No records found" in body_text:
                    logger.info(f"No results found for: {search_term}")
                    return f"No company records found for '{search_term}' on the CRO website."
                
                # Extract company details
                company_details = self._extract_company_info(driver)
                
                if company_details:
                    # Format the results as a readable string
                    result_text = self._format_company_details(company_details, search_term)
                    return result_text
                else:
                    return f"Found search results for '{search_term}' but couldn't extract specific company details."
            
            finally:
                logger.info("Closing the browser")
                driver.quit()
        
        except Exception as e:
            logger.error(f"Error during CRO search: {str(e)}")
            return f"Error searching CRO for '{search_term}': {str(e)}"

    def _find_element_with_fallbacks(self, driver, locator_list, element_name, timeout=5):
        """
        Try multiple locator strategies to find an element, with logging
        
        Args:
            driver: The Selenium driver
            locator_list: List of (By, locator) tuples to try
            element_name: Name of the element for logging
            timeout: Seconds to wait for each attempt
        
        Returns:
            The element if found, None otherwise
        """
        for i, (by, locator) in enumerate(locator_list):
            try:
                logger.info(f"Trying to find {element_name} with {by}={locator}")
                element = WebDriverWait(driver, timeout).until(
                    EC.presence_of_element_located((by, locator))
                )
                logger.info(f"Found {element_name} with {by}={locator}")
                return element
            except Exception as e:
                logger.debug(f"Could not find {element_name} with {by}={locator}: {e}")
        
        logger.warning(f"Could not find {element_name} with any of the provided locators")
        return None

    def _extract_company_info(self, driver) -> Dict:
        """Extract company information from the page"""
        company_details = {}
        
        # Check the current URL for context
        current_url = driver.current_url
        logger.info(f"Extracting info from URL: {current_url}")
        
        # First try to extract from the search results table
        try:
            # Look for table rows in search results
            logger.info("Trying to extract from search results table")
            rows = driver.find_elements(By.XPATH, "//mat-row | //tr")
            
            if rows and len(rows) > 0:
                logger.info(f"Found {len(rows)} rows in search results")
                # Try to extract data from the first row
                first_row = rows[0]
                cells = first_row.find_elements(By.XPATH, ".//mat-cell | .//td")
                
                if cells and len(cells) > 0:
                    logger.info(f"Found {len(cells)} cells in first row")
                    
                    # Extract data from cells
                    if len(cells) >= 1:
                        company_number = cells[0].text.strip()
                        if company_number and "Number" not in company_number:
                            company_details["company_number"] = company_number
                    
                    if len(cells) >= 2:
                        company_name = cells[1].text.strip()
                        if company_name and "Name" not in company_name and "Search" not in company_name:
                            company_details["company_name"] = company_name
                    
                    if len(cells) >= 3:
                        company_address = cells[2].text.strip()
                        if company_address and "Address" not in company_address:
                            company_details["company_address"] = company_address
        except Exception as e:
            logger.warning(f"Error extracting from search results table: {e}")
        
        # If extraction from cells failed, try direct XPath approaches
        if not company_details.get("company_name") or not company_details.get("company_number"):
            try:
                # Try specific XPaths for company info
                number_elements = driver.find_elements(By.XPATH, "//mat-cell[1]//p | //td[1]//p")
                name_elements = driver.find_elements(By.XPATH, "//mat-cell[2]//p | //td[2]//p")
                
                if number_elements and len(number_elements) > 0:
                    company_number = number_elements[0].text.strip()
                    if company_number and "Number" not in company_number:
                        company_details["company_number"] = company_number
                
                if name_elements and len(name_elements) > 0:
                    company_name = name_elements[0].text.strip()
                    if company_name and "Name" not in company_name and "Search" not in company_name:
                        company_details["company_name"] = company_name
            except Exception as e:
                logger.warning(f"Error with direct XPath extraction: {e}")
        
        # If still no results, try extracting from page text
        if not company_details.get("company_name") or not company_details.get("company_number"):
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                lines = body_text.split('\n')
                
                # Look for patterns in text that might indicate company details
                for i, line in enumerate(lines):
                    # Skip very short lines or ones with common UI elements
                    if len(line) < 3 or "Search" in line or "Go" in line:
                        continue
                    
                    # Look for company number pattern
                    if "Company Number" in line or "Number" in line or line.isdigit():
                        if i+1 < len(lines) and lines[i+1].strip().isdigit():
                            company_details["company_number"] = lines[i+1].strip()
                        elif line.split()[-1].isdigit():
                            company_details["company_number"] = line.split()[-1]
                    
                    # Look for company name patterns
                    if "Company Name" in line or "Name" in line:
                        if i+1 < len(lines) and len(lines[i+1].strip()) > 3:
                            company_name = lines[i+1].strip()
                            if "Search" not in company_name and "Number" not in company_name:
                                company_details["company_name"] = company_name
                    
                    # Look for specific long numbers that might be company IDs
                    if line.strip().isdigit() and len(line.strip()) >= 5:
                        company_details.setdefault("company_number", line.strip())
            except Exception as e:
                logger.warning(f"Error extracting from page text: {e}")
        
        return company_details

    def _format_company_details(self, details: Dict, search_term: str) -> str:
        """Format company details into a readable string"""
        if not details:
            return f"No detailed information found for '{search_term}'."
        
        result_parts = [f"Company information for '{search_term}' from CRO:"]
        
        if "company_name" in details:
            result_parts.append(f"Name: {details['company_name']}")
        
        if "company_number" in details:
            result_parts.append(f"Number: {details['company_number']}")
        
        if "company_address" in details:
            result_parts.append(f"Address/Type: {details['company_address']}")
        
        # Include any other details that might be present
        for key, value in details.items():
            if key not in ["company_name", "company_number", "company_address", "page_text"]:
                result_parts.append(f"{key.replace('_', ' ').title()}: {value}")
        
        return "\n".join(result_parts)

    def _create_driver(self):
        """Create a SeleniumBase driver with appropriate configuration"""
        logger.info("Setting up SeleniumBase driver")
        
        try:
            # First try standard driver (more reliable for CRO)
            user_agent = random.choice(self.user_agents)
            logger.info(f"Using user agent: {user_agent}")
            
            driver = Driver(uc=False, headless=self.headless)
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": user_agent})
            
            # Set timeouts
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            
            # Add stealth script to avoid detection
            driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
                "source": """
                // Overwrite the 'navigator.webdriver' property
                Object.defineProperty(navigator, 'webdriver', {
                    get: () => undefined
                });
                """
            })
            
            logger.info("Driver setup complete")
            return driver
            
        except Exception as e:
            # Fallback to UC mode if standard fails
            logger.warning(f"Error setting up standard driver: {e}. Trying undetected mode...")
            driver = Driver(uc=True, headless=self.headless)
            driver.set_page_load_timeout(60)
            driver.set_script_timeout(60)
            return driver 