import time
import logging
from typing import Any, Optional, Type, Dict

from crewai.tools import BaseTool
from pydantic import BaseModel, Field
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# Configure logging
logger = logging.getLogger(__name__)


class ZefixScrapingToolSchema(BaseModel):
    """Input schema for ZefixScrapingTool."""
    
    search_term: str = Field(
        ...,
        description="The company name or UID number to search for on Zefix.ch",
    )


class ZefixScrapingTool(BaseTool):
    """
    A tool specialized for searching and scraping company information from Zefix.ch.
    
    This tool automates the process of:
    1. Navigating to Zefix.ch
    2. Entering a search term (company name or UID)
    3. Clicking the search button
    4. Extracting the resulting company information
    """
    
    name: str = "Zefix Company Search Tool"
    description: str = "Search for Swiss company information using company name or UID numbers on Zefix.ch"
    args_schema: Type[BaseModel] = ZefixScrapingToolSchema
    
    # Website configuration
    base_url: str = "https://www.zefix.ch/"
    
    # Updated selectors - these need to be checked with the actual website
    # The previous selectors might not be working if the website has changed
    search_input_selector: str = "#mat-input-0"  # Try a more specific ID-based selector
    search_button_selector: str = "button[type='submit']"  # Generic submit button selector
    company_info_selector: str = "#content > zfx-entity > zfx-firm > div > div.company-header.col-block > div.col.notranslate > h3"
    
    # Selenium configuration
    wait_time: int = 5  # Time to wait for page to load in seconds
    chrome_options: Optional[Dict[str, Any]] = None  # Added as a proper field
    headless: bool = False  # Default to showing the browser
    
    def __init__(
        self,
        wait_time: Optional[int] = None,
        chrome_options: Optional[Dict[str, Any]] = None,
        headless: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        
        # Allow wait time to be customized
        if wait_time is not None:
            self.wait_time = wait_time
            
        # Store chrome options for selenium
        if chrome_options is not None:
            self.chrome_options = chrome_options
            
        # Set headless mode
        self.headless = headless
    
    def _run(
        self,
        search_term: str,
        **kwargs: Any,
    ) -> str:
        """Execute the Zefix search and return the results."""
        
        try:
            # Create a custom driver since we need to control headless mode
            driver = self._create_custom_driver()
            
            try:
                logger.info(f"Navigating to {self.base_url}")
                driver.get(self.base_url)
                time.sleep(self.wait_time)  # Wait for the page to load
                
                # Save a screenshot for debugging
                logger.info("Taking screenshot of the page")
                screenshot_path = "/tmp/zefix_page.png"
                driver.save_screenshot(screenshot_path)
                logger.info(f"Screenshot saved to {screenshot_path}")
                
                # Log the page source for debugging
                logger.info("Page title: %s", driver.title)
                
                # Wait for the search input to be present and find it
                logger.info(f"Looking for search input with selector: {self.search_input_selector}")
                wait = WebDriverWait(driver, 10)
                try:
                    search_input = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, self.search_input_selector))
                    )
                except Exception as e:
                    logger.error(f"Could not find search input: {str(e)}")
                    
                    # Try alternative selectors
                    logger.info("Trying alternative selectors...")
                    
                    # Try to find any input elements
                    inputs = driver.find_elements(By.TAG_NAME, "input")
                    logger.info(f"Found {len(inputs)} input elements")
                    
                    if inputs:
                        logger.info("Using the first input element found")
                        search_input = inputs[0]
                    else:
                        raise Exception("No input elements found on the page")
                
                # Clear and fill the search input
                logger.info(f"Entering search term: {search_term}")
                search_input.clear()
                search_input.send_keys(search_term)
                
                # Find and click the search button
                logger.info(f"Looking for search button with selector: {self.search_button_selector}")
                try:
                    search_button = wait.until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, self.search_button_selector))
                    )
                except Exception as e:
                    logger.error(f"Could not find search button: {str(e)}")
                    
                    # Try alternative selectors
                    logger.info("Trying alternative selectors for button...")
                    
                    # Try to find any button elements
                    buttons = driver.find_elements(By.TAG_NAME, "button")
                    logger.info(f"Found {len(buttons)} button elements")
                    
                    if buttons:
                        logger.info("Using the first button element found")
                        search_button = buttons[0]
                    else:
                        raise Exception("No button elements found on the page")
                
                logger.info("Clicking search button")
                search_button.click()
                
                # Wait for results to load
                logger.info(f"Waiting {self.wait_time} seconds for results to load")
                time.sleep(self.wait_time)
                
                # Take another screenshot after search
                driver.save_screenshot("/tmp/zefix_search_results.png")
                logger.info("Search results screenshot saved to /tmp/zefix_search_results.png")
                
                # Extract the company information
                try:
                    company_info_element = driver.find_element(By.CSS_SELECTOR, self.company_info_selector)
                    result = company_info_element.text
                    logger.info(f"Found company info: {result}")
                except Exception as e:
                    logger.error(f"Could not find company info: {str(e)}")
                    
                    # Get whatever content we can find
                    body_text = driver.find_element(By.TAG_NAME, "body").text
                    result = f"No company information found with selector, but page contains: {body_text[:500]}..."
                
                return result
                
            finally:
                # Always close the driver
                logger.info("Closing the browser")
                driver.quit()
                
        except Exception as e:
            logger.error(f"Error during Zefix search: {str(e)}")
            return f"Error searching Zefix for '{search_term}': {str(e)}"
            
    def _create_custom_driver(self):
        """Create a custom Chrome driver with our options."""
        options = Options()
        
        # Force non-headless mode
        logger.info("Setting up Chrome with visible browser window")
        
        # Add Chrome options from config
        if self.chrome_options:
            if "args" in self.chrome_options:
                for arg in self.chrome_options["args"]:
                    # Skip headless argument if it exists
                    if "--headless" not in arg:
                        options.add_argument(arg)
                        logger.info(f"Adding Chrome argument: {arg}")
                    
            if "experimental_options" in self.chrome_options:
                for option_name, option_value in self.chrome_options["experimental_options"].items():
                    options.add_experimental_option(option_name, option_value)
                    logger.info(f"Adding experimental option: {option_name}")
        
        # Add options to make the browser visible
        options.add_argument("--start-maximized")  # Start maximized
        options.add_experimental_option("detach", True)  # Keep browser open
        options.add_experimental_option("excludeSwitches", ["enable-automation"])  # Hide automation message
        options.add_experimental_option("useAutomationExtension", False)  # Disable automation extension
        
        # NEVER add headless mode
        if self.headless:
            logger.warning("Headless mode was requested but is being ignored to ensure browser visibility")
        
        logger.info("Creating Chrome driver")
        return webdriver.Chrome(options=options) 