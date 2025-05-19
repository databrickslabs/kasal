import datetime
import json
import os
import logging
import requests
import time
import uuid
from typing import Any, Type, Dict, Optional

from dotenv import load_dotenv
load_dotenv()

from crewai.tools import BaseTool
from pydantic import BaseModel, Field


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class BrowserUseAPI:
    def __init__(self, url):
        self.url = url

    def submit_task(self, browser_use_objective):
        """Submit a task and get a task_id."""
        try:
            logger.info(f"{self.url}/submit")
            response = requests.post(
                f"{self.url}/submit",
                json={"browser_use_objective": browser_use_objective}
            )
            if response.status_code == 202:
                return response.json().get("task_id")
            else:
                logger.error(f"Request failed with status code: {response.status_code}")
                return None
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            return None

    def query_task_status(self, task_id):
        """Query the status of a task using task_id."""
        try:
            logger.info(f"{self.url}/query/{task_id}")
            response = requests.get(f"{self.url}/query/{task_id}")
            if response.status_code == 200:
                return {"status": "completed", "message":"completed", "data": response.json()}
            elif response.status_code == 202:
                return {"status": "processing", "message":"processing"}
            else:
                logger.error(f"Request failed with status code: {response.status_code}")
                return {"status": "error", "message": f"Unexpected status code: {response.status_code}"}
        except requests.exceptions.RequestException as e:
            logger.error(f"An error occurred: {e}")
            return {"status": "error", "message": f"An error occurred: {str(e)}"}


class BrowserUseToolSchema(BaseModel):
    """Input for BrowserUseTool."""

    browser_use_objective: str = Field(
        ..., description="Mandatory objective description for browser-use to execute command"
    )


class BrowserUseTool(BaseTool):
    name: str = "BrowserUseTool"
    description: str = (
        "A tool to complete GUI automation task on web browser autonomously. "
        "param: browser_use_objective is used to define the general task for the automation, "
        "and this param is usually some detailed steps of a web automation. "
        "usually specified in multi-line, in the form of a numbered list e.g. 1, 2, 3, ... "
        "with each line representing a step"
    )
    args_schema: Type[BaseModel] = BrowserUseToolSchema
    
    # Don't use __init__ to customize initialization, as it breaks Pydantic validation
    # Instead, use variables to store our configuration
    
    _api_url: Optional[str] = None
    
    @classmethod
    def from_config(cls, browser_use_api_url: Optional[str] = None):
        """Factory method to create a BrowserUseTool with proper configuration.
        
        Args:
            browser_use_api_url: URL of the browser use API service
        """
        instance = cls()
        instance._api_url = browser_use_api_url
        
        if not instance._api_url:
            logger.error("browser_use_api_url not provided in configuration")
        else:
            logger.info(f"BrowserUseTool initialized with API URL: {instance._api_url}")
            
        return instance

    def _run(self, browser_use_objective: str, **kwargs: Any) -> Any:
        """Execute the GUI automation instructions on a web browser."""
        timeout = 300  # 5 minutes timeout
        check_interval = 2  # Check status every 1 second

        if not self._api_url:
            return {
                "status": "error", 
                "message": "Browser Use API URL not configured", 
                "result": {},
                "browser_use_objective": browser_use_objective
            }

        try:
            browser_use_api = BrowserUseAPI(url=self._api_url)
            task_id = browser_use_api.submit_task(browser_use_objective)

            if not task_id:
                return {
                    "status": "error", 
                    "browser_use_objective": browser_use_objective,
                    "result": {},
                    "message": "Failed to submit task"
                }

            start_time = time.time()
            while time.time() - start_time < timeout:
                status = browser_use_api.query_task_status(task_id)
                if status.get("status") == "completed":
                    return {
                        "status": "success",
                        "browser_use_objective": browser_use_objective,
                        "result": status.get("data", {}),
                        "message": status.get("message")
                    }
                elif status.get("status") == "processing":
                    time.sleep(check_interval)
                else:
                    return {
                        "status": "error", 
                        "message": "Unknown status",
                        "result": {},
                        "browser_use_objective": browser_use_objective
                    }

            return {
                "status": "error", 
                "message": "Task timed out", 
                "result": {},
                "browser_use_objective": browser_use_objective
            }
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            return {
                "status": "error", 
                "message": f"An error occurred: {str(e)}", 
                "result": {},
                "browser_use_objective": browser_use_objective
            } 