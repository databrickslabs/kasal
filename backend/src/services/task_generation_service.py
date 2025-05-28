"""
Service for task generation operations.

This module provides business logic for generating tasks 
using LLM models and prompt templates.
"""

import logging
import os
from typing import Optional
import re
import litellm

from src.schemas.model_provider import ModelProvider
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
from src.services.template_service import TemplateService
from src.utils.prompt_utils import robust_json_parser
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager

# Configure logging
logger = logging.getLogger(__name__)

# Default model for task generation
DEFAULT_TASK_MODEL = os.getenv("DEFAULT_TASK_MODEL", "databricks-llama-4-maverick")

class TaskGenerationService:
    """Service for task generation operations."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
    
    @classmethod
    def create(cls) -> 'TaskGenerationService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of TaskGenerationService with all required dependencies
        """
        log_service = LLMLogService.create()
        return cls(log_service=log_service)
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str, 
                                  status: str = 'success', error_message: Optional[str] = None):
        """
        Log LLM interaction using the log service.
        
        Args:
            endpoint: API endpoint name
            prompt: Input prompt
            response: Model response
            model: LLM model used
            status: Status of the interaction (success/error)
            error_message: Optional error message
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
    
    async def generate_task(self, request: TaskGenerationRequest) -> TaskGenerationResponse:
        """
        Generate a task based on the provided prompt and context.
        
        Args:
            request: Task generation request with prompt text, model, and agent context
            
        Returns:
            TaskGenerationResponse with generated task details
            
        Raises:
            ValueError: If required prompt template is not found
            Exception: For other errors
        """
        # Get model from request or fallback to environment variables
        model = request.model or os.getenv("TASK_MODEL", DEFAULT_TASK_MODEL)
        logger.info(f"Using model for task generation: {model}")
        
        # Get prompt template from database
        base_message = await TemplateService.get_template_content("generate_task")
        
        # Check if we have a prompt template
        if not base_message:
            logger.error("No prompt template found in database for generate_task")
            raise ValueError("Required prompt template 'generate_task' not found in database")
        
        logger.info("Using prompt template for generate_task from database")
        
        # Add agent context if provided
        if request.agent:
            agent = request.agent
            base_message += f"\n\nCreate a task specifically for an agent with the following profile:\n"
            base_message += f"Name: {agent.name}\n"
            base_message += f"Role: {agent.role}\n"
            base_message += f"Goal: {agent.goal}\n"
            base_message += f"Backstory: {agent.backstory}\n"
            base_message += "\nEnsure the task aligns with this agent's expertise and goals."

        # Prepare messages for LLM
        messages = [
            {"role": "system", "content": base_message},
            {"role": "user", "content": request.text}
        ]
        
        try:
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(model)
            
            # Generate completion with litellm directly
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.7,
                max_tokens=4000
            )
            
            # Extract content from response
            content = response["choices"][0]["message"]["content"]
            
            if not content:
                raise ValueError("Empty content received from LLM")
                
            logger.info(f"Generated task setup: {content[:100]}...")
            
            # Special handling for responses with embedded function calls or unusual JSON
            if "```json" in content or "```" in content:
                logger.info("Found code block in response, extracting JSON...")
                # Extract JSON from code block if present
                code_block_pattern = re.compile(r'```(?:json)?\s*([\s\S]*?)\s*```')
                matches = code_block_pattern.search(content)
                if matches:
                    content = matches.group(1).strip()
                    logger.info("Extracted JSON from code block")
                
            # Try to clean up some common JSON formatting issues before parsing
            content = content.strip()
            # Remove trailing commas which can cause parsing failures
            content = re.sub(r',\s*([\]}])', r'\1', content)
            
            # Log successful interaction
            await self._log_llm_interaction(
                endpoint='generate-task',
                prompt=f"System: {base_message}\nUser: {request.text}",
                response=content,
                model=model
            )
            
        except Exception as e:
            error_msg = f"Error generating completion: {str(e)}"
            logger.error(error_msg)
            await self._log_llm_interaction(
                endpoint='generate-task',
                prompt=f"System: {base_message}\nUser: {request.text}",
                response=str(e),
                model=model,
                status='error',
                error_message=error_msg
            )
            raise ValueError(error_msg)
        
        # Directly try robust_json_parser which handles a variety of JSON issues
        try:
            setup = robust_json_parser(content)
            logger.info("Successfully parsed response using robust_json_parser")
        except ValueError as e:
            error_msg = f"JSON parsing failed: {str(e)}"
            logger.error(f"{error_msg}, content: {content[:500]}")
            await self._log_llm_interaction(
                endpoint='generate-task',
                prompt=f"System: {base_message}\nUser: {request.text}",
                response=content,
                model=model,
                status='error',
                error_message=error_msg
            )
            raise ValueError(f"Could not parse response as JSON: {str(e)}")
        
        # Validate required fields
        required_fields = ['name', 'description', 'expected_output']
        for field in required_fields:
            if field not in setup:
                raise ValueError(f"Missing required field: {field}")
        
        # Set empty tools array if not present
        if "tools" not in setup:
            setup["tools"] = []

        # Ensure advanced_config exists with defaults if not provided
        if "advanced_config" not in setup:
            setup["advanced_config"] = {
                "async_execution": False,
                "context": [],
                "output_json": None,
                "output_pydantic": None,
                "output_file": None,
                "human_input": False,
                "markdown": False,
                "retry_on_fail": True,
                "max_retries": 3,
                "timeout": None,
                "priority": 1,
                "dependencies": [],
                "retry_delay": 0,
                "allow_delegation": False,
                "llm": model
            }
        else:
            # Fix common type issues in advanced_config
            adv_config = setup["advanced_config"]
            
            # Fix output_json if it's a boolean instead of dict/None
            if "output_json" in adv_config and isinstance(adv_config["output_json"], bool):
                adv_config["output_json"] = None
                
            # Fix output_pydantic if it's a boolean instead of string/None
            if "output_pydantic" in adv_config and isinstance(adv_config["output_pydantic"], bool):
                adv_config["output_pydantic"] = None
                
            # Ensure context is a list
            if "context" in adv_config and not isinstance(adv_config["context"], list):
                adv_config["context"] = []
                
            # Ensure dependencies is a list
            if "dependencies" in adv_config and not isinstance(adv_config["dependencies"], list):
                adv_config["dependencies"] = []
            
            # Ensure LLM field is set in advanced_config
            adv_config["llm"] = model
            
            # Set defaults for missing fields
            adv_config.setdefault("async_execution", False)
            adv_config.setdefault("context", [])
            adv_config.setdefault("output_json", None)
            adv_config.setdefault("output_pydantic", None)
            adv_config.setdefault("output_file", None)
            adv_config.setdefault("human_input", False)
            adv_config.setdefault("markdown", False)
            adv_config.setdefault("retry_on_fail", True)
            adv_config.setdefault("max_retries", 3)
            adv_config.setdefault("timeout", None)
            adv_config.setdefault("priority", 1)
            adv_config.setdefault("dependencies", [])
            adv_config.setdefault("retry_delay", 0)
            adv_config.setdefault("allow_delegation", False)
        
        # Add markdown instructions if enabled
        if setup.get("advanced_config", {}).get("markdown", False):
            setup["description"] += "\n\nPlease format the output using Markdown syntax."
            setup["expected_output"] += "\n\nThe output should be formatted using Markdown."

        # Create response object
        response = TaskGenerationResponse(
            name=setup["name"],
            description=setup["description"],
            expected_output=setup["expected_output"],
            tools=setup.get("tools", []),
            advanced_config=setup.get("advanced_config", {})
        )
        
        return response 