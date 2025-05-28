"""
Service for agent generation operations.

This module provides business logic for generating agent configurations
using LLM models to convert natural language descriptions into
structured CrewAI agent configurations.
"""

import logging
import json
import os
import traceback
from typing import Dict, Any, List, Optional
from datetime import datetime

import litellm

from src.models.log import LLMLog
from src.utils.prompt_utils import robust_json_parser
from src.services.template_service import TemplateService
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager

# Configure logging
logger = logging.getLogger(__name__)

class AgentGenerationService:
    """Service for agent generation operations."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
    
    @classmethod
    def create(cls) -> 'AgentGenerationService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of AgentGenerationService with all required dependencies
        """
        log_service = LLMLogService.create()
        return cls(log_service=log_service)
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str) -> None:
        """
        Log LLM interaction using the log service.
        
        Args:
            endpoint: API endpoint that was called
            prompt: Input prompt text
            response: Response from the LLM
            model: Model used for generation
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status='success'
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def generate_agent(self, prompt_text: str, model: str = None, tools: List[str] = None) -> Dict[str, Any]:
        """
        Generate agent configuration from natural language description.
        
        This method processes a natural language description of an agent
        and returns a structured configuration that can be used with CrewAI.
        
        Args:
            prompt_text: Natural language description of the agent
            model: Model to use for generation, defaults to environment variable or "databricks-llama-4-maverick"
            tools: List of tools available to the agent
            
        Returns:
            Dict[str, Any]: Agent configuration in JSON format
            
        Raises:
            ValueError: If there's a problem with the configuration
            Exception: For any other errors during generation
        """
        # Default values
        model = model or os.getenv("AGENT_MODEL", "databricks-llama-4-maverick")
        tools = tools or []
        
        logger.info(f"Generating agent with model: {model} and tools: {tools}")
        
        try:
            # Get and prepare prompt template
            system_message = await self._prepare_prompt_template(tools)
            
            # Generate agent configuration
            agent_config = await self._generate_agent_config(prompt_text, system_message, model)
            
            # Log the interaction
            try:
                await self._log_llm_interaction(
                    endpoint='generate-agent',
                    prompt=f"System: {system_message}\nUser: {prompt_text}",
                    response=json.dumps(agent_config),
                    model=model
                )
            except Exception as e:
                # Just log the error, don't fail the request
                logger.error(f"Failed to log interaction: {str(e)}")
            
            return agent_config
            
        except Exception as e:
            logger.error(f"Error generating agent: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def _prepare_prompt_template(self, tools: List[str]) -> str:
        """
        Prepare the prompt template with tools context.
        
        Args:
            tools: List of tools to include in the prompt
            
        Returns:
            str: Complete system message with tools context
            
        Raises:
            ValueError: If prompt template is not found
        """
        # Get prompt template from database using the TemplateService
        system_message = await TemplateService.get_template_content("generate_agent")
        
        if not system_message:
            raise ValueError("Required prompt template 'generate_agent' not found in database")
        
        # Build tools context for the prompt
        tools_context = ""
        if tools:
            tools_context = "\n\nAvailable tools for the agent:\n"
            for tool in tools:
                tools_context += f"- {tool}\n"
            
            tools_context += "\n\nEnsure that the agent has these tools assigned in the response."
        
        # Add tools context to the system message
        return system_message + tools_context
    
    async def _generate_agent_config(self, prompt_text: str, system_message: str, model: str) -> Dict[str, Any]:
        """
        Generate and process agent configuration.
        
        Args:
            prompt_text: Natural language description of the agent
            system_message: System message with template and tools context
            model: Model to use for generation
            
        Returns:
            Dict[str, Any]: Processed agent configuration
            
        Raises:
            ValueError: If agent configuration is invalid
            Exception: For generation errors
        """
        # Prepare messages for LLM
        messages = [
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt_text}
        ]
        
        # Configure litellm using the LLMManager
        model_params = await LLMManager.configure_litellm(model)
        
        # Generate completion with litellm directly
        try:
            # Use the rate limit handler utility to handle potential rate limit errors
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.7,
                max_tokens=4000
            )
            
            # Extract and parse content
            content = response["choices"][0]["message"]["content"]
            setup = robust_json_parser(content)
            
            # Validate and process the configuration
            return self._process_agent_config(setup, model)
        except Exception as e:
            logger.error(f"Error generating completion: {str(e)}")
            raise ValueError(f"Failed to generate agent configuration: {str(e)}")
    
    def _process_agent_config(self, setup: Dict[str, Any], model: str, tools: List[str] = None) -> Dict[str, Any]:
        """
        Process and validate agent configuration.
        
        Args:
            setup: Raw agent configuration from LLM
            model: Model used for generation
            tools: List of approved tools
            
        Returns:
            Dict[str, Any]: Processed agent configuration
            
        Raises:
            ValueError: If required fields are missing
        """
        tools = tools or []
        
        # Validate required fields
        required_fields = ["name", "role", "goal", "backstory"]
        for field in required_fields:
            if field not in setup:
                raise ValueError(f"Missing required field in agent configuration: {field}")
        
        # Update the advanced_config.llm field to use the selected model
        if "advanced_config" not in setup:
            setup["advanced_config"] = {
                "llm": model,
                "function_calling_llm": None,
                "max_iter": 25,
                "max_rpm": None,
                "max_execution_time": None,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "system_template": None,
                "prompt_template": None,
                "response_template": None,
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 2,
                "use_system_prompt": True,
                "respect_context_window": True
            }
        else:
            # Update the LLM field in advanced_config to use the selected model
            setup["advanced_config"]["llm"] = model
            
            # Ensure all required advanced_config fields exist
            default_config = {
                "function_calling_llm": None,
                "max_iter": 25,
                "max_rpm": None,
                "max_execution_time": None,
                "verbose": False,
                "allow_delegation": False,
                "cache": True,
                "system_template": None,
                "prompt_template": None,
                "response_template": None,
                "allow_code_execution": False,
                "code_execution_mode": "safe",
                "max_retry_limit": 2,
                "use_system_prompt": True,
                "respect_context_window": True
            }
            
            for key, value in default_config.items():
                if key not in setup["advanced_config"]:
                    setup["advanced_config"][key] = value
        
        # Handle tools strictly - only use tools that were provided in the request
        # First ensure tools exists
        if "tools" not in setup:
            setup["tools"] = tools.copy() if tools else []
        elif tools:
            # Only include tools that were specified in the request
            # This ensures no unauthorized tools are added
            filtered_tools = []
            for tool in setup["tools"]:
                if tool in tools:
                    filtered_tools.append(tool)
            
            # Add any missing tools from the request
            for tool in tools:
                if tool not in filtered_tools:
                    filtered_tools.append(tool)
                    
            setup["tools"] = filtered_tools
        
        return setup 