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

from src.utils.prompt_utils import robust_json_parser
from src.services.template_service import TemplateService

from src.repositories.log_repository import LLMLogRepository
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)

class AgentGenerationService:
    """Service for agent generation operations."""

    def __init__(self, session: Any):
        """
        Initialize the service with database session.

        Args:
            session: Database session from dependency injection
        """
        self.session = session
        # Initialize log service with repository using the same session
        self.log_service = LLMLogService(LLMLogRepository(session))
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str,
                                  group_context: Optional[GroupContext] = None) -> None:
        """
        Log LLM interaction using the log service.

        Args:
            endpoint: API endpoint that was called
            prompt: Input prompt text
            response: Response from the LLM
            model: Model used for generation
            group_context: Optional group context for multi-group isolation
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status='success',
                group_context=group_context
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")

    # NOTE: the crewai-docs retrieval helper (_get_relevant_documentation)
    # was removed with the crewai->kasal migration: it was never invoked and
    # the docs.crewai.com seeder that fed it is gone.

    async def generate_agent(self, prompt_text: str, model: str = None, tools: List[str] = None,
                            group_context: Optional[GroupContext] = None, fast_planning: bool = True,
                            available_tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """Public entrypoint — wraps agent generation in an MLflow root trace so
        it lands in the shared UC experiment (alongside dispatcher intent, crew
        generation, task generation and crew execution)."""
        from contextlib import nullcontext
        from src.services.otel_tracing.mlflow_parent_setup import (
            configure_parent_mlflow_tracing,
            set_root_span_outputs,
        )

        mlflow_on = await configure_parent_mlflow_tracing(
            self.session, group_context, label="AgentGeneration"
        )
        if mlflow_on:
            from src.services.mlflow.tracing import start_root_trace
            trace_ctx = start_root_trace(
                "agent_generation",
                inputs={"prompt": prompt_text, "model": model or "default"},
            )
        else:
            trace_ctx = nullcontext()

        with trace_ctx as root_span:
            result = await self._generate_agent_impl(
                prompt_text,
                model=model,
                tools=tools,
                group_context=group_context,
                fast_planning=fast_planning,
                available_tools=available_tools,
            )
            set_root_span_outputs(root_span, result)
            return result

    async def _generate_agent_impl(self, prompt_text: str, model: str = None, tools: List[str] = None,
                            group_context: Optional[GroupContext] = None, fast_planning: bool = True,
                            available_tools: Optional[List[Dict]] = None) -> Dict[str, Any]:
        """
        Generate agent configuration from natural language description.

        This method processes a natural language description of an agent
        and returns a structured configuration that can be used with CrewAI.

        Args:
            prompt_text: Natural language description of the agent
            model: Model to use for generation, defaults to environment variable or "databricks-llama-4-maverick"
            tools: List of tools available to the agent (ignored — use available_tools)
            group_context: Optional group context for multi-group isolation
            available_tools: Optional list of dicts with 'name' and 'description' for tool selection

        Returns:
            Dict[str, Any]: Agent configuration in JSON format

        Raises:
            ValueError: If there's a problem with the configuration
            Exception: For any other errors during generation
        """
        # Default values
        model = model or os.getenv("AGENT_MODEL", "databricks-gpt-5-3-codex")
        tools = tools or []

        logger.info(f"Generating agent with model: {model}")

        try:
            # Get and prepare prompt template (composed with group/user overrides)
            system_message = await self._prepare_prompt_template(tools, group_context)

            # Tools are assigned at the task level, not the agent level.
            # Do not pass available tools to the agent generation prompt.

            # Documentation context disabled: skip vector search/embedding for agent generation
            documentation_context = None

            # Generate agent configuration without external documentation context
            agent_config = await self._generate_agent_config(
                prompt_text, system_message, model, documentation_context, 
                fast_planning=fast_planning, group_context=group_context
            )

            # Log the interaction
            try:
                await self._log_llm_interaction(
                    endpoint='generate-agent',
                    prompt=f"System: {system_message}\nUser: {prompt_text}",
                    response=json.dumps(agent_config),
                    model=model,
                    group_context=group_context
                )
            except Exception as e:
                # Just log the error, don't fail the request
                logger.error(f"Failed to log interaction: {str(e)}")

            return agent_config
            
        except Exception as e:
            logger.error(f"Error generating agent: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
    
    async def _prepare_prompt_template(self, tools: List[str], group_context: Optional[GroupContext]) -> str:
        """
        Prepare the prompt template (with group/user appended overrides).

        Args:
            tools: List of tools (ignored for generation)
            group_context: Current request's group context

        Returns:
            str: Complete system message

        Raises:
            ValueError: If prompt template is not found
        """
        # Get composed prompt template from database using the TemplateService
        system_message = await TemplateService.get_effective_template_content("generate_agent", group_context)

        if not system_message:
            raise ValueError("Required prompt template 'generate_agent' not found in database")

        return system_message
    
    async def _generate_agent_config(self, prompt_text: str, system_message: str, model: str,
                                     documentation_context: str = None, fast_planning: bool = False,
                                     group_context: Optional[GroupContext] = None) -> Dict[str, Any]:
        """
        Generate and process agent configuration.

        Args:
            prompt_text: Natural language description of the agent
            system_message: System message with template and tools context
            model: Model to use for generation
            documentation_context: Optional relevant documentation for enhanced generation
            group_context: Optional group context for authentication

        Returns:
            Dict[str, Any]: Processed agent configuration

        Raises:
            ValueError: If agent configuration is invalid
            Exception: For generation errors
        """
        # Prepare messages for LLM
        messages = [
            {"role": "system", "content": system_message}
        ]

        # (No documentation context injected)

        # Add the user's prompt
        messages.append({"role": "user", "content": prompt_text})
        
        # Generate completion via unified LLMManager.completion()
        try:
            from src.utils.telemetry import get_user_agent_header, KasalProduct
            content = await LLMManager.completion(
                messages=messages,
                model=model,
                temperature=0.2 if fast_planning else 0.7,
                max_tokens=1200 if fast_planning else 4000,
                extra_headers=get_user_agent_header(KasalProduct.AGENT_GENERATION)
            )

            # Parse content
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
                "max_rpm": 10,
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
                "max_rpm": 10,
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
        
        # Tools are assigned at the task level, not the agent level
        setup["tools"] = []
        
        return setup 