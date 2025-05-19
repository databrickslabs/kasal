"""
Service for template generation operations.

This module provides business logic for generating agent templates
using LLM models and prompt templates.
"""

import logging
import json
import traceback

from typing import Optional

import litellm

from src.utils.model_config import get_model_config
from src.services.template_service import TemplateService
from src.schemas.template_generation import TemplateGenerationRequest, TemplateGenerationResponse
from src.services.log_service import LLMLogService
from src.utils.prompt_utils import robust_json_parser
from src.core.llm_manager import LLMManager

# Configure logging
logger = logging.getLogger(__name__)

class TemplateGenerationService:
    """Service for template generation operations."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
    
    @classmethod
    def create(cls) -> 'TemplateGenerationService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of TemplateGenerationService with all required dependencies
        """
        log_service = LLMLogService.create()
        return cls(log_service=log_service)
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str, 
                                  status: str = 'success', error_message: Optional[str] = None) -> None:
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
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def generate_templates(self, request: TemplateGenerationRequest) -> TemplateGenerationResponse:
        """
        Generate templates for an agent based on role, goal, and backstory.
        
        Args:
            request: Template generation request with role, goal, backstory, and model
            
        Returns:
            TemplateGenerationResponse with system, prompt, and response templates
            
        Raises:
            ValueError: If required prompt template is not found
            ValueError: If response is missing required fields
            Exception: For other errors
        """
        try:
            # Get model config
            model_config = get_model_config(request.model)
            logger.info(f"Using model for template generation: {model_config['name']}")
            
            # Get prompt template from database
            system_message = await TemplateService.get_template_content("generate_templates")
            
            # Check if we have a prompt template
            if not system_message:
                logger.error("No prompt template found in database for generate_templates")
                raise ValueError("Required prompt template 'generate_templates' not found in database")
            
            logger.info("Using prompt template for generate_templates from database")
            
            # Create the user prompt with agent details
            user_prompt = f"""Create templates for an AI agent with:
            Role: {request.role}
            Goal: {request.goal}
            Backstory: {request.backstory}
            
            Generate all three templates following CrewAI and LangChain best practices."""
            
            # Prepare messages for LLM
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_prompt}
            ]
            
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(model_config["name"])
            
            try:
                # Generate completion with litellm directly
                response = await litellm.acompletion(
                    **model_params,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=4000
                )
                
                content = response["choices"][0]["message"]["content"]
                logger.info(f"Generated templates successfully")
                logger.debug(f"Generated templates: {content}")
                
                # Log the successful interaction
                await self._log_llm_interaction(
                    endpoint='generate-templates',
                    prompt=f"System: {system_message}\nUser: {user_prompt}",
                    response=content,
                    model=model_config["name"]
                )
            except Exception as e:
                error_msg = f"Error generating completion: {str(e)}"
                logger.error(error_msg)
                
                # Log the error interaction
                await self._log_llm_interaction(
                    endpoint='generate-templates',
                    prompt=f"System: {system_message}\nUser: {user_prompt}",
                    response=str(e),
                    model=model_config["name"],
                    status='error',
                    error_message=error_msg
                )
                
                raise ValueError(f"Failed to generate templates: {str(e)}")
            
            # Parse the response as JSON using robust parser
            templates = robust_json_parser(content)
            
            # Normalize the field names to lowercase if needed
            normalized_templates = {
                "system_template": templates.get("system_template") or templates.get("System Template") or templates.get("System_Template"),
                "prompt_template": templates.get("prompt_template") or templates.get("Prompt Template") or templates.get("Prompt_Template"),
                "response_template": templates.get("response_template") or templates.get("Response Template") or templates.get("Response_Template")
            }
            
            # Validate that all required fields are present and non-empty
            for field, value in normalized_templates.items():
                if not value:
                    raise ValueError(f"Missing or empty required field: {field}")
            
            # Create response object from normalized templates
            response = TemplateGenerationResponse(**normalized_templates)
            return response
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse AI response as JSON: {str(e)}")
            raise ValueError("Failed to parse AI response as JSON")
        except Exception as e:
            logger.error(f"Error generating templates: {str(e)}")
            raise 