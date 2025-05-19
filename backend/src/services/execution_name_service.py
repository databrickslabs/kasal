"""
Service for generating execution names.

This module provides business logic for generating descriptive names
for executions based on agents and tasks configuration.
"""

import logging
import traceback


from src.schemas.execution import ExecutionNameGenerationRequest, ExecutionNameGenerationResponse
from src.services.template_service import TemplateService
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager

# Configure logging
logger = logging.getLogger(__name__)

class ExecutionNameService:
    """Service for execution name generation operations."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
    
    @classmethod
    def create(cls) -> 'ExecutionNameService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of ExecutionNameService with all required dependencies
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
    
    async def generate_execution_name(self, request: ExecutionNameGenerationRequest) -> ExecutionNameGenerationResponse:
        """
        Generate a descriptive name for an execution based on agents and tasks configuration.
        
        Args:
            request: Request containing agents and tasks configuration
            
        Returns:
            Response containing the generated name
        """
        try:
            # Get the template for name generation
            default_prompt = "Generate a 2-4 word descriptive name for this execution based on the agents and tasks."
            system_message = await TemplateService.get_template_content("generate_job_name", default_prompt)
            
            # Prepare the prompt
            prompt = f"""Based on the following configuration, generate a 2-4 word descriptive name:

Agents:
{request.agents_yaml}

Tasks:
{request.tasks_yaml}

Generate a short, memorable name that captures the essence of this execution."""
            
            # Prepare messages for LLM
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ]
            
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(request.model)
            
            # Generate completion
            import litellm
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.7,
                max_tokens=20
            )
            
            # Extract and clean the name
            name = response["choices"][0]["message"]["content"].strip()
            name = name.replace('"', '').replace("'", "")
            
            # Log the interaction
            try:
                await self._log_llm_interaction(
                    endpoint='generate-execution-name',
                    prompt=f"System: {system_message}\nUser: {prompt}",
                    response=name,
                    model=request.model
                )
            except Exception as e:
                # Just log the error, don't fail the request
                logger.error(f"Failed to log interaction: {str(e)}")
            
            return ExecutionNameGenerationResponse(name=name)
            
        except Exception as e:
            logger.error(f"Error generating execution name: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return a default name with timestamp
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return ExecutionNameGenerationResponse(name=f"Execution-{timestamp}") 