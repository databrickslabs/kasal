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

    def __init__(self, log_service: LLMLogService, template_service):
        """
        Initialize the service.

        Args:
            log_service: Service for logging LLM interactions
            template_service: Service for template operations
        """
        self.log_service = log_service
        self.template_service = template_service

    @classmethod
    def create(cls, session) -> 'ExecutionNameService':
        """
        Factory method to create a properly configured instance of the service.

        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.

        Args:
            session: Database session for repository operations

        Returns:
            An instance of ExecutionNameService with all required dependencies
        """
        from src.services.template_service import TemplateService

        log_service = LLMLogService.create(session)
        template_service = TemplateService(session)
        return cls(log_service=log_service, template_service=template_service)
    
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
            # This template already includes instructions to only return the name without explanations
            system_message = await self.template_service.get_template_content("generate_job_name")

            # Fallback if template is not found (shouldn't happen if seeds are run)
            if not system_message:
                system_message = """Generate a concise, descriptive name (2-4 words) for an AI job run based on the agents and tasks involved.
Only return the name, no explanations or additional text."""
                logger.warning("generate_job_name template not found, using fallback")

            # Prepare the prompt with just the data
            prompt = f"""Agents:
{request.agents_yaml}

Tasks:
{request.tasks_yaml}"""

            # Prepare messages for LLM
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt}
            ]
            
            # Configure litellm using the LLMManager
            # LLMManager now handles authentication internally (OBO → PAT → SPN)
            model_params = await LLMManager.configure_litellm(request.model)

            # Generate completion
            # Note: Some models (like Gemini) may use reasoning_tokens internally before generating output.
            # We set max_tokens=100 to safely accommodate both reasoning and completion tokens,
            # ensuring we can generate a full 2-4 word name without hitting token limits.
            # For models without reasoning tokens, we'll truncate to ensure concise names.
            import litellm
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.7,
                max_tokens=100  # Increased to prevent truncation of 2-4 word names
            )

            # Extract and clean the name
            name = response["choices"][0]["message"]["content"].strip()
            name = name.replace('"', '').replace("'", "")

            # Check if the model used reasoning tokens (e.g., Gemini models)
            usage = response.get('usage', {})
            reasoning_tokens = usage.get('reasoning_tokens', 0)

            if reasoning_tokens == 0:
                # Model didn't use reasoning tokens, so we should ensure the name is concise
                # Truncate to first 4 words if longer (2-4 word requirement)
                words = name.split()
                if len(words) > 4:
                    name = " ".join(words[:4])
                    logger.info(f"Truncated name to 4 words (no reasoning tokens used): '{name}'")
            else:
                logger.info(f"Model used {reasoning_tokens} reasoning tokens, keeping full response: '{name}'")
            
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