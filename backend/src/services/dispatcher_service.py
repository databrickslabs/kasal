"""
Service for dispatching natural language requests to appropriate generation services.

This module provides business logic for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

import logging
import os
from typing import Dict, Any, Optional, List
import litellm

from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
from src.services.agent_generation_service import AgentGenerationService
from src.services.task_generation_service import TaskGenerationService
from src.services.crew_generation_service import CrewGenerationService
from src.services.template_service import TemplateService
from src.services.log_service import LLMLogService
from src.core.llm_manager import LLMManager
from src.utils.prompt_utils import robust_json_parser

# Configure logging
logger = logging.getLogger(__name__)

# Default model for intent detection
DEFAULT_DISPATCHER_MODEL = os.getenv("DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick")


class DispatcherService:
    """Service for dispatching natural language requests to generation services."""
    
    def __init__(self, log_service: LLMLogService):
        """
        Initialize the service.
        
        Args:
            log_service: Service for logging LLM interactions
        """
        self.log_service = log_service
        self.agent_service = AgentGenerationService.create()
        self.task_service = TaskGenerationService.create()
        self.crew_service = CrewGenerationService.create()
    
    @classmethod
    def create(cls) -> 'DispatcherService':
        """
        Factory method to create a properly configured instance of the service.
        
        Returns:
            An instance of DispatcherService with all required dependencies
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
    
    async def _detect_intent(self, message: str, model: str) -> Dict[str, Any]:
        """
        Detect the intent from the user's message using LLM.
        
        Args:
            message: User's natural language message
            model: LLM model to use
            
        Returns:
            Dictionary containing intent, confidence, and extracted information
        """
        # Get prompt template from database
        system_prompt = await TemplateService.get_template_content("detect_intent")
        
        if not system_prompt:
            # Use a default prompt if template not found
            system_prompt = """You are an intent detection system for a CrewAI workflow designer.
            
Analyze the user's message and determine their intent:
- generate_agent: User wants to create a single agent with specific capabilities
- generate_task: User wants to create a single task with specific requirements
- generate_crew: User wants to create multiple agents and/or tasks working together

Return a JSON object with:
{
    "intent": "generate_agent" | "generate_task" | "generate_crew" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        // Any relevant information extracted from the message
    },
    "suggested_prompt": "Enhanced version of the user's message for the specific service"
}

Examples:
- "Create an agent that can analyze data" -> generate_agent
- "I need a task to summarize documents" -> generate_task
- "Build a team of agents to handle customer support" -> generate_crew
- "Create a research agent and a writer agent with tasks for each" -> generate_crew
"""
        
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": message}
        ]
        
        try:
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(model)
            
            # Generate completion
            response = await litellm.acompletion(
                **model_params,
                messages=messages,
                temperature=0.3,  # Lower temperature for more consistent intent detection
                max_tokens=1000
            )
            
            content = response["choices"][0]["message"]["content"]
            
            # Parse the response
            result = robust_json_parser(content)
            
            # Validate the response
            if "intent" not in result:
                result["intent"] = "unknown"
            if "confidence" not in result:
                result["confidence"] = 0.5
            if "extracted_info" not in result:
                result["extracted_info"] = {}
            if "suggested_prompt" not in result:
                result["suggested_prompt"] = message
                
            return result
            
        except Exception as e:
            logger.error(f"Error detecting intent: {str(e)}")
            return {
                "intent": "unknown",
                "confidence": 0.0,
                "extracted_info": {},
                "suggested_prompt": message
            }
    
    async def dispatch(self, request: DispatcherRequest) -> Dict[str, Any]:
        """
        Dispatch the user's request to the appropriate generation service.
        
        Args:
            request: Dispatcher request with user message and options
            
        Returns:
            Dictionary containing the intent detection result and generation response
        """
        model = request.model or DEFAULT_DISPATCHER_MODEL
        
        # Detect intent
        intent_result = await self._detect_intent(request.message, model)
        
        # Log the intent detection
        await self._log_llm_interaction(
            endpoint='detect-intent',
            prompt=request.message,
            response=str(intent_result),
            model=model
        )
        
        # Create dispatcher response
        dispatcher_response = DispatcherResponse(
            intent=IntentType(intent_result["intent"]),
            confidence=intent_result["confidence"],
            extracted_info=intent_result["extracted_info"],
            suggested_prompt=intent_result["suggested_prompt"]
        )
        
        # Dispatch to appropriate service based on intent
        generation_result = None
        
        try:
            if dispatcher_response.intent == IntentType.GENERATE_AGENT:
                # Call agent generation service directly
                generation_result = await self.agent_service.generate_agent(
                    prompt_text=dispatcher_response.suggested_prompt or request.message,
                    model=request.model,
                    tools=request.tools
                )
                
            elif dispatcher_response.intent == IntentType.GENERATE_TASK:
                # Call task generation service
                task_request = TaskGenerationRequest(
                    text=dispatcher_response.suggested_prompt or request.message,
                    model=request.model
                )
                generation_result = await self.task_service.generate_task(task_request)
                
            elif dispatcher_response.intent == IntentType.GENERATE_CREW:
                # Call crew generation service
                crew_request = CrewGenerationRequest(
                    prompt=dispatcher_response.suggested_prompt or request.message,
                    model=request.model,
                    tools=request.tools
                )
                generation_result = await self.crew_service.create_crew_complete(crew_request)
                
            else:
                # Unknown intent
                logger.warning(f"Unknown intent detected: {dispatcher_response.intent}")
                
        except Exception as e:
            logger.error(f"Error in generation service: {str(e)}")
            await self._log_llm_interaction(
                endpoint=f'dispatch-{dispatcher_response.intent}',
                prompt=request.message,
                response=str(e),
                model=model,
                status='error',
                error_message=str(e)
            )
            raise
        
        # Return combined response
        return {
            "dispatcher": dispatcher_response.model_dump(),
            "generation_result": generation_result,
            "service_called": dispatcher_response.intent.value if dispatcher_response.intent != IntentType.UNKNOWN else None
        } 