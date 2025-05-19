"""
Service for generating connections between agents and tasks.

This module provides business logic for analyzing agents and tasks
and determining optimal connections and dependencies.
"""

import json
import logging
import os
import traceback
from typing import Dict, Any, List, Optional
from datetime import datetime

import litellm

from src.utils.prompt_utils import robust_json_parser
from src.services.template_service import TemplateService
from src.schemas.connection import ConnectionRequest, ConnectionResponse
from src.core.llm_manager import LLMManager

# Configure logging
logger = logging.getLogger(__name__)

class ConnectionService:
    """Service for generating connections between agents and tasks."""
    
    def __init__(self):
        """Initialize the service."""
        pass
    
    async def _log_llm_interaction(self, endpoint: str, prompt: str, response: str, model: str, 
                                 status: str = 'success', error_message: str = None) -> None:
        """
        Log LLM interaction using standard Python logging.
        
        Args:
            endpoint: API endpoint that was called
            prompt: Input prompt text
            response: Response from the LLM
            model: Model used for generation
            status: Status of the interaction ('success' or 'error')
            error_message: Error message if status is 'error'
        """
        try:
            log_message = f"LLM Interaction - Endpoint: {endpoint}, Model: {model}, Status: {status}"
            if error_message:
                log_message += f", Error: {error_message}"
            logger.info(log_message)
            logger.debug(f"Prompt: {prompt}")
            logger.debug(f"Response: {response}")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    async def _format_agents_and_tasks(self, request: ConnectionRequest) -> tuple[str, str]:
        """
        Format agents and tasks information for the prompt.
        
        Args:
            request: Connection request containing agents and tasks
            
        Returns:
            Tuple of (formatted_agents_info, formatted_tasks_info)
        """
        # Format agents for the prompt
        agents_info = "AVAILABLE AGENTS:\n"
        for idx, agent in enumerate(request.agents, 1):
            agents_info += f"\n{idx}. Agent: {agent.name}\n"
            agents_info += f"   Role: {agent.role}\n"
            agents_info += f"   Goal: {agent.goal}\n"
            agents_info += f"   Background: {agent.backstory or 'Not provided'}\n"
            if agent.tools:
                agents_info += f"   Tools: {', '.join(agent.tools)}\n"

        # Format tasks for the prompt
        tasks_info = "TASKS TO ASSIGN:\n"
        for idx, task in enumerate(request.tasks, 1):
            tasks_info += f"\n{idx}. Task: {task.name}\n"
            tasks_info += f"   Description: {task.description}\n"
            if task.expected_output:
                tasks_info += f"   Expected Output: {task.expected_output}\n"
            if task.tools:
                tasks_info += f"   Required Tools: {', '.join(task.tools)}\n"
            if task.context:
                context = task.context
                tasks_info += f"   Type: {context.type}\n"
                tasks_info += f"   Priority: {context.priority}\n"
                tasks_info += f"   Complexity: {context.complexity}\n"
                if context.required_skills:
                    tasks_info += f"   Required Skills: {', '.join(context.required_skills)}\n"
        
        return agents_info, tasks_info
    
    async def _validate_response(self, response_data: Dict[str, Any], request: ConnectionRequest) -> None:
        """
        Validate the generated connections response.
        
        Args:
            response_data: Generated connections data
            request: Original connection request
            
        Raises:
            ValueError: If response is invalid or tasks are unassigned
        """
        # Check response structure
        if not isinstance(response_data, dict) or 'assignments' not in response_data or 'dependencies' not in response_data:
            raise ValueError("Invalid response structure from API")
        
        # Validate assignments
        assigned_tasks = set()
        for assignment in response_data['assignments']:
            if not isinstance(assignment, dict) or 'agent_name' not in assignment or 'tasks' not in assignment:
                raise ValueError("Invalid assignment structure in response")
            
            for task in assignment['tasks']:
                if not isinstance(task, dict) or 'task_name' not in task or 'reasoning' not in task:
                    raise ValueError("Invalid task structure in assignment")
                
                assigned_tasks.add(task['task_name'])
        
        # Check for unassigned tasks
        all_tasks = {task.name for task in request.tasks}
        unassigned_tasks = all_tasks - assigned_tasks
        
        if unassigned_tasks:
            error_detail = f"The AI model failed to assign the following tasks: {', '.join(unassigned_tasks)}. This could be due to:\n"
            error_detail += "1. The model couldn't determine which agent best fits these tasks based on their descriptions\n"
            error_detail += "2. The agent and task descriptions might not have clear compatibility\n\n"
            error_detail += "Suggestions:\n"
            error_detail += "- Try making agent descriptions more clearly aligned with tasks\n"
            error_detail += "- Add more specific details to task descriptions\n"
            error_detail += "- Try a different model"
            
            raise ValueError(error_detail)
    
    async def generate_connections(self, request: ConnectionRequest) -> ConnectionResponse:
        """
        Generate connections between agents and tasks.
        
        Args:
            request: Connection request containing agents and tasks
            
        Returns:
            ConnectionResponse with assignments and dependencies
            
        Raises:
            ValueError: If there's a problem with the generation
            Exception: For any other errors during generation
        """
        # Default model if not specified
        model = request.model or os.getenv("CONNECTION_MODEL", "gpt-4o-mini")
        
        logger.info(f"Generating connections with model: {model}")
        logger.info(f"Number of agents: {len(request.agents)}, Number of tasks: {len(request.tasks)}")
        
        try:
            # Get the prompt template
            system_message = await TemplateService.get_template_content("generate_connections")
            if not system_message:
                raise ValueError("Required prompt template 'generate_connections' not found")
            
            # Format the agents and tasks for the prompt
            agents_info, tasks_info = await self._format_agents_and_tasks(request)
            
            # Combine all information into the user message
            user_message = f"{agents_info}\n\n{tasks_info}"
            
            # Add extra instructions if provided
            if request.instructions:
                user_message += f"\n\nADDITIONAL INSTRUCTIONS:\n{request.instructions}"
            
            # Prepare messages for LLM
            messages = [
                {"role": "system", "content": system_message},
                {"role": "user", "content": user_message}
            ]
            
            # Configure litellm using the LLMManager
            model_params = await LLMManager.configure_litellm(model)
            
            try:
                # Generate completion with litellm directly
                response = await litellm.acompletion(
                    **model_params,
                    messages=messages,
                    temperature=0.7,
                    max_tokens=4000
                )
                
                # Extract content from response
                content = response["choices"][0]["message"]["content"]
                
                # Log the successful interaction
                await self._log_llm_interaction(
                    endpoint='generate-connections',
                    prompt=f"System: {system_message}\nUser: {user_message[:100]}...",
                    response=content,
                    model=model
                )
                
            except Exception as e:
                error_msg = f"Error generating completion: {str(e)}"
                logger.error(error_msg)
                
                # Log failed interaction
                await self._log_llm_interaction(
                    endpoint='generate-connections',
                    prompt=f"System: {system_message}\nUser: {user_message[:100]}...",
                    response=str(e),
                    model=model,
                    status='error',
                    error_message=error_msg
                )
                
                raise ValueError(f"Failed to generate connections: {str(e)}")
            
            # Parse and process the response
            try:
                # Extract JSON from the content if needed
                if '```json' in content:
                    # Extract JSON block if formatted as markdown code
                    content = content.split('```json')[1].split('```')[0].strip()
                elif '```' in content:
                    # Extract any code block if present
                    content = content.split('```')[1].split('```')[0].strip()
                
                # Parse the JSON content
                response_data = robust_json_parser(content)
                
                # Validate the response structure
                await self._validate_response(response_data, request)
                
                # Create ConnectionResponse object
                result = ConnectionResponse(
                    assignments=response_data['assignments'],
                    dependencies=response_data['dependencies'],
                    explanation=response_data.get('explanation', '')
                )
                
                return result
                
            except ValueError as e:
                error_msg = f"Error processing connection response: {str(e)}"
                logger.error(error_msg)
                
                # Log parsing error
                await self._log_llm_interaction(
                    endpoint='process-connections',
                    prompt=content[:100],
                    response=str(e),
                    model=model,
                    status='error',
                    error_message=error_msg
                )
                
                raise ValueError(error_msg)
                
        except Exception as e:
            logger.error(f"Error generating connections: {str(e)}")
            logger.error(traceback.format_exc())
            raise
    
    async def validate_api_key(self, api_key: str) -> tuple[bool, str]:
        """
        Validate an OpenAI API key by making a simple models list request.
        
        Args:
            api_key: OpenAI API key to validate
            
        Returns:
            Tuple of (is_valid, message)
        """
        if not api_key:
            return False, "No API key provided"
        
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        try:
            import aiohttp
            async with aiohttp.ClientSession() as session:
                # Just test listing models which is a lightweight call
                async with session.get("https://api.openai.com/v1/models", headers=headers) as response:
                    if response.status == 200:
                        return True, "API key is valid"
                    else:
                        error_text = await response.text()
                        return False, f"API key validation failed: {response.status} - {error_text}"
        except Exception as e:
            return False, f"API key validation error: {str(e)}"
    
    async def test_api_keys(self) -> Dict[str, Any]:
        """
        Test API keys and configurations.
        
        Returns:
            Dictionary with test results for each provider
        """
        results = {}
        
        # Test OpenAI API key
        openai_key = os.environ.get('OPENAI_API_KEY')
        if openai_key:
            valid, message = await self.validate_api_key(openai_key)
            results["openai"] = {
                "has_key": True,
                "valid": valid,
                "message": message,
                "key_prefix": openai_key[:4] + "..." if openai_key else "None"
            }
        else:
            results["openai"] = {
                "has_key": False,
                "valid": False,
                "message": "No API key found in environment variables"
            }
        
        # Test Anthropic API key (simple presence check)
        anthropic_key = os.environ.get('ANTHROPIC_API_KEY')
        results["anthropic"] = {
            "has_key": bool(anthropic_key),
            "key_prefix": anthropic_key[:4] + "..." if anthropic_key else "None"
        }
        
        # Test DeepSeek API key (simple presence check)
        deepseek_key = os.environ.get('DEEPSEEK_API_KEY')
        results["deepseek"] = {
            "has_key": bool(deepseek_key),
            "key_prefix": deepseek_key[:4] + "..." if deepseek_key else "None"
        }
        
        # Include Python version info
        import sys
        results["python_info"] = {
            "version": sys.version,
            "executable": sys.executable,
            "platform": sys.platform
        }
            
        return results 