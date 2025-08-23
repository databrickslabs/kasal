"""
Crew preparation module for CrewAI engine.

This module handles the preparation and configuration of CrewAI agents and tasks.
"""

from typing import Dict, Any, List, Optional
import logging
from datetime import datetime
from crewai import Agent, Crew, Task
from src.core.logger import LoggerManager
from src.engines.crewai.helpers.task_helpers import create_task, is_data_missing
from src.engines.crewai.helpers.agent_helpers import create_agent
from src.schemas.memory_backend import MemoryBackendConfig, MemoryBackendType
from src.engines.crewai.memory.memory_backend_factory import MemoryBackendFactory
from src.utils.databricks_url_utils import DatabricksURLUtils


logger = LoggerManager.get_instance().crew

def validate_crew_config(config: Dict[str, Any]) -> bool:
    """
    Validate crew configuration
    
    Args:
        config: Crew configuration dictionary
        
    Returns:
        True if configuration is valid
    """
    # Simple validation - check required sections
    required_sections = ['agents', 'tasks']
    for section in required_sections:
        if section not in config or not config[section]:
            logger.error(f"Missing or empty required section: {section}")
            return False
    
    return True
    
def handle_crew_error(e: Exception, message: str) -> None:
    """
    Handle crew-related errors
    
    Args:
        e: Exception that occurred
        message: Base error message
    """
    error_msg = f"{message}: {str(e)}"
    logger.error(error_msg, exc_info=True)

async def process_crew_output(result: Any) -> Dict[str, Any]:
    """
    Process crew execution output
    
    Args:
        result: Raw output from crew execution
        
    Returns:
        Processed output dictionary
    """
    try:
        if isinstance(result, dict):
            return result
        elif hasattr(result, 'raw'):
            # CrewAI result object with raw attribute
            return {"result": result.raw, "type": "crew_result"}
        else:
            # Convert any other result to string
            return {"result": str(result), "type": "processed"}
    except Exception as e:
        logger.error(f"Error processing crew output: {e}")
        return {"error": f"Failed to process output: {str(e)}"}

class CrewPreparation:
    """Handles the preparation of CrewAI agents and tasks"""
    
    def __init__(self, config: Dict[str, Any], tool_service=None, tool_factory=None, user_token: Optional[str] = None):
        """
        Initialize the CrewPreparation class
        
        Args:
            config: Configuration dictionary containing crew setup
            tool_service: Tool service instance for resolving tool IDs
            tool_factory: Tool factory for creating tool instances
            user_token: Optional user access token for OBO authentication
        """
        self.config = config
        self.agents: Dict[str, Agent] = {}
        self.tasks: List[Task] = []
        self.crew: Optional[Crew] = None
        self.tool_service = tool_service
        self.tool_factory = tool_factory
        self.user_token = user_token  # Store user token for OBO auth
        self._original_storage_dir = None  # To store original CREWAI_STORAGE_DIR
        
        # Log the configuration to debug memory backend
        logger.info(f"[CrewPreparation.__init__] Config keys: {list(config.keys())}")
        if 'memory_backend_config' in config:
            logger.info(f"[CrewPreparation.__init__] Memory backend config found: {config['memory_backend_config']}")
    
    def _needs_entity_extraction_fallback(self, model_name: str) -> bool:
        """
        Check if a model needs fallback for entity extraction.
        
        Args:
            model_name: The model name to check
            
        Returns:
            True if the model needs fallback for entity extraction
        """
        if not model_name:
            return False
        
        model_lower = str(model_name).lower()
        
        # Models that have issues with entity extraction via Instructor/tool calling
        problematic_patterns = [
            'databricks-claude',     # Databricks Claude has strict JSON schema requirements
            'gpt-oss',              # GPT-OSS returns empty responses for complex schemas
            'databricks-gpt-oss',   # Full name for GPT-OSS
        ]
        
        for pattern in problematic_patterns:
            if pattern in model_lower:
                logger.info(f"Model {model_name} identified as needing entity extraction fallback")
                return True
        
        return False
    
    def _should_disable_memory_for_agent(self, agent_config: Dict[str, Any]) -> bool:
        """
        Check if memory is explicitly disabled for an agent.
        
        Args:
            agent_config: Agent configuration dictionary
            
        Returns:
            True if memory is explicitly set to False in the agent config
        """
        # Only check if memory is explicitly disabled in agent config
        if 'memory' in agent_config and agent_config['memory'] is False:
            logger.info(f"Memory explicitly disabled for agent {agent_config.get('name', agent_config.get('role', 'Unknown'))}")
            return True
        return False
    
    async def _apply_entity_extraction_fallback_patch(self):
        """
        Apply runtime patch to CrewAI's Converter to use fallback model for entity extraction.
        This is needed because EntityMemory doesn't accept a custom LLM parameter.
        """
        try:
            from crewai.utilities.converter import Converter
            from crewai.utilities.evaluators.task_evaluator import TaskEvaluator
            from crewai.llm import LLM
            from src.core.llm_manager import LLMManager
            
            # Store the original methods
            if not hasattr(Converter, '_original_to_pydantic'):
                Converter._original_to_pydantic = Converter.to_pydantic
            if not hasattr(TaskEvaluator, '_original_evaluate'):
                TaskEvaluator._original_evaluate = TaskEvaluator.evaluate
            
            # Create the fallback LLM instance
            fallback_llm = await LLMManager.configure_crewai_llm("databricks-llama-4-maverick")
            
            # Ensure fallback LLM has all necessary attributes
            if not hasattr(fallback_llm, 'function_calling_llm'):
                fallback_llm.function_calling_llm = fallback_llm
            
            def patched_to_pydantic(self, current_attempt=1):
                """Patched version that uses fallback LLM for problematic models."""
                # Always use fallback for problematic models - simpler and more reliable
                if hasattr(self, 'llm') and hasattr(self.llm, 'model'):
                    model_name = str(self.llm.model).lower()
                    # Check if this is a problematic model
                    if 'databricks-claude' in model_name or 'gpt-oss' in model_name:
                        # Store original LLM and its attributes
                        original_llm = self.llm
                        original_func_calling = getattr(self.llm, 'function_calling_llm', None)
                        
                        # Use fallback and ensure it has function_calling_llm
                        self.llm = fallback_llm
                        if not hasattr(self.llm, 'function_calling_llm'):
                            self.llm.function_calling_llm = fallback_llm
                        
                        logger.info(f"Using databricks-llama-4-maverick fallback for {model_name}")
                        try:
                            result = Converter._original_to_pydantic(self, current_attempt)
                            return result
                        finally:
                            # Restore original LLM and attributes
                            self.llm = original_llm
                            if original_func_calling is not None:
                                self.llm.function_calling_llm = original_func_calling
                
                # Use original method for non-problematic models
                return Converter._original_to_pydantic(self, current_attempt)
            
            def patched_evaluate(self, task, output):
                """Patched TaskEvaluator that uses fallback LLM for problematic models."""
                # Check if the original agent has a problematic LLM
                if hasattr(self, 'original_agent') and hasattr(self.original_agent, 'llm'):
                    model_name = str(self.original_agent.llm.model).lower() if hasattr(self.original_agent.llm, 'model') else ""
                    if 'databricks-claude' in model_name or 'gpt-oss' in model_name:
                        # Temporarily replace the LLM with fallback
                        original_llm = self.llm
                        self.llm = fallback_llm
                        logger.info(f"Using fallback LLM for TaskEvaluator with {model_name}")
                        try:
                            return TaskEvaluator._original_evaluate(self, task, output)
                        finally:
                            self.llm = original_llm
                
                # Use original method for non-problematic models
                return TaskEvaluator._original_evaluate(self, task, output)
            
            # Apply the patches
            Converter.to_pydantic = patched_to_pydantic
            TaskEvaluator.evaluate = patched_evaluate
            logger.info("Applied entity extraction and task evaluation fallback patches")
            
        except Exception as e:
            logger.error(f"Failed to apply entity extraction fallback patch: {e}")
            # Continue without patch - will fail on entity extraction but rest will work
        
    async def prepare(self) -> bool:
        """
        Prepare the crew by creating agents and tasks
        
        Returns:
            bool: True if preparation was successful
        """
        try:
            # Validate configuration
            if not validate_crew_config(self.config):
                logger.error("Invalid crew configuration")
                return False
            
            # Create agents
            if not await self._create_agents():
                logger.error("Failed to create agents")
                return False
            
            # Create tasks
            if not await self._create_tasks():
                logger.error("Failed to create tasks")
                return False
            
            # Create crew
            if not await self._create_crew():
                logger.error("Failed to create crew")
                return False
            
            logger.info("Crew preparation completed successfully")
            return True
            
        except Exception as e:
            handle_crew_error(e, "Error during crew preparation")
            return False
    
    async def _create_agents(self) -> bool:
        """
        Create all agents defined in the configuration
        
        Returns:
            bool: True if all agents were created successfully
        """
        try:
            for i, agent_config in enumerate(self.config.get('agents', [])):
                # Use the agent's 'name' if present, then 'id', then 'role', or generate a name if none exist
                agent_name = agent_config.get('name', agent_config.get('id', agent_config.get('role', f'agent_{i}')))
                
                agent = await create_agent(
                    agent_key=agent_name,
                    agent_config=agent_config,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory
                )
                if not agent:
                    logger.error(f"Failed to create agent: {agent_name}")
                    return False
                    
                # Store the agent with the agent_name as key
                self.agents[agent_name] = agent
                logger.info(f"Created agent: {agent_name}")
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating agents")
            return False
    
    async def _create_tasks(self) -> bool:
        """
        Create all tasks defined in the configuration
        
        Returns:
            bool: True if all tasks were created successfully
        """
        try:
            from src.engines.crewai.helpers.task_helpers import create_task
            
            tasks = self.config.get('tasks', [])
            total_tasks = len(tasks)
            
            # Create a dictionary to store tasks by ID for reference
            task_dict = {}
            
            # First pass: create all tasks without setting context
            for i, task_config in enumerate(tasks):
                # Get the agent for this task, default to first agent if not specified
                agent_name = task_config.get('agent', 'unknown')
                agent = self.agents.get(agent_name)
                
                # Handle missing agent
                if not agent:
                    if not self.agents:
                        logger.error("No agents available for tasks")
                        return False
                    
                    # Use the first available agent as fallback
                    fallback_agent_name, agent = next(iter(self.agents.items()))
                    logger.warning(f"Invalid agent '{agent_name}' specified for task. Using '{fallback_agent_name}' instead.")
                
                # Define task_name first so it can be used in logging
                task_name = task_config.get('name', f"task_{len(self.tasks)}")
                task_id = task_config.get('id', task_name)
                
                # Store any context IDs for second pass resolution (only if multiple tasks)
                if len(tasks) > 1 and "context" in task_config:
                    context_value = task_config.pop("context")
                    # Only process non-empty context values
                    if context_value:  # Skip empty lists, empty strings, etc.
                        logger.info(f"Saved context references for task {task_name}: {context_value}")
                        # Store the references for resolution in second pass
                        if isinstance(context_value, list) and context_value:
                            task_config['_context_refs'] = [str(item) for item in context_value]
                        elif isinstance(context_value, str) and context_value.strip():
                            task_config['_context_refs'] = [context_value]
                        elif isinstance(context_value, dict) and "task_ids" in context_value and context_value["task_ids"]:
                            task_config['_context_refs'] = context_value["task_ids"]
                elif "context" in task_config:
                    # Remove context from single-task configurations to avoid issues
                    task_config.pop("context")
                
                # Get the async execution setting
                is_async = task_config.get('async_execution', False)
                
                # Store original setting for later adjustment
                if is_async:
                    # Save that this task wanted to be async for logging
                    task_config['_wanted_async'] = True
                    if i < total_tasks - 1:
                        # Only the last task can be async in CrewAI, force others to be sync
                        logger.warning(f"Task '{task_name}' was set to async but isn't the last task. Only the last task can be async in CrewAI. Setting to synchronous.")
                        task_config['async_execution'] = False
                        is_async = False
                
                logger.info(f"Task '{task_name}' async_execution setting: {is_async}")
                    
                # Create the task
                task = await create_task(
                    task_key=task_name,
                    task_config=task_config,
                    agent=agent,
                    output_dir=self.config.get('output_dir'),
                    config=None,
                    tool_service=self.tool_service,
                    tool_factory=self.tool_factory
                )
                
                self.tasks.append(task)
                # Store in our dictionary for context resolution
                task_dict[task_id] = task
                logger.info(f"Created task: {task_name} for agent: {agent_name}")
                
            # Second pass: Resolve context references to actual Task objects
            for task_config in tasks:
                task_id = task_config.get('id', task_config.get('name'))
                task = task_dict.get(task_id)
                
                if not task:
                    logger.warning(f"Could not find task for ID {task_id} during context resolution")
                    continue
                    
                # If this task has context references, resolve them
                if '_context_refs' in task_config:
                    context_refs = task_config['_context_refs']
                    context_tasks = []
                    
                    for ref in context_refs:
                        if ref in task_dict:
                            context_tasks.append(task_dict[ref])
                        else:
                            logger.warning(f"Could not resolve context reference '{ref}' for task {task_id}")
                    
                    if context_tasks:
                        logger.info(f"Setting context for task {task_id} to {len(context_tasks)} Task objects")
                        task.context = context_tasks
                    else:
                        logger.warning(f"No context tasks could be resolved for task {task_id}")
            
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating tasks")
            return False
    
    async def _create_crew(self) -> bool:
        """
        Create the crew with all prepared agents and tasks
        
        Returns:
            bool: True if crew was created successfully
        """
        try:
            # Get crew configuration
            crew_config = self.config.get('crew', {})
            
            # Create the crew directly instead of calling an external function
            crew_kwargs = {
                'agents': list(self.agents.values()),
                'tasks': self.tasks,
                'process': crew_config.get('process', 'sequential'),
                'verbose': True,
                'memory': crew_config.get('memory', True)
            }
            
            # Set default LLM for crew manager using the submitted model
            try:
                import os
                from src.utils.databricks_auth import is_databricks_apps_environment
                from src.core.llm_manager import LLMManager
                
                # In Databricks Apps environment, handle OpenAI API key properly
                if is_databricks_apps_environment():
                    logger.info("Databricks Apps environment detected - skipping API key requirement")
                    # Note: OpenAI API key handling is done later before crew creation
                
                # Get the model from the execution config or crew config
                requested_model = self.config.get('model') or crew_config.get('model')
                
                # Only set manager_llm if not already specified in crew_config
                if 'manager_llm' not in crew_config:
                    if requested_model:
                        logger.info(f"Using submitted model for crew manager: {requested_model}")
                        try:
                            manager_llm = await LLMManager.get_llm(requested_model)
                            crew_kwargs['manager_llm'] = manager_llm
                            logger.info(f"Set crew manager LLM to: {requested_model}")
                        except Exception as llm_error:
                            logger.warning(f"Could not create LLM for model {requested_model}: {llm_error}")
                            # Fall back to environment-appropriate default
                            if is_databricks_apps_environment():
                                logger.info("Falling back to Databricks model in Apps environment")
                                fallback_llm = await LLMManager.get_llm("databricks-llama-4-maverick")
                                crew_kwargs['manager_llm'] = fallback_llm
                    elif is_databricks_apps_environment():
                        logger.info("No model specified - using Databricks default in Apps environment")
                        default_llm = await LLMManager.get_llm("databricks-llama-4-maverick")
                        crew_kwargs['manager_llm'] = default_llm
                    else:
                        logger.info("No model specified - will use CrewAI defaults")
                        
            except ImportError:
                logger.warning("Enhanced Databricks auth not available for crew preparation")
            except Exception as e:
                logger.warning(f"Error configuring crew manager LLM: {e}")
                # Continue without setting manager_llm - CrewAI will handle defaults
            
            # Configure embedder for memory using CrewAI's native configuration
            embedder_config = None
            for agent_config in self.config.get('agents', []):
                if 'embedder_config' in agent_config and agent_config['embedder_config']:
                    # Validate the embedder config has required fields
                    ec = agent_config['embedder_config']
                    if isinstance(ec, dict) and 'provider' in ec:
                        embedder_config = ec
                        logger.info(f"Found valid embedder configuration: {embedder_config}")
                        break
                    else:
                        logger.warning(f"Found invalid embedder config (missing provider): {ec}")
            
            # Always default to Databricks if no valid embedder config found
            # This ensures Databricks is used unless explicitly configured otherwise
            if not embedder_config:
                embedder_config = {
                    'provider': 'databricks',
                    'config': {'model': 'databricks-gte-large-en'}
                }
                logger.info("No valid embedder config found, using default Databricks configuration")
            
            # Use CrewAI's native embedder configuration
            if embedder_config:
                provider = embedder_config.get('provider', 'openai')
                config = embedder_config.get('config', {})
                
                if provider == 'databricks':
                    # For Databricks, create a custom embedding function using enhanced auth
                    try:
                        from src.utils.databricks_auth import is_databricks_apps_environment, get_databricks_auth_headers
                        from src.services.api_keys_service import ApiKeysService
                        
                        # Use enhanced Databricks authentication
                        databricks_key = None
                        auth_headers = None
                        
                        if is_databricks_apps_environment():
                            logger.info("Using Databricks Apps OAuth for embeddings in crew")
                            logger.info(f"User token available: {bool(self.user_token)}, token length: {len(self.user_token) if self.user_token else 0}")
                            # Get OAuth headers for embeddings - pass user_token for OBO authentication
                            auth_headers, error = await get_databricks_auth_headers(user_token=self.user_token)
                            if error:
                                logger.error(f"Failed to get OAuth headers for embeddings: {error}")
                                logger.error(f"User token was: {'provided' if self.user_token else 'None/empty'}")
                        else:
                            logger.info("Using enhanced Databricks auth for embeddings in local environment")
                            # Use enhanced auth system for local development too - pass user_token if available
                            auth_headers, error = await get_databricks_auth_headers(user_token=self.user_token)
                            if error:
                                logger.warning(f"Enhanced auth failed, falling back to API key: {error}")
                                # Fallback to API key if enhanced auth fails
                                databricks_key = await ApiKeysService.get_provider_api_key("DATABRICKS")
                        
                        if databricks_key or auth_headers:
                            import os
                            from chromadb import EmbeddingFunction, Documents, Embeddings
                            import litellm
                            from typing import cast
                            
                            # Get Databricks endpoint - prioritize environment variable
                            databricks_endpoint = os.getenv('DATABRICKS_HOST', '')
                            
                            # Use centralized URL utility to normalize the workspace URL
                            if databricks_endpoint:
                                databricks_endpoint = DatabricksURLUtils.normalize_workspace_url(databricks_endpoint)
                                if databricks_endpoint:
                                    logger.info(f"Using normalized Databricks endpoint: {databricks_endpoint}")
                            
                            if not databricks_endpoint:
                                # Try DATABRICKS_ENDPOINT environment variable
                                databricks_endpoint = os.getenv('DATABRICKS_ENDPOINT', '')
                                if databricks_endpoint:
                                    # Extract workspace URL from full endpoint if needed
                                    databricks_endpoint = DatabricksURLUtils.extract_workspace_from_endpoint(databricks_endpoint)
                            
                            # If no endpoint from environment, get from database
                            if not databricks_endpoint:
                                try:
                                    from src.services.databricks_service import DatabricksService
                                    from src.core.unit_of_work import UnitOfWork
                                    async with UnitOfWork() as uow:
                                        databricks_service = await DatabricksService.from_unit_of_work(uow)
                                        db_config = await databricks_service.get_databricks_config()
                                        if db_config and db_config.workspace_url:
                                            # Normalize the workspace URL from database
                                            databricks_endpoint = DatabricksURLUtils.normalize_workspace_url(db_config.workspace_url)
                                            if databricks_endpoint:
                                                logger.info(f"Using Databricks workspace URL from database: {databricks_endpoint}")
                                except Exception as e:
                                    logger.warning(f"Could not get Databricks workspace URL from database: {e}")
                            
                            model_name = config.get('model', 'databricks-gte-large-en')
                            
                            # Store the user token from the outer scope for use in the embedder
                            crew_user_token = self.user_token
                            
                            # Create custom embedding function for Databricks
                            class DatabricksEmbeddingFunction(EmbeddingFunction):
                                def __init__(self, api_key: str = None, api_base: str = None, model: str = None, auth_headers: dict = None, user_token: str = None):
                                    self.api_key = api_key
                                    self.api_base = api_base 
                                    # Store the original model name without prefix manipulation
                                    self.model = model
                                    self.auth_headers = auth_headers
                                    self.user_token = user_token  # Store user token for dynamic auth
                                
                                def __call__(self, input: Documents) -> Embeddings:
                                    try:
                                        # Always use direct HTTP request to avoid litellm/SDK authentication issues
                                        import requests
                                        
                                        # Construct the correct endpoint URL using centralized utility
                                        # Extract workspace URL from api_base then build full invocation URL
                                        workspace_url = DatabricksURLUtils.extract_workspace_from_endpoint(self.api_base)
                                        endpoint_url = DatabricksURLUtils.construct_model_invocation_url(workspace_url, self.model)
                                        if not endpoint_url:
                                            raise Exception("Failed to construct valid endpoint URL")
                                        logger.debug(f"Databricks embedding endpoint URL: {endpoint_url}")
                                        payload = {"input": input if isinstance(input, list) else [input]}
                                        
                                        # Prepare headers - prioritize user token for OBO auth
                                        if self.user_token:
                                            # Use OBO token directly for Databricks Apps
                                            headers = {
                                                "Authorization": f"Bearer {self.user_token}",
                                                "Content-Type": "application/json"
                                            }
                                            logger.debug("Using OBO token for embeddings")
                                        elif self.auth_headers:
                                            # Use pre-fetched OAuth headers
                                            headers = self.auth_headers
                                        elif self.api_key:
                                            # Use API key authentication
                                            headers = {
                                                "Authorization": f"Bearer {self.api_key}",
                                                "Content-Type": "application/json"
                                            }
                                        else:
                                            logger.error("No authentication method available for Databricks embeddings")
                                            raise Exception("No authentication method available")
                                        
                                        try:
                                            response = requests.post(
                                                endpoint_url, 
                                                headers=headers, 
                                                json=payload,
                                                timeout=30
                                            )
                                            if response.status_code == 200:
                                                result = response.json()
                                                if 'data' in result and len(result['data']) > 0:
                                                    embeddings = [item.get('embedding', item) for item in result['data']]
                                                    return cast(Embeddings, embeddings)
                                                else:
                                                    raise Exception(f"Unexpected response format: {result}")
                                            else:
                                                error_text = response.text
                                                raise Exception(f"Embedding API error {response.status_code}: {error_text}")
                                        except requests.exceptions.RequestException as e:
                                            logger.error(f"Request failed for Databricks embeddings: {e}")
                                            raise e
                                    except Exception as e:
                                        logger.error(f"Error in Databricks embedding function: {e}")
                                        raise e
                            
                            # Create the custom embedding function instance
                            if not databricks_endpoint:
                                logger.warning("No Databricks endpoint found for embeddings - embedding operations will fail")
                            
                            # Use centralized URL utility to construct the serving endpoints URL
                            api_base_url = DatabricksURLUtils.construct_serving_endpoints_url(databricks_endpoint) if databricks_endpoint else ''
                            logger.info(f"Databricks embedding api_base: {api_base_url}, model: {model_name}")
                            
                            databricks_embedder = DatabricksEmbeddingFunction(
                                api_key=databricks_key,
                                api_base=api_base_url,
                                model=model_name,
                                auth_headers=auth_headers,
                                user_token=crew_user_token  # Pass user token for OBO auth
                            )
                            
                            crew_kwargs['embedder'] = {
                                'provider': 'custom',
                                'config': {
                                    'embedder': databricks_embedder
                                }
                            }
                            logger.info(f"Configured CrewAI custom embedder for Databricks with model: {model_name}")
                        else:
                            logger.warning("No Databricks API key found, falling back to default embedder")
                            
                    except Exception as e:
                        logger.error(f"Error configuring Databricks embedder: {e}")
                        
                elif provider == 'openai':
                    # Standard OpenAI configuration
                    try:
                        from src.services.api_keys_service import ApiKeysService
                        
                        # Get OpenAI credentials directly with async/await
                        openai_key = await ApiKeysService.get_provider_api_key("OPENAI")
                            
                        if openai_key:
                            crew_kwargs['embedder'] = {
                                'provider': 'openai',
                                'config': {
                                    'api_key': openai_key,
                                    'model': config.get('model', 'text-embedding-3-small')
                                }
                            }
                            logger.info(f"Configured CrewAI embedder for OpenAI: {crew_kwargs['embedder']}")
                    except Exception as e:
                        logger.error(f"Error configuring OpenAI embedder: {e}")
                        
                elif provider == 'ollama':
                    # Local Ollama configuration
                    crew_kwargs['embedder'] = {
                        'provider': 'ollama',
                        'config': {
                            'model': config.get('model', 'nomic-embed-text')
                        }
                    }
                    logger.info(f"Configured CrewAI embedder for Ollama: {crew_kwargs['embedder']}")
                    
                elif provider == 'google':
                    # Google AI configuration
                    try:
                        from src.services.api_keys_service import ApiKeysService
                        from src.schemas.model_provider import ModelProvider
                        
                        # Get Google credentials directly with async/await
                        google_key = await ApiKeysService.get_provider_api_key(ModelProvider.GEMINI)
                            
                        if google_key:
                            crew_kwargs['embedder'] = {
                                'provider': 'google',
                                'config': {
                                    'api_key': google_key,
                                    'model': config.get('model', 'text-embedding-004')
                                }
                            }
                            logger.info(f"Configured CrewAI embedder for Google: {crew_kwargs['embedder']}")
                    except Exception as e:
                        logger.error(f"Error configuring Google embedder: {e}")
                else:
                    # Other providers - pass through config as-is
                    crew_kwargs['embedder'] = embedder_config
                    logger.info(f"Configured CrewAI embedder for {provider}: {crew_kwargs['embedder']}")
                    
            logger.info(f"Final embedder configuration: {crew_kwargs.get('embedder', 'None (default)')}")
            
            # Check if memory should be disabled for all agents in this crew
            # This is done before fetching memory backend configuration to save resources
            should_disable_memory_for_crew = False
            
            # Check if user explicitly requested to disable memory
            if crew_kwargs.get('memory', True) is False:
                logger.info("Memory explicitly disabled in crew configuration")
                should_disable_memory_for_crew = True
                crew_kwargs['memory'] = False
            elif crew_kwargs.get('memory', False):
                # Analyze all agents to see if any require memory
                agents_needing_memory = []
                agents_not_needing_memory = []
                
                for agent_config in self.config.get('agents', []):
                    agent_name = agent_config.get('name', agent_config.get('role', 'Unknown'))
                    if self._should_disable_memory_for_agent(agent_config):
                        agents_not_needing_memory.append(agent_name)
                    else:
                        agents_needing_memory.append(agent_name)
                
                # Log the analysis results
                if agents_needing_memory:
                    logger.info(f"Agents that need memory: {agents_needing_memory}")
                if agents_not_needing_memory:
                    logger.info(f"Agents that don't need memory: {agents_not_needing_memory}")
                
                # If NO agents need memory, disable it for the entire crew
                if not agents_needing_memory and agents_not_needing_memory:
                    logger.info("All agents are stateless - disabling memory for the entire crew to improve performance")
                    crew_kwargs['memory'] = False
                    should_disable_memory_for_crew = True
                    logger.info("Set should_disable_memory_for_crew=True to prevent memory backend creation")
                elif not agents_needing_memory:
                    # No agents defined or all agents have empty configs
                    logger.warning("No agents with valid configurations found - keeping memory enabled by default")
            
            # Always fetch memory backend configuration from database
            # This ensures consistency regardless of whether the crew is executed via frontend or API
            # The backend is the single source of truth for memory configuration
            memory_backend_config = None
            if crew_kwargs.get('memory', False) and not should_disable_memory_for_crew:
                try:
                    from src.services.memory_backend_service import MemoryBackendService
                    from src.core.unit_of_work import UnitOfWork
                    
                    # Use async unit of work to get the service
                    async with UnitOfWork() as uow:
                        service = MemoryBackendService(uow)
                        # Get the active memory backend configuration from database
                        # This configuration is managed through the Memory Backend UI or API
                        # and stored in the database, not passed from the frontend
                        group_id = self.config.get('group_id')
                        logger.info(f"Fetching memory backend config for group_id: {group_id}")
                        active_config = await service.get_active_config(group_id)
                        if active_config:
                            logger.info(f"Found active config: backend_type={active_config.backend_type}, enable_short_term={active_config.enable_short_term}, enable_long_term={active_config.enable_long_term}, enable_entity={active_config.enable_entity}")
                            
                            # Check if this is a "Disabled Configuration" (all memory types disabled)
                            # This is used to disable Databricks memory backend, not the default memory
                            is_disabled_config = (
                                not active_config.enable_short_term and
                                not active_config.enable_long_term and
                                not active_config.enable_entity
                            )
                            
                            if is_disabled_config:
                                logger.info("Found 'Disabled Configuration' - ignoring database config and using default memory")
                                # Treat as if no config exists - use default memory
                                memory_backend_config = None
                            else:
                                # Convert to dict format for processing
                                # The configuration will be properly converted to MemoryBackendConfig below
                                memory_backend_config = {
                                    'backend_type': active_config.backend_type.value,
                                    'databricks_config': active_config.databricks_config,
                                    'enable_short_term': active_config.enable_short_term,
                                    'enable_long_term': active_config.enable_long_term,
                                    'enable_entity': active_config.enable_entity,
                                    'enable_relationship_retrieval': active_config.enable_relationship_retrieval,
                                }
                                logger.info(f"Loaded memory backend config from database: {memory_backend_config['backend_type']}")
                        else:
                            logger.warning("No active memory backend configuration found in database")
                            memory_backend_config = None
                except Exception as e:
                    logger.warning(f"Failed to load memory backend config from database: {e}")
                    memory_backend_config = None
            
            # If no memory backend config or disabled config, create default configuration
            if not memory_backend_config and crew_kwargs.get('memory', False):
                logger.info("No memory backend config or disabled config found - using default memory")
                memory_backend_config = {
                    'backend_type': 'default',
                    'enable_short_term': True,
                    'enable_long_term': True,
                    'enable_entity': True,
                    'enable_relationship_retrieval': False,
                }
                logger.info("Created default memory backend configuration (ChromaDB + SQLite)")
            
            logger.info(f"Memory backend config present: {bool(memory_backend_config)}, Memory enabled: {crew_kwargs.get('memory', False)}")
            if memory_backend_config:
                logger.info(f"Memory backend config details: {memory_backend_config}")
            
            # Generate a deterministic crew ID based on the crew configuration
            # This ensures the same crew configuration always gets the same ID
            if self.config.get('crew_id'):
                # Use provided crew_id if available
                crew_id = self.config.get('crew_id')
            else:
                # Check if we have a database crew_id (from execution)
                db_crew_id = self.config.get('database_crew_id')
                if db_crew_id:
                    # Use the database crew ID as the base for consistent memory
                    crew_id = f"crew_db_{db_crew_id}"
                    logger.info(f"Using database crew_id: {crew_id} for consistent memory across runs")
                else:
                    # Generate a hash-based crew_id from the crew's configuration
                    import hashlib
                    import json
                    
                    # Extract stable identifiers from agents and tasks
                    agents = self.config.get('agents', [])
                    tasks = self.config.get('tasks', [])
                    
                    # Create sorted lists of agent roles and task names for stable hashing
                    agent_roles = sorted([agent.get('role', '') for agent in agents if isinstance(agent, dict)])
                    task_names = sorted([task.get('name', task.get('description', '')[:50]) for task in tasks if isinstance(task, dict)])
                    
                    # Try to get run_name from config for stable identification
                    run_name = self.config.get('run_name') or self.config.get('inputs', {}).get('run_name')
                    
                    # Get group_id for tenant isolation
                    group_id = self.config.get('group_id')
                    if not group_id:
                        logger.warning("No group_id found in config - crew memory may not be properly isolated between tenants")
                    
                    # Create a stable identifier based on crew components
                    crew_identifier = {
                        'agent_roles': agent_roles,
                        'task_names': task_names,
                        'crew_name': self.config.get('name', self.config.get('crew', {}).get('name', 'unnamed_crew')),
                        # Add the model to make crews with different models have different memories
                        'model': self.config.get('model', 'default'),
                        # Include run_name if available for better consistency
                        'run_name': run_name,
                        # IMPORTANT: Include group_id for tenant isolation - ensures different groups have separate memories
                        'group_id': group_id or 'default'
                    }
                    
                    # Create a hash of the crew configuration
                    crew_hash = hashlib.md5(
                        json.dumps(crew_identifier, sort_keys=True).encode()
                    ).hexdigest()[:8]  # Use first 8 characters of hash
                    
                    # Include group_id in the crew_id string for proper extraction later
                    # Format: {group_id}_crew_{hash}
                    crew_id = f"{group_id or 'default'}_crew_{crew_hash}"
                    logger.info(f"Generated deterministic crew_id: {crew_id} based on configuration: {crew_identifier}")
                    logger.info(f"This crew_id will persist across runs with the same configuration, ensuring memory continuity")
                    logger.info(f"Group isolation: crew_id includes group_id '{group_id or 'default'}' for tenant memory isolation")
            
            # Set a custom storage directory for memory backends to ensure isolation
            # This is important for both Databricks (dimension conflicts) and default (organization)
            if memory_backend_config and memory_backend_config.get('backend_type') in ['databricks', 'default']:
                import os
                import shutil
                from pathlib import Path
                
                # Save the original value to restore later if needed
                original_storage_dir = os.environ.get("CREWAI_STORAGE_DIR")
                
                # Use a unique directory name based on backend type and crew_id
                backend_type = memory_backend_config.get('backend_type')
                if backend_type == 'databricks':
                    storage_dirname = f"kasal_databricks_{crew_id}"
                else:  # default
                    storage_dirname = f"kasal_default_{crew_id}"
                os.environ["CREWAI_STORAGE_DIR"] = storage_dirname
                logger.info(f"Using custom storage directory for {backend_type}: {storage_dirname}")
                
                # Clean up any existing storage directory to ensure fresh start
                # This prevents dimension mismatch errors from old ChromaDB collections
                from crewai.utilities.paths import db_storage_path
                storage_path = Path(db_storage_path())
                if storage_path.exists():
                    logger.info(f"Cleaning up existing storage at: {storage_path}")
                    try:
                        shutil.rmtree(storage_path)
                        logger.info("Successfully cleaned up existing storage")
                    except Exception as e:
                        logger.warning(f"Could not clean up storage: {e}")
                
                # Store the original value to restore it later if needed
                self._original_storage_dir = original_storage_dir
            
            # Check if all memory types are disabled or if memory was disabled by agent analysis
            if should_disable_memory_for_crew:
                # Memory was already disabled by agent analysis
                logger.info("Memory disabled by agent analysis - skipping all memory backend configuration")
                crew_kwargs['memory'] = False
                # Ensure no memory backends are configured
                crew_kwargs.pop('short_term_memory', None)
                crew_kwargs.pop('long_term_memory', None)
                crew_kwargs.pop('entity_memory', None)
            elif memory_backend_config:
                all_disabled = (
                    not memory_backend_config.get('enable_short_term', False) and
                    not memory_backend_config.get('enable_long_term', False) and
                    not memory_backend_config.get('enable_entity', False)
                )
                if all_disabled:
                    logger.info("All memory types are disabled in backend configuration, disabling crew memory")
                    crew_kwargs['memory'] = False
                    should_disable_memory_for_crew = True
                    # Ensure no memory backends are configured
                    crew_kwargs.pop('short_term_memory', None)
                    crew_kwargs.pop('long_term_memory', None)
                    crew_kwargs.pop('entity_memory', None)
                else:
                    # If any memory type is enabled, ensure crew memory is enabled
                    logger.info("Memory types are enabled in backend configuration, enabling crew memory")
                    crew_kwargs['memory'] = True
            
            # Configure memory based on whether we have a backend configuration
            # IMPORTANT: Only create memory backends if memory is truly enabled and not disabled by analysis
            if crew_kwargs.get('memory', False) and not should_disable_memory_for_crew:
                # Memory is enabled - check if we have a custom backend configuration
                if memory_backend_config:
                    logger.info(f"Found memory backend configuration from database: {memory_backend_config}")
                    logger.info(f"Backend type: {memory_backend_config.get('backend_type')}")
                
                    # The memory_backend_config at this point is always a dict from the database
                    # Convert databricks_config dict to DatabricksMemoryConfig object if present
                    if 'databricks_config' in memory_backend_config and isinstance(memory_backend_config['databricks_config'], dict):
                        from src.schemas.memory_backend import DatabricksMemoryConfig
                        memory_backend_config['databricks_config'] = DatabricksMemoryConfig(**memory_backend_config['databricks_config'])
                
                    # Create the MemoryBackendConfig object
                    memory_config = MemoryBackendConfig(**memory_backend_config)
                
                    # Create memory backends
                    logger.info(f"Creating memory backends for crew {crew_id} with backend type: {memory_config.backend_type}")
                    memory_backends = await MemoryBackendFactory.create_memory_backends(
                        config=memory_config,
                        crew_id=crew_id,
                        embedder=crew_kwargs.get('embedder'),
                        user_token=self.user_token  # Pass user token for OBO authentication
                    )
                    logger.info(f"Created memory backends: {list(memory_backends.keys())}")
                
                    # Configure CrewAI with memory backends
                    if memory_backends or memory_config.backend_type == MemoryBackendType.DEFAULT:
                        # Import CrewAI memory classes
                        try:
                            from crewai.memory import ShortTermMemory, LongTermMemory, EntityMemory
                            from crewai.memory.storage.rag_storage import RAGStorage
                        
                            # Skip individual memory configuration for DEFAULT backend
                            if memory_config.backend_type == MemoryBackendType.DEFAULT:
                                # For default backend, we don't configure individual memory backends
                                # CrewAI will handle this automatically when memory=True
                                logger.info("Skipping individual memory configuration for DEFAULT backend")
                                logger.info("CrewAI will automatically initialize memory with our embedder config")
                            
                            # Configure short-term memory for non-default backends
                            elif 'short_term' in memory_backends and memory_config.enable_short_term:
                                logger.info(f"Configuring custom short-term memory backend for type: {memory_config.backend_type}")
                                # For Databricks, use the wrapper directly (it includes embedding functionality)
                                if memory_config.backend_type == MemoryBackendType.DATABRICKS:
                                    crew_kwargs['short_term_memory'] = ShortTermMemory(storage=memory_backends['short_term'])
                                    logger.info(f"Successfully configured Databricks short-term memory with storage: {type(memory_backends['short_term'])}")
                                else:
                                    # For other backends, use RAGStorage
                                    if crew_kwargs.get('embedder'):
                                        rag_storage = RAGStorage(
                                            type="short_term",
                                            embedder_config=crew_kwargs.get('embedder')
                                        )
                                        crew_kwargs['short_term_memory'] = ShortTermMemory(storage=rag_storage)
                                        logger.info("Successfully configured default short-term memory with RAGStorage")
                                    else:
                                        logger.warning("No embedder configured for custom short-term memory")
                        
                            # Configure long-term memory for non-default backends
                            if memory_config.backend_type != MemoryBackendType.DEFAULT and 'long_term' in memory_backends and memory_config.enable_long_term:
                                logger.info("Configuring custom long-term memory backend")
                                # LongTermMemory uses a different storage pattern
                                crew_kwargs['long_term_memory'] = LongTermMemory(storage=memory_backends['long_term'])
                                logger.info("Successfully configured Databricks long-term memory")
                        
                            # Configure entity memory for non-default backends
                            if memory_config.backend_type != MemoryBackendType.DEFAULT and 'entity' in memory_backends and memory_config.enable_entity:
                                logger.info("Configuring custom entity memory backend")
                                # For Databricks, use the wrapper directly
                                if memory_config.backend_type == MemoryBackendType.DATABRICKS:
                                    # Check if any agent is using a problematic model
                                    needs_fallback = False
                                    problematic_model = None
                                    
                                    # Check agents for problematic models
                                    for agent_config in self.config.get('agents', []):
                                        agent_model = agent_config.get('llm')
                                        if agent_model and self._needs_entity_extraction_fallback(agent_model):
                                            needs_fallback = True
                                            problematic_model = agent_model
                                            break
                                    
                                    # Also check crew-level model if no agent model found
                                    if not needs_fallback:
                                        crew_model = self.config.get('crew', {}).get('model') or self.config.get('model')
                                        if crew_model and self._needs_entity_extraction_fallback(crew_model):
                                            needs_fallback = True
                                            problematic_model = crew_model
                                    
                                    # Apply fallback patch if needed
                                    if needs_fallback:
                                        logger.info(f"Model {problematic_model} needs entity extraction fallback, applying patch for databricks-llama-4-maverick")
                                        # Apply the entity extraction fallback patch
                                        await self._apply_entity_extraction_fallback_patch()
                                    
                                    # IMPORTANT: Pass the embedder config to prevent CrewAI from creating default RAGStorage
                                    crew_kwargs['entity_memory'] = EntityMemory(
                                        storage=memory_backends['entity'],
                                        embedder_config=crew_kwargs.get('embedder')
                                        # Removed crew=None to allow EntityMemory to connect to the crew for entity extraction
                                    )
                                    logger.info("Successfully configured Databricks entity memory with custom embedder")
                                else:
                                    # For other backends, use RAGStorage
                                    if crew_kwargs.get('embedder'):
                                        rag_storage = RAGStorage(
                                            type="entities",
                                            embedder_config=crew_kwargs.get('embedder')
                                        )
                                        crew_kwargs['entity_memory'] = EntityMemory(storage=rag_storage)
                                        logger.info("Successfully configured default entity memory with RAGStorage")
                                    else:
                                        logger.warning("No embedder configured for custom entity memory")
                        
                            logger.info(f"Memory backend configuration completed for crew {crew_id}")
                            
                            # Handle DEFAULT backend type
                            if memory_config.backend_type == MemoryBackendType.DEFAULT:
                                # For default backend, we let CrewAI handle the memory initialization
                                # with the embedder we've configured
                                crew_kwargs['memory'] = True
                                logger.info("Set memory=True for default backend to use CrewAI's built-in memory")
                                logger.info("CrewAI will create ChromaDB collections for short-term/entity and SQLite for long-term")
                            
                            # IMPORTANT: Set memory=False when using Databricks to prevent any default RAGStorage creation
                            elif memory_config.backend_type == MemoryBackendType.DATABRICKS:
                                crew_kwargs['memory'] = False
                                logger.info("Set memory=False for Databricks backend to prevent conflicts")
                                
                        except ImportError as e:
                            logger.error(f"Failed to import CrewAI memory classes: {e}")
                            logger.warning("Falling back to default memory implementation")
                        except Exception as e:
                            logger.error(f"Error configuring custom memory backends: {e}")
                            logger.warning("Falling back to default memory implementation")
                else:
                    # No memory backend configuration found - use CrewAI default memory (ChromaDB + SQLite)
                    logger.info("No memory backend configuration found in database")
                    logger.info("Using CrewAI default memory implementation (ChromaDB + SQLite)")
                    # Keep memory=True to let CrewAI initialize its default memory
                    crew_kwargs['memory'] = True
                    # Log the embedder that will be used by default memory
                    if crew_kwargs.get('embedder'):
                        logger.info(f"Default memory will use configured embedder: {crew_kwargs['embedder'].get('provider', 'unknown')}")
                    else:
                        logger.info("Default memory will use CrewAI's default embedder")
            
            # Add optional parameters if they exist in config
            if 'max_rpm' in self.config:
                crew_kwargs['max_rpm'] = self.config['max_rpm']
                
            if 'planning' in crew_config:
                crew_kwargs['planning'] = crew_config['planning']
                
            if 'planning_llm' in crew_config:
                try:
                    planning_llm = await LLMManager.get_llm(crew_config['planning_llm'])
                    crew_kwargs['planning_llm'] = planning_llm
                    logger.info(f"Set crew planning LLM to: {crew_config['planning_llm']}")
                except Exception as llm_error:
                    logger.warning(f"Could not create planning LLM for model {crew_config['planning_llm']}: {llm_error}")
            
            if 'reasoning' in crew_config:
                crew_kwargs['reasoning'] = crew_config['reasoning']
                
            if 'reasoning_llm' in crew_config:
                try:
                    reasoning_llm = await LLMManager.get_llm(crew_config['reasoning_llm'])
                    crew_kwargs['reasoning_llm'] = reasoning_llm
                    logger.info(f"Set crew reasoning LLM to: {crew_config['reasoning_llm']}")
                except Exception as llm_error:
                    logger.warning(f"Could not create reasoning LLM for model {crew_config['reasoning_llm']}: {llm_error}")
            
            # If memory was disabled by agent analysis, ensure no memory backends are present
            if should_disable_memory_for_crew:
                logger.info("=" * 80)
                logger.info("MEMORY COMPLETELY DISABLED FOR THIS CREW")
                logger.info("Reason: All agents are stateless and don't require memory")
                logger.info("Performance benefit: No memory operations will be performed")
                logger.info("=" * 80)
                # Final check to ensure no memory components exist
                crew_kwargs['memory'] = False
                crew_kwargs.pop('short_term_memory', None)
                crew_kwargs.pop('long_term_memory', None)
                crew_kwargs.pop('entity_memory', None)
                crew_kwargs.pop('embedder', None)  # Also remove embedder since it's not needed
            # If we have custom memory backends configured, we should disable the default memory
            # to prevent CrewAI from creating its own RAGStorage
            elif 'short_term_memory' in crew_kwargs or 'long_term_memory' in crew_kwargs or 'entity_memory' in crew_kwargs:
                logger.info("Custom memory backends configured, disabling default memory initialization")
                # IMPORTANT: When using custom memory backends (especially Databricks with different embedding dimensions),
                # we must set memory=False to prevent CrewAI from creating any default RAGStorage instances
                # that might conflict with our custom embeddings
                if memory_backend_config and memory_backend_config.get('backend_type') == 'databricks':
                    crew_kwargs['memory'] = False
                    logger.info("Set memory=False to prevent CrewAI default memory initialization for Databricks backend")
            
            # Log memory configuration before crew creation
            logger.info("=== MEMORY CONFIGURATION BEFORE CREW CREATION ===")
            logger.info(f"Memory enabled: {crew_kwargs.get('memory', False)}")
            if memory_backend_config:
                logger.info(f"Memory backend: {memory_backend_config.get('backend_type', 'unknown')}")
            else:
                logger.info("Memory backend: Default (ChromaDB + SQLite)")
            logger.info(f"Short-term memory: {'custom configured' if 'short_term_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
            logger.info(f"Long-term memory: {'custom configured' if 'long_term_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
            logger.info(f"Entity memory: {'custom configured' if 'entity_memory' in crew_kwargs else 'default' if crew_kwargs.get('memory', False) else 'disabled'}")
            logger.info(f"Embedder: {'configured' if 'embedder' in crew_kwargs else 'not configured'}")
            if 'embedder' in crew_kwargs:
                embedder_info = crew_kwargs['embedder']
                if isinstance(embedder_info, dict):
                    logger.info(f"Embedder provider: {embedder_info.get('provider', 'unknown')}")
                    if embedder_info.get('provider') == 'custom':
                        custom_embedder = embedder_info.get('config', {}).get('embedder')
                        if hasattr(custom_embedder, 'model'):
                            logger.info(f"Custom embedder model: {custom_embedder.model}")
                            logger.info(f"Expected embedding dimension: 1024")
            logger.info("================================================")
            
            # Create the crew instance
            # Handle OpenAI API key properly in Databricks Apps environment
            try:
                import os
                from src.utils.databricks_auth import is_databricks_apps_environment
                if is_databricks_apps_environment():
                    # Check if OpenAI API key is configured in the database
                    from src.services.api_keys_service import ApiKeysService
                    try:
                        openai_key = await ApiKeysService.get_provider_api_key("openai")
                        if openai_key:
                            # OpenAI key is configured, keep it for CrewAI to use
                            os.environ["OPENAI_API_KEY"] = openai_key
                            logger.info("OpenAI API key is configured, keeping it for CrewAI")
                        else:
                            # No OpenAI key configured, set dummy key to satisfy CrewAI validation
                            # This won't be used since we explicitly set manager_llm and agent LLMs
                            os.environ["OPENAI_API_KEY"] = "sk-dummy-databricks-apps-validation-key"
                            logger.info("No OpenAI API key configured, set dummy key for CrewAI validation")
                    except Exception as api_error:
                        logger.warning(f"Error checking OpenAI API key configuration: {api_error}")
                        # Fall back to removing the env var to prevent validation error
                        os.environ.pop("OPENAI_API_KEY", None)
            except Exception as e:
                logger.warning(f"Error handling OpenAI API key for Databricks Apps: {e}")
            
            self.crew = Crew(**crew_kwargs)
            
            if not self.crew:
                logger.error("Failed to create crew")
                return False
            
            # Set crew reference on memory wrappers for proper agent/LLM model attribution
            try:
                # Update long-term memory with crew reference for LLM model extraction
                if hasattr(self.crew, '_long_term_memory') and self.crew._long_term_memory:
                    long_term_storage = self.crew._long_term_memory.storage
                    # Check if it's our custom wrapper
                    if hasattr(long_term_storage, 'crew'):
                        long_term_storage.crew = self.crew
                        logger.info("Set crew reference for long-term memory to enable LLM model extraction")
                    else:
                        logger.debug("Long-term memory storage doesn't support crew reference (likely default storage)")
                
                # Set agent context on entity memory wrapper for proper agent_id attribution
                if hasattr(self.crew, '_entity_memory') and self.crew._entity_memory:
                    entity_storage = self.crew._entity_memory.storage
                    # Check if it's our custom wrapper that supports set_agent_context
                    if hasattr(entity_storage, 'set_agent_context'):
                        # Set the first agent as the default context for entity memory
                        # This provides a fallback since CrewAI's entity memory doesn't pass agent in save()
                        if self.crew.agents:
                            first_agent = self.crew.agents[0]
                            entity_storage.set_agent_context(first_agent)
                            logger.info(f"Set agent context for entity memory: {getattr(first_agent, 'role', 'Unknown')}")
                        else:
                            logger.warning("No agents found in crew for entity memory context")
                    else:
                        logger.debug("Entity memory storage doesn't support set_agent_context (likely default storage)")
                    
                    # Also set crew reference for entity memory
                    if hasattr(entity_storage, 'crew'):
                        entity_storage.crew = self.crew
                        logger.info("Set crew reference for entity memory")
                
                # Update short-term memory with crew reference
                if hasattr(self.crew, '_short_term_memory') and self.crew._short_term_memory:
                    short_term_storage = self.crew._short_term_memory.storage
                    if hasattr(short_term_storage, 'crew'):
                        short_term_storage.crew = self.crew
                        logger.info("Set crew reference for short-term memory")
                        
            except Exception as context_error:
                logger.warning(f"Failed to set context on memory backends: {context_error}")
            
            logger.info("Created crew successfully")
            return True
        except Exception as e:
            handle_crew_error(e, "Error creating crew")
            return False
    

    async def execute(self) -> Dict[str, Any]:
        """
        Execute the prepared crew
        
        Returns:
            Dict[str, Any]: Results from crew execution
        """
        if not self.crew:
            logger.error("Cannot execute crew: crew not prepared")
            return {"error": "Crew not prepared"}
        
        try:
            # Execute the crew
            result = await self.crew.kickoff()
            
            # Process the output
            processed_output = await process_crew_output(result)
            
            # Check if data is missing
            if is_data_missing(processed_output):
                logger.warning("Crew execution completed but data may be missing")
            
            return processed_output
            
        except Exception as e:
            handle_crew_error(e, "Error during crew execution")
            return {"error": str(e)}
    
    def cleanup(self):
        """
        Cleanup method to restore original environment settings.
        This should be called when done with the crew to restore the original storage directory.
        """
        if hasattr(self, '_original_storage_dir'):
            import os
            if self._original_storage_dir is not None:
                os.environ["CREWAI_STORAGE_DIR"] = self._original_storage_dir
                logger.info(f"Restored original CREWAI_STORAGE_DIR: {self._original_storage_dir}")
            elif "CREWAI_STORAGE_DIR" in os.environ:
                # If there was no original value, remove the environment variable
                del os.environ["CREWAI_STORAGE_DIR"]
                logger.info("Removed CREWAI_STORAGE_DIR environment variable") 