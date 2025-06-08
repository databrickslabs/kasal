"""
Crew preparation module for CrewAI engine.

This module handles the preparation and configuration of CrewAI agents and tasks.
"""

from typing import Dict, Any, List, Optional
import logging
from crewai import Agent, Crew, Task
from src.core.logger import LoggerManager
from src.engines.crewai.helpers.task_helpers import create_task, is_data_missing
from src.engines.crewai.helpers.agent_helpers import create_agent


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

class CrewPreparation:
    """Handles the preparation of CrewAI agents and tasks"""
    
    def __init__(self, config: Dict[str, Any], tool_service=None, tool_factory=None):
        """
        Initialize the CrewPreparation class
        
        Args:
            config: Configuration dictionary containing crew setup
            tool_service: Tool service instance for resolving tool IDs
            tool_factory: Tool factory for creating tool instances
        """
        self.config = config
        self.agents: Dict[str, Agent] = {}
        self.tasks: List[Task] = []
        self.crew: Optional[Crew] = None
        self.tool_service = tool_service
        self.tool_factory = tool_factory
        
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
                # Use the agent's 'role' if 'name' is not present, or generate a name if neither exists
                agent_name = agent_config.get('name', agent_config.get('role', f'agent_{i}'))
                
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
                
                # Store any context IDs for second pass resolution
                if "context" in task_config:
                    context_value = task_config.pop("context")
                    logger.info(f"Saved context references for task {task_name}: {context_value}")
                    # Store the references for resolution in second pass
                    if isinstance(context_value, list):
                        task_config['_context_refs'] = [str(item) for item in context_value]
                    elif isinstance(context_value, str):
                        task_config['_context_refs'] = [context_value]
                    elif isinstance(context_value, dict) and "task_ids" in context_value:
                        task_config['_context_refs'] = context_value["task_ids"]
                
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
                'memory': True
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
                    embedder_config = agent_config['embedder_config']
                    logger.info(f"Found embedder configuration: {embedder_config}")
                    break
            
            # Set default to Databricks if no embedder config found
            if not embedder_config:
                embedder_config = {
                    'provider': 'databricks',
                    'config': {'model': 'databricks-gte-large-en'}
                }
                logger.info("No embedder config found, using default Databricks configuration")
            
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
                            # Get OAuth headers for embeddings
                            auth_headers, error = await get_databricks_auth_headers()
                            if error:
                                logger.error(f"Failed to get OAuth headers for embeddings: {error}")
                        else:
                            logger.info("Using enhanced Databricks auth for embeddings in local environment")
                            # Use enhanced auth system for local development too
                            auth_headers, error = await get_databricks_auth_headers()
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
                            if databricks_endpoint and not databricks_endpoint.startswith('https://'):
                                databricks_endpoint = f"https://{databricks_endpoint}"
                            if not databricks_endpoint:
                                databricks_endpoint = os.getenv('DATABRICKS_ENDPOINT', '')
                            
                            model_name = config.get('model', 'databricks-gte-large-en')
                            
                            # Create custom embedding function for Databricks
                            class DatabricksEmbeddingFunction(EmbeddingFunction):
                                def __init__(self, api_key: str = None, api_base: str = None, model: str = None, auth_headers: dict = None):
                                    self.api_key = api_key
                                    self.api_base = api_base 
                                    self.model = model if model.startswith('databricks/') else f"databricks/{model}"
                                    self.auth_headers = auth_headers
                                
                                def __call__(self, input: Documents) -> Embeddings:
                                    try:
                                        # Use direct HTTP request for OAuth or LiteLLM for API key
                                        if self.auth_headers:
                                            # Use direct HTTP request with OAuth headers
                                            import aiohttp
                                            import asyncio
                                            import json
                                            
                                            async def get_embeddings():
                                                endpoint_url = f"{self.api_base}/{self.model.replace('databricks/', '')}/invocations"
                                                payload = {"input": input if isinstance(input, list) else [input]}
                                                
                                                async with aiohttp.ClientSession() as session:
                                                    async with session.post(endpoint_url, headers=self.auth_headers, json=payload) as response:
                                                        if response.status == 200:
                                                            result = await response.json()
                                                            if 'data' in result and len(result['data']) > 0:
                                                                embeddings = [item.get('embedding', item) for item in result['data']]
                                                                return embeddings
                                                        else:
                                                            error_text = await response.text()
                                                            raise Exception(f"Embedding API error {response.status}: {error_text}")
                                            
                                            # Run async function in sync context
                                            loop = asyncio.new_event_loop()
                                            asyncio.set_event_loop(loop)
                                            try:
                                                embeddings = loop.run_until_complete(get_embeddings())
                                                return cast(Embeddings, embeddings)
                                            finally:
                                                loop.close()
                                        else:
                                            # Use LiteLLM for API key authentication
                                            response = litellm.embedding(
                                                model=self.model,
                                                input=input,
                                                api_key=self.api_key,
                                                api_base=self.api_base
                                            )
                                        
                                        # Extract embeddings from response
                                        embeddings = [item['embedding'] for item in response['data']]
                                        return cast(Embeddings, embeddings)
                                    except Exception as e:
                                        logger.error(f"Error in Databricks embedding function: {e}")
                                        raise e
                            
                            # Create the custom embedding function instance
                            databricks_embedder = DatabricksEmbeddingFunction(
                                api_key=databricks_key,
                                api_base=f"{databricks_endpoint.rstrip('/')}/serving-endpoints" if databricks_endpoint else '',
                                model=model_name,
                                auth_headers=auth_headers
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
            
            # Add optional parameters if they exist in config
            if 'max_rpm' in self.config:
                crew_kwargs['max_rpm'] = self.config['max_rpm']
                
            if 'planning' in crew_config:
                crew_kwargs['planning'] = crew_config['planning']
                
            if 'planning_llm' in crew_config:
                crew_kwargs['planning_llm'] = crew_config['planning_llm']
            
            if 'reasoning' in crew_config:
                crew_kwargs['reasoning'] = crew_config['reasoning']
                
            if 'reasoning_llm' in crew_config:
                crew_kwargs['reasoning_llm'] = crew_config['reasoning_llm']
            
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