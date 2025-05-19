"""
Base BackendFlow class for handling flow execution.

Handles the creation and execution of CrewAI flows.
"""
import os
import logging
import uuid
import asyncio
import json
import traceback
import time
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, UTC
from pydantic import BaseModel, Field

from src.core.logger import LoggerManager
from src.repositories.flow_repository import SyncFlowRepository
from crewai import Agent, Task, Crew
from crewai import Process
from crewai.flow.flow import Flow as CrewAIFlow
from crewai import LLM
from src.core.llm_manager import LLMManager
from crewai.tools import BaseTool

# Import the refactored modules
from src.engines.crewai.flow.modules.agent_config import AgentConfig
from src.engines.crewai.flow.modules.task_config import TaskConfig
from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
from src.engines.crewai.flow.modules.callback_manager import CallbackManager
from src.engines.crewai.tools.tool_factory import ToolFactory

# Initialize logger manager
logger = LoggerManager.get_instance().crew

class BackendFlow:
    """Base BackendFlow class for handling flow execution"""

    def __init__(self, job_id: Optional[str] = None, flow_id: Optional[Union[uuid.UUID, str]] = None):
        """
        Initialize a new BackendFlow instance.
        
        Args:
            job_id: Optional job ID for tracking
            flow_id: Optional flow ID to load from database
        """
        self._job_id = job_id
        
        # Handle flow_id conversion more safely
        if flow_id is None:
            self._flow_id = None
        elif isinstance(flow_id, uuid.UUID):
            self._flow_id = flow_id
        else:
            try:
                self._flow_id = uuid.UUID(flow_id)
            except (ValueError, AttributeError, TypeError):
                logger.error(f"Invalid flow_id format: {flow_id}")
                raise ValueError(f"Invalid flow_id format: {flow_id}")
                
        self._flow_data = None
        self._output_dir = None
        # Don't store API keys directly, just other configuration
        self._config = {}
        # Repository container
        self._repositories = {}
        logger.info(f"Initializing BackendFlow{' for job ' + job_id if job_id else ''}")

    @property
    def config(self):
        return self._config

    @config.setter
    def config(self, value):
        self._config = value

    @property
    def output_dir(self):
        logger.info(f"Getting output_dir: {self._output_dir}")
        return self._output_dir

    @output_dir.setter
    def output_dir(self, value):
        logger.info(f"Setting output_dir to: {value}")
        if value is not None:
            os.makedirs(value, exist_ok=True)
        self._output_dir = value
        
    @property
    def repositories(self):
        return self._repositories
        
    @repositories.setter
    def repositories(self, value):
        self._repositories = value

    def load_flow(self, repository: Optional[SyncFlowRepository] = None) -> Dict:
        """
        Load flow data from the database using repository if provided, 
        otherwise get one from the factory.
        
        Args:
            repository: Optional SyncFlowRepository instance
            
        Returns:
            Dictionary containing flow data
        """
        logger.info(f"Loading flow with ID: {self._flow_id}")
        
        if not self._flow_id:
            logger.error("No flow_id provided")
            raise ValueError("No flow_id provided")
            
        try:
            # Use provided repository or get one from the factory
            if repository:
                flow = repository.find_by_id(self._flow_id)
            else:
                # Get repository from factory
                from src.repositories.flow_repository import get_sync_flow_repository
                flow_repository = get_sync_flow_repository()
                flow = flow_repository.find_by_id(self._flow_id)
                    
            if not flow:
                logger.error(f"Flow with ID {self._flow_id} not found")
                raise ValueError(f"Flow with ID {self._flow_id} not found")
                
            self._flow_data = {
                'id': flow.id,
                'name': flow.name,
                'crew_id': flow.crew_id,
                'nodes': flow.nodes,
                'edges': flow.edges,
                'flow_config': flow.flow_config
            }
            logger.info(f"Successfully loaded flow: {flow.name}")
            logger.info(f"Flow configuration: {flow.flow_config}")
            return self._flow_data
        except Exception as e:
            logger.error(f"Error loading flow data: {e}", exc_info=True)
            raise

    async def _get_llm(self) -> LLM:
        """
        Get a properly configured LLM for CrewAI using LLMManager.
        This ensures API keys are properly set from the database.
        """
        try:
            # Get the default model name from environment or use a default
            model_name = os.getenv('DEFAULT_LLM_MODEL', 'gpt-4o')
            logger.info(f"Getting LLM model: {model_name} for flow execution")
            
            # Use LLMManager to get a properly configured LLM
            llm = await LLMManager.get_llm(model_name)
            logger.info(f"Successfully configured LLM: {model_name}")
            return llm
        except Exception as e:
            logger.error(f"Error configuring LLM: {e}", exc_info=True)
            raise

    async def flow(self) -> CrewAIFlow:
        """Creates and returns a CrewAI Flow instance based on the loaded flow configuration"""
        logger.info("Creating CrewAI Flow")
        
        if not self._flow_data:
            # Use flow repository if available
            flow_repo = self._repositories.get('flow')
            self.load_flow(repository=flow_repo)
            
        if not self._flow_data:
            logger.error("Flow data could not be loaded")
            raise ValueError("Flow data could not be loaded")
        
        try:
            # Initialize callbacks for this flow execution
            self._init_callbacks()
            
            # Build the flow using the FlowBuilder module
            dynamic_flow = await FlowBuilder.build_flow(
                flow_data=self._flow_data,
                repositories=self._repositories,
                callbacks=self._config.get('callbacks', {})
            )
            
            logger.info("Flow created successfully")
            return dynamic_flow
            
        except Exception as e:
            logger.error(f"Error creating flow: {e}", exc_info=True)
            raise ValueError(f"Failed to create flow: {str(e)}")

    def _init_callbacks(self):
        """
        Initialize all necessary callbacks for flow execution.
        Uses the CallbackManager module.
        """
        self._config['callbacks'] = CallbackManager.init_callbacks(
            job_id=self._job_id,
            config=self._config
        )

    async def kickoff(self) -> Dict[str, Any]:
        """Execute the flow and return the result"""
        logger.info(f"Kicking off flow execution for job {self._job_id}")
        
        # Get callbacks for use in finally block
        callbacks = self._config.get('callbacks', {})
        
        try:
            # Start the trace writer if needed
            if callbacks.get('start_trace_writer', False):
                try:
                    from src.engines.crewai.trace_management import TraceManager
                    await TraceManager.ensure_writer_started()
                    logger.info("Successfully started trace writer for event processing")
                except Exception as e:
                    logger.warning(f"Error starting trace writer: {e}", exc_info=True)
                    # Continue execution even if trace writer fails
            
            # Make sure we have flow data loaded
            if not self._flow_data:
                try:
                    # Use the repository from the service if provided
                    flow_repo = self._repositories.get('flow')
                    self.load_flow(repository=flow_repo)
                    logger.info("Successfully loaded flow data during kickoff")
                except Exception as e:
                    logger.error(f"Error loading flow data during kickoff: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": f"Failed to load flow data: {str(e)}",
                        "flow_id": self._flow_id
                    }
            
            # Create the CrewAI flow
            try:
                # Create the flow instance by awaiting the coroutine
                crewai_flow = await self.flow()
                logger.info("Successfully created CrewAI flow instance")
            except Exception as e:
                logger.error(f"Error creating CrewAI flow: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": f"Failed to create CrewAI flow: {str(e)}",
                    "flow_id": self._flow_id
                }
            
            # Execute the flow asynchronously - find start methods
            logger.info("Starting flow execution")
            
            # Get all methods of the flow instance that are decorated with @start
            start_methods = []
            for attr_name in dir(crewai_flow):
                if attr_name.startswith('start_flow_') and callable(getattr(crewai_flow, attr_name)):
                    start_methods.append(attr_name)
            
            logger.info(f"Found {len(start_methods)} start methods in flow: {start_methods}")
            
            # Execute each start method
            combined_results = {}
            for method_name in start_methods:
                try:
                    logger.info(f"Executing start method: {method_name}")
                    # Use the appropriate kickoff method based on what's available
                    start_method = getattr(crewai_flow, method_name)
                    try:
                        # First try to call the method directly
                        method_result = await start_method()
                        logger.info(f"Directly called {method_name} method successfully")
                    except Exception as direct_call_error:
                        logger.warning(f"Error directly calling method {method_name}: {direct_call_error}")
                        try:
                            # Try using newer kickoff approach without 'method' parameter
                            if hasattr(crewai_flow, 'kickoff_async'):
                                logger.info(f"Attempting to use kickoff_async without method parameter")
                                method_result = await crewai_flow.kickoff_async()
                            else:
                                # Fallback to synchronous kickoff
                                logger.info(f"Falling back to synchronous kickoff")
                                method_result = crewai_flow.kickoff()
                        except Exception as kickoff_error:
                            logger.error(f"Error using alternative kickoff for {method_name}: {kickoff_error}", exc_info=True)
                            raise
                    
                    logger.info(f"Start method {method_name} executed successfully")
                    
                    # Add this result to the combined results
                    if method_result:
                        if isinstance(method_result, dict):
                            combined_results.update(method_result)
                        else:
                            # Add with the method name as key
                            combined_results[method_name] = method_result
                except Exception as method_error:
                    logger.error(f"Error executing start method {method_name}: {method_error}", exc_info=True)
                    # Continue with other methods even if one fails
            
            logger.info(f"Flow executed successfully with {len(combined_results)} results")
            
            # Convert all results to dictionary format
            result_dict = {}
            for key, value in combined_results.items():
                try:
                    # Process result based on its type
                    if value is None:
                        result_value = {}
                    elif isinstance(value, dict):
                        result_value = value
                    else:
                        # Handle CrewOutput object
                        if hasattr(value, 'to_dict'):
                            # Use to_dict method if available
                            result_value = value.to_dict()
                        elif hasattr(value, '__dict__'):
                            # Use __dict__ if to_dict not available
                            result_value = value.__dict__
                        elif hasattr(value, 'raw'):
                            # Extract raw content and token usage if available
                            result_value = {
                                "content": value.raw,
                                "token_usage": str(value.token_usage) if hasattr(value, 'token_usage') else None
                            }
                        else:
                            # Fallback to string representation
                            result_value = {"content": str(value)}
                        
                    # Add to result dictionary
                    result_dict[key] = result_value
                except Exception as conv_error:
                    logger.error(f"Error converting result to dictionary for {key}: {conv_error}", exc_info=True)
                    # Use a simple string representation as fallback
                    result_dict[key] = {"content": str(value)}
            
            return {
                "success": True,
                "result": result_dict,
                "flow_id": self._flow_id
            }
        except Exception as e:
            logger.error(f"Error during flow kickoff: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "flow_id": self._flow_id
            }
        finally:
            # Clean up callbacks using the CallbackManager
            CallbackManager.cleanup_callbacks(callbacks)

    def _ensure_event_listeners_registered(self, listeners):
        """
        Make sure event listeners are properly registered with CrewAI's event bus.
        This method is now handled by the CallbackManager.
        
        Args:
            listeners: List of listener instances to register
        """
        CallbackManager.ensure_event_listeners_registered(listeners)

    async def _configure_agent_and_tools(self, agent_data):
        """
        Configure an agent with its associated tools from the database.
        This method is now handled by the AgentConfig module.
        
        Args:
            agent_data: Agent data from the database
            
        Returns:
            Agent: A properly configured CrewAI Agent instance
        """
        return await AgentConfig.configure_agent_and_tools(
            agent_data=agent_data,
            flow_data=self._flow_data,
            repositories=self._repositories
        )

    async def _configure_task(self, task_data, agent=None, task_output_callback=None):
        """
        Configure a task with its associated agent and callbacks.
        This method is now handled by the TaskConfig module.
        
        Args:
            task_data: Task data from the database
            agent: Pre-configured agent instance (optional)
            task_output_callback: Callback for task output (optional)
            
        Returns:
            Task: A properly configured CrewAI Task instance
        """
        return await TaskConfig.configure_task(
            task_data=task_data,
            agent=agent,
            task_output_callback=task_output_callback,
            flow_data=self._flow_data,
            repositories=self._repositories
        ) 