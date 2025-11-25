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
from src.repositories.flow_repository import FlowRepository
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

# Initialize logger manager - use flow logger for flow execution
logger = LoggerManager.get_instance().flow

class BackendFlow:
    """Base BackendFlow class for handling flow execution"""

    def __init__(
        self,
        job_id: Optional[str] = None,
        flow_id: Optional[Union[uuid.UUID, str]] = None,
        tracing: bool = False
    ):
        """
        Initialize a new BackendFlow instance.

        Args:
            job_id: Optional job ID for tracking
            flow_id: Optional flow ID to load from database
            tracing: Enable MLflow tracing for this flow execution
        """
        self._job_id = job_id
        self._tracing_enabled = tracing
        
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

    async def load_flow(self, repository: Optional[FlowRepository] = None) -> Dict:
        """
        Load flow data from the database using repository if provided,
        otherwise get one from the factory.

        Args:
            repository: Optional FlowRepository instance

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
                flow = await repository.get(self._flow_id)
            else:
                # Log error if no repository provided
                logger.error(f"No flow repository provided for flow_id {self._flow_id}")
                raise ValueError(f"No flow repository provided for flow_id {self._flow_id}")
                    
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

        # CRITICAL: Set group context for multi-tenant isolation before ANY LLM calls
        group_context = self._config.get('group_context')
        if group_context:
            try:
                from src.utils.user_context import UserContext
                UserContext.set_group_context(group_context)
                logger.info(f"Set group context for flow execution: {getattr(group_context, 'primary_group_id', 'unknown')}")
            except Exception as e:
                logger.warning(f"Could not set group context: {e}")

        if not self._flow_data:
            # Use flow repository if available
            flow_repo = self._repositories.get('flow')
            await self.load_flow(repository=flow_repo)

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
                callbacks=self._config.get('callbacks', {}),
                group_context=self._config.get('group_context')
            )
            
            logger.info("Flow created successfully")
            return dynamic_flow
            
        except Exception as e:
            logger.error(f"Error creating flow: {e}", exc_info=True)
            raise ValueError(f"Failed to create flow: {str(e)}")

    def _init_callbacks(self):
        """
        Initialize callbacks for flow execution.

        Note: For flows, we don't use JobOutputCallback (async) like regular crews.
        Instead, we rely on:
        1. AgentTraceEventListener for execution traces (set up in subprocess)
        2. Synchronous step_callback and task_callback set on each Crew instance
        """
        # Set group context in UserContext for multi-tenant isolation
        group_context = self._config.get('group_context')
        if group_context:
            try:
                from src.utils.user_context import UserContext
                UserContext.set_group_context(group_context)
                logger.info(f"Set group context for flow execution callbacks")
            except Exception as e:
                logger.warning(f"Could not set group context in _init_callbacks: {e}")

        # For flows, we only need minimal callback setup with job_id
        # The actual logging/tracing is handled by:
        # 1. TraceManager + AgentTraceEventListener (initialized in subprocess)
        # 2. Synchronous callbacks set on each Crew instance in flow methods
        self._config['callbacks'] = {
            'handlers': [],  # No async handlers for flows
            'job_id': self._job_id,  # Pass job_id directly for sync callbacks
            'start_trace_writer': True  # Signal to start trace writer in subprocess
        }
        logger.info(f"Initialized flow callbacks with job_id={self._job_id} (using event listeners and sync crew callbacks)")

    async def kickoff_async(self) -> Dict[str, Any]:
        """
        Async version of kickoff for better performance.
        Uses CrewAI's native kickoff_async() when available.
        """
        logger.info(f"Kicking off async flow execution for job {self._job_id}")

        # CRITICAL: Set group context for multi-tenant isolation before ANY operations
        group_context = self._config.get('group_context')
        if group_context:
            try:
                from src.utils.user_context import UserContext
                UserContext.set_group_context(group_context)
                logger.info(f"Set group context for kickoff_async: {getattr(group_context, 'primary_group_id', 'unknown')}")
            except Exception as e:
                logger.warning(f"Could not set group context in kickoff_async: {e}")

        # Get callbacks for use in finally block
        callbacks = self._config.get('callbacks', {})

        try:
            # Start the trace writer if tracing is enabled
            if self._tracing_enabled or callbacks.get('start_trace_writer', False):
                try:
                    from src.engines.crewai.trace_management import TraceManager
                    await TraceManager.ensure_writer_started()
                    logger.info("Successfully started trace writer for event processing")
                except Exception as e:
                    logger.warning(f"Error starting trace writer: {e}", exc_info=True)

            # Load flow data if needed
            if not self._flow_data:
                try:
                    flow_repo = self._repositories.get('flow')
                    await self.load_flow(repository=flow_repo)
                    logger.info("Successfully loaded flow data during kickoff_async")
                except Exception as e:
                    logger.error(f"Error loading flow data during kickoff_async: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": f"Failed to load flow data: {str(e)}",
                        "flow_id": self._flow_id
                    }

            # Create the CrewAI flow instance
            try:
                crewai_flow = await self.flow()
                logger.info("Successfully created CrewAI flow instance for async execution")
            except Exception as e:
                logger.error(f"Error creating CrewAI flow: {e}", exc_info=True)
                return {
                    "success": False,
                    "error": f"Failed to create CrewAI flow: {str(e)}",
                    "flow_id": self._flow_id
                }

            # Execute using CrewAI's native kickoff_async if available
            logger.info("Starting async flow execution")
            logger.info(f"Flow instance type: {type(crewai_flow)}")
            logger.info(f"Flow has kickoff_async: {hasattr(crewai_flow, 'kickoff_async')}")

            try:
                if hasattr(crewai_flow, 'kickoff_async'):
                    logger.info("Using CrewAI's native kickoff_async method")
                    logger.info(f"About to call kickoff_async() on flow instance")
                    result = await crewai_flow.kickoff_async()
                    logger.info(f"kickoff_async() returned: {type(result)}")
                else:
                    logger.info("kickoff_async not available, using synchronous kickoff")
                    logger.info(f"About to call kickoff() on flow instance")
                    result = crewai_flow.kickoff()
                    logger.info(f"kickoff() returned: {type(result)}")

                logger.info("Flow executed successfully via kickoff_async")

                # Process result
                result_dict = {}
                if result is None:
                    result_value = {}
                elif isinstance(result, dict):
                    result_value = result
                else:
                    # Handle CrewOutput object
                    if hasattr(result, 'to_dict'):
                        result_value = result.to_dict()
                    elif hasattr(result, '__dict__'):
                        result_value = result.__dict__
                    elif hasattr(result, 'raw'):
                        result_value = {
                            "content": result.raw,
                            "token_usage": str(result.token_usage) if hasattr(result, 'token_usage') else None
                        }
                    else:
                        result_value = {"content": str(result)}

                return {
                    "success": True,
                    "result": result_value,
                    "flow_id": self._flow_id
                }
            except Exception as exec_error:
                logger.error(f"Error during async flow execution: {exec_error}", exc_info=True)
                return {
                    "success": False,
                    "error": str(exec_error),
                    "flow_id": self._flow_id
                }

        except Exception as e:
            logger.error(f"Error during flow kickoff_async: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "flow_id": self._flow_id
            }
        finally:
            # Clean up callbacks using the CallbackManager
            CallbackManager.cleanup_callbacks(callbacks)

    async def kickoff(self) -> Dict[str, Any]:
        """Execute the flow and return the result"""
        logger.info(f"Kicking off flow execution for job {self._job_id}")

        # CRITICAL: Set group context for multi-tenant isolation before ANY operations
        group_context = self._config.get('group_context')
        if group_context:
            try:
                from src.utils.user_context import UserContext
                UserContext.set_group_context(group_context)
                logger.info(f"Set group context for kickoff: {getattr(group_context, 'primary_group_id', 'unknown')}")
            except Exception as e:
                logger.warning(f"Could not set group context in kickoff: {e}")

        # Get callbacks for use in finally block
        callbacks = self._config.get('callbacks', {})

        try:
            # Start the trace writer if tracing is enabled
            if self._tracing_enabled or callbacks.get('start_trace_writer', False):
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
                    await self.load_flow(repository=flow_repo)
                    logger.info("Successfully loaded flow data during kickoff")
                except Exception as e:
                    logger.error(f"Error loading flow data during kickoff: {e}", exc_info=True)
                    return {
                        "success": False,
                        "error": f"Failed to load flow data: {str(e)}",
                        "flow_id": self._flow_id
                    }

            # CRITICAL: If config has an updated flow_config (with startingPoints), use it
            # This ensures frontend-provided flow_config takes precedence over database version
            if 'flow_config' in self._config:
                logger.info("[kickoff] Using flow_config from self._config (has latest updates)")
                self._flow_data['flow_config'] = self._config['flow_config']

                # Also update nodes/edges if they're in config
                if 'nodes' in self._config:
                    self._flow_data['nodes'] = self._config['nodes']
                if 'edges' in self._config:
                    self._flow_data['edges'] = self._config['edges']

                logger.info(f"[kickoff] Updated flow_data with flow_config from config")
                if 'startingPoints' in self._config.get('flow_config', {}):
                    logger.info(f"[kickoff] flow_config has {len(self._config['flow_config']['startingPoints'])} startingPoints")

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
            logger.info("="*100)
            logger.info("STARTING FLOW EXECUTION - Looking for start methods")
            logger.info("="*100)

            # Get all methods of the flow instance that are decorated with @start
            start_methods = []
            all_methods = []
            for attr_name in dir(crewai_flow):
                if callable(getattr(crewai_flow, attr_name)):
                    all_methods.append(attr_name)
                    if attr_name.startswith('starting_point_'):
                        start_methods.append(attr_name)

            logger.info(f"Flow instance type: {type(crewai_flow)}")
            logger.info(f"Total callable methods: {len(all_methods)}")
            logger.info(f"All methods: {[m for m in all_methods if not m.startswith('_')]}")
            logger.info(f"Found {len(start_methods)} start methods: {start_methods}")

            if not start_methods:
                logger.error("❌ NO START METHODS FOUND! Flow cannot execute.")
                logger.error("This usually means FlowBuilder._create_dynamic_flow() didn't create start methods properly")
                return {
                    "success": False,
                    "error": "No start methods found in flow. Check flow configuration and startingPoints.",
                    "flow_id": self._flow_id
                }
            
            # Execute the flow using CrewAI's kickoff mechanism
            # Use kickoff_async() for async flow execution
            # Do NOT call start methods directly - this bypasses the event system!
            logger.info("Calling flow.kickoff_async() to execute the flow with proper event handling")

            combined_results = {}
            try:
                # Let CrewAI Flow handle the execution (start methods + listeners)
                flow_result = await crewai_flow.kickoff_async()
                logger.info(f"Flow kickoff_async completed, result type: {type(flow_result)}")

                # Store the result
                if flow_result:
                    combined_results['flow_output'] = flow_result
                else:
                    logger.warning("Flow kickoff_async returned None")
            except Exception as flow_error:
                logger.error(f"Error during flow kickoff_async: {flow_error}", exc_info=True)
                raise

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

    async def plot(self, filename: str = "flow_diagram") -> Optional[str]:
        """
        Generate flow visualization using CrewAI's plot functionality.

        Args:
            filename: Name of the output file (without extension)

        Returns:
            Path to the generated visualization file, or None if plot is not available
        """
        logger.info(f"Generating flow visualization: {filename}")

        try:
            # Create the CrewAI flow instance
            crewai_flow = await self.flow()

            # Check if plot method is available
            if hasattr(crewai_flow, 'plot'):
                logger.info("Using CrewAI's native plot method")
                output_path = os.path.join(self._output_dir or ".", filename)
                crewai_flow.plot(filename=output_path)
                logger.info(f"Flow visualization saved to: {output_path}")
                return output_path
            else:
                logger.warning("CrewAI flow does not support plot() method")
                return None

        except Exception as e:
            logger.error(f"Error generating flow visualization: {e}", exc_info=True)
            return None

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