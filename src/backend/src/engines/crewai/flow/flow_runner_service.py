"""
Service for running Flow executions with CrewAI Flow.

This file contains the FlowRunnerService which handles running flow executions in the system.
It uses the BackendFlow class (from backend_flow.py) to interact with the CrewAI Flow engine.
"""
import os
import logging
import asyncio
import uuid
from typing import Dict, List, Optional, Any, Union
from datetime import datetime, UTC
from sqlalchemy.orm import Session

from fastapi import HTTPException, status

from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)
from src.repositories.flow_execution_repository import (
    SyncFlowExecutionRepository,
    SyncFlowNodeExecutionRepository
)
from src.repositories.flow_repository import SyncFlowRepository
from src.repositories.task_repository import SyncTaskRepository
from src.repositories.agent_repository import SyncAgentRepository
from src.repositories.tool_repository import SyncToolRepository
from src.core.logger import LoggerManager
from src.db.session import SessionLocal
from src.services.api_keys_service import ApiKeysService
from src.engines.crewai.flow.backend_flow import BackendFlow

# Initialize logger manager
logger = LoggerManager.get_instance().crew

class FlowRunnerService:
    """Service for running Flow executions"""
    
    def __init__(self, db: Session):
        """Initialize with database session"""
        self.db = db
        self.flow_execution_repo = SyncFlowExecutionRepository(db)
        self.node_execution_repo = SyncFlowNodeExecutionRepository(db)
        self.flow_repo = SyncFlowRepository(db)
        self.task_repo = SyncTaskRepository(db)
        self.agent_repo = SyncAgentRepository(db)
        self.tool_repo = SyncToolRepository(db)
    
    def create_flow_execution(self, flow_id: Union[uuid.UUID, str], job_id: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Create a new flow execution record and prepare for execution.
        
        Args:
            flow_id: The ID of the flow to execute
            job_id: Job ID for tracking
            config: Optional configuration for the execution
            
        Returns:
            Dictionary with execution details
        """
        logger.info(f"Creating flow execution for flow {flow_id}, job {job_id}")
        
        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError as e:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                return {
                    "success": False,
                    "error": f"Invalid UUID format: {str(e)}",
                    "job_id": job_id,
                    "flow_id": flow_id
                }
        
        try:
            # Create flow execution record
            execution_data = FlowExecutionCreate(
                flow_id=flow_id,
                job_id=job_id,
                status=FlowExecutionStatus.PENDING,
                config=config or {}
            )
            
            flow_execution = self.flow_execution_repo.create(execution_data)
            
            return {
                "success": True,
                "execution_id": flow_execution.id,
                "job_id": job_id,
                "flow_id": flow_id,
                "status": flow_execution.status
            }
        except Exception as e:
            logger.error(f"Error creating flow execution: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id,
                "flow_id": flow_id
            }
    
    async def run_flow(self, flow_id: Optional[Union[uuid.UUID, str]], job_id: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run a flow execution.
        
        Args:
            flow_id: ID of the flow to run, or None for a dynamic flow
            job_id: Job ID for tracking execution
            config: Additional configuration
            
        Returns:
            Execution result
        """
        try:
            # Add detailed logging about inputs
            logger.info(f"run_flow called with flow_id={flow_id}, job_id={job_id}")
            
            # Convert string to UUID if provided and not None
            if flow_id is not None and isinstance(flow_id, str):
                try:
                    flow_id = uuid.UUID(flow_id)
                    logger.info(f"Converted string flow_id to UUID: {flow_id}")
                except ValueError as e:
                    logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Invalid UUID format: {str(e)}"
                    )
            
            logger.info(f"Running flow execution for flow {flow_id}, job {job_id}")
            
            if config is None:
                config = {}
                
            logger.info(f"Flow execution config keys: {config.keys()}")
            if 'flow_id' in config:
                logger.info(f"Found flow_id in config: {config['flow_id']}")
            
            # Check if flow_id from parameter is None but exists in config
            if flow_id is None and 'flow_id' in config:
                flow_id_str = config['flow_id']
                try:
                    flow_id = uuid.UUID(flow_id_str)
                    logger.info(f"Using flow_id from config: {flow_id}")
                except (ValueError, TypeError):
                    logger.warning(f"Invalid flow_id in config: {flow_id_str}, ignoring")
            
            # Different execution paths based on whether we have nodes in config
            nodes = config.get('nodes', [])
            edges = config.get('edges', [])
            
            # Check if we need to load flow data from database
            if not nodes and flow_id is not None:
                logger.info(f"No nodes provided in config, loading flow data from database for flow {flow_id}")
                try:
                    # Load flow data from database using repository
                    flow = self.flow_repo.find_by_id(flow_id)
                    if not flow:
                        logger.error(f"Flow with ID {flow_id} not found in database")
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Flow with ID {flow_id} not found"
                        )
                    
                    # Update the config with loaded data
                    config['nodes'] = flow.nodes
                    config['edges'] = flow.edges
                    config['flow_config'] = flow.flow_config
                    
                    # Update local variables
                    nodes = flow.nodes
                    edges = flow.edges
                    
                    logger.info(f"Loaded flow data from database: {len(nodes)} nodes, {len(edges)} edges")
                except Exception as e:
                    logger.error(f"Error loading flow data from database: {e}", exc_info=True)
                    raise HTTPException(
                        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                        detail=f"Error loading flow data: {str(e)}"
                    )
            
            # Validate nodes if this is a dynamic flow (no flow_id) or we have nodes in config
            if flow_id is None and (not nodes or not isinstance(nodes, list)):
                logger.error(f"No valid nodes provided for dynamic flow. Got: {type(nodes)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="No valid nodes provided for dynamic flow. Nodes must be a non-empty array."
                )
            
            # Create a flow execution record
            execution_data = FlowExecutionCreate(
                flow_id=flow_id if flow_id is not None else uuid.UUID('00000000-0000-0000-0000-000000000000'),
                job_id=job_id,
                status=FlowExecutionStatus.PENDING,
                config=config
            )
            
            repo = SyncFlowExecutionRepository(self.db)
            execution = repo.create(execution_data)
            logger.info(f"Created flow execution record with ID {execution.id}")
            
            # Start the appropriate execution method based on flow_id
            if flow_id is not None:
                logger.info(f"Starting execution for existing flow {flow_id}")
                asyncio.create_task(self._run_flow_execution(execution.id, flow_id, job_id, config))
            else:
                logger.info(f"Starting execution for dynamic flow")
                asyncio.create_task(self._run_dynamic_flow(execution.id, job_id, config))
            
            return {
                "job_id": job_id,
                "execution_id": execution.id,
                "status": FlowExecutionStatus.PENDING,
                "message": "Flow execution started"
            }
        except Exception as e:
            logger.error(f"Error running flow execution: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error running flow execution: {str(e)}"
            )
    
    async def _run_dynamic_flow(self, execution_id: int, job_id: str, config: Dict[str, Any]) -> None:
        """
        Run a dynamic flow execution created from the configuration.
        
        Args:
            execution_id: ID of the flow execution record
            job_id: Job ID for tracking
            config: Configuration containing nodes, edges, and flow configuration
        """
        try:
            logger.info(f"Starting dynamic flow execution {execution_id} for job {job_id}")
            
            # Update status to indicate we're preparing the flow
            repo = SyncFlowExecutionRepository(self.db)
            update_data = FlowExecutionUpdate(
                status=FlowExecutionStatus.PREPARING,
                updated_at=datetime.now(UTC)
            )
            repo.update(execution_id, update_data)
            
            # Initialize API keys before execution
            try:
                # Initialize all the API keys needed for execution
                for provider in ["OPENAI", "ANTHROPIC", "PERPLEXITY", "SERPER"]:
                    try:
                        # Since this is an async method in a sync context, use sync approach
                        provider_key = await ApiKeysService.get_provider_api_key(provider)
                        if not provider_key:
                            logger.warning(f"No API key found for provider: {provider}")
                        else:
                            # Set the environment variable for the provider
                            env_var_name = f"{provider}_API_KEY"
                            os.environ[env_var_name] = provider_key
                            logger.info(f"Set {env_var_name} for dynamic flow execution")
                    except Exception as key_error:
                        logger.warning(f"Error loading API key for {provider}: {key_error}")
                
                logger.info("API keys have been initialized for dynamic flow execution")
            except Exception as e:
                logger.warning(f"Error initializing API keys: {e}")
                # Continue with execution, as keys might be available through other means
            
            # Create a CrewAI flow from the configuration
            # Import here to avoid circular imports
            from src.engines.crewai.crewai_engine_service import CrewAIEngineService
            
            # Initialize the engine service
            engine_service = CrewAIEngineService()
            await engine_service.initialize()
            
            # Run the flow using the engine service
            try:
                await engine_service.run_flow(job_id, config)
                
                # Update the execution status (engine service will handle further updates)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.RUNNING,
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
                
                logger.info(f"Successfully initiated dynamic flow execution {execution_id} for job {job_id}")
            except Exception as engine_error:
                logger.error(f"Error in engine service while running dynamic flow {execution_id}: {engine_error}", exc_info=True)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.FAILED,
                    error=str(engine_error),
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
                
        except Exception as e:
            logger.error(f"Error running dynamic flow execution {execution_id}: {e}", exc_info=True)
            try:
                # Update status to FAILED
                repo = SyncFlowExecutionRepository(self.db)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.FAILED,
                    error=str(e),
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
            except Exception as update_error:
                logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
    
    async def _run_flow_execution(self, execution_id: int, flow_id: Union[uuid.UUID, str], job_id: str, config: Dict[str, Any]) -> None:
        """
        Run a flow execution for an existing flow.
        
        Args:
            execution_id: ID of the flow execution record
            flow_id: ID of the flow to execute
            job_id: Job ID for tracking
            config: Additional configuration
        """
        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError as e:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                # Update status to FAILED
                repo = SyncFlowExecutionRepository(self.db)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.FAILED,
                    error=f"Invalid UUID format: {str(e)}",
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
                return
        
        try:
            logger.info(f"Starting flow execution {execution_id} for flow {flow_id}, job {job_id}")
            
            # Update status to indicate we're preparing the flow
            repo = SyncFlowExecutionRepository(self.db)
            update_data = FlowExecutionUpdate(
                status=FlowExecutionStatus.PREPARING,
                updated_at=datetime.now(UTC)
            )
            repo.update(execution_id, update_data)
            
            # Initialize API keys before execution
            try:
                # Initialize all the API keys needed for execution
                for provider in ["OPENAI", "ANTHROPIC", "PERPLEXITY", "SERPER"]:
                    try:
                        # Since this is an async method in a sync context, use sync approach
                        provider_key = await ApiKeysService.get_provider_api_key(provider)
                        if not provider_key:
                            logger.warning(f"No API key found for provider: {provider}")
                        else:
                            # Set the environment variable for the provider
                            env_var_name = f"{provider}_API_KEY"
                            os.environ[env_var_name] = provider_key
                            logger.info(f"Set {env_var_name} for flow execution")
                    except Exception as key_error:
                        logger.warning(f"Error loading API key for {provider}: {key_error}")
                
                logger.info("API keys have been initialized for flow execution")
            except Exception as e:
                logger.warning(f"Error initializing API keys: {e}")
                # Continue with execution, as keys might be available through other means
            
            # Initialize BackendFlow with the flow_id and job_id
            backend_flow = BackendFlow(job_id=job_id, flow_id=flow_id)
            backend_flow.repositories = {
                'flow': self.flow_repo,
                'task': self.task_repo,
                'agent': self.agent_repo,
                'tool': self.tool_repo
            }
            
            # If this flow has no nodes/edges in the config, try to load them from the database
            if 'nodes' not in config or not config.get('nodes'):
                logger.info(f"No nodes in config for flow {flow_id}, trying to load from database")
                try:
                    # Load flow data using the BackendFlow instance, passing the repository
                    flow_data = backend_flow.load_flow(repository=self.flow_repo)
                    logger.info(f"Loaded flow data for flow {flow_id}")
                    
                    # Update config with flow data
                    if 'nodes' in flow_data and flow_data['nodes']:
                        config['nodes'] = flow_data['nodes']
                        logger.info(f"Loaded {len(flow_data['nodes'])} nodes from flow data for flow {flow_id}")
                    if 'edges' in flow_data and flow_data['edges']:
                        config['edges'] = flow_data['edges']
                        logger.info(f"Loaded {len(flow_data['edges'])} edges from flow data for flow {flow_id}")
                    if 'flow_config' in flow_data and flow_data['flow_config']:
                        config['flow_config'] = flow_data['flow_config']
                        logger.info(f"Loaded flow_config from flow data for flow {flow_id}")
                    
                    # If we still don't have nodes, try direct database access as fallback
                    if 'nodes' not in config or not config.get('nodes'):
                        logger.warning(f"Failed to load nodes from BackendFlow for flow {flow_id}, trying direct database access")
                        # Get the flow from the database using repository
                        flow = self.flow_repo.find_by_id(flow_id)
                        if flow:
                            if flow.nodes:
                                config['nodes'] = flow.nodes
                                logger.info(f"Loaded {len(flow.nodes)} nodes from database for flow {flow_id}")
                            if flow.edges:
                                config['edges'] = flow.edges
                                logger.info(f"Loaded {len(flow.edges)} edges from database for flow {flow_id}")
                            if flow.flow_config:
                                config['flow_config'] = flow.flow_config
                                logger.info(f"Loaded flow_config from database for flow {flow_id}")
                except Exception as e:
                    logger.error(f"Error loading flow data: {e}", exc_info=True)
            
            # If config is provided, update the backend flow's config
            if config:
                logger.info(f"Updating flow config with provided configuration")
                backend_flow.config.update(config)
            
            # Set up output directory for the flow
            output_dir = os.path.join(os.getenv('OUTPUT_DIR', 'output'), job_id)
            os.makedirs(output_dir, exist_ok=True)
            backend_flow.output_dir = output_dir
            logger.info(f"Set output directory to {output_dir}")
            
            # Update status to RUNNING
            update_data = FlowExecutionUpdate(
                status=FlowExecutionStatus.RUNNING,
                updated_at=datetime.now(UTC)
            )
            repo.update(execution_id, update_data)
            
            try:
                # Execute the flow and get the result
                result = await backend_flow.kickoff()
                logger.info(f"Flow execution completed with result: {result}")
                
                # Update the execution with the result
                if result.get("success", False):
                    # Ensure result is a dictionary
                    result_data = result.get("result", {})
                    if not isinstance(result_data, dict):
                        logger.warning(f"Expected result to be a dictionary, got {type(result_data)}. Converting to dict.")
                        try:
                            if hasattr(result_data, 'to_dict'):
                                result_data = result_data.to_dict()
                            elif hasattr(result_data, '__dict__'):
                                result_data = result_data.__dict__
                            else:
                                result_data = {"content": str(result_data)}
                        except Exception as conv_error:
                            logger.error(f"Error converting result to dictionary: {conv_error}. Using fallback.", exc_info=True)
                            result_data = {"content": str(result_data)}
                    
                    update_data = FlowExecutionUpdate(
                        status=FlowExecutionStatus.COMPLETED,
                        result=result_data,
                        updated_at=datetime.now(UTC)
                    )
                else:
                    update_data = FlowExecutionUpdate(
                        status=FlowExecutionStatus.FAILED,
                        error=result.get("error", "Unknown error"),
                        updated_at=datetime.now(UTC)
                    )
                
                repo.update(execution_id, update_data)
                logger.info(f"Updated flow execution {execution_id} with final status")
            except Exception as kickoff_error:
                logger.error(f"Error executing flow {flow_id}: {kickoff_error}", exc_info=True)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.FAILED,
                    error=str(kickoff_error),
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
        except Exception as e:
            logger.error(f"Error in flow execution {execution_id}: {e}", exc_info=True)
            try:
                # Update status to FAILED
                repo = SyncFlowExecutionRepository(self.db)
                update_data = FlowExecutionUpdate(
                    status=FlowExecutionStatus.FAILED,
                    error=str(e),
                    updated_at=datetime.now(UTC)
                )
                repo.update(execution_id, update_data)
            except Exception as update_error:
                logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
    
    def get_flow_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get flow execution details.
        
        Args:
            execution_id: ID of the flow execution
            
        Returns:
            Dictionary with execution details
        """
        try:
            execution = self.flow_execution_repo.get(execution_id)
            
            if not execution:
                return {
                    "success": False,
                    "error": f"Flow execution with ID {execution_id} not found"
                }
            
            # Get node executions if any
            nodes = self.node_execution_repo.get_by_flow_execution(execution_id)
            
            return {
                "success": True,
                "execution": {
                    "id": execution.id,
                    "flow_id": execution.flow_id,
                    "job_id": execution.job_id,
                    "status": execution.status,
                    "result": execution.result,
                    "error": execution.error,
                    "created_at": execution.created_at,
                    "updated_at": execution.updated_at,
                    "completed_at": execution.completed_at,
                    "nodes": [
                        {
                            "id": node.id,
                            "node_id": node.node_id,
                            "status": node.status,
                            "agent_id": node.agent_id,
                            "task_id": node.task_id,
                            "result": node.result,
                            "error": node.error,
                            "created_at": node.created_at,
                            "updated_at": node.updated_at,
                            "completed_at": node.completed_at
                        }
                        for node in nodes
                    ]
                }
            }
        except Exception as e:
            logger.error(f"Error getting flow execution: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "execution_id": execution_id
            }
    
    def get_flow_executions_by_flow(self, flow_id: Union[uuid.UUID, str]) -> Dict[str, Any]:
        """
        Get all executions for a specific flow.
        
        Args:
            flow_id: ID of the flow
            
        Returns:
            Dictionary with list of executions
        """
        # Convert string to UUID if needed
        if isinstance(flow_id, str):
            try:
                flow_id = uuid.UUID(flow_id)
            except ValueError as e:
                logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                return {
                    "success": False,
                    "error": f"Invalid UUID format: {str(e)}",
                    "flow_id": flow_id
                }
        
        try:
            executions = self.flow_execution_repo.get_by_flow_id(flow_id)
            
            return {
                "success": True,
                "flow_id": flow_id,
                "executions": [
                    {
                        "id": execution.id,
                        "job_id": execution.job_id,
                        "status": execution.status,
                        "created_at": execution.created_at,
                        "completed_at": execution.completed_at
                    }
                    for execution in executions
                ]
            }
        except Exception as e:
            logger.error(f"Error getting flow executions: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "flow_id": flow_id
            }

    def _create_flow_from_config(self, flow_id, job_id, config):
        """
        Create a CrewAI Flow class dynamically from the flow configuration.
        
        Args:
            flow_id: The ID of the flow
            job_id: Job ID for tracking
            config: The flow configuration including nodes, edges, and flow_config
            
        Returns:
            An instance of a dynamically created Flow class
        """
        from crewai.flow.flow import Flow, start, listen, router
        from crewai.agent import Agent
        from crewai.task import Task
        from crewai.crew import Crew
        
        logger.info(f"Creating flow from config: {flow_id}")
        
        # Extract flow configuration
        flow_config = config.get('flow_config', {})
        nodes = config.get('nodes', [])
        edges = config.get('edges', [])
        
        if not flow_config:
            logger.warning("No flow_config found in configuration")
            flow_config = {}
            
        # Extract starting points and listeners
        starting_points = flow_config.get('startingPoints', [])
        listeners = flow_config.get('listeners', [])
        
        # Find all crew nodes from the nodes list
        crew_nodes = {}
        for node in nodes:
            if node.get('type', '').lower() == 'crewnode':
                crew_nodes[node.get('id')] = node
        
        if not starting_points and len(crew_nodes) > 0:
            logger.warning("No starting points found in flow_config, creating default")
            # Create a default starting point
            first_crew = list(crew_nodes.values())[0]
            starting_points = [{
                'crewId': first_crew.get('id'),
                'crewName': first_crew.get('data', {}).get('label', 'Default Crew'),
                'taskId': 'default-task',
                'taskName': 'Default Task'
            }]
            
        logger.info(f"Found {len(starting_points)} starting points in flow config")
        
        # Define a dynamic Flow class
        dynamic_flow_class = type('DynamicFlow', (Flow,), {})
        
        # Define the __init__ method
        def __init__(self, flow_id=None, job_id=None, config=None):
            super(dynamic_flow_class, self).__init__()
            self.flow_id = flow_id
            self.job_id = job_id
            self.config = config or {}
            self.crews = {}
            self._initialize_crews()
        
        # Define the _initialize_crews method with improved agent and task configuration
        def _initialize_crews(self):
            try:
                # Import agent/task services
                from src.services.agent_service import AgentService
                from src.services.task_service import TaskService
                from src.services.crew_service import CrewService
                from src.tools.tool_factory import ToolFactory
                
                # Create a tool factory instance for tool creation
                tool_factory = ToolFactory()
                
                with SessionLocal() as db:
                    agent_service = AgentService(db)
                    task_service = TaskService(db)
                    crew_service = CrewService(db)
                    
                    # Initialize crews from the configuration
                    for node_id, node in crew_nodes.items():
                        crew_name = node.get('data', {}).get('label', f"Crew-{node_id}")
                        
                        # Try to get crew from database
                        try:
                            crew_id = node.get('data', {}).get('crewId') or node.get('id')
                            if isinstance(crew_id, str) and crew_id.isdigit():
                                crew_id = int(crew_id)
                            
                            crew_data = crew_service.get_crew(crew_id)
                            if crew_data:
                                # Get agents for this crew with proper tool configuration
                                agents = []
                                for agent_data in crew_data.agents:
                                    agent_obj = agent_service.get_agent(agent_data.id)
                                    if agent_obj:
                                        # Create tools for the agent
                                        tools = []
                                        if hasattr(agent_obj, 'tools') and agent_obj.tools:
                                            for tool_id in agent_obj.tools:
                                                try:
                                                    # Get tool configuration
                                                    from src.services.tool_service import ToolService
                                                    tool_service = ToolService(db)
                                                    tool_obj = tool_service.get_tool(tool_id)
                                                    
                                                    if tool_obj:
                                                        # Create the tool instance
                                                        tool_name = tool_obj.title
                                                        result_as_answer = False
                                                        if hasattr(tool_obj, 'config') and tool_obj.config:
                                                            if isinstance(tool_obj.config, dict):
                                                                result_as_answer = tool_obj.config.get('result_as_answer', False)
                                                            
                                                        tool = tool_factory.create_tool(
                                                            tool_name,
                                                            result_as_answer=result_as_answer
                                                        )
                                                        
                                                        if tool:
                                                            tools.append(tool)
                                                            logger.info(f"Added tool: {tool_name} to agent: {agent_obj.name}")
                                                except Exception as tool_error:
                                                    logger.error(f"Error configuring tool for agent {agent_obj.name}: {tool_error}")
                                        
                                        # Configure the agent with its tools
                                        agent_kwargs = {
                                            "role": agent_obj.role,
                                            "goal": agent_obj.goal,
                                            "backstory": agent_obj.backstory,
                                            "verbose": True,
                                            "allow_delegation": agent_obj.allow_delegation,
                                            "tools": tools
                                        }
                                        
                                        # Add LLM if specified
                                        if hasattr(agent_obj, 'llm') and agent_obj.llm:
                                            agent_kwargs["llm"] = agent_obj.llm
                                        
                                        # Create the agent
                                        agent = Agent(**agent_kwargs)
                                        agents.append(agent)
                                        logger.info(f"Created agent {agent_obj.name} with {len(tools)} tools")
                                
                                # Get tasks for this crew with proper configuration
                                tasks = []
                                for task_data in crew_data.tasks:
                                    task_obj = task_service.get_task(task_data.id)
                                    if task_obj and task_obj.agent_id:
                                        # Find the corresponding agent
                                        agent = None
                                        for i, a in enumerate(crew_data.agents):
                                            if str(a.id) == str(task_obj.agent_id) and i < len(agents):
                                                agent = agents[i]
                                                break
                                                
                                        if agent:
                                            # Configure task with context if needed
                                            task_kwargs = {
                                                "description": task_obj.description,
                                                "expected_output": task_obj.expected_output,
                                                "agent": agent,
                                                "verbose": True
                                            }
                                            
                                            # Handle task context (dependencies on other tasks)
                                            if hasattr(task_obj, 'context_task_ids') and task_obj.context_task_ids:
                                                context_tasks = []
                                                for ctx_task_id in task_obj.context_task_ids:
                                                    # Find the context task in our task list
                                                    for t in tasks:
                                                        if hasattr(t, 'id') and str(t.id) == str(ctx_task_id):
                                                            context_tasks.append(t)
                                                            break
                                                    
                                                if context_tasks:
                                                    task_kwargs["context"] = context_tasks
                                            
                                            # Add async_execution if specified
                                            if hasattr(task_obj, 'async_execution'):
                                                task_kwargs["async_execution"] = task_obj.async_execution
                                            
                                            # Create the task
                                            task = Task(**task_kwargs)
                                            tasks.append(task)
                                            logger.info(f"Created task {task_obj.name} with agent {agent.role}")
                                
                                # Create the crew if we have agents and tasks
                                if agents and tasks:
                                    # Determine process type from crew configuration
                                    process_type = Process.sequential
                                    if hasattr(crew_data, 'process') and crew_data.process:
                                        process_str = str(crew_data.process).lower()
                                        if process_str == 'hierarchical':
                                            process_type = Process.hierarchical
                                        elif process_str == 'parallel':
                                            process_type = Process.parallel
                                    
                                    crew = Crew(
                                        agents=agents,
                                        tasks=tasks,
                                        verbose=True,
                                        process=process_type
                                    )
                                    
                                    # Configure LLM if specified at crew level
                                    if hasattr(crew_data, 'llm') and crew_data.llm:
                                        crew.llm = crew_data.llm
                                    
                                    self.crews[node_id] = crew
                                    logger.info(f"Created crew {crew_name} with {len(agents)} agents and {len(tasks)} tasks using {process_type} process")
                        except Exception as e:
                            logger.error(f"Error creating crew {crew_name}: {str(e)}")
            except Exception as e:
                logger.error(f"Error initializing crews: {str(e)}")
        
        # Define the start_flow method with improved error handling
        @start()
        def start_flow(self):
            logger.info(f"Starting flow execution for job {self.job_id}")
            
            # Initialize state with flow_id and job_id for tracking
            self.state["flow_id"] = str(self.flow_id) if self.flow_id else "dynamic-flow"
            self.state["job_id"] = self.job_id
            self.state["start_time"] = datetime.now(UTC).isoformat()
            
            # Execute the starting point crew if available
            if starting_points and len(starting_points) > 0:
                start_point = starting_points[0]
                crew_id = start_point.get('crewId')
                crew_name = start_point.get('crewName')
                task_id = start_point.get('taskId')
                task_name = start_point.get('taskName')
                
                logger.info(f"Starting flow with crew {crew_name} and task {task_name}")
                
                # Find the crew by ID
                crew = self.crews.get(str(crew_id))
                if crew:
                    # Execute the crew
                    try:
                        logger.info(f"Executing crew {crew_name}")
                        result = crew.kickoff()
                        logger.info(f"Crew execution completed successfully")
                        # Store result in state for downstream listeners
                        self.state["result"] = result.raw if hasattr(result, 'raw') else str(result)
                        self.state["end_time"] = datetime.now(UTC).isoformat()
                        return result
                    except Exception as e:
                        logger.error(f"Error executing crew: {str(e)}")
                        self.state["error"] = str(e)
                        self.state["end_time"] = datetime.now(UTC).isoformat()
                        return {"error": str(e)}
                else:
                    error_msg = f"Crew {crew_id} not found"
                    logger.error(error_msg)
                    self.state["error"] = error_msg
                    self.state["end_time"] = datetime.now(UTC).isoformat()
                    return {"error": error_msg}
            else:
                error_msg = "No starting points defined"
                logger.warning(error_msg)
                self.state["error"] = error_msg 
                self.state["end_time"] = datetime.now(UTC).isoformat()
                return {"error": error_msg}
        
        # Add methods to the class
        setattr(dynamic_flow_class, '__init__', __init__)
        setattr(dynamic_flow_class, '_initialize_crews', _initialize_crews)
        setattr(dynamic_flow_class, 'start_flow', start_flow)
        
        # Add listener methods
        for i, listener in enumerate(listeners):
            crew_id = listener.get('crewId')
            crew_name = listener.get('crewName')
            
            # Define the listener method
            def make_listener_method(crew_id, crew_name):
                @listen("start_flow")
                def listener_method(self, result):
                    logger.info(f"Listener triggered for crew {crew_name}")
                    crew = self.crews.get(str(crew_id))
                    if crew:
                        try:
                            logger.info(f"Executing listener crew {crew_name}")
                            self.state["previous_result"] = result
                            listener_result = crew.kickoff()
                            logger.info(f"Listener crew execution completed: {listener_result}")
                            return listener_result
                        except Exception as e:
                            logger.error(f"Error executing listener crew: {str(e)}")
                            return {"error": str(e)}
                    else:
                        logger.error(f"Listener crew {crew_id} not found")
                        return {"error": f"Crew {crew_id} not found"}
                return listener_method
            
            method_name = f"listener_{i}"
            setattr(dynamic_flow_class, method_name, make_listener_method(crew_id, crew_name))
        
        # Create and return an instance
        flow_instance = dynamic_flow_class(flow_id=flow_id, job_id=job_id, config=config)
        logger.info(f"Created dynamic flow instance for job {job_id}")
        return flow_instance 