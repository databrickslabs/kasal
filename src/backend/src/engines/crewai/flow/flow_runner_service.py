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
from sqlalchemy.ext.asyncio import AsyncSession

from fastapi import HTTPException, status

from src.schemas.flow_execution import (
    FlowExecutionCreate,
    FlowExecutionUpdate,
    FlowNodeExecutionCreate,
    FlowNodeExecutionUpdate,
    FlowExecutionStatus
)
from src.services.flow_execution_service import FlowExecutionService
from src.repositories.flow_repository import FlowRepository
from src.repositories.task_repository import TaskRepository
from src.repositories.agent_repository import AgentRepository
from src.repositories.tool_repository import ToolRepository
from src.repositories.crew_repository import CrewRepository
from src.core.logger import LoggerManager
from src.db.session import async_session_factory
from src.services.api_keys_service import ApiKeysService
from src.engines.crewai.flow.backend_flow import BackendFlow

# Initialize flow-specific logger
logger = LoggerManager.get_instance().flow

class FlowRunnerService:
    """Service for running Flow executions"""

    def __init__(self, db: AsyncSession):
        """Initialize with async database session"""
        self.db = db
        self.flow_execution_service = FlowExecutionService(db)
        self.flow_repo = FlowRepository(db)
        self.task_repo = TaskRepository(db)
        self.agent_repo = AgentRepository(db)
        self.tool_repo = ToolRepository(db)
        self.crew_repo = CrewRepository(db)
    
    async def create_flow_execution(self, flow_id: Union[uuid.UUID, str], job_id: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

        try:
            # Extract group_id from config for multi-tenant isolation
            group_id = config.get('group_id') if config else None

            # Create flow execution via service layer
            flow_execution = await self.flow_execution_service.create_execution(
                flow_id=flow_id,
                job_id=job_id,
                config=config,
                group_id=group_id
            )

            return {
                "success": True,
                "execution_id": flow_execution.id,
                "job_id": job_id,
                "flow_id": flow_execution.flow_id,
                "status": flow_execution.status
            }
        except ValueError as e:
            logger.error(f"Invalid UUID format for flow_id: {flow_id}")
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id,
                "flow_id": flow_id
            }
        except Exception as e:
            logger.error(f"Error creating flow execution: {e}", exc_info=True)
            return {
                "success": False,
                "error": str(e),
                "job_id": job_id,
                "flow_id": flow_id
            }
    
    async def run_flow(self, flow_id: Optional[Union[uuid.UUID, str]], job_id: str, run_name: Optional[str] = None, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Run a flow execution.

        Args:
            flow_id: ID of the flow to run, or None for a dynamic flow
            job_id: Job ID for tracking execution
            run_name: Optional descriptive name for the execution
            config: Additional configuration

        Returns:
            Execution result
        """
        logger.info("="*100)
        logger.info("FLOW RUNNER SERVICE - run_flow() CALLED")
        logger.info(f"  flow_id: {flow_id}")
        logger.info(f"  job_id: {job_id}")
        logger.info(f"  run_name: {run_name}")
        if config:
            logger.info(f"  config type: {type(config)}")
            logger.info(f"  config keys: {list(config.keys())}")
            logger.info(f"  nodes: {len(config.get('nodes', []))}")
            logger.info(f"  edges: {len(config.get('edges', []))}")
            logger.info(f"  flow_config present: {'flow_config' in config}")
        logger.info("="*100)
        try:
            # Add detailed logging about inputs
            logger.info(f"run_flow called with flow_id={flow_id}, job_id={job_id}, run_name={run_name}")
            
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
                    flow = await self.flow_repo.get(flow_id)
                    if not flow:
                        logger.error(f"Flow with ID {flow_id} not found in database")
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"Flow with ID {flow_id} not found"
                        )

                    # Check group access if group_context is provided in config
                    group_context = config.get('group_context')
                    if flow.group_id and group_context:
                        # Extract group_ids from group_context
                        group_ids = getattr(group_context, 'group_ids', [])
                        if group_ids and flow.group_id not in group_ids:
                            logger.error(f"Access denied: Flow {flow_id} belongs to group {flow.group_id}, user has access to {group_ids}")
                            raise HTTPException(
                                status_code=status.HTTP_403_FORBIDDEN,
                                detail=f"Access denied to flow {flow_id}"
                            )

                    # Update the config with loaded data
                    config['nodes'] = flow.nodes
                    config['edges'] = flow.edges
                    config['flow_config'] = flow.flow_config

                    # Update local variables
                    nodes = flow.nodes
                    edges = flow.edges

                    logger.info(f"Loaded flow data from database: {len(nodes)} nodes, {len(edges)} edges")
                except HTTPException:
                    raise
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
            
            # Extract group_id from config for multi-tenant isolation
            group_id = config.get('group_id') if config else None

            # Create a sanitized config for database storage (remove non-serializable objects)
            sanitized_config = {k: v for k, v in config.items() if k != 'group_context'}

            # Create a flow execution record via service layer
            execution = await self.flow_execution_service.create_execution(
                flow_id=flow_id,  # None for ad-hoc executions, UUID for saved flows
                job_id=job_id,
                run_name=run_name,
                config=sanitized_config,
                group_id=group_id
            )
            logger.info(f"Created flow execution record with ID {execution.id} for group {group_id}")
            
            # Start the appropriate execution method based on flow_id
            # IMPORTANT: Use await instead of create_task to ensure subprocess waits for completion
            # This allows stdout capture in the finally block to get the actual CrewAI output
            if flow_id is not None:
                logger.info(f"Starting execution for existing flow {flow_id}")
                await self._run_flow_execution(execution.id, flow_id, job_id, config)
            else:
                logger.info(f"Starting execution for dynamic flow")
                await self._run_dynamic_flow(execution.id, job_id, config)

            return {
                "job_id": job_id,
                "execution_id": execution.id,
                "status": FlowExecutionStatus.COMPLETED,
                "message": "Flow execution completed"
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
        # Create a fresh database session for this background task
        async with async_session_factory() as session:
            # Create fresh service instance with the new session
            flow_execution_service = FlowExecutionService(session)

            try:
                logger.info(f"Starting dynamic flow execution {execution_id} for job {job_id}")

                # Update status to indicate we're preparing the flow
                await flow_execution_service.update_execution_status(
                    execution_id=execution_id,
                    status=FlowExecutionStatus.PREPARING
                )

                # Initialize API keys before execution
                try:
                    # SECURITY: Get group_id from config for multi-tenant isolation
                    group_id = config.get('group_id') if config else None

                    # Initialize all the API keys needed for execution
                    for provider in ["OPENAI", "ANTHROPIC", "PERPLEXITY", "SERPER"]:
                        try:
                            # Since this is an async method in a sync context, use sync approach
                            provider_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
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

                # Execute the flow directly using BackendFlow (do NOT call engine_service.run_flow() - that creates another subprocess)
                from src.engines.crewai.flow.backend_flow import BackendFlow
                from src.repositories.flow_repository import FlowRepository
                from src.repositories.task_repository import TaskRepository
                from src.repositories.agent_repository import AgentRepository
                from src.repositories.tool_repository import ToolRepository
                from src.repositories.crew_repository import CrewRepository

                # Initialize repositories for loading crew data from database
                flow_repo = FlowRepository(session)
                task_repo = TaskRepository(session)
                agent_repo = AgentRepository(session)
                tool_repo = ToolRepository(session)
                crew_repo = CrewRepository(session)

                # Initialize BackendFlow with the job_id (no flow_id for dynamic flows)
                backend_flow = BackendFlow(job_id=job_id, flow_id=None)
                backend_flow.repositories = {
                    'flow': flow_repo,
                    'task': task_repo,
                    'agent': agent_repo,
                    'tool': tool_repo,
                    'crew': crew_repo
                }

                # CRITICAL: For dynamic flows, we need to populate _flow_data from config
                # (not load from database since there's no flow_id)
                if config:
                    logger.info(f"Updating flow config with provided configuration")
                    backend_flow.config.update(config)

                    # Use the flow_config built by the frontend (buildFlowConfiguration utility)
                    # This contains listeners, actions, and startingPoints in the NEW simple format
                    flow_config = config.get('flow_config', {})
                    nodes = config.get('nodes', [])
                    edges = config.get('edges', [])

                    logger.info(f"Using flow_config from frontend with {len(flow_config.get('listeners', []))} listeners, "
                              f"{len(flow_config.get('actions', []))} actions, and {len(flow_config.get('startingPoints', []))} starting points")

                    backend_flow._flow_data = {
                        'id': None,
                        'name': f'Dynamic Flow {job_id[:8]}',
                        'crew_id': None,
                        'nodes': nodes,
                        'edges': edges,
                        'flow_config': flow_config
                    }
                    logger.info(f"Constructed flow_data for dynamic flow with {len(nodes)} nodes and {len(flow_config.get('listeners', []))} listeners")

                # Set up output directory for the flow
                output_dir = os.path.join(os.getenv('OUTPUT_DIR', 'output'), job_id)
                os.makedirs(output_dir, exist_ok=True)
                backend_flow.output_dir = output_dir
                logger.info(f"Set output directory to {output_dir}")

                # Update status to RUNNING
                await flow_execution_service.update_execution_status(
                    execution_id=execution_id,
                    status=FlowExecutionStatus.RUNNING
                )

                logger.info("="*100)
                logger.info(f"ABOUT TO EXECUTE DYNAMIC FLOW - execution_id: {execution_id}, job_id: {job_id}")
                logger.info("="*100)

                try:
                    # Execute the flow and get the result
                    logger.info("Calling backend_flow.kickoff() - THIS IS THE MAIN EXECUTION POINT")
                    result = await backend_flow.kickoff()
                    logger.info("="*100)
                    logger.info(f"FLOW EXECUTION COMPLETED - result: {result}")
                    logger.info("="*100)

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

                        await flow_execution_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.COMPLETED,
                            result=result_data
                        )
                        logger.info(f"Successfully completed dynamic flow execution {execution_id}")
                    else:
                        # Flow returned with success=False
                        error_msg = result.get("error", "Flow execution failed")
                        await flow_execution_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=error_msg
                        )
                        logger.error(f"Dynamic flow execution {execution_id} failed: {error_msg}")

                except Exception as kickoff_error:
                    logger.error(f"Error during backend_flow.kickoff() for dynamic flow {execution_id}: {kickoff_error}", exc_info=True)
                    await flow_execution_service.update_execution_status(
                        execution_id=execution_id,
                        status=FlowExecutionStatus.FAILED,
                        error=str(kickoff_error)
                    )

            except Exception as e:
                logger.error(f"Error running dynamic flow execution {execution_id}: {e}", exc_info=True)
                try:
                    # Update status to FAILED via service layer
                    await flow_execution_service.update_execution_status(
                        execution_id=execution_id,
                        status=FlowExecutionStatus.FAILED,
                        error=str(e)
                    )
                except Exception as update_error:
                    logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
    
    async def _get_required_providers(self, session: AsyncSession, config: Dict[str, Any], group_id: Optional[str] = None) -> List[str]:
        """
        Extract unique providers required for this flow execution based on configured models.

        Args:
            session: Database session to use for lookups
            config: Flow configuration containing model information
            group_id: Group ID for multi-tenant isolation

        Returns:
            List of unique provider names (uppercase) needed for this execution
        """
        providers = set()

        # Extract all model names from config
        model_names = []

        # Main model
        if 'model' in config:
            model_names.append(config['model'])

        # Check for crew config models
        crew_config = config.get('crew', {})
        if 'planning_llm' in crew_config:
            model_names.append(crew_config['planning_llm'])
        if 'reasoning_llm' in crew_config:
            model_names.append(crew_config['reasoning_llm'])
        if 'manager_llm' in crew_config:
            model_names.append(crew_config['manager_llm'])

        # Check for models in top-level config (alternative location)
        if 'planning_llm' in config:
            model_names.append(config['planning_llm'])
        if 'reasoning_llm' in config:
            model_names.append(config['reasoning_llm'])
        if 'manager_llm' in config:
            model_names.append(config['manager_llm'])

        logger.info(f"Extracted {len(model_names)} model references from config: {model_names}")

        # Get provider for each model using the provided session
        from src.services.model_config_service import ModelConfigService

        for model_name in model_names:
            if not model_name:
                continue

            try:
                # Use the existing session instead of creating a new UnitOfWork
                model_service = ModelConfigService(session, group_id=group_id)
                model_config = await model_service.get_model_config(model_name)

                if model_config and 'provider' in model_config:
                    provider = model_config['provider']
                    if provider:
                        # Convert provider to uppercase for consistency (OPENAI, ANTHROPIC, etc.)
                        provider_upper = provider.upper()
                        providers.add(provider_upper)
                        logger.info(f"Model '{model_name}' uses provider: {provider_upper}")
            except Exception as e:
                logger.warning(f"Could not determine provider for model '{model_name}': {e}")
                # Continue with other models rather than failing

        provider_list = list(providers)
        logger.info(f"Flow execution requires {len(provider_list)} unique providers: {provider_list}")

        return provider_list

    async def _run_flow_execution(self, execution_id: int, flow_id: Union[uuid.UUID, str], job_id: str, config: Dict[str, Any]) -> None:
        """
        Run a flow execution for an existing flow.

        Args:
            execution_id: ID of the flow execution record
            flow_id: ID of the flow to execute
            job_id: Job ID for tracking
            config: Additional configuration
        """
        # Create a fresh database session for this background task
        async with async_session_factory() as session:
            # Create fresh service and repository instances with the new session
            flow_execution_service = FlowExecutionService(session)
            flow_repo = FlowRepository(session)
            task_repo = TaskRepository(session)
            agent_repo = AgentRepository(session)
            tool_repo = ToolRepository(session)
            crew_repo = CrewRepository(session)

            # Convert string to UUID if needed
            if isinstance(flow_id, str):
                try:
                    flow_id = uuid.UUID(flow_id)
                except ValueError as e:
                    logger.error(f"Invalid UUID format for flow_id: {flow_id}")
                    # Update status to FAILED via service layer
                    await flow_execution_service.update_execution_status(
                        execution_id=execution_id,
                        status=FlowExecutionStatus.FAILED,
                        error=f"Invalid UUID format: {str(e)}"
                    )
                    return

            try:
                logger.info(f"Starting flow execution {execution_id} for flow {flow_id}, job {job_id}")

                # Update status to indicate we're preparing the flow
                await flow_execution_service.update_execution_status(
                    execution_id=execution_id,
                    status=FlowExecutionStatus.PREPARING
                )

                # Initialize API keys before execution
                try:
                    # SECURITY: Get group_id from config for multi-tenant isolation
                    group_id = config.get('group_id') if config else None

                    # Get only the providers actually needed for this flow's configured models
                    required_providers = await self._get_required_providers(session, config, group_id)

                    if not required_providers:
                        logger.warning("No providers identified from model configuration - flow may not have models configured")
                    else:
                        logger.info(f"Initializing API keys for {len(required_providers)} required providers: {required_providers}")

                    # Initialize only the API keys needed for the configured models
                    for provider in required_providers:
                        try:
                            provider_key = await ApiKeysService.get_provider_api_key(provider, group_id=group_id)
                            if not provider_key:
                                logger.warning(f"No API key found for provider: {provider} with group_id: {group_id}")
                            else:
                                # Set the environment variable for the provider
                                env_var_name = f"{provider}_API_KEY"
                                os.environ[env_var_name] = provider_key
                                logger.info(f"Set {env_var_name} for flow execution")
                        except Exception as key_error:
                            logger.warning(f"Error loading API key for {provider}: {key_error}")

                    logger.info(f"API keys initialized for {len(required_providers)} providers")
                except Exception as e:
                    logger.warning(f"Error initializing API keys: {e}")
                    # Continue with execution, as keys might be available through other means

                # Initialize BackendFlow with the flow_id and job_id
                backend_flow = BackendFlow(job_id=job_id, flow_id=flow_id)
                backend_flow.repositories = {
                    'flow': flow_repo,
                    'task': task_repo,
                    'agent': agent_repo,
                    'tool': tool_repo,
                    'crew': crew_repo
                }

                # Log what we have in the config BEFORE loading from database
                logger.info(f"[_run_flow_execution] Config keys before DB load: {list(config.keys())}")
                logger.info(f"[_run_flow_execution] Has nodes in config: {'nodes' in config and bool(config.get('nodes'))}")
                logger.info(f"[_run_flow_execution] Has flow_config in config: {'flow_config' in config}")
                if 'flow_config' in config:
                    logger.info(f"[_run_flow_execution] flow_config keys from frontend: {list(config.get('flow_config', {}).keys())}")

                # If this flow has no nodes/edges in the config, try to load them from the database
                if 'nodes' not in config or not config.get('nodes'):
                    logger.info(f"No nodes in config for flow {flow_id}, trying to load from database")
                    try:
                        # Load flow data using the BackendFlow instance, passing the repository
                        flow_data = await backend_flow.load_flow(repository=flow_repo)
                        logger.info(f"Loaded flow data for flow {flow_id}")

                        # Update config with flow data from DB
                        if 'nodes' in flow_data and flow_data['nodes']:
                            config['nodes'] = flow_data['nodes']
                            logger.info(f"Loaded {len(flow_data['nodes'])} nodes from flow data for flow {flow_id}")
                        if 'edges' in flow_data and flow_data['edges']:
                            config['edges'] = flow_data['edges']
                            logger.info(f"Loaded {len(flow_data['edges'])} edges from flow data for flow {flow_id}")
                        if 'flow_config' in flow_data and flow_data['flow_config']:
                            # Merge flow_config from DB with any flow_config from frontend
                            db_flow_config = flow_data['flow_config']
                            frontend_flow_config = config.get('flow_config', {})

                            # Prioritize frontend flow_config if it has startingPoints
                            # but MERGE listeners from database if they exist
                            if 'startingPoints' in frontend_flow_config:
                                logger.info(f"Using flow_config from frontend (has startingPoints)")
                                config['flow_config'] = frontend_flow_config

                                # CRITICAL: Merge listeners from database if frontend doesn't have them
                                if 'listeners' in db_flow_config and db_flow_config.get('listeners'):
                                    if 'listeners' not in config['flow_config'] or not config['flow_config'].get('listeners'):
                                        config['flow_config']['listeners'] = db_flow_config['listeners']
                                        logger.info(f"Merged {len(db_flow_config['listeners'])} listeners from database into flow_config")
                            else:
                                logger.info(f"Using flow_config from database")
                                config['flow_config'] = db_flow_config

                        # If we still don't have nodes, try direct database access as fallback
                        if 'nodes' not in config or not config.get('nodes'):
                            logger.warning(f"Failed to load nodes from BackendFlow for flow {flow_id}, trying direct database access")
                            # Get the flow from the database using repository
                            flow = flow_repo.find_by_id(flow_id)
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

                # CRITICAL: Ensure flow_config has startingPoints before execution
                # If flow_config is missing startingPoints, build them from nodes/edges
                if 'nodes' in config and 'edges' in config:
                    flow_config = config.get('flow_config', {})

                    if 'startingPoints' not in flow_config or not flow_config.get('startingPoints'):
                        logger.warning(f"flow_config missing startingPoints - building from nodes/edges")

                        # Identify starting nodes (nodes with no incoming edges)
                        nodes = config['nodes']
                        edges = config['edges']

                        node_ids = set(node['id'] for node in nodes)
                        target_node_ids = set(edge['target'] for edge in edges)
                        starting_node_ids = list(node_ids - target_node_ids)

                        logger.info(f"Identified {len(starting_node_ids)} starting nodes: {starting_node_ids}")

                        # Build startingPoints array
                        starting_points = []
                        for node_id in starting_node_ids:
                            node = next((n for n in nodes if n['id'] == node_id), None)
                            if node:
                                starting_points.append({
                                    'nodeId': node_id,
                                    'nodeType': node.get('type', 'unknown'),
                                    'nodeData': node.get('data', {})
                                })

                        # Update flow_config with startingPoints
                        if 'flow_config' not in config:
                            config['flow_config'] = {}

                        config['flow_config']['startingPoints'] = starting_points
                        config['flow_config']['nodes'] = nodes
                        config['flow_config']['edges'] = edges

                        logger.info(f"Built startingPoints for flow_config: {len(starting_points)} starting points")
                    else:
                        logger.info(f"flow_config already has {len(flow_config.get('startingPoints', []))} startingPoints")

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
                await flow_execution_service.update_execution_status(
                    execution_id=execution_id,
                    status=FlowExecutionStatus.RUNNING
                )

                logger.info("="*100)
                logger.info(f"ABOUT TO EXECUTE FLOW - execution_id: {execution_id}, flow_id: {flow_id}, job_id: {job_id}")
                logger.info("="*100)

                try:
                    # Execute the flow and get the result
                    logger.info("Calling backend_flow.kickoff() - THIS IS THE MAIN EXECUTION POINT")
                    result = await backend_flow.kickoff()
                    logger.info("="*100)
                    logger.info(f"FLOW EXECUTION COMPLETED - result: {result}")
                    logger.info("="*100)

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

                        await flow_execution_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.COMPLETED,
                            result=result_data
                        )
                    else:
                        await flow_execution_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=result.get("error", "Unknown error")
                        )

                    logger.info(f"Updated flow execution {execution_id} with final status")
                except Exception as kickoff_error:
                    logger.error(f"Error executing flow {flow_id}: {kickoff_error}", exc_info=True)
                    await flow_execution_service.update_execution_status(
                        execution_id=execution_id,
                        status=FlowExecutionStatus.FAILED,
                        error=str(kickoff_error)
                    )
            except Exception as e:
                logger.error(f"Error in flow execution {execution_id}: {e}", exc_info=True)
                try:
                    # Update status to FAILED via service layer
                    await flow_execution_service.update_execution_status(
                        execution_id=execution_id,
                        status=FlowExecutionStatus.FAILED,
                        error=str(e)
                    )
                except Exception as update_error:
                    logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
    
    async def get_flow_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get flow execution details.

        Args:
            execution_id: ID of the flow execution

        Returns:
            Dictionary with execution details
        """
        try:
            execution = await self.flow_execution_service.get_execution(execution_id)

            if not execution:
                return {
                    "success": False,
                    "error": f"Flow execution with ID {execution_id} not found"
                }

            # Get node executions if any
            nodes = await self.flow_execution_service.get_node_executions(execution_id)
            
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
    
    async def get_flow_executions_by_flow(self, flow_id: Union[uuid.UUID, str]) -> Dict[str, Any]:
        """
        Get all executions for a specific flow.

        Args:
            flow_id: ID of the flow

        Returns:
            Dictionary with list of executions
        """
        try:
            executions = await self.flow_execution_service.get_executions_by_flow(flow_id)
            
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
        logger.info(f"Found {len(listeners)} listeners in flow config")

        # Log complete flow configuration before execution
        logger.info("="*80)
        logger.info("FLOW CONFIGURATION BEFORE EXECUTION:")
        logger.info(f"  Flow ID: {flow_id}")
        logger.info(f"  Job ID: {job_id}")
        logger.info(f"  Total Crew Nodes: {len(crew_nodes)}")
        logger.info(f"  Starting Points ({len(starting_points)}):")
        for i, sp in enumerate(starting_points):
            logger.info(f"    [{i}] Crew: {sp.get('crewName')} (ID: {sp.get('crewId')})")
        logger.info(f"  Listeners ({len(listeners)}):")
        for i, listener in enumerate(listeners):
            listen_to = "start_flow" if i == 0 else f"listener_{i-1}"
            logger.info(f"    [{i}] Crew: {listener.get('crewName')} (ID: {listener.get('crewId')}) → Listens to: '{listen_to}'")
        logger.info("="*80)

        # Define a dynamic Flow class
        dynamic_flow_class = type('DynamicFlow', (Flow,), {})
        
        # Define the __init__ method
        def __init__(self, flow_id=None, job_id=None, config=None):
            super(dynamic_flow_class, self).__init__()
            self._flow_id = flow_id
            self._job_id = job_id
            self._config = config or {}
            self.crews = {}
            self._initialize_crews()
        
        # Define the _initialize_crews method with improved agent and task configuration
        def _initialize_crews(self):
            try:
                # Import agent/task services
                from src.services.agent_service import AgentService
                from src.services.task_service import TaskService
                from src.services.crew_service import CrewService
                from src.engines.crewai.tools.tool_factory import ToolFactory

                # Create a tool factory instance for tool creation
                # Pass an empty config dict as required by ToolFactory
                tool_factory = ToolFactory(config={})
                
                # TODO: Convert to async - temporarily disabled SessionLocal usage
                # Need to refactor this to use async patterns properly
                if False:  # Temporarily disabled
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
                                        # Note: CrewAI does not have Process.parallel
                                        # Use hierarchical for delegation or async_execution for task-level parallelism
                                    
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
            logger.info(f"Starting flow execution for job {self._job_id}")
            
            # Initialize state with flow_id and job_id for tracking
            self.state["flow_id"] = str(self._flow_id) if self._flow_id else "dynamic-flow"
            self.state["job_id"] = self._job_id
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
        
        # Add listener methods - chain them sequentially
        logger.info(f"Setting up {len(listeners)} listeners for sequential flow execution")
        for i, listener in enumerate(listeners):
            crew_id = listener.get('crewId')
            crew_name = listener.get('crewName')

            # Determine what this listener should listen to:
            # - First listener (i=0) listens to "start_flow"
            # - Subsequent listeners listen to the previous listener
            listen_to = "start_flow" if i == 0 else f"listener_{i-1}"

            logger.info(f"Listener {i} for crew '{crew_name}' will listen to event: '{listen_to}'")

            # Define the listener method
            def make_listener_method(crew_id, crew_name, listen_to_event, method_name):
                @listen(listen_to_event)
                def listener_method(self, result):
                    logger.info(f"✓ Listener triggered for crew '{crew_name}' (listening to '{listen_to_event}')")
                    crew = self.crews.get(str(crew_id))
                    if crew:
                        try:
                            logger.info(f"▶ Executing listener crew '{crew_name}'")
                            self.state["previous_result"] = result
                            listener_result = crew.kickoff()
                            logger.info(f"✓ Listener crew '{crew_name}' execution completed")
                            return listener_result
                        except Exception as e:
                            logger.error(f"✗ Error executing listener crew '{crew_name}': {str(e)}")
                            return {"error": str(e)}
                    else:
                        logger.error(f"✗ Listener crew {crew_id} not found")
                        return {"error": f"Crew {crew_id} not found"}

                # CRITICAL: Set __name__ to match method_name so CrewAI Flow emits the correct event
                listener_method.__name__ = method_name
                return listener_method

            method_name = f"listener_{i}"
            setattr(dynamic_flow_class, method_name, make_listener_method(crew_id, crew_name, listen_to, method_name))
        
        # Create and return an instance
        flow_instance = dynamic_flow_class(flow_id=flow_id, job_id=job_id, config=config)
        logger.info(f"Created dynamic flow instance for job {job_id}")
        return flow_instance 