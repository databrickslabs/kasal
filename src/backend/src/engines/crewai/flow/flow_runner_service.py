"""
Service for running Flow executions with CrewAI Flow.

This file contains the FlowRunnerService which handles running flow executions in the system.
It uses the BackendFlow class (from backend_flow.py) to interact with the CrewAI Flow engine.
"""
import os
import logging
import asyncio
import uuid
from contextlib import asynccontextmanager
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
from src.repositories.execution_history_repository import ExecutionHistoryRepository
from src.repositories.execution_trace_repository import ExecutionTraceRepository
from src.core.logger import LoggerManager
from src.db.session import async_session_factory
from src.services.api_keys_service import ApiKeysService
from src.engines.crewai.flow.backend_flow import BackendFlow
from src.engines.crewai.flow.exceptions import FlowPausedForApprovalException

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

    async def _emit_error_span(self, job_id: str, error_msg: str, group_id: Optional[str] = None, group_email: Optional[str] = None):
        """Emit an error span via OTel so it appears in the trace timeline.

        Routes through the same OTel pipeline (TracerProvider → KasalDBSpanExporter)
        as engine-level spans, so the error gets proper span_id / trace_id columns
        and a consistent representation in the trace timeline.
        """
        try:
            from types import SimpleNamespace
            from opentelemetry.sdk.trace.export import SimpleSpanProcessor
            from opentelemetry.trace import StatusCode
            from src.services.otel_tracing.otel_config import create_kasal_tracer_provider
            from src.services.otel_tracing.db_exporter import KasalDBSpanExporter

            group_context = None
            if group_id or group_email:
                group_context = SimpleNamespace(primary_group_id=group_id, group_email=group_email)

            provider = create_kasal_tracer_provider(
                job_id=job_id,
                service_name="kasal-flow-runner",
            )
            provider.add_span_processor(
                SimpleSpanProcessor(KasalDBSpanExporter(job_id, group_context))
            )

            tracer = provider.get_tracer("kasal.flow_runner")
            with tracer.start_as_current_span("kasal.flow.execution_failed") as span:
                span.set_attribute("kasal.event_type", "flow_execution_failed")
                span.set_attribute("kasal.job_id", job_id)
                span.set_attribute("kasal.task_name", "Flow execution error")
                span.set_attribute("kasal.output_content", error_msg)
                span.set_attribute("kasal.extra.error", error_msg)
                if group_id:
                    span.set_attribute("kasal.group_id", group_id)
                if group_email:
                    span.set_attribute("kasal.group_email", group_email)
                span.set_status(StatusCode.ERROR, error_msg)

            provider.shutdown()
            logger.info(f"Emitted OTel error span for job {job_id}: {error_msg[:200]}")
        except Exception as span_err:
            logger.warning(f"Failed to emit OTel error span for job {job_id}: {span_err}")

    @staticmethod
    @asynccontextmanager
    async def _safe_session():
        """Create a session with safe cleanup that suppresses stale-connection errors.

        After long-running operations (like CrewAI kickoff), SQLite+aiosqlite sessions
        can lose their greenlet context, causing MissingGreenlet errors during cleanup.
        This context manager suppresses those cleanup errors to prevent them from
        corrupting successful execution results (overwriting COMPLETED with FAILED).
        """
        session = async_session_factory()
        try:
            yield session
        finally:
            try:
                await session.close()
            except Exception as close_err:
                logger.debug(f"Session cleanup error suppressed (expected after long execution): {close_err}")

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

            # Check if this is a resume scenario - if so, use existing execution record
            resume_from_execution_id = config.get('resume_from_execution_id') if config else None

            if resume_from_execution_id:
                # RESUME SCENARIO: Reuse existing execution record
                logger.info(f"🔄 RESUME: Reusing existing execution for job_id={resume_from_execution_id}")

                # Get the existing execution record by job_id
                exec_repo = ExecutionHistoryRepository(self.db)
                existing_execution = await exec_repo.get_execution_by_job_id(resume_from_execution_id)

                if existing_execution:
                    execution = existing_execution
                    # Update status to RUNNING for the resume
                    execution.status = FlowExecutionStatus.RUNNING.value
                    await self.db.commit()
                    logger.info(f"🔄 RESUME: Found existing execution with ID {execution.id}, status set to RUNNING")
                else:
                    logger.error(f"🔄 RESUME: Could not find execution for job_id={resume_from_execution_id}")
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Execution not found for resume: {resume_from_execution_id}"
                    )
            else:
                # NEW EXECUTION: Create a flow execution record via service layer
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
            flow_result = None
            if flow_id is not None:
                logger.info(f"Starting execution for existing flow {flow_id}")
                flow_result = await self._run_flow_execution(execution.id, flow_id, job_id, config)
            else:
                logger.info(f"Starting execution for dynamic flow")
                flow_result = await self._run_dynamic_flow(execution.id, job_id, config)

            # Return the actual flow result instead of just a status message
            if flow_result and flow_result.get("success"):
                # Check if flow was paused for HITL approval - pass through the result as-is
                if flow_result.get("hitl_paused") or flow_result.get("paused_for_approval"):
                    logger.info(f"🚦 run_flow: Passing through HITL pause result for {job_id}")
                    return flow_result  # Return the HITL pause result directly

                return_dict = {
                    "job_id": job_id,
                    "execution_id": execution.id,
                    "status": FlowExecutionStatus.COMPLETED,
                    "result": flow_result.get("result"),
                    "message": "Flow execution completed"
                }
                # Include flow_uuid for checkpoint/resume functionality
                if flow_result.get("flow_uuid"):
                    return_dict["flow_uuid"] = flow_result.get("flow_uuid")
                return return_dict
            else:
                error_msg = flow_result.get("error", "Unknown error") if flow_result else "No result returned"
                return {
                    "job_id": job_id,
                    "execution_id": execution.id,
                    "status": FlowExecutionStatus.FAILED,
                    "error": error_msg,
                    "message": f"Flow execution failed: {error_msg}"
                }
        except HTTPException:
            # Re-raise HTTPException as-is to preserve status codes (404, 400, etc.)
            raise
        except Exception as e:
            logger.error(f"Error running flow execution: {e}", exc_info=True)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error running flow execution: {str(e)}"
            )
    
    async def _run_dynamic_flow(self, execution_id: int, job_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run a dynamic flow execution created from the configuration.

        Args:
            execution_id: ID of the flow execution record
            job_id: Job ID for tracking
            config: Configuration containing nodes, edges, and flow configuration

        Returns:
            Dict containing the flow execution result with 'success', 'result' or 'error' keys
        """
        # Create a session with safe cleanup to prevent stale-connection errors
        # from corrupting successful results after long-running kickoff()
        async with self._safe_session() as session:
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
                execution_history_repo = ExecutionHistoryRepository(session)
                execution_trace_repo = ExecutionTraceRepository(session)

                # Initialize BackendFlow with the job_id (no flow_id for dynamic flows)
                backend_flow = BackendFlow(job_id=job_id, flow_id=None)
                backend_flow.repositories = {
                    'flow': flow_repo,
                    'task': task_repo,
                    'agent': agent_repo,
                    'tool': tool_repo,
                    'crew': crew_repo,
                    'execution_history': execution_history_repo,
                    'execution_trace': execution_trace_repo
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

                    # CRITICAL: Create a fresh session for post-execution DB updates.
                    # The original session may have a stale/dead SQLite connection after
                    # the long-running kickoff() (minutes to hours). This prevents
                    # "(sqlite3.OperationalError) no active connection" errors.
                    async with async_session_factory() as post_session:
                        post_flow_service = FlowExecutionService(post_session)

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

                            await post_flow_service.update_execution_status(
                                execution_id=execution_id,
                                status=FlowExecutionStatus.COMPLETED,
                                result=result_data
                            )

                            # Save checkpoint info if flow_uuid is available (from @persist)
                            flow_uuid = result.get("flow_uuid")
                            if flow_uuid:
                                try:
                                    from src.services.execution_history_service import ExecutionHistoryService
                                    history_service = ExecutionHistoryService(post_session)
                                    await history_service.set_checkpoint_active(
                                        execution_id=execution_id,
                                        flow_uuid=flow_uuid,
                                        checkpoint_method="flow_complete"
                                    )
                                    logger.info(f"Saved checkpoint info for execution {execution_id} with flow_uuid {flow_uuid}")
                                except Exception as checkpoint_err:
                                    logger.warning(f"Could not save checkpoint info: {checkpoint_err}")

                            logger.info(f"Successfully completed dynamic flow execution {execution_id}")
                            return_dict = {"success": True, "result": result_data, "execution_id": execution_id}
                            # Include flow_uuid for checkpoint/resume functionality
                            if flow_uuid:
                                return_dict["flow_uuid"] = flow_uuid
                            return return_dict
                        else:
                            # Flow returned with success=False
                            error_msg = result.get("error", "Flow execution failed")
                            await post_flow_service.update_execution_status(
                                execution_id=execution_id,
                                status=FlowExecutionStatus.FAILED,
                                error=error_msg
                            )
                            await self._emit_error_span(job_id, error_msg, group_id=config.get('group_id'))
                            logger.error(f"Dynamic flow execution {execution_id} failed: {error_msg}")
                            return {"success": False, "error": error_msg, "execution_id": execution_id}

                except FlowPausedForApprovalException as pause_exc:
                    # Flow paused at HITL gate - this is not an error, it's a controlled pause
                    logger.info(f"🚦 Flow paused for approval at gate {pause_exc.gate_node_id}")
                    logger.info(f"   Approval ID: {pause_exc.approval_id}")
                    logger.info(f"   Execution ID: {pause_exc.execution_id}")
                    logger.info(f"   Crew sequence: {pause_exc.crew_sequence}")

                    # Fresh session for HITL status update
                    async with async_session_factory() as hitl_session:
                        hitl_flow_service = FlowExecutionService(hitl_session)

                        # Update execution status to WAITING_FOR_APPROVAL
                        await hitl_flow_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.WAITING_FOR_APPROVAL,  # HITL pause
                            result={
                                "hitl_paused": True,
                                "approval_id": pause_exc.approval_id,
                                "gate_node_id": pause_exc.gate_node_id,
                                "message": pause_exc.message,
                                "crew_sequence": pause_exc.crew_sequence
                            }
                        )

                        # Save checkpoint info for resume (resume data is in result field)
                        logger.info(f"🔍 HITL checkpoint save check for execution {execution_id} [run_flow_internal]")
                        logger.info(f"   pause_exc.flow_uuid: {pause_exc.flow_uuid}")
                        if pause_exc.flow_uuid:
                            try:
                                from src.services.execution_history_service import ExecutionHistoryService
                                history_service = ExecutionHistoryService(hitl_session)
                                await history_service.set_checkpoint_active(
                                    execution_id=execution_id,
                                    flow_uuid=pause_exc.flow_uuid,
                                    checkpoint_method="hitl_gate_pause"
                                )
                                logger.info(f"✅ Saved HITL checkpoint for execution {execution_id} with flow_uuid={pause_exc.flow_uuid}")
                            except Exception as checkpoint_err:
                                logger.error(f"❌ Could not save HITL checkpoint info: {checkpoint_err}", exc_info=True)
                        else:
                            logger.warning(f"⚠️ No flow_uuid available - checkpoint NOT saved! Resume dialog will not work.")

                    return {
                        "success": True,  # Not a failure - controlled pause
                        "paused_for_approval": True,
                        "hitl_paused": True,  # Critical: process_flow_executor.py checks this key
                        "approval_id": pause_exc.approval_id,
                        "gate_node_id": pause_exc.gate_node_id,
                        "message": pause_exc.message,
                        "execution_id": execution_id,
                        "flow_uuid": pause_exc.flow_uuid,
                        "crew_sequence": pause_exc.crew_sequence
                    }

                except Exception as kickoff_error:
                    logger.error(f"Error during backend_flow.kickoff() for dynamic flow {execution_id}: {kickoff_error}", exc_info=True)
                    # Fresh session for error status update
                    async with async_session_factory() as err_session:
                        err_flow_service = FlowExecutionService(err_session)
                        await err_flow_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=str(kickoff_error)
                        )
                    await self._emit_error_span(job_id, str(kickoff_error), group_id=config.get('group_id'))
                    return {"success": False, "error": str(kickoff_error), "execution_id": execution_id}

            except FlowPausedForApprovalException as pause_exc:
                # Re-raise to handle at top level (shouldn't happen but just in case)
                raise

            except Exception as e:
                logger.error(f"Error running dynamic flow execution {execution_id}: {e}", exc_info=True)
                try:
                    # Fresh session for outer error status update
                    async with async_session_factory() as outer_err_session:
                        outer_err_service = FlowExecutionService(outer_err_session)
                        await outer_err_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=str(e)
                        )
                except Exception as update_error:
                    logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
                await self._emit_error_span(job_id, str(e), group_id=config.get('group_id'))
                return {"success": False, "error": str(e), "execution_id": execution_id}

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

    async def _run_flow_execution(self, execution_id: int, flow_id: Union[uuid.UUID, str], job_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Run a flow execution for an existing flow.

        Args:
            execution_id: ID of the flow execution record
            flow_id: ID of the flow to execute
            job_id: Job ID for tracking
            config: Additional configuration

        Returns:
            Dict containing the flow execution result with 'success', 'result' or 'error' keys
        """
        # Create a session with safe cleanup to prevent stale-connection errors
        # from corrupting successful results after long-running kickoff()
        async with self._safe_session() as session:
            # Create fresh service and repository instances with the new session
            flow_execution_service = FlowExecutionService(session)
            flow_repo = FlowRepository(session)
            task_repo = TaskRepository(session)
            agent_repo = AgentRepository(session)
            tool_repo = ToolRepository(session)
            crew_repo = CrewRepository(session)
            execution_history_repo = ExecutionHistoryRepository(session)
            execution_trace_repo = ExecutionTraceRepository(session)

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
                    return {"success": False, "error": f"Invalid UUID format: {str(e)}"}

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
                    'crew': crew_repo,
                    'execution_history': execution_history_repo,
                    'execution_trace': execution_trace_repo
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

                    # CRITICAL: Create a fresh session for post-execution DB updates.
                    # The original session may have a stale/dead SQLite connection after
                    # the long-running kickoff() (minutes to hours). This prevents
                    # "(sqlite3.OperationalError) no active connection" errors.
                    async with async_session_factory() as post_session:
                        post_flow_service = FlowExecutionService(post_session)

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

                            await post_flow_service.update_execution_status(
                                execution_id=execution_id,
                                status=FlowExecutionStatus.COMPLETED,
                                result=result_data
                            )

                            # Save checkpoint info if flow_uuid is available (from @persist)
                            flow_uuid = result.get("flow_uuid")
                            if flow_uuid:
                                try:
                                    from src.services.execution_history_service import ExecutionHistoryService
                                    history_service = ExecutionHistoryService(post_session)
                                    await history_service.set_checkpoint_active(
                                        execution_id=execution_id,
                                        flow_uuid=flow_uuid,
                                        checkpoint_method="flow_complete"
                                    )
                                    logger.info(f"Saved checkpoint info for execution {execution_id} with flow_uuid {flow_uuid}")
                                except Exception as checkpoint_err:
                                    logger.warning(f"Could not save checkpoint info: {checkpoint_err}")

                            logger.info(f"Updated flow execution {execution_id} with final status: COMPLETED")
                            return_dict = {"success": True, "result": result_data, "execution_id": execution_id}
                            # Include flow_uuid for checkpoint/resume functionality
                            if flow_uuid:
                                return_dict["flow_uuid"] = flow_uuid
                            return return_dict
                        else:
                            error_msg = result.get("error", "Unknown error")
                            await post_flow_service.update_execution_status(
                                execution_id=execution_id,
                                status=FlowExecutionStatus.FAILED,
                                error=error_msg
                            )
                            await self._emit_error_span(job_id, error_msg, group_id=config.get('group_id'))
                            logger.info(f"Updated flow execution {execution_id} with final status: FAILED")
                            return {"success": False, "error": error_msg, "execution_id": execution_id}

                except FlowPausedForApprovalException as pause_exc:
                    # Flow paused at HITL gate - this is not an error, it's a controlled pause
                    logger.info(f"🚦 Flow paused for approval at gate {pause_exc.gate_node_id}")
                    logger.info(f"   Approval ID: {pause_exc.approval_id}")
                    logger.info(f"   Execution ID: {pause_exc.execution_id}")
                    logger.info(f"   Crew sequence: {pause_exc.crew_sequence}")

                    # Fresh session for HITL status update
                    async with async_session_factory() as hitl_session:
                        hitl_flow_service = FlowExecutionService(hitl_session)

                        # Update execution status to WAITING_FOR_APPROVAL
                        await hitl_flow_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.WAITING_FOR_APPROVAL,
                            result={
                                "hitl_paused": True,
                                "approval_id": pause_exc.approval_id,
                                "gate_node_id": pause_exc.gate_node_id,
                                "message": pause_exc.message,
                                "crew_sequence": pause_exc.crew_sequence
                            }
                        )

                        # Save checkpoint info for resume (resume data is in result field)
                        logger.info(f"🔍 HITL checkpoint save check for execution {execution_id} [run_flow]")
                        logger.info(f"   pause_exc.flow_uuid: {pause_exc.flow_uuid}")
                        if pause_exc.flow_uuid:
                            try:
                                from src.services.execution_history_service import ExecutionHistoryService
                                history_service = ExecutionHistoryService(hitl_session)
                                await history_service.set_checkpoint_active(
                                    execution_id=execution_id,
                                    flow_uuid=pause_exc.flow_uuid,
                                    checkpoint_method="hitl_gate_pause"
                                )
                                logger.info(f"✅ Saved HITL checkpoint for execution {execution_id} with flow_uuid={pause_exc.flow_uuid}")
                            except Exception as checkpoint_err:
                                logger.error(f"❌ Could not save HITL checkpoint info: {checkpoint_err}", exc_info=True)
                        else:
                            logger.warning(f"⚠️ No flow_uuid available - checkpoint NOT saved! Resume dialog will not work.")

                    return {
                        "success": True,
                        "paused_for_approval": True,
                        "approval_id": pause_exc.approval_id,
                        "gate_node_id": pause_exc.gate_node_id,
                        "message": pause_exc.message,
                        "execution_id": execution_id,
                        "flow_uuid": pause_exc.flow_uuid,
                        "crew_sequence": pause_exc.crew_sequence
                    }

                except Exception as kickoff_error:
                    logger.error(f"Error executing flow {flow_id}: {kickoff_error}", exc_info=True)
                    # Fresh session for error status update
                    async with async_session_factory() as err_session:
                        err_flow_service = FlowExecutionService(err_session)
                        await err_flow_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=str(kickoff_error)
                        )
                    await self._emit_error_span(job_id, str(kickoff_error), group_id=config.get('group_id'))
                    return {"success": False, "error": str(kickoff_error), "execution_id": execution_id}

            except FlowPausedForApprovalException:
                # Re-raise to handle at caller level
                raise

            except Exception as e:
                logger.error(f"Error in flow execution {execution_id}: {e}", exc_info=True)
                try:
                    # Fresh session for outer error status update
                    async with async_session_factory() as outer_err_session:
                        outer_err_service = FlowExecutionService(outer_err_session)
                        await outer_err_service.update_execution_status(
                            execution_id=execution_id,
                            status=FlowExecutionStatus.FAILED,
                            error=str(e)
                        )
                except Exception as update_error:
                    logger.error(f"Error updating flow execution {execution_id} status: {update_error}", exc_info=True)
                await self._emit_error_span(job_id, str(e), group_id=config.get('group_id'))
                return {"success": False, "error": str(e), "execution_id": execution_id}

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