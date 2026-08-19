"""Execution service for managing AI agent workflow executions.

This module provides the core service layer for managing execution operations
in the AI agent system. It handles flow execution, status tracking, and
coordination between different execution engines.

Key Features:
    - Asynchronous flow execution with job tracking
    - Thread pool management for concurrent operations
    - Integration with CrewAI execution engine
    - Automatic execution name generation
    - Status monitoring and error handling

The service acts as the main orchestrator for all execution-related operations,
delegating specific tasks to specialized services while maintaining a unified
interface for the API layer.

Example:
    >>> service = ExecutionService()
    >>> result = await service.execute_flow(
    ...     flow_id=flow_uuid,
    ...     job_id="job_123",
    ...     config={"timeout": 300}
    ... )
"""

import asyncio
import concurrent.futures
import copy
import json
import logging
import os
import sys
import traceback
import uuid
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.orm import Session

from src.core.exceptions import KasalError
from src.core.logger import LoggerManager
from src.schemas.execution import (
    CrewConfig,
    ExecutionCreateResponse,
    ExecutionNameGenerationRequest,
    ExecutionStatus,
)
from src.services.execution.kasal_service import KasalExecutionService
from src.services.execution.naming import ExecutionNameService
from src.services.execution.status import ExecutionStatusService
from src.utils.asyncio_utils import create_and_run_loop, run_in_thread_with_loop
from src.utils.sensitive_data_utils import mask_sensitive_fields
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)
crew_logger = LoggerManager.get_instance().crew
exec_logger = LoggerManager.get_instance().crew


class ExecutionService:
    """High-level service for orchestrating AI agent workflow executions.

    This service provides the main interface for executing flows, managing
    execution lifecycles, and coordinating between different execution engines.
    It maintains a thread pool for concurrent operations and tracks active
    executions across the system.

    Attributes:
        executions: Class-level dictionary tracking all active executions
        _thread_pool: Thread pool executor for concurrent operations (10 workers)
        execution_name_service: Service for generating descriptive execution names
        kasal_execution_service: Service for CrewAI-specific execution logic

    Note:
        The service uses class-level attributes for shared state across instances,
        enabling centralized execution tracking in a multi-threaded environment.
    """

    # Initialize the executions dictionary as a class attribute
    executions = {}

    # Initialize the thread pool executor
    _thread_pool = concurrent.futures.ThreadPoolExecutor(max_workers=10)

    def __init__(self, session=None):
        """Initialize the ExecutionService with required dependencies.

        Args:
            session: Optional database session for repository operations.
                     If provided, repositories will use this session instead
                     of creating their own.

        Sets up the execution name service for generating descriptive names
        and the CrewAI execution service for handling CrewAI-specific operations.

        Note:
            Uses factory methods to ensure proper configuration of dependent services.
        """
        # Store the session for repository operations
        self.session = session

        # Use factory method to create properly configured ExecutionNameService
        self.execution_name_service = ExecutionNameService.create(session)
        # Create a KasalExecutionService instance for all execution operations
        self.kasal_execution_service = KasalExecutionService()

    def _mask_inputs_sensitive_data(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mask sensitive fields in execution inputs before returning to API or storing to database.

        This method processes the inputs dictionary which may contain:
        - agents_yaml: Dict of agent configs with tool_configs containing secrets
        - tasks_yaml: Dict of task configs with tool_configs containing secrets
        - inputs: Dict of user-provided inputs that may contain secrets (client_secret, etc.)

        Args:
            inputs: The raw inputs dictionary from execution

        Returns:
            A copy of inputs with sensitive fields masked
        """
        if not inputs:
            return inputs

        # Create a deep copy to avoid modifying the original
        masked_inputs = copy.deepcopy(inputs)

        # Mask tool_configs in agents_yaml
        if "agents_yaml" in masked_inputs and isinstance(
            masked_inputs["agents_yaml"], dict
        ):
            for agent_key, agent_config in masked_inputs["agents_yaml"].items():
                if isinstance(agent_config, dict) and "tool_configs" in agent_config:
                    agent_config["tool_configs"] = mask_sensitive_fields(
                        agent_config["tool_configs"]
                    )

        # Mask tool_configs in tasks_yaml
        if "tasks_yaml" in masked_inputs and isinstance(
            masked_inputs["tasks_yaml"], dict
        ):
            for task_key, task_config in masked_inputs["tasks_yaml"].items():
                if isinstance(task_config, dict) and "tool_configs" in task_config:
                    task_config["tool_configs"] = mask_sensitive_fields(
                        task_config["tool_configs"]
                    )

        # Mask sensitive fields in the nested 'inputs' dictionary (user-provided values)
        # This catches fields like client_secret, password, token, api_key, etc.
        if "inputs" in masked_inputs and isinstance(masked_inputs["inputs"], dict):
            masked_inputs["inputs"] = mask_sensitive_fields(masked_inputs["inputs"])

        return masked_inputs

    async def execute_flow(
        self,
        flow_id: Optional[uuid.UUID] = None,
        nodes: Optional[List[Dict[str, Any]]] = None,
        edges: Optional[List[Dict[str, Any]]] = None,
        job_id: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Execute a flow asynchronously with job tracking.

        Orchestrates the execution of either a saved flow (by ID) or a dynamic
        flow (by nodes/edges). Generates a job ID if not provided and delegates
        to the CrewAI execution service for actual processing.

        Args:
            flow_id: Optional UUID of a saved flow to execute. If provided,
                the flow definition will be loaded from storage.
            nodes: Optional list of node definitions for dynamic flow execution.
                Each node represents an agent or task in the workflow.
            edges: Optional list of edge definitions connecting nodes.
                Defines the execution order and dependencies.
            job_id: Optional unique identifier for tracking this execution.
                Auto-generated if not provided.
            config: Optional configuration dictionary with execution parameters
                such as timeout, retry settings, or environment variables.

        Returns:
            Dictionary containing execution result with keys:
                - job_id: The execution job identifier
                - status: Current execution status
                - result: Execution output (when completed)
                - error: Error details (if failed)

        Raises:
            HTTPException: Re-raised from underlying services for HTTP errors
            HTTPException(500): For unexpected errors during execution

        Example:
            >>> result = await service.execute_flow(
            ...     flow_id=uuid.UUID("123e4567-e89b-12d3-a456-426614174000"),
            ...     config={"timeout": 300, "max_retries": 3}
            ... )
        """
        logger.info(f"Executing flow with ID: {flow_id}, job_id: {job_id}")

        try:
            # If no job_id is provided, generate a random UUID
            if not job_id:
                job_id = str(uuid.uuid4())
                logger.info(f"Generated random job_id: {job_id}")

            # Prepare the execution config
            execution_config = config or {}

            # Delegate to KasalExecutionService for flow execution
            logger.info("Delegating flow execution to KasalExecutionService")
            result = await self.kasal_execution_service.run_flow_execution(
                flow_id=str(flow_id) if flow_id else None,
                nodes=nodes,
                edges=edges,
                job_id=job_id,
                config=execution_config,
            )
            logger.info(f"Flow execution started successfully: {result}")
            return result
        except KasalError:
            # Re-raise Kasal exceptions
            raise
        except Exception as e:
            error_msg = f"Unexpected error in execute_flow: {str(e)}"
            logger.error(error_msg, exc_info=True)
            raise KasalError(detail=error_msg)

    async def get_run_by_job_id(
        self, job_id: str, group_ids: Optional[List[str]] = None
    ) -> Optional[Any]:
        """The ``ExecutionHistory`` row for a job id, scoped to ``group_ids``.

        The most-wanted read in the codebase: flow_builder, agent_builder, mlflow
        and hitl all needed it and each built ``ExecutionHistoryRepository`` itself.
        Runs are this service's domain, so the accessor belongs here — and having one
        means the group filter is applied in one place instead of five.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_run_by_job_id")
        return await ExecutionHistoryRepository(session).get_execution_by_job_id(
            job_id, group_ids=group_ids
        )

    async def get_run_by_id(
        self, execution_id: int, group_ids: Optional[List[str]] = None
    ) -> Optional[Any]:
        """The ``ExecutionHistory`` row for an integer id, scoped when asked.

        ``group_ids`` is optional because two kinds of caller share this: internal
        resume paths that already hold a group-checked row, and the trace service,
        which authorizes by passing the caller's groups. PASS IT whenever the id
        came from outside — omitting it returns any tenant's run.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_run_by_id")
        repository = ExecutionHistoryRepository(session)
        if group_ids is None:
            return await repository.find_by_id(execution_id)
        return await repository.get_execution_by_id(execution_id, group_ids=group_ids)

    async def get_run_summary_by_job_id(
        self, job_id: str, group_ids: Optional[List[str]] = None
    ) -> Optional[Any]:
        """Scalar-only run lookup for HOT paths (SSE polling, trace authorization).

        The full-row variant drags result/inputs/checkpoint JSON through the driver
        on every poll just to authorize access.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_run_summary_by_job_id")
        return await ExecutionHistoryRepository(
            session
        ).get_execution_summary_by_job_id(job_id, group_ids=group_ids)

    async def get_job_ids_for_groups(self, group_ids: List[str]) -> List[str]:
        """Job ids belonging to any of these groups.

        Used by bulk cleanup — deleting every trace a workspace owns needs the run
        ids first. The trace service called a NON-EXISTENT
        ``get_all_executions_for_groups`` on the repository for this, so
        ``delete_all_traces_for_group`` raised AttributeError and deleted nothing;
        its router test mocked the whole method, so nothing caught it.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_job_ids_for_groups")
        return await ExecutionHistoryRepository(session).get_job_ids_for_groups(
            group_ids
        )

    async def get_job_ids_with_statuses(self, statuses: List[str]) -> List[str]:
        """Job ids currently in any of ``statuses`` — for the SSE poller."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_job_ids_with_statuses")
        return await ExecutionHistoryRepository(session).get_job_ids_by_statuses(
            statuses
        )

    async def get_recent_runs(
        self, limit: int, status: Optional[str] = None
    ) -> List[Any]:
        """The most recent runs, newest first, optionally filtered to one status."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_recent_runs")
        return await ExecutionHistoryRepository(session).get_recent(
            limit=limit, status=status
        )

    async def latest_checkpoint_containing(self, key: str) -> Optional[dict]:
        """Most recent run whose ``checkpoint_data`` holds ``key``.

        For the UCMV tools, which look for edits a user saved in an earlier step of
        a multi-step flow.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("latest_checkpoint_containing")
        return await ExecutionHistoryRepository(session).latest_checkpoint_containing(
            key
        )

    async def latest_result_with_keys(self, keys: List[str]) -> Optional[dict]:
        """Most recent run whose ``result`` dict holds ALL of ``keys``."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("latest_result_with_keys")
        return await ExecutionHistoryRepository(session).latest_result_with_keys(keys)

    async def get_run_of_type(
        self, execution_id: int, execution_type: str
    ) -> Optional[Any]:
        """A run by id, only if it is of ``execution_type`` ("flow"/"crew"/"agent")."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_run_of_type")
        return await ExecutionHistoryRepository(session).get_by_id_and_type(
            execution_id, execution_type
        )

    async def get_run_of_type_by_job_id(
        self, job_id: str, execution_type: str
    ) -> Optional[Any]:
        """A run by job id, only if it is of ``execution_type``."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_run_of_type_by_job_id")
        return await ExecutionHistoryRepository(session).get_by_job_id_and_type(
            job_id, execution_type
        )

    async def get_runs_of_type_for_flow(
        self, flow_id: Any, execution_type: str
    ) -> List[Any]:
        """Every run of ``execution_type`` belonging to one flow."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_runs_of_type_for_flow")
        return await ExecutionHistoryRepository(session).get_by_flow_id_and_type(
            flow_id, execution_type
        )

    async def reload_run(self, run: Any) -> Any:
        """Re-read a run after commit so server-side defaults are populated."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("reload_run")
        return await ExecutionHistoryRepository(session).reload(run)

    async def delete_run(self, run: Any, commit: bool = True) -> None:
        """Delete one run row."""
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("delete_run")
        await ExecutionHistoryRepository(session).remove(run, commit=commit)

    def _require_session(self, method: str):
        """The session this service was constructed with, or a clear error."""
        if self.session is None:
            raise ValueError(
                f"{method} requires a session; construct ExecutionService(session)."
            )
        return self.session

    @staticmethod
    async def create_run_record(
        session,
        *,
        job_id: str,
        run_name: str,
        inputs: Dict[str, Any],
        execution_type: str,
        group_id: Optional[str] = None,
        group_email: Optional[str] = None,
        flow_id: Optional[Any] = None,
        status: Optional[str] = None,
        trigger_type: Optional[str] = None,
        created_at: Optional[datetime] = None,
        commit: bool = True,
    ) -> Any:
        """Create the ``executionhistory`` row for a run that is about to start.

        The scheduler and the flow builder both need this and both used to build the
        model and call ``ExecutionHistoryRepository.insert`` themselves — the two
        cross-domain WRITES into this table. Runs are this service's domain, so the
        construction belongs here: the status a new run starts in, and the fact that
        ``flow_id`` is only meaningful for a flow, are decisions about runs.

        ``session`` is passed IN rather than acquired: both callers already hold one
        chosen for a reason — the flow builder a PRIVATE connection (a shared SQLite
        one can have a concurrent rollback discard this committed row), the scheduler
        its own routed session.

        ``commit`` defaults True because both callers then hand the job to a
        subprocess, which must be able to see the row.

        Args:
            session: the caller's session — see above.
            job_id: the run's external identifier.
            run_name: human-readable name, already generated by the caller.
            inputs: the stored config for the run.
            execution_type: ``"crew"``, ``"flow"`` or ``"agent"``.
            group_id / group_email: tenant stamps.
            flow_id: set only for a flow with a saved definition.
            status: defaults to PENDING; pass one only to override.
            trigger_type: e.g. ``"scheduled"`` — how the run was started.
            created_at: defaults to now; the scheduler passes the (naive) time the
                schedule was DUE, so a delayed sweep records when it should have run.
            commit: commit before returning (default) or leave it to the caller.

        Returns:
            The persisted ``ExecutionHistory`` row.
        """
        from src.models.execution_history import ExecutionHistory
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        from src.services.execution.harness_choice import resolve_run_harness

        run = ExecutionHistory(
            job_id=job_id,
            status=status or ExecutionStatus.PENDING.value,
            inputs=inputs or {},
            run_name=run_name,
            execution_type=execution_type,
            group_id=group_id,
            group_email=group_email,
            created_at=created_at or datetime.utcnow(),
            # Decided once, here, and read back from the row forever after.
            harness=(await resolve_run_harness(session)).value,
        )
        if trigger_type:
            run.trigger_type = trigger_type
        # Only for a flow with a saved definition — an ad-hoc flow run (nodes passed
        # inline) has no row to point at.
        if execution_type == "flow" and flow_id:
            run.flow_id = flow_id

        return await ExecutionHistoryRepository(session).insert(run, commit=commit)

    async def get_execution_record(
        self, execution_id: int, group_ids: Optional[List[str]] = None
    ) -> Optional[Any]:
        """The raw ``ExecutionHistory`` row for one run, scoped to ``group_ids``.

        Distinct from :meth:`get_execution`, which returns a flow-shaped DICT via
        ``KasalFlowService`` and applies NO tenant filter. Callers that need the
        stored config off the row — the scheduler, building a schedule from a past
        run — need both the ORM object and the scoping, and previously had to reach
        into ``ExecutionHistoryRepository`` themselves to get them.

        ``group_ids`` is what stops a caller creating a schedule from ANOTHER
        tenant's execution and reading its config and prompts. Pass it whenever a
        group context exists; None is for local/non-multitenant use only.

        Args:
            execution_id: integer primary key of the run
            group_ids: groups the caller may see, or None to skip filtering

        Returns:
            The ``ExecutionHistory`` row, or None when absent or not visible.
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )

        session = self._require_session("get_execution_record")
        return await ExecutionHistoryRepository(session).get_execution_by_id(
            execution_id, group_ids=group_ids
        )

    async def get_execution(self, execution_id: int) -> Dict[str, Any]:
        """
        Get details of a specific execution

        Args:
            execution_id: ID of the execution to retrieve

        Returns:
            Dictionary with execution details
        """
        try:
            return await self.kasal_execution_service.get_flow_execution(execution_id)
        except Exception as e:
            logger.error(f"Error getting execution: {str(e)}", exc_info=True)
            raise KasalError(detail=f"Error getting execution: {str(e)}")

    async def get_executions_by_flow(self, flow_id: uuid.UUID) -> Dict[str, Any]:
        """
        Get all executions for a specific flow

        Args:
            flow_id: ID of the flow to get executions for

        Returns:
            Dictionary with execution details
        """
        try:
            return await self.kasal_execution_service.get_flow_executions_by_flow(
                str(flow_id)
            )
        except Exception as e:
            logger.error(f"Error getting executions: {str(e)}", exc_info=True)
            raise KasalError(detail=f"Error getting executions: {str(e)}")

    # Methods from ExecutionRunnerService
    @staticmethod
    def create_execution_id() -> str:
        """
        Generate a unique execution ID.

        Returns:
            A unique execution ID
        """
        return str(uuid.uuid4())

    @staticmethod
    def get_execution(execution_id: str) -> Optional[Dict[str, Any]]:
        """
        Get execution data from in-memory storage.

        Args:
            execution_id: ID of the execution to retrieve

        Returns:
            Execution data dictionary or None if not found
        """
        return ExecutionService.executions.get(execution_id)

    @staticmethod
    def add_execution_to_memory(
        execution_id: str,
        status: str,
        run_name: str,
        created_at: datetime = None,
        group_id: Optional[int] = None,
        group_email: Optional[str] = None,
    ) -> None:
        """
        Add an execution to in-memory storage.

        Args:
            execution_id: ID of the execution
            status: Status of the execution
            run_name: Name of the execution run
            created_at: Creation timestamp (defaults to now)
            group_id: ID of the group that owns this execution
            group_email: Email of the group that owns this execution
        """
        ExecutionService.executions[execution_id] = {
            "execution_id": execution_id,
            "status": status,
            "created_at": created_at or datetime.now(),  # Use timezone-naive datetime
            "run_name": run_name,
            "output": "",
            "group_id": group_id,
            "group_email": group_email,
        }

    @staticmethod
    def _derive_placeholder_run_name(
        agents_yaml: Dict[str, Any], tasks_yaml: Dict[str, Any]
    ) -> str:
        """Instant, deterministic run name used until the LLM rename lands.

        Prefers the first task's name/description, then the first agent role,
        then a timestamped fallback — no model call, no DB roundtrip.
        """
        for cfg in (tasks_yaml or {}).values():
            if not isinstance(cfg, dict):
                continue
            candidate = str(cfg.get("name") or cfg.get("description") or "").strip()
            if candidate:
                return " ".join(candidate.split()[:4])
        for cfg in (agents_yaml or {}).values():
            if not isinstance(cfg, dict):
                continue
            candidate = str(cfg.get("role") or cfg.get("name") or "").strip()
            if candidate:
                return " ".join(candidate.split()[:4]) + " Run"
        return f"Execution-{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    @staticmethod
    async def _generate_run_name_async(
        execution_id: str,
        agents_yaml: Dict[str, Any],
        tasks_yaml: Dict[str, Any],
        model: str,
    ) -> None:
        """Generate the descriptive run name OFF the critical path and apply it.

        The naming completion used to be awaited inline in create_execution,
        adding a full LLM roundtrip (1-10+s on reasoning models) before every
        run could start. The run starts under a placeholder instead; this task
        renames it in the DB and the in-memory registry when the LLM returns.
        Failures are non-fatal — the placeholder simply remains.
        """
        try:
            name_service = ExecutionNameService.create(None)
            request = ExecutionNameGenerationRequest(
                agents_yaml=agents_yaml, tasks_yaml=tasks_yaml, model=model
            )
            response = await name_service.generate_execution_name(request)
            new_name = (response.name or "").strip()
            if not new_name:
                return

            from src.services.execution.status import ExecutionStatusService

            await ExecutionStatusService.update_run_name(execution_id, new_name)

            # Keep the in-memory fallback (used when the DB row is missing)
            # consistent with the renamed record.
            mem_entry = ExecutionService.executions.get(execution_id)
            if mem_entry is not None:
                mem_entry["run_name"] = new_name
        except Exception as e:
            crew_logger.warning(
                f"[ExecutionService] Deferred run-name generation failed for {execution_id} "
                f"(placeholder name kept): {e}"
            )

    @classmethod
    def clear_in_memory_cache(cls) -> int:
        """Drop the in-memory execution registry. Invoked when the underlying DB
        is swapped (Lakebase activate/deactivate). Entries are keyed to the
        PREVIOUS database, so without this a status lookup after a swap serves /
        polls executions that don't exist in the new DB — the recurring
        "Execution <id> not found in database" 404 storm. Registered as an
        on-swap hook on async_session_factory (see bottom of module)."""
        n = len(cls.executions)
        cls.executions.clear()
        if n:
            logger.info(
                f"[ExecutionService] Cleared {n} in-memory execution(s) after DB swap"
            )
        return n

    @staticmethod
    def sanitize_for_database(data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensure all data is properly serializable for database storage.

        Args:
            data: Dictionary containing execution data

        Returns:
            Sanitized data safe for database storage
        """
        # Create a deep copy to avoid modifying the original
        result = {}

        for key, value in data.items():
            if isinstance(value, dict):
                result[key] = ExecutionService.sanitize_for_database(value)
            elif isinstance(value, list):
                result[key] = [
                    (
                        ExecutionService.sanitize_for_database(item)
                        if isinstance(item, dict)
                        else item
                    )
                    for item in value
                ]
            elif isinstance(value, uuid.UUID):
                # Convert UUID to string
                result[key] = str(value)
            else:
                # Ensure value is JSON serializable
                try:
                    json.dumps(value)
                    result[key] = value
                except (TypeError, OverflowError):
                    # Convert to string if not serializable
                    result[key] = str(value)

        return result

    @staticmethod
    async def run_crew_execution(
        execution_id: str,
        config: CrewConfig,
        execution_type: str = "crew",
        group_context: GroupContext = None,
        session=None,
    ) -> Dict[str, Any]:
        """
        Run a crew execution with the provided configuration.

        Args:
            execution_id: Unique identifier for the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew, flow)

        Returns:
            Dictionary with execution result
        """
        # Create a dedicated logger for execution-specific logging
        # Use flow logger for flow executions, crew logger for crew executions
        if execution_type and execution_type.lower() == "flow":
            exec_logger = LoggerManager.get_instance().flow
        else:
            exec_logger = LoggerManager.get_instance().crew

        exec_logger.info(
            f"[run_crew_execution] Starting {execution_type} execution for execution_id: {execution_id}"
        )

        try:
            # Execution is already created with RUNNING status, no need to update to PREPARING
            exec_logger.info(
                f"[run_crew_execution] Execution {execution_id} already has RUNNING status from creation"
            )

            # Create an instance of KasalExecutionService
            crew_execution_service = KasalExecutionService()

            # Process different execution types
            if execution_type.lower() == "flow":
                exec_logger.info(
                    "[run_crew_execution] This is a FLOW execution - delegating to KasalExecutionService"
                )

                # Convert config to dictionary
                execution_config = {}
                if hasattr(config, "model_dump"):
                    try:
                        execution_config = config.model_dump()
                    except Exception as dump_error:
                        exec_logger.warning(
                            f"[run_crew_execution] Error calling model_dump() on config: {dump_error}"
                        )
                        # Create minimal config manually if model_dump() fails
                        # Include checkpoint resume parameters
                        for attr in [
                            "nodes",
                            "edges",
                            "flow_config",
                            "model",
                            "inputs",
                            "resume_from_flow_uuid",
                            "resume_from_execution_id",
                            "resume_from_crew_sequence",
                            "session_id",
                            "user_message",
                            "approval_decision",
                        ]:
                            if hasattr(config, attr):
                                execution_config[attr] = getattr(config, attr)
                else:
                    # Create config dictionary manually
                    # Include checkpoint resume parameters
                    for attr in [
                        "nodes",
                        "edges",
                        "flow_config",
                        "model",
                        "inputs",
                        "resume_from_flow_uuid",
                        "resume_from_execution_id",
                        "resume_from_crew_sequence",
                        # The conversation this run belongs to. A flow derives
                        # its checkpoint lineage from it, so turn 2 continues
                        # turn 1 instead of starting a new one — and the user's
                        # line is what that turn appends to the history.
                        "session_id",
                        "user_message",
                        # A HITL decision on a resume run.
                        "approval_decision",
                    ]:
                        if hasattr(config, attr):
                            execution_config[attr] = getattr(config, attr)

                # Extract flow_id from config
                flow_id = None
                if hasattr(config, "flow_id") and config.flow_id:
                    flow_id = config.flow_id
                    exec_logger.info(
                        f"[run_crew_execution] Found flow_id in direct attribute: {flow_id}"
                    )
                elif (
                    hasattr(config, "inputs")
                    and config.inputs
                    and isinstance(config.inputs, dict)
                    and "flow_id" in config.inputs
                ):
                    flow_id = config.inputs["flow_id"]
                    exec_logger.info(
                        f"[run_crew_execution] Found flow_id in inputs dict: {flow_id}"
                    )

                # Sanitize the config for database
                sanitized_config = ExecutionService.sanitize_for_database(
                    execution_config
                )

                # Delegate flow execution to KasalExecutionService
                result = await crew_execution_service.run_flow_execution(
                    flow_id=str(flow_id) if flow_id else None,
                    nodes=sanitized_config.get("nodes"),
                    edges=sanitized_config.get("edges"),
                    job_id=execution_id,
                    config=sanitized_config,
                    group_context=group_context,
                )
                exec_logger.info(
                    f"[run_crew_execution] Flow execution initiated: {result}"
                )
                return result

            # For crew executions, use the proper method from KasalExecutionService
            elif execution_type.lower() == "crew":
                exec_logger.debug(
                    "[run_crew_execution] This is a CREW execution - delegating to KasalExecutionService"
                )

                # NOTE: Databricks authentication is now handled via get_auth_context() in databricks_auth.py
                # No need to set up environment variables here - each component uses unified auth

                exec_logger.debug(
                    f"[run_crew_execution] Calling crew_execution_service.run_crew_execution for job_id: {execution_id}"
                )
                # This call should handle PREPARING/RUNNING updates internally
                result = await crew_execution_service.run_crew_execution(
                    execution_id=execution_id,
                    config=config,
                    group_context=group_context,
                    session=session,
                )
                exec_logger.info(
                    f"[run_crew_execution] Successfully initiated crew execution via KasalExecutionService for job_id: {execution_id}. Result: {result}"
                )
                return result  # Return result from run_crew_execution

            # Light "chat" mode: run a SINGLE agent via Agent.kickoff_async (no crew,
            # no tasks/process). Reuses the same RUNNING row + status/SSE plumbing.
            elif execution_type.lower() == "agent":
                exec_logger.info(
                    f"[run_crew_execution] This is a LIGHT AGENT execution - delegating to KasalExecutionService for job_id: {execution_id}"
                )
                result = await crew_execution_service.run_light_agent_execution(
                    execution_id=execution_id,
                    config=config,
                    group_context=group_context,
                    session=session,
                )
                exec_logger.info(
                    f"[run_crew_execution] Light agent execution finished for job_id: {execution_id}. Result: {result}"
                )
                return result
            else:
                # For other execution types, use the standard thread pool approach
                exec_logger.debug(
                    f"[run_crew_execution] Using thread pool execution for {execution_type} job_id {execution_id}"
                )
                future = ExecutionService._thread_pool.submit(
                    run_in_thread_with_loop,
                    ExecutionService._execute_crew,
                    execution_id,
                    config,
                    execution_type,
                )

                # Return immediate response with execution details
                return {
                    "execution_id": execution_id,
                    "status": ExecutionStatus.RUNNING.value,
                    "message": f"{execution_type.capitalize()} execution started (logging may be incomplete)",
                }

        except Exception as e:
            exec_logger.error(
                f"[run_crew_execution] Error during initiation of {execution_type} execution {execution_id}: {str(e)}",
                exc_info=True,
            )
            # Attempt to update status to FAILED using ExecutionStatusService
            try:
                exec_logger.error(
                    f"[run_crew_execution] Attempting to update status to FAILED for execution_id: {execution_id} due to error."
                )
                await ExecutionStatusService.update_status(
                    job_id=execution_id,
                    status="failed",
                    message=f"Failed during initiation: {str(e)}",
                )
                exec_logger.info(
                    f"[run_crew_execution] Successfully updated status to FAILED for execution_id: {execution_id}."
                )
            except Exception as update_err:
                exec_logger.critical(
                    f"[run_crew_execution] CRITICAL: Failed to update status to FAILED for execution_id: {execution_id} after initiation error: {update_err}",
                    exc_info=True,
                )

            raise  # Re-raise the original exception

    async def list_executions(
        self,
        group_ids: List[str] = None,
        user_email: str = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        List executions from both database and in-memory storage with group and user filtering.

        Args:
            group_ids: List of group IDs for filtering
            user_email: User email for user-level filtering
            limit: Maximum number of executions to return
            offset: Number of executions to skip

        Returns:
            List of execution data dictionaries
        """
        try:
            # Get executions from database using ExecutionRepository
            from src.repositories.execution_repository import ExecutionRepository

            logger.debug(
                f"[list_executions] Starting database query - group_ids: {group_ids}, user_email: {user_email}"
            )

            if self.session:
                logger.debug(
                    f"[list_executions] Using injected database session: {self.session}"
                )
                repo = ExecutionRepository(self.session)
                logger.debug(f"[list_executions] Created repository: {repo}")

                # Get executions with group and user filtering using the correct repository method
                logger.debug(
                    f"[list_executions] Calling repo.get_execution_history with group_ids={group_ids}"
                )
                db_executions_list, total_count = await repo.get_execution_history(
                    limit=limit,
                    offset=offset,
                    group_ids=group_ids,
                    user_email=user_email,
                )
                logger.debug(
                    f"[list_executions] Repository returned {len(db_executions_list)} items, total_count={total_count}"
                )

                logger.debug(
                    f"[list_executions] Database returned {len(db_executions_list)} executions for group_ids: {group_ids}"
                )

                # Debug what we got
                if db_executions_list:
                    logger.debug(
                        f"[list_executions] First execution: job_id={db_executions_list[0].job_id}, group_id={db_executions_list[0].group_id}, run_name={db_executions_list[0].run_name}"
                    )
                else:
                    logger.warning(
                        f"[list_executions] No executions found for group_ids: {group_ids}"
                    )

                # Convert to list of dicts, including inputs with agents_yaml and tasks_yaml
                import json

                db_executions = []
                for e in db_executions_list:
                    # Mask sensitive data in inputs before returning to API
                    masked_inputs = (
                        self._mask_inputs_sensitive_data(e.inputs) if e.inputs else None
                    )

                    exec_dict = {
                        "execution_id": e.job_id,
                        "status": e.status,
                        "created_at": e.created_at,
                        "completed_at": e.completed_at,
                        "run_name": e.run_name,
                        "result": e.result,
                        "error": e.error,
                        "group_email": e.group_email,
                        "group_id": e.group_id,  # CRITICAL: Include group_id for frontend security filtering
                        "inputs": masked_inputs,  # Include the masked inputs field (sensitive data redacted)
                        # Flow scheduling support - include execution_type and flow_id
                        "execution_type": getattr(e, "execution_type", None)
                        or (
                            masked_inputs.get("execution_type")
                            if masked_inputs
                            else None
                        )
                        or "crew",
                        # Which runtime ran it, so the run list can say so. The
                        # column is added by the startup self-heal, hence
                        # getattr: a row that predates it still lists.
                        "harness": getattr(e, "harness", None),
                        "flow_id": (
                            str(e.flow_id)
                            if getattr(e, "flow_id", None)
                            else (
                                masked_inputs.get("flow_id") if masked_inputs else None
                            )
                        ),
                    }

                    # Also extract agents_yaml and tasks_yaml from masked inputs for direct access
                    if masked_inputs and isinstance(masked_inputs, dict):
                        if "agents_yaml" in masked_inputs:
                            exec_dict["agents_yaml"] = (
                                json.dumps(masked_inputs["agents_yaml"])
                                if isinstance(masked_inputs["agents_yaml"], dict)
                                else masked_inputs.get("agents_yaml", "")
                            )
                        if "tasks_yaml" in masked_inputs:
                            exec_dict["tasks_yaml"] = (
                                json.dumps(masked_inputs["tasks_yaml"])
                                if isinstance(masked_inputs["tasks_yaml"], dict)
                                else masked_inputs.get("tasks_yaml", "")
                            )

                    db_executions.append(exec_dict)
            else:
                logger.error("[list_executions] No database session available")
                db_executions = []

            # Get in-memory executions that might not be in the database yet
            memory_executions = {}
            for execution_id, execution_data in ExecutionService.executions.items():
                # Check if this execution is already in the list from the database
                if not any(
                    e.get("execution_id") == execution_id for e in db_executions
                ):
                    memory_executions[execution_id] = execution_data

            # Combine results
            results = db_executions.copy()
            for execution_id, data in memory_executions.items():
                execution_data = data.copy()
                if "execution_id" not in execution_data:
                    execution_data["execution_id"] = execution_id
                results.append(execution_data)

            logger.debug(
                f"Returning {len(results)} total executions ({len(db_executions)} from DB, {len(memory_executions)} from memory)"
            )
            return results

        except Exception as e:
            logger.error(
                f"Database connection failed while listing executions: {str(e)}"
            )
            logger.error(f"Error type: {type(e).__name__}")
            import traceback

            logger.error(f"Full traceback: {traceback.format_exc()}")

            # CRITICAL: Re-raise the exception so we can see what's happening
            raise

            # Check database configuration
            from src.config.settings import settings

            logger.error(f"Database URI: {settings.DATABASE_URI}")
            logger.error(f"Database type: {settings.DATABASE_TYPE}")

            # If database access fails, just return in-memory executions
            memory_only_results = [
                {**data, "execution_id": execution_id}
                for execution_id, data in ExecutionService.executions.items()
            ]
            logger.info(
                f"Falling back to {len(memory_only_results)} in-memory executions"
            )
            return memory_only_results

    @staticmethod
    def _execute_crew(
        execution_id: str, config: CrewConfig, execution_type: str
    ) -> None:
        """
        Execute a crew or flow with proper database updates.

        Args:
            execution_id: String ID for the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew or flow)
        """
        exec_logger.info(f"Executing {execution_type} with ID {execution_id}")

        result = None
        success = False

        try:
            # NOTE: Databricks authentication is now handled via get_auth_context() in databricks_auth.py
            # No need to set up environment variables here - each component uses unified auth

            # Main execution logic would go here
            # For non-crew executions, such as flows
            if execution_type == "flow":
                # Run flow execution
                result = {"status": "completed", "message": "Flow execution completed"}
            else:
                # Generic execution handling
                result = {
                    "status": "completed",
                    "message": f"{execution_type} execution completed",
                }

            # Mark as successful
            success = True
            exec_logger.info(
                f"{execution_type.capitalize()} execution {execution_id} completed successfully"
            )

        except Exception as e:
            exec_logger.error(
                f"Error during {execution_type} execution {execution_id}: {str(e)}"
            )
            result = {"status": "failed", "error": str(e)}

        finally:
            # Update execution status in database using a new session
            # We need a new session since this runs in a different thread
            try:
                # Use create_and_run_loop to properly manage the event loop
                create_and_run_loop(
                    ExecutionService._update_execution_status(
                        execution_id,
                        (
                            ExecutionStatus.COMPLETED.value
                            if success
                            else ExecutionStatus.FAILED.value
                        ),
                        result,
                    )
                )
            except Exception as update_error:
                exec_logger.error(
                    f"Error updating execution status: {str(update_error)}"
                )

    @staticmethod
    async def _update_execution_status(
        execution_id: str, status: str, result: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Update execution status in the database.

        Args:
            execution_id: String ID of the execution
            status: New status for the execution
            result: Optional result data
        """
        try:
            # Use ExecutionStatusService to update status
            from src.services.execution.status import ExecutionStatusService

            # Sanitize result for database storage if needed
            update_data = {"status": status}
            if result:
                update_data["result"] = ExecutionService.sanitize_for_database(result)

            # Update execution status using the service
            # No need to use create_and_run_loop here since execute_db_operation_smart
            # already handles event loop isolation
            success = await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=status,
                message=f"Status updated to {status}",
                result=result,
            )

            if not success:
                exec_logger.error(
                    f"Failed to update execution {execution_id} status to {status}"
                )
            else:
                exec_logger.info(f"Updated execution {execution_id} status to {status}")
                # Clean up in-memory entry once terminal status is persisted to DB
                terminal_statuses = {
                    ExecutionStatus.COMPLETED.value,
                    ExecutionStatus.FAILED.value,
                    ExecutionStatus.STOPPED.value,
                    ExecutionStatus.CANCELLED.value,
                    ExecutionStatus.REJECTED.value,
                }
                if status in terminal_statuses:
                    ExecutionService.executions.pop(execution_id, None)
                    exec_logger.debug(
                        f"Cleaned up in-memory execution entry for {execution_id}"
                    )

        except Exception as e:
            exec_logger.error(f"Error updating execution status: {str(e)}")

    async def get_execution_status(
        self, execution_id: str, group_ids: List[str] = None
    ) -> Dict[str, Any]:
        """
        Get the current status of an execution from the database with group filtering.

        Args:
            execution_id: String ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Dictionary with execution status information or None if not found
        """
        try:
            # Use ExecutionHistoryRepository to get execution with group filtering
            from src.repositories.execution_history_repository import (
                ExecutionHistoryRepository,
            )

            # Create repository with session if available
            if self.session:
                repository = ExecutionHistoryRepository(self.session)
                # Slim scalar-only probe first: the vast majority of poll ticks
                # observe an in-flight run, where nobody needs the result blob —
                # the full row (result/inputs JSON) is fetched only once the
                # status is terminal.
                execution = await repository.get_execution_summary_by_job_id(
                    execution_id, group_ids=group_ids
                )
            else:
                # Log error if no session available
                exec_logger.error(
                    "No database session available for getting execution status"
                )
                return None

            if not execution:
                # DB miss — before declaring a 404, fall back to the in-memory
                # registry. A just-created run can be polled before its row is
                # committed (the parent INSERT now commits on a separate isolated
                # connection), and a still-running run whose row was lost can keep
                # being observed here while it lives in this process. Honour group
                # scoping so this never reveals a run from another workspace.
                in_memory = ExecutionService.executions.get(execution_id)
                if in_memory and (
                    not group_ids or in_memory.get("group_id") in group_ids
                ):
                    exec_logger.debug(
                        f"Execution {execution_id} absent from DB; serving in-memory "
                        f"state ({in_memory.get('status')})."
                    )
                    return {
                        "execution_id": execution_id,
                        "status": in_memory.get("status"),
                        "created_at": in_memory.get("created_at"),
                        "completed_at": None,
                        "result": None,
                        "run_name": in_memory.get("run_name"),
                        "error": None,
                        "execution_type": None,
                        "mlflow_trace_id": None,
                        "mlflow_experiment_name": None,
                        "mlflow_evaluation_run_id": None,
                    }
                exec_logger.warning(f"Execution {execution_id} not found in database.")
                return None

            # Only terminal statuses carry a result the caller can use; fetch
            # the full row (with the JSON blobs) just for those. In-flight
            # statuses answer straight from the slim probe with result=None —
            # the same value they returned before, without dragging a completed
            # prior payload through the driver on every 2s poll.
            in_flight = {
                "PENDING",
                "PREPARING",
                "RUNNING",
                "WAITING_FOR_APPROVAL",
                "STOPPING",
            }
            result_value = None
            if (execution.status or "").upper() not in in_flight:
                full_row = await repository.get_execution_by_job_id(
                    execution_id, group_ids=group_ids
                )
                if full_row is not None:
                    result_value = full_row.result

            return {
                "execution_id": execution_id,
                "status": execution.status,
                "created_at": execution.created_at,
                "completed_at": execution.completed_at,
                "result": result_value,
                "run_name": execution.run_name,
                "error": execution.error,
                # Lets the trace poller skip requests that don't apply to this
                # run type (e.g. task-states for light/agent chat runs).
                "execution_type": execution.execution_type,
                # Which runtime ran it. Decided once at creation and stamped on
                # the row; read back here so a finished run can SAY what ran it
                # — it was recorded from the start and surfaced nowhere.
                #
                # getattr: the column is added by the startup self-heal, and a
                # status read must not fail on a row object that predates it.
                # Status is on the hot path of every poll; a missing label is a
                # smaller failure than a run that cannot report its state.
                "harness": getattr(execution, "harness", None),
                # MLflow integration fields
                "mlflow_trace_id": execution.mlflow_trace_id,
                "mlflow_experiment_name": execution.mlflow_experiment_name,
                "mlflow_evaluation_run_id": execution.mlflow_evaluation_run_id,
            }
        except Exception as e:
            exec_logger.error(
                f"Error getting execution status for {execution_id}: {str(e)}"
            )
            return None

    async def get_execution_status_detail(
        self, execution_id: str, group_ids: List[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Get detailed execution status including task progress for the status endpoint.

        Args:
            execution_id: String ID of the execution
            group_ids: List of group IDs for filtering

        Returns:
            Dictionary with execution status, stop info, and task progress, or None
        """
        try:
            from src.repositories.execution_history_repository import (
                ExecutionHistoryRepository,
            )

            if not self.session:
                exec_logger.error(
                    "No database session available for getting execution status detail"
                )
                return None

            repository = ExecutionHistoryRepository(self.session)
            execution = await repository.get_execution_by_job_id(
                execution_id, group_ids=group_ids
            )

            if not execution:
                return None

            progress = None
            if execution.status in ["RUNNING", "STOPPING"]:
                tasks = await repository.get_task_statuses_by_job_id(execution_id)

                if tasks:
                    completed = [t for t in tasks if t.status == "completed"]
                    running = [t for t in tasks if t.status == "running"]
                    progress = {
                        "total_tasks": len(tasks),
                        "completed_tasks": len(completed),
                        "running_tasks": len(running),
                        "current_task": running[0].task_id if running else None,
                    }

            return {
                "execution_id": execution_id,
                "status": execution.status,
                "is_stopping": getattr(execution, "is_stopping", False),
                "stopped_at": getattr(execution, "stopped_at", None),
                "stop_reason": getattr(execution, "stop_reason", None),
                "progress": progress,
            }
        except Exception as e:
            exec_logger.error(
                f"Error getting execution status detail for {execution_id}: {str(e)}"
            )
            return None

    async def create_execution(
        self,
        config: CrewConfig,
        background_tasks=None,
        group_context: GroupContext = None,
    ) -> Dict[str, Any]:
        """
        Create a new execution and start it in the background.

        Args:
            config: Configuration for the execution
            background_tasks: Optional FastAPI background tasks object
            group_context: Group context for multi-tenant execution

        Returns:
            Dictionary with execution details
        """
        # Use consistent logger instance defined at the module level
        # Choose logger based on execution type
        execution_type = (
            config.execution_type
            if hasattr(config, "execution_type") and config.execution_type
            else "crew"
        )
        if execution_type.lower() == "flow":
            logger = LoggerManager.get_instance().flow
            # Also update exec_logger for backward compatibility with existing code
            exec_logger = LoggerManager.get_instance().flow
        else:
            logger = crew_logger
            exec_logger = crew_logger

        logger.debug(
            "[ExecutionService.create_execution] Received request to create execution."
        )

        try:
            # Generate a new execution ID
            execution_id = ExecutionService.create_execution_id()
            logger.debug(
                f"[ExecutionService.create_execution] Generated execution_id: {execution_id}"
            )

            # Generate a descriptive run name
            # Determine model safely
            model = (
                config.model if config.model else "default-model"
            )  # Provide a default if model can be None
            # Ensure agents_yaml and tasks_yaml are dictionaries
            agents_yaml = (
                config.agents_yaml if isinstance(config.agents_yaml, dict) else {}
            )
            tasks_yaml = (
                config.tasks_yaml if isinstance(config.tasks_yaml, dict) else {}
            )

            # For flow executions, extract agents and tasks from flow nodes if agents_yaml/tasks_yaml are empty
            # This is needed because flows store agent/task data in nodes, not in agents_yaml/tasks_yaml
            if execution_type == "flow" and not agents_yaml and not tasks_yaml:
                logger.info(
                    "[ExecutionService.create_execution] Flow execution detected with empty agents_yaml/tasks_yaml - extracting from nodes"
                )
                agents_yaml, tasks_yaml = self._extract_agents_tasks_from_flow_config(
                    config
                )
                logger.info(
                    f"[ExecutionService.create_execution] Extracted {len(agents_yaml)} agents and {len(tasks_yaml)} tasks from flow config for name generation"
                )

            # Log the agents_yaml to see if knowledge_sources are present
            logger.info(
                f"[ExecutionService.create_execution] Received agents_yaml with {len(agents_yaml)} agents"
            )
            for agent_id, agent_config in agents_yaml.items():
                logger.info(
                    f"[ExecutionService.create_execution] Agent {agent_id} keys: {list(agent_config.keys())}"
                )
                if "knowledge_sources" in agent_config:
                    knowledge_sources = agent_config.get("knowledge_sources", [])
                    logger.info(
                        f"[ExecutionService.create_execution] Agent {agent_id} has {len(knowledge_sources)} knowledge_sources"
                    )
                    for idx, source in enumerate(knowledge_sources):
                        logger.info(
                            f"[ExecutionService.create_execution] Agent {agent_id} knowledge_source[{idx}]: {source}"
                        )
                else:
                    logger.debug(
                        f"[ExecutionService.create_execution] Agent {agent_id} has NO knowledge_sources field"
                    )

            # Ensure GroupContext is available in UserContext for authentication
            # This is critical for both OBO (user_token) and PAT (group_id) authentication
            if group_context:
                from src.utils.user_context import UserContext

                UserContext.set_group_context(group_context)
                logger.info(
                    f"[ExecutionService.create_execution] Set GroupContext for execution name generation: primary_group_id={group_context.primary_group_id}, has_access_token={bool(group_context.access_token)}"
                )

                # Also set user_token if available for OBO authentication
                if (
                    hasattr(group_context, "access_token")
                    and group_context.access_token
                ):
                    UserContext.set_user_token(group_context.access_token)
                    logger.info(
                        "[ExecutionService.create_execution] Set user_token for OBO authentication"
                    )

            # Start with an instant deterministic placeholder name. The LLM
            # naming call (a full model roundtrip — seconds on reasoning models)
            # used to be awaited here, delaying time-to-first-answer of EVERY
            # run; it now happens off the critical path (see the rename task
            # scheduled after the DB record exists) and simply renames the run.
            run_name = ExecutionService._derive_placeholder_run_name(
                agents_yaml, tasks_yaml
            )
            logger.debug(
                f"[ExecutionService.create_execution] Placeholder run_name: {run_name} for execution_id: {execution_id}"
            )

            # Add run_name to config inputs for crew consistency
            if not config.inputs:
                config.inputs = {}
            config.inputs["run_name"] = run_name
            logger.info(
                "[ExecutionService.create_execution] Added run_name to config.inputs for consistent crew_id generation"
            )

            # Extract execution type and flow_id
            # Note: execution_type is already captured above for logger selection
            flow_id = None

            if execution_type == "flow":
                logger.info(
                    f"[ExecutionService.create_execution] Creating flow execution for execution_id: {execution_id}"
                )

                # Check if flow_id is directly available in config
                if hasattr(config, "flow_id") and config.flow_id:
                    flow_id = config.flow_id
                    logger.info(
                        f"[ExecutionService.create_execution] Using flow_id from config: {flow_id}"
                    )
                # Also try to get flow_id from inputs
                elif config.inputs and "flow_id" in config.inputs:
                    flow_id = config.inputs.get("flow_id")
                    logger.info(
                        f"[ExecutionService.create_execution] Using flow_id from inputs: {flow_id}"
                    )

                # If no flow_id is provided, check if nodes/edges are provided for ad-hoc execution
                # This allows "test before save" workflow from the canvas
                if not flow_id:
                    # Check if nodes and edges are provided in config for ad-hoc execution
                    has_nodes = (
                        hasattr(config, "nodes")
                        and config.nodes is not None
                        and len(config.nodes) > 0
                    )
                    has_edges = hasattr(config, "edges") and config.edges is not None

                    if has_nodes:
                        # Ad-hoc flow execution with nodes from canvas (no database save required)
                        # Edges may be empty for single-crew flows - that's valid
                        edge_count = len(config.edges) if config.edges else 0
                        exec_logger.info(
                            f"[ExecutionService.create_execution] No flow_id provided, but nodes ({len(config.nodes)}) and edges ({edge_count}) present - allowing ad-hoc flow execution"
                        )
                    else:
                        # No flow_id and no nodes/edges, try to find the most recent flow from database
                        exec_logger.info(
                            f"[ExecutionService.create_execution] No flow_id or nodes/edges provided for execution_id: {execution_id}, trying to find most recent flow from database"
                        )
                        try:
                            # Use async query for the most recent flow from the database
                            from src.db.session import routed_scoped_session
                            from src.services.flow_builder.flow_service import (
                                FlowService,
                            )

                            async with routed_scoped_session() as db:
                                # Flows are FlowService's domain.
                                most_recent_flow = await FlowService(
                                    db
                                ).get_most_recent_flow()

                                if most_recent_flow:
                                    flow_id = most_recent_flow.id
                                    exec_logger.info(
                                        f"[ExecutionService.create_execution] Found most recent flow with ID {flow_id} for execution_id: {execution_id}"
                                    )
                                else:
                                    exec_logger.error(
                                        f"[ExecutionService.create_execution] No flows found in database for execution_id: {execution_id}"
                                    )
                                    raise ValueError(
                                        "No flow found in the database. Please create a flow first, or provide nodes and edges for ad-hoc execution."
                                    )
                        except Exception as e:
                            exec_logger.error(
                                f"[ExecutionService.create_execution] Error finding most recent flow: {str(e)}"
                            )
                            raise ValueError(
                                f"Error finding most recent flow: {str(e)}"
                            )

            # Create database entry
            inputs = {
                "agents_yaml": config.agents_yaml,
                "tasks_yaml": config.tasks_yaml,
                "inputs": config.inputs,
                "reasoning": (
                    config.reasoning if hasattr(config, "reasoning") else False
                ),
                "model": config.model,
                "execution_type": execution_type,
                "schema_detection_enabled": config.schema_detection_enabled,
            }

            # For flow executions, make sure to include nodes and edges in the inputs
            if execution_type == "flow":
                # Make sure we have nodes and edges for flow execution
                if hasattr(config, "nodes") and config.nodes:
                    inputs["nodes"] = config.nodes
                    logger.info(
                        f"[ExecutionService.create_execution] Added {len(config.nodes)} nodes to flow execution"
                    )
                elif not flow_id:
                    # Only warn about missing nodes if we don't have a flow_id
                    logger.warning(
                        f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id} and no flow_id present, this will cause an error"
                    )
                else:
                    logger.info(
                        f"[ExecutionService.create_execution] No nodes provided for flow execution {execution_id}, but flow_id {flow_id} is present. Nodes will be loaded from the database."
                    )

                if hasattr(config, "edges") and config.edges:
                    inputs["edges"] = config.edges
                    logger.info(
                        f"[ExecutionService.create_execution] Added {len(config.edges)} edges to flow execution"
                    )

                # Add flow configuration if available
                if hasattr(config, "flow_config") and config.flow_config:
                    inputs["flow_config"] = config.flow_config
                    logger.info(
                        "[ExecutionService.create_execution] Added flow_config to flow execution"
                    )

            # Add flow_id to inputs if it exists
            if flow_id:
                inputs["flow_id"] = flow_id
                # Also set it directly on the config's inputs dictionary
                if not config.inputs:
                    config.inputs = {}
                config.inputs["flow_id"] = str(flow_id)
                logger.info(
                    f"[ExecutionService.create_execution] Added flow_id {flow_id} to config.inputs"
                )

            # SECURITY: Mask sensitive fields (client_secret, password, token, etc.) BEFORE storing to database
            # This ensures secrets are never persisted in plaintext
            masked_inputs = self._mask_inputs_sensitive_data(inputs)
            logger.info(
                "[ExecutionService.create_execution] Masked sensitive fields in inputs before database storage"
            )

            # Sanitize inputs to ensure all values are JSON serializable
            sanitized_inputs = ExecutionService.sanitize_for_database(masked_inputs)

            # Create execution data with RUNNING status for immediate visibility
            execution_data = {
                "job_id": execution_id,
                "status": ExecutionStatus.RUNNING.value,  # Start with RUNNING status for immediate visibility
                "inputs": sanitized_inputs,
                # ExecutionHistory.planning stays as a historical-record column but is
                # no longer populated from the request — Kasal has no planner, so the
                # column default (False) applies to every new row.
                "run_name": run_name,
                "created_at": datetime.now(),  # Remove timezone to match database column type
                "execution_type": (
                    execution_type.lower() if execution_type else "crew"
                ),  # Track execution type
            }

            # The harness this run asked for, if it named one. Recorded here so
            # everything downstream keeps working unchanged: the row is the
            # source of truth, the subprocess inherits it, and a resume reuses
            # it. A run naming none leaves the key absent, and `_fill_harness`
            # falls back to the configured default — which is what a scheduled
            # or API-triggered run gets, having no picker.
            requested = getattr(config, "harness", None)
            if requested:
                from src.services.execution.harnesses import coerce

                chosen = coerce(requested)
                if chosen is None:
                    logger.warning(
                        "[ExecutionService.create_execution] Ignoring unknown "
                        "harness %r for %s; using the configured default",
                        requested,
                        execution_id,
                    )
                else:
                    execution_data["harness"] = chosen.value
                    logger.info(
                        "[ExecutionService.create_execution] %s runs on the %s "
                        "harness (named by the run)",
                        execution_id,
                        chosen.value,
                    )

            # Add flow_id for flow executions (flow_id is already a UUID object)
            if execution_type == "flow" and flow_id:
                import uuid as uuid_module

                # Ensure flow_id is a UUID object for the database
                if isinstance(flow_id, str):
                    flow_id = uuid_module.UUID(flow_id)
                execution_data["flow_id"] = flow_id
                logger.info(
                    f"[ExecutionService.create_execution] Setting flow_id {flow_id} in execution_data for flow execution"
                )

            # The crew equivalent: which saved crew this run was built from, so a
            # later resume can rebuild from the current definition instead of the
            # snapshot stored above. Malformed ids are dropped rather than
            # raised on — the run itself does not need the link, and failing a
            # kickoff over a bad optional reference would be a worse trade.
            crew_id = getattr(config, "crew_id", None)
            if execution_type != "flow" and crew_id:
                import uuid as uuid_module

                try:
                    execution_data["crew_id"] = (
                        uuid_module.UUID(crew_id)
                        if isinstance(crew_id, str)
                        else crew_id
                    )
                except (TypeError, ValueError):
                    logger.warning(
                        f"[ExecutionService.create_execution] Ignoring unparseable "
                        f"crew_id {crew_id!r}; this run will resume from its "
                        f"stored inputs instead of the saved crew"
                    )

            # A flow resume has always created a new execution; it just never
            # recorded WHICH run it came from. Stamping the same link the crew
            # path uses means one resume chain shape for both, so the run detail
            # view does not need two ways to ask "what was this resumed from?".
            resumed_from = getattr(config, "resume_from_execution_id", None)
            if resumed_from:
                try:
                    execution_data["resumed_from_execution_id"] = int(resumed_from)
                except (TypeError, ValueError):
                    # The field is typed int, but the flow API has historically
                    # accepted a job_id string here; resolve it rather than
                    # dropping the link.
                    from src.repositories.execution_history_repository import (
                        ExecutionHistoryRepository,
                    )

                    if self.session:
                        source = await ExecutionHistoryRepository(
                            self.session
                        ).get_execution_by_job_id(str(resumed_from))
                        if source:
                            execution_data["resumed_from_execution_id"] = source.id

            logger.debug(
                f"[ExecutionService.create_execution] Attempting to create DB record for execution_id: {execution_id} with status RUNNING, execution_type: {execution_type}"
            )

            # Use ExecutionStatusService to create the execution
            from src.services.execution.status import ExecutionStatusService

            success = await ExecutionStatusService.create_execution(
                execution_data, group_context=group_context
            )

            if not success:
                raise ValueError(
                    f"Failed to create execution record for {execution_id}"
                )

            logger.info(
                f"[ExecutionService.create_execution] Successfully created DB record for execution_id: {execution_id} with status RUNNING"
            )

            # Add to in-memory storage with RUNNING status. Carry the group so a
            # status lookup that falls back to this entry (see get_execution_status)
            # can enforce the same tenant scoping the DB query would.
            ExecutionService.add_execution_to_memory(
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING.value,
                run_name=run_name,
                created_at=datetime.now(),  # Remove timezone to match database column type
                group_id=group_context.primary_group_id if group_context else None,
                group_email=group_context.group_email if group_context else None,
            )
            logger.debug(
                f"[ExecutionService.create_execution] Added execution_id: {execution_id} to in-memory store with status RUNNING"
            )

            # Fire-and-forget the LLM rename now that the record exists. The
            # created asyncio task inherits this request's contextvars, so the
            # group context / OBO token set above still applies inside it.
            asyncio.create_task(
                ExecutionService._generate_run_name_async(
                    execution_id=execution_id,
                    agents_yaml=agents_yaml,
                    tasks_yaml=tasks_yaml,
                    model=model,
                )
            )

            # Start execution in background
            logger.info(
                f"[ExecutionService.create_execution] Preparing to launch background task for execution_id: {execution_id}..."
            )

            if background_tasks:

                async def run_execution_task():
                    # Use context-aware logger based on execution type
                    task_logger = (
                        LoggerManager.get_instance().flow
                        if execution_type.lower() == "flow"
                        else LoggerManager.get_instance().crew
                    )
                    task_logger.info(
                        f"[run_execution_task] Background task started for execution_id: {execution_id}"
                    )
                    try:
                        task_logger.debug(
                            f"[run_execution_task] Calling ExecutionService.run_crew_execution for execution_id: {execution_id}"
                        )
                        await ExecutionService.run_crew_execution(
                            execution_id=execution_id,
                            config=config,
                            execution_type=execution_type,
                            group_context=group_context,
                            session=self.session,
                        )
                        task_logger.info(
                            f"[run_execution_task] ExecutionService.run_crew_execution completed for execution_id: {execution_id}"
                        )
                    except Exception as task_error:
                        # This catches errors that escape run_crew_execution (e.g., if it re-raises)
                        task_logger.error(
                            f"[run_execution_task] Error escaped from ExecutionService.run_crew_execution for execution_id: {execution_id}: {str(task_error)}",
                            exc_info=True,
                        )
                        # Fallback: Attempt to update status if the status update in run_crew_execution failed
                        task_logger.error(
                            f"[run_execution_task] Fallback: Attempting to update status to FAILED for execution_id: {execution_id} due to escaped task error."
                        )
                        try:
                            await ExecutionStatusService.update_status(
                                job_id=execution_id,
                                status="failed",
                                message=f"Execution failed due to error: {str(task_error)}",
                            )
                            task_logger.info(
                                f"[run_execution_task] Fallback: Successfully committed FAILED status for {execution_id} due to escaped task error."
                            )
                        except Exception as status_ex:
                            task_logger.error(
                                f"[run_execution_task] Fallback: Failed to update status for {execution_id}: {status_ex}"
                            )
                    task_logger.info(
                        f"[run_execution_task] Background task finished for execution_id: {execution_id}"
                    )

                background_tasks.add_task(run_execution_task)
                logger.info(
                    f"[ExecutionService.create_execution] Added run_execution_task to FastAPI BackgroundTasks for execution_id: {execution_id}"
                )
            else:
                # Fallback using asyncio.create_task
                logger.warning(
                    f"[ExecutionService.create_execution] FastAPI BackgroundTasks not available for {execution_id}, using asyncio.create_task."
                )
                task = asyncio.create_task(
                    ExecutionService._run_in_background(
                        execution_id=execution_id,
                        config=config,
                        execution_type=execution_type,
                        group_context=group_context,
                        session=self.session,
                    )
                )
                # Store the task reference so we can cancel it later
                if execution_id in ExecutionService.executions:
                    ExecutionService.executions[execution_id]["task"] = task
                    logger.debug(
                        f"[ExecutionService.create_execution] Stored asyncio task reference for {execution_id}"
                    )
                logger.info(
                    f"[ExecutionService.create_execution] Launched _run_in_background task via asyncio for execution_id: {execution_id}"
                )

            logger.info(
                f"[ExecutionService.create_execution] Execution {execution_id} launch initiated. Returning initial response."
            )

            # Return execution details immediately after DB creation and task launch
            from src.schemas.execution import ExecutionCreateResponse

            return ExecutionCreateResponse(  # Use Pydantic model for response
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING.value,  # Return RUNNING status for immediate visibility
                run_name=run_name,
            ).model_dump()  # Return as dict

        except Exception as e:
            logger.error(
                f"[ExecutionService.create_execution] Error during initial creation for execution: {str(e)}",
                exc_info=True,
            )
            # Re-raise as KasalError for the API boundary
            raise KasalError(detail=f"Failed to create execution: {str(e)}")

    @staticmethod
    async def _run_in_background(
        execution_id: str,
        config: CrewConfig,
        execution_type: str = "crew",
        group_context: GroupContext = None,
        session=None,
    ):
        """
        Run an execution in the background using a new database session.
        This is used when FastAPI's background_tasks is not available.

        Args:
            execution_id: ID of the execution
            config: Configuration for the execution
            execution_type: Type of execution (crew or flow)
        """
        # Use a separate logger instance potentially if needed, or reuse exec_logger
        task_logger = LoggerManager.get_instance().crew
        task_logger.info(
            f"[_run_in_background] Asyncio background task started for execution_id: {execution_id}"
        )
        try:
            task_logger.debug(
                f"[_run_in_background] Calling ExecutionService.run_crew_execution for execution_id: {execution_id}"
            )
            await ExecutionService.run_crew_execution(
                execution_id=execution_id,
                config=config,
                execution_type=execution_type,
                group_context=group_context,
                session=session,
            )
            task_logger.info(
                f"[_run_in_background] ExecutionService.run_crew_execution completed for execution_id: {execution_id}"
            )
        except Exception as e:
            task_logger.error(
                f"[_run_in_background] Error during ExecutionService.run_crew_execution for execution_id: {execution_id}: {str(e)}",
                exc_info=True,
            )
            # Note: No explicit FAILED status update here, assuming run_crew_execution handles its internal errors
            # and updates status before raising, or the session rollback handles cleanup.
        task_logger.info(
            f"[_run_in_background] Asyncio background task finished for execution_id: {execution_id}"
        )

    # Which terminal states are resumable now lives in
    # services/execution/checkpointing/lifecycle.py, so the endpoint, the UI's
    # "why is resume disabled" message and this method cannot disagree.

    async def resume_execution(
        self,
        execution_id: str,
        group_context: GroupContext = None,
        from_unit: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Resume a crashed/terminated execution from its checkpoint.

        Creates a NEW execution linked to the source by
        ``resumed_from_execution_id``, seeded with the source's completed units
        so a second crash resumes from the full prefix. The source record is
        left FAILED and its checkpoint moves to 'resumed'.

        This replaced re-running the same job_id in place. Three reasons, in
        order of how much they hurt: ``execution_trace`` and ``execution_logs``
        are both keyed by job_id, so a resumed run's rows interleaved with the
        crashed attempt's under one id and the timeline showed two attempts as
        one; a terminal FAILED record that mutates back to RUNNING is not an
        audit trail; and token cost could not be attributed per attempt, which
        makes budgets unenforceable across a resume.

        Args:
            execution_id: job_id of the execution to resume
            group_context: Group context for tenant isolation
            from_unit: Optional unit key to resume AT — everything before it is
                restored, it and everything after re-runs. Omit to continue
                from the first incomplete unit.

        Returns:
            Dict with the NEW execution_id, status, run_name and restored count

        Raises:
            ValueError: If the execution is missing, not resumable, or has no
                stored configuration to resume from
        """
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )
        from src.services.execution.checkpointing import (
            lifecycle,
            normalize,
            ordered_units,
            store,
        )
        from src.services.execution.checkpointing.resume_config import (
            build_crew_resume_config,
            build_flow_resume_config,
        )

        if not self.session:
            raise ValueError("resume_execution requires a database session")

        group_ids = group_context.group_ids if group_context else None
        repo = ExecutionHistoryRepository(self.session)
        source = await repo.get_execution_by_job_id(execution_id, group_ids=group_ids)
        if not source:
            raise ValueError(f"Execution {execution_id} not found")

        execution_type = (source.execution_type or "crew").lower()
        if execution_type not in ("crew", "flow"):
            # The chat path is a single in-process agent turn with nothing to
            # resume; see services/execution/checkpointing/__init__.py.
            raise ValueError(
                f"Execution {execution_id} is type '{execution_type}', which "
                f"has no checkpointing"
            )

        blocker = lifecycle.resumable_blocker(source.status, source.checkpoint_status)
        if blocker and not lifecycle.is_resumable_execution(source.status):
            # A missing checkpoint is not fatal — the run simply starts over —
            # but a non-terminal execution is: resuming one would race the
            # process still writing to it.
            raise ValueError(f"Execution {execution_id} cannot be resumed: {blocker}")

        stored_inputs = source.inputs or {}
        record = normalize(source.checkpoint_data)

        # The two paths restart in completely different ways — a flow rebuilds
        # from nodes/edges/flow_config and replays completed CREWS, a crew from
        # agents_yaml/tasks_yaml and replays completed TASKS — which is why
        # each has its own builder. (Running one through the other's config was
        # a 409 on every flow resume, because a flow has no agents_yaml.)
        #
        # Both prefer the SAVED definition over the stored snapshot, so a
        # resume picks up edits made since the original run; see
        # checkpointing/resume_config.py.
        if execution_type == "flow":
            config, restored_units, resume_inputs = await build_flow_resume_config(
                self.session, source, stored_inputs, record, from_unit, group_context
            )
        else:
            config, restored_units, resume_inputs = await build_crew_resume_config(
                self.session, source, stored_inputs, record, from_unit, group_context
            )

        new_execution_id = str(uuid.uuid4())
        run_name = source.run_name

        crew_logger.info(
            f"[resume_execution] Resuming {execution_id} as {new_execution_id} "
            f"(source status: {source.status}, restored units: {restored_units})"
        )

        execution_data = {
            "job_id": new_execution_id,
            "status": ExecutionStatus.RUNNING.value,
            # The definition that will actually RUN, which is the rebuilt one
            # when there was a saved crew/flow to rebuild from. Storing the old
            # snapshot here would make the new run's record describe work it
            # did not do, and send a second resume back to the stale text.
            "inputs": ExecutionService.sanitize_for_database(
                self._mask_inputs_sensitive_data(resume_inputs)
            ),
            "run_name": run_name,
            "created_at": datetime.now(),
            "execution_type": execution_type,
            "resumed_from_execution_id": source.id,
        }
        if source.flow_id:
            execution_data["flow_id"] = source.flow_id
        if source.flow_uuid:
            execution_data["flow_uuid"] = source.flow_uuid
        # Carry the crew link down the resume chain. Without it the SECOND
        # resume of a run would have nothing to rebuild from and would fall
        # back to a snapshot — which is not stale here, but would freeze at the
        # first resume and stop picking up later edits.
        if getattr(source, "crew_id", None):
            execution_data["crew_id"] = source.crew_id

        created = await ExecutionStatusService.create_execution(
            execution_data, group_context=group_context
        )
        if not created:
            raise ValueError(
                f"Failed to create resumed execution record for {execution_id}"
            )

        # Seed the new run's checkpoint from the restored prefix, so a SECOND
        # crash resumes from everything already done rather than only from what
        # this attempt manages to redo.
        if record and restored_units:
            seeded_units = ordered_units(record)[:restored_units]
            await store.write_record(
                self.session,
                new_execution_id,
                {
                    **record,
                    "units": {unit["key"]: unit for unit in seeded_units},
                    # The SOURCE's unit count does not describe this run — the
                    # definition may have gained or lost tasks since, and the
                    # engine appends synthetic ones in some configurations.
                    # Cleared so the recorder's own count wins on first write
                    # rather than the seed advertising a number that was never
                    # true here (it is what made the log read "6 -> 7").
                    "unit_count": None,
                },
                checkpoint_status=lifecycle.CheckpointStatus.ACTIVE,
            )

        # The source is now spent: it stays FAILED, but its checkpoint must stop
        # offering itself so the same crash cannot be resumed twice in parallel.
        await lifecycle.mark_resumed(self.session, execution_id, group_ids=group_ids)
        await self.session.commit()

        # In-memory entry: prepare_and_run_crew reads run_name from here, and
        # status fallbacks enforce group scoping on it.
        ExecutionService.add_execution_to_memory(
            execution_id=new_execution_id,
            status=ExecutionStatus.RUNNING.value,
            run_name=run_name,
            created_at=datetime.now(),
            group_id=group_context.primary_group_id if group_context else None,
            group_email=group_context.group_email if group_context else None,
        )

        task = asyncio.create_task(
            ExecutionService._run_in_background(
                execution_id=new_execution_id,
                config=config,
                execution_type=execution_type,
                group_context=group_context,
                session=self.session,
            )
        )
        if new_execution_id in ExecutionService.executions:
            ExecutionService.executions[new_execution_id]["task"] = task

        return {
            "execution_id": new_execution_id,
            "resumed_from": execution_id,
            "status": ExecutionStatus.RUNNING.value,
            "run_name": run_name,
            "restored_tasks": restored_units,
        }

    async def _check_for_running_jobs(self, group_context: GroupContext = None) -> None:
        """
        Check for running jobs to enforce single job execution constraint.

        Args:
            group_context: Group context for filtering (ensures users can only see their own group's jobs)

        Raises:
            ValueError: If there are any running jobs
        """
        try:
            # Get active statuses that should block new executions
            active_statuses = [
                ExecutionStatus.PENDING.value,
                ExecutionStatus.PREPARING.value,
                ExecutionStatus.RUNNING.value,
            ]

            # Use ExecutionRepository to check for active executions
            from src.db.session import routed_scoped_session
            from src.repositories.execution_repository import ExecutionRepository

            async with routed_scoped_session() as db:
                repo = ExecutionRepository(db)

                # Get executions with group filtering
                group_ids = group_context.group_ids if group_context else None
                active_executions, _ = await repo.get_execution_history(
                    limit=1,  # We only need to know if any exist
                    offset=0,
                    group_ids=group_ids,
                    status_filter=active_statuses,  # Filter for active statuses
                )

                if active_executions:
                    active_execution = active_executions[0]
                    error_msg = (
                        f"Cannot start new job. Another job is currently running: "
                        f"'{active_execution.run_name}' (Status: {active_execution.status}). "
                        f"Please wait for it to complete. "
                        f"Note: In future releases, we plan to add support for concurrent job execution."
                    )
                    crew_logger.warning(
                        f"[ExecutionService._check_for_running_jobs] {error_msg}"
                    )
                    raise ValueError(error_msg)

        except ValueError:
            # Re-raise ValueError (our constraint violation)
            raise
        except Exception as e:
            # Log other errors but don't block execution creation
            crew_logger.error(
                f"[ExecutionService._check_for_running_jobs] Error checking for running jobs: {str(e)}"
            )
            # We don't raise here to avoid blocking execution if the check fails for technical reasons

    async def generate_execution_name(
        self, request: ExecutionNameGenerationRequest
    ) -> Dict[str, str]:
        """
        Generate a descriptive name for an execution based on agents and tasks configuration.

        Args:
            request: The execution name generation request

        Returns:
            Dict containing the generated name
        """
        response = await self.execution_name_service.generate_execution_name(request)
        return {"name": response.name}

    def _extract_agents_tasks_from_flow_config(self, config: CrewConfig) -> tuple:
        """
        Extract agents and tasks information from flow configuration for name generation.

        For flow executions, agents and tasks are stored in nodes (flow_config.startingPoints,
        flow_config.listeners) rather than in agents_yaml/tasks_yaml. This method extracts
        that information and formats it for the execution name generation service.

        Args:
            config: The CrewConfig containing flow configuration

        Returns:
            Tuple of (agents_yaml, tasks_yaml) dictionaries for name generation
        """
        agents_yaml = {}
        tasks_yaml = {}

        try:
            # Get nodes and flow_config from the config
            nodes = config.nodes if hasattr(config, "nodes") and config.nodes else []
            flow_config = (
                config.flow_config
                if hasattr(config, "flow_config") and config.flow_config
                else {}
            )

            logger.info(
                f"[_extract_agents_tasks_from_flow_config] Processing {len(nodes)} nodes"
            )

            # Extract from nodes (direct node data)
            for node in nodes:
                node_type = node.get("type", "").lower()
                node_data = node.get("data", {})
                node_id = node.get("id", "")

                if node_type == "crewnode":
                    # Extract crew information which contains agents and tasks
                    crew_name = node_data.get("label", node_data.get("name", "Crew"))
                    all_agents = node_data.get("allAgents", node_data.get("agents", []))
                    all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                    logger.info(
                        f"[_extract_agents_tasks_from_flow_config] Found crewNode with {len(all_agents)} agents and {len(all_tasks)} tasks"
                    )

                    # Extract agents
                    for agent in all_agents:
                        agent_id = agent.get("id", f"agent_{len(agents_yaml)}")
                        agents_yaml[agent_id] = {
                            "role": agent.get("role", agent.get("name", "Agent")),
                            "goal": agent.get("goal", ""),
                            "backstory": agent.get("backstory", ""),
                        }

                    # Extract tasks
                    for task in all_tasks:
                        task_id = task.get("id", f"task_{len(tasks_yaml)}")
                        tasks_yaml[task_id] = {
                            "name": task.get(
                                "name", task.get("description", "Task")[:50]
                            ),
                            "description": task.get("description", ""),
                            "expected_output": task.get(
                                "expected_output", task.get("expectedOutput", "")
                            ),
                        }

                elif node_type == "agentnode":
                    # Extract single agent
                    agent_id = node_data.get("agentId", node_id)
                    agents_yaml[agent_id] = {
                        "role": node_data.get("role", node_data.get("label", "Agent")),
                        "goal": node_data.get("goal", ""),
                        "backstory": node_data.get("backstory", ""),
                    }

                elif node_type == "tasknode":
                    # Extract single task
                    task_id = node_data.get("taskId", node_id)
                    tasks_yaml[task_id] = {
                        "name": node_data.get("name", node_data.get("label", "Task")),
                        "description": node_data.get("description", ""),
                        "expected_output": node_data.get(
                            "expected_output", node_data.get("expectedOutput", "")
                        ),
                    }

            # Also extract from flow_config's startingPoints and listeners
            starting_points = flow_config.get("startingPoints", [])
            listeners = flow_config.get("listeners", [])

            logger.info(
                f"[_extract_agents_tasks_from_flow_config] Processing {len(starting_points)} starting points and {len(listeners)} listeners"
            )

            # Process starting points
            for sp in starting_points:
                node_type = sp.get("nodeType", "")
                node_data = sp.get("nodeData", {})

                if node_type == "crewNode":
                    # Extract from crew node
                    all_agents = node_data.get("allAgents", node_data.get("agents", []))
                    all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                    for agent in all_agents:
                        agent_id = agent.get("id", f"agent_sp_{len(agents_yaml)}")
                        if agent_id not in agents_yaml:
                            agents_yaml[agent_id] = {
                                "role": agent.get("role", agent.get("name", "Agent")),
                                "goal": agent.get("goal", ""),
                                "backstory": agent.get("backstory", ""),
                            }

                    for task in all_tasks:
                        task_id = task.get("id", f"task_sp_{len(tasks_yaml)}")
                        if task_id not in tasks_yaml:
                            tasks_yaml[task_id] = {
                                "name": task.get(
                                    "name",
                                    (
                                        task.get("description", "Task")[:50]
                                        if task.get("description")
                                        else "Task"
                                    ),
                                ),
                                "description": task.get("description", ""),
                                "expected_output": task.get(
                                    "expected_output", task.get("expectedOutput", "")
                                ),
                            }

                # Also extract crew info if present at top level of starting point
                crew_name = sp.get("crewName", "")
                if crew_name and crew_name not in agents_yaml:
                    agents_yaml[f"crew_{crew_name}"] = {
                        "role": crew_name,
                        "goal": f"Execute {crew_name} workflow",
                        "backstory": "",
                    }

            # Process listeners (same structure as starting points)
            for listener in listeners:
                node_type = listener.get("nodeType", "")
                node_data = listener.get("nodeData", {})

                if node_type == "crewNode":
                    all_agents = node_data.get("allAgents", node_data.get("agents", []))
                    all_tasks = node_data.get("allTasks", node_data.get("tasks", []))

                    for agent in all_agents:
                        agent_id = agent.get("id", f"agent_listener_{len(agents_yaml)}")
                        if agent_id not in agents_yaml:
                            agents_yaml[agent_id] = {
                                "role": agent.get("role", agent.get("name", "Agent")),
                                "goal": agent.get("goal", ""),
                                "backstory": agent.get("backstory", ""),
                            }

                    for task in all_tasks:
                        task_id = task.get("id", f"task_listener_{len(tasks_yaml)}")
                        if task_id not in tasks_yaml:
                            tasks_yaml[task_id] = {
                                "name": task.get(
                                    "name",
                                    (
                                        task.get("description", "Task")[:50]
                                        if task.get("description")
                                        else "Task"
                                    ),
                                ),
                                "description": task.get("description", ""),
                                "expected_output": task.get(
                                    "expected_output", task.get("expectedOutput", "")
                                ),
                            }

                # Also extract crew info if present at top level
                crew_name = listener.get("crewName", "")
                if crew_name and f"crew_{crew_name}" not in agents_yaml:
                    agents_yaml[f"crew_{crew_name}"] = {
                        "role": crew_name,
                        "goal": f"Execute {crew_name} workflow",
                        "backstory": "",
                    }

            logger.info(
                f"[_extract_agents_tasks_from_flow_config] Final extraction: {len(agents_yaml)} agents, {len(tasks_yaml)} tasks"
            )

        except Exception as e:
            logger.error(
                f"[_extract_agents_tasks_from_flow_config] Error extracting agents/tasks from flow config: {str(e)}"
            )
            logger.error(traceback.format_exc())
            # Return empty dicts on error - the fallback name generation will handle it

        return agents_yaml, tasks_yaml

    async def stop_execution(
        self,
        execution_id: str,
        stop_type: str,
        reason: Optional[str] = None,
        requested_by: Optional[str] = None,
        preserve_partial_results: bool = True,
        db: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Stop a running execution.

        Args:
            execution_id: ID of the execution to stop
            stop_type: Type of stop (graceful or force)
            reason: Optional reason for stopping
            requested_by: User who requested the stop
            preserve_partial_results: Whether to save partial results
            db: Database session

        Returns:
            Dict with stop status and partial results if available
        """
        from datetime import datetime

        from src.models.execution_status import ExecutionStatus
        from src.repositories.execution_history_repository import (
            ExecutionHistoryRepository,
        )
        from src.schemas.execution import StopExecutionResponse

        crew_logger.info("[STOP] ========== STOP EXECUTION CALLED ==========")
        crew_logger.info(
            f"[STOP] execution_id: {execution_id}, stop_type: {stop_type}, reason: {reason}"
        )

        try:
            # Update execution status to STOPPING
            if db:
                history_repo = ExecutionHistoryRepository(db)

                # First set the is_stopping flag and status to STOPPING
                await history_repo.mark_stopping(execution_id, reason, requested_by)
                await db.commit()

                # Get current execution state for partial results
                execution = await history_repo.get_execution_by_job_id(execution_id)

                partial_results = None
                if preserve_partial_results and execution:
                    partial_results = execution.result

            # Check if execution is in our active executions dictionary
            if execution_id in self.executions:
                execution_info = self.executions[execution_id]

                # For force stop, cancel the asyncio task if it exists
                if stop_type == "force" and "task" in execution_info:
                    task = execution_info["task"]
                    if not task.done():
                        task.cancel()
                        crew_logger.info(
                            f"Force cancelled task for execution {execution_id}"
                        )

                # For graceful stop, set a flag that the execution should check
                if stop_type == "graceful":
                    execution_info["stop_requested"] = True
                    crew_logger.info(
                        f"Graceful stop requested for execution {execution_id}"
                    )

            # Try to stop using ProcessFlowExecutor first (for flow-based executions)
            crew_logger.info(
                f"[STOP] Attempting to stop execution {execution_id} via ProcessFlowExecutor"
            )
            flow_terminated = False
            try:
                from src.services.flow_builder.process_executor import (
                    process_flow_executor,
                )

                # Try to terminate the flow process
                flow_terminated = await process_flow_executor.terminate_execution(
                    execution_id, graceful=(stop_type == "graceful")
                )
                if flow_terminated:
                    crew_logger.info(
                        f"[STOP] Successfully terminated flow process for execution {execution_id}"
                    )
                else:
                    crew_logger.info(
                        f"[STOP] Execution {execution_id} not found in ProcessFlowExecutor tracking - trying psutil fallback"
                    )
                    # Fallback: Use psutil to find and kill processes by execution_id
                    try:
                        import psutil

                        current_process = psutil.Process()
                        children = current_process.children(recursive=True)
                        killed_count = 0
                        for child in children:
                            try:
                                # Check if this process is related to our execution
                                cmdline = " ".join(child.cmdline())
                                if (
                                    execution_id in cmdline
                                    or execution_id[:8] in cmdline
                                ):
                                    crew_logger.info(
                                        f"[STOP] Found process {child.pid} matching execution {execution_id}, terminating..."
                                    )
                                    child.terminate()
                                    killed_count += 1
                            except (psutil.NoSuchProcess, psutil.AccessDenied):
                                pass
                        if killed_count > 0:
                            crew_logger.info(
                                f"[STOP] Terminated {killed_count} processes via psutil fallback"
                            )
                            flow_terminated = True
                        else:
                            crew_logger.info(
                                f"[STOP] No matching processes found via psutil for {execution_id}"
                            )
                    except Exception as psutil_error:
                        crew_logger.warning(
                            f"[STOP] psutil fallback failed: {psutil_error}"
                        )

            except Exception as flow_error:
                crew_logger.info(
                    f"[STOP] Could not stop via ProcessFlowExecutor: {flow_error}"
                )

            # Try to stop using ProcessCrewExecutor (for crew-based executions)
            crew_logger.info(
                f"[STOP] Attempting to stop execution {execution_id} via ProcessCrewExecutor"
            )
            process_terminated = False
            try:
                from src.services.agent_builder.process_executor import (
                    process_crew_executor,
                )

                # Try to terminate the process
                process_terminated = await process_crew_executor.terminate_execution(
                    execution_id
                )
                if process_terminated:
                    crew_logger.info(
                        f"[STOP] Successfully terminated crew process for execution {execution_id}"
                    )
                else:
                    crew_logger.info(
                        f"[STOP] Execution {execution_id} not found in ProcessCrewExecutor (may be thread-based)"
                    )

            except Exception as process_error:
                crew_logger.info(
                    f"[STOP] Could not stop via ProcessCrewExecutor: {process_error}"
                )

            # If not process-based (neither flow nor crew), try the thread-based crew_executor
            if not flow_terminated and not process_terminated:
                try:
                    from src.services.execution.thread_executor import crew_executor

                    # Request cooperative stop through the executor
                    stop_requested = crew_executor.request_stop(execution_id)
                    if stop_requested:
                        crew_logger.info(
                            f"Stop requested for execution {execution_id} via CrewExecutor"
                        )

                        # For force stop, also cancel the asyncio task if it exists
                        if (
                            stop_type == "force"
                            and execution_id in self.executions
                            and "task" in self.executions[execution_id]
                        ):
                            task = self.executions[execution_id]["task"]
                            if not task.done():
                                task.cancel()
                                crew_logger.info(
                                    f"Force cancelled asyncio task for execution {execution_id}"
                                )

                                # Wait briefly for cancellation
                                try:
                                    await asyncio.wait_for(
                                        asyncio.shield(task), timeout=2.0
                                    )
                                except (asyncio.CancelledError, asyncio.TimeoutError):
                                    crew_logger.info(
                                        f"Task cancellation completed for {execution_id}"
                                    )
                    else:
                        crew_logger.warning(
                            f"Execution {execution_id} not found in CrewExecutor"
                        )

                except Exception as executor_error:
                    crew_logger.warning(
                        f"Could not stop via CrewExecutor: {executor_error}"
                    )

            # Also try to cancel via KasalEngineService
            try:
                from src.services.execution.engine_service import KasalEngineService

                crew_service = KasalEngineService()
                cancelled = await crew_service.cancel_execution(execution_id)
                if cancelled:
                    crew_logger.info(
                        f"Successfully cancelled execution {execution_id} via KasalEngineService"
                    )
            except Exception as cancel_error:
                crew_logger.warning(
                    f"Could not cancel via KasalEngineService: {cancel_error}"
                )

            # Remove from active executions to stop tracking it
            if execution_id in self.executions:
                del self.executions[execution_id]
                crew_logger.info(
                    f"Removed {execution_id} from active executions tracking"
                )

            # Log that we've attempted to stop the execution threads
            crew_logger.info(
                f"Execution {execution_id} stop initiated. ThreadManager attempted to stop related threads. "
                "Note: CrewAI does not natively support cancellation, but threads have been targeted for termination."
            )

            # Final update to mark as STOPPED
            if db:
                await ExecutionHistoryRepository(db).mark_stopped(
                    execution_id,
                    partial_results if preserve_partial_results else None,
                )
                await db.commit()

            return {
                "execution_id": execution_id,
                "status": ExecutionStatus.STOPPED.value,
                "message": f"Execution {stop_type} stopped successfully",
                "partial_results": partial_results,
            }

        except Exception as e:
            crew_logger.error(f"Error stopping execution {execution_id}: {str(e)}")

            # Try to update status to indicate stop failed
            if db:
                try:
                    await ExecutionHistoryRepository(db).mark_stop_failed(
                        execution_id, f"Failed to stop: {str(e)}"
                    )
                    await db.commit()
                except:
                    pass

            raise Exception(f"Failed to stop execution: {str(e)}")


# Flush the in-memory execution registry whenever the active database is swapped
# (Lakebase activate/deactivate). Entries are keyed to the previous DB, so a
# status lookup after a swap would otherwise 404 on rows that only existed in
# the old database. Best-effort registration at import time — the API process
# imports ExecutionService before any runtime swap, so the hook is in place.
try:  # pragma: no cover - import-time wiring
    from src.db.session import async_session_factory as _async_session_factory

    _async_session_factory.register_on_swap(ExecutionService.clear_in_memory_cache)
except Exception as _reg_err:  # noqa: BLE001
    logger.warning(
        f"[ExecutionService] Could not register DB-swap cache hook: {_reg_err}"
    )
