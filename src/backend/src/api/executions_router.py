"""
API router for execution-related operations.

This module provides API endpoints for creating and managing executions
of crews and flows, as well as utility operations like name generation.
"""

import logging
import asyncio
from typing import Annotated
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
import uuid
from datetime import datetime, UTC
from sqlalchemy import select
from sqlalchemy import desc

from src.core.logger import LoggerManager
from src.core.dependencies import GroupContextDep, SessionDep
from src.core.permissions import check_role_in_context
from src.models.execution_history import ExecutionHistory
from src.schemas.execution import (
    CrewConfig,
    ExecutionStatus,
    ExecutionResponse,
    ExecutionCreateResponse,
    ExecutionNameGenerationRequest,
    ExecutionNameGenerationResponse,
    StopExecutionRequest,
    StopExecutionResponse,
    ExecutionStatusResponse
)
from src.services.execution_service import ExecutionService
from src.services.flow_service import FlowService
from src.engines.crewai.config_adapter import get_execution_logger

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

# Create router
router = APIRouter(
    prefix="/executions",
    tags=["executions"],
)

# Dependency to get ExecutionService with explicit SessionDep
def get_execution_service(session: SessionDep) -> ExecutionService:
    """
    Factory function for ExecutionService with explicit session dependency.

    Args:
        session: Database session from FastAPI DI

    Returns:
        ExecutionService instance with injected session
    """
    return ExecutionService(session=session)

@router.post("", response_model=ExecutionCreateResponse)
async def create_execution(
    config: CrewConfig,
    background_tasks: BackgroundTasks,
    service: Annotated[ExecutionService, Depends(get_execution_service)],
    group_context: GroupContextDep
):
    """
    Create a new execution.

    Args:
        config: Configuration for the execution
        background_tasks: FastAPI background tasks
        service: Execution service (injected)
        group_context: Group context for permissions

    Returns:
        Dict with execution_id, status, and run_name
    """
    # Get appropriate logger based on config type (flow vs crew)
    exec_logger = get_execution_logger(config.model_dump() if hasattr(config, 'model_dump') else {})

    try:
        # Process flow_id if present
        if hasattr(config, 'flow_id') and config.flow_id:
            exec_logger.info(f"Executing flow with ID: {config.flow_id}")
            
            # Convert string to UUID if necessary
            if isinstance(config.flow_id, str):
                try:
                    # Validate that it's a proper UUID
                    config.flow_id = uuid.UUID(config.flow_id)
                    exec_logger.info(f"Converted flow_id to UUID: {config.flow_id}")
                except ValueError:
                    exec_logger.error(f"Invalid flow_id format: {config.flow_id}")
                    raise ValueError(f"Invalid flow_id format: {config.flow_id}. Must be a valid UUID.")

            # Verify the flow exists in database
            flow_service = FlowService(service.session)
            try:
                flow = await flow_service.get_flow(config.flow_id)
                exec_logger.info(f"Found flow in database: {flow.name} ({flow.id})")
            except HTTPException as he:
                if he.status_code == 404:
                    exec_logger.error(f"Flow with ID {config.flow_id} not found")
                    raise ValueError(f"Flow with ID {config.flow_id} not found")
                raise

        # Log the incoming config to debug knowledge_sources
        exec_logger.info(f"[create_execution] Received config with agents_yaml: {hasattr(config, 'agents_yaml')}")
        if hasattr(config, 'agents_yaml') and config.agents_yaml:
            exec_logger.info(f"[create_execution] agents_yaml has {len(config.agents_yaml)} agents")
            for agent_id, agent_data in config.agents_yaml.items():
                exec_logger.info(f"[create_execution] Agent {agent_id} keys: {list(agent_data.keys())}")
                if 'knowledge_sources' in agent_data:
                    ks = agent_data['knowledge_sources']
                    exec_logger.info(f"[create_execution] Agent {agent_id} has {len(ks)} knowledge_sources: {ks}")
                else:
                    exec_logger.warning(f"[create_execution] Agent {agent_id} has NO knowledge_sources")
        
        # Use the injected service
        # Delegate all business logic to the service
        result = await service.create_execution(
            config=config,
            background_tasks=background_tasks,
            group_context=group_context
        )
        
        # Return the result as an API response
        return ExecutionCreateResponse(**result)
        
    except HTTPException:
        # Re-raise HTTP exceptions (like 409 conflicts) as-is
        raise
    except ValueError as e:
        # Handle validation errors with 400 status
        logger.warning(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other errors with 500 status
        logger.error(f"Error creating execution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))




@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@router.get("/debug-context")
async def debug_context(
    group_context: GroupContextDep
):
    """Debug endpoint to check group context extraction."""
    return {
        "group_ids": group_context.group_ids,
        "group_email": group_context.group_email,
        "email_domain": group_context.email_domain,
        "has_access_token": bool(group_context.access_token)
    }


@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution_status(
    execution_id: str,
    group_context: GroupContextDep,
    db: SessionDep
):
    """
    Get the status of a specific execution with group filtering.
    
    Args:
        execution_id: ID of the execution to get status for
        group_context: Group context for filtering
        
    Returns:
        ExecutionResponse with execution details
    """
    # Create service instance and use method to get execution data with group filtering
    service = ExecutionService(session=db)
    execution_data = await service.get_execution_status(execution_id, group_ids=group_context.group_ids)
    
    if not execution_data:
        raise HTTPException(status_code=404, detail="Execution not found")
    
    # Process result field if needed
    if execution_data.get("result") and isinstance(execution_data["result"], str):
        try:
            # Try to parse as JSON
            import json
            execution_data["result"] = json.loads(execution_data["result"])
        except json.JSONDecodeError:
            # If not valid JSON, wrap in a dict to satisfy the schema
            execution_data["result"] = {"value": execution_data["result"]}
    
    # If result is a list, convert it to a dictionary to match the schema
    if execution_data.get("result") and isinstance(execution_data["result"], list):
        execution_data["result"] = {"items": execution_data["result"]}
    
    # If result is a boolean, convert it to a dictionary to match the schema
    if execution_data.get("result") and isinstance(execution_data["result"], bool):
        execution_data["result"] = {"success": execution_data["result"]}
    
    # If result is not a dict at this point, set it to an empty dict
    if execution_data.get("result") is not None and not isinstance(execution_data["result"], dict):
        execution_data["result"] = {}
    
    # Return the execution data
    return ExecutionResponse(**execution_data)


@router.get("", response_model=list[ExecutionResponse])
async def list_executions(
    group_context: GroupContextDep,
    db: SessionDep,
    limit: int = 50,
    offset: int = 0
):
    """
    List executions with group filtering.

    Args:
        group_context: Group context for filtering
        limit: Maximum number of executions to return (default: 50)
        offset: Number of executions to skip (default: 0)

    Returns:
        List of ExecutionResponse objects
    """
    # Log the group context for debugging
    logger.info(f"list_executions called with group_ids: {group_context.group_ids}, email: {group_context.group_email}")

    # Additional debug logging
    if not group_context.group_ids:
        logger.warning("No group_ids in context - this will return no results")

    # Create service instance and use the list_executions method with group filtering only
    service = ExecutionService(session=db)
    executions_list = await service.list_executions(
        group_ids=group_context.group_ids,
        user_email=None,  # Don't filter by user - show all executions in user's groups
        limit=limit,
        offset=offset
    )

    logger.info(f"ExecutionService returned {len(executions_list)} executions for user {group_context.group_email}")

    # Process results before converting to response models
    processed_executions = []
    for execution_data in executions_list:
        # Check if result exists and is a string - try to convert it to a dict
        if execution_data.get("result") and isinstance(execution_data["result"], str):
            try:
                # Try to parse as JSON
                import json
                execution_data["result"] = json.loads(execution_data["result"])
            except json.JSONDecodeError:
                # If not valid JSON, wrap in a dict to satisfy the schema
                execution_data["result"] = {"value": execution_data["result"]}
        # If result is a list, convert it to a dictionary to match the schema
        if execution_data.get("result") and isinstance(execution_data["result"], list):
            execution_data["result"] = {"items": execution_data["result"]}
        # If result is a boolean, convert it to a dictionary to match the schema
        if execution_data.get("result") and isinstance(execution_data["result"], bool):
            execution_data["result"] = {"success": execution_data["result"]}
        # If result is not a dict at this point, set it to an empty dict
        if execution_data.get("result") is not None and not isinstance(execution_data["result"], dict):
            execution_data["result"] = {}
        processed_executions.append(execution_data)
    
    # Convert to response models
    return [ExecutionResponse(**execution_data) for execution_data in processed_executions]


@router.post("/generate-name", response_model=ExecutionNameGenerationResponse)
async def generate_execution_name(
    request: ExecutionNameGenerationRequest,
    service: Annotated[ExecutionService, Depends(get_execution_service)],
    group_context: GroupContextDep
):
    """
    Generate a descriptive name for an execution based on agents and tasks configuration.
    
    This endpoint analyzes the given agent and task configurations and generates
    a short, memorable name (2-4 words) that captures the essence of the execution.
    """
    return await service.generate_execution_name(request)




@router.post("/{execution_id}/stop", response_model=StopExecutionResponse)
async def stop_execution(
    execution_id: str,
    request: StopExecutionRequest,
    service: Annotated[ExecutionService, Depends(get_execution_service)],
    group_context: GroupContextDep,
    db: SessionDep
):
    """
    Stop a running execution.
    Only Admins and Editors can stop executions.

    This endpoint allows graceful or forceful stopping of an execution.
    Graceful stop will try to complete the current task before stopping.
    Force stop will immediately terminate the execution.

    Args:
        execution_id: The ID of the execution to stop
        request: Stop request details including stop type and reason
        group_context: Group context for access control
        db: Database session

    Returns:
        StopExecutionResponse with status and partial results if available
    """
    # Check permissions - only admins and editors can stop executions
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=403,
            detail="Only admins and editors can stop executions"
        )

    # Use the injected service
    
    try:
        # Get the execution from database to verify it exists and user has access
        stmt = select(ExecutionHistory).where(
            ExecutionHistory.job_id == execution_id,
            ExecutionHistory.group_id == group_context.primary_group_id
        )
        result = await db.execute(stmt)
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(
                status_code=404,
                detail=f"Execution {execution_id} not found"
            )
        
        # Check if execution is in a stoppable state
        if execution.status not in ["RUNNING", "PREPARING"]:
            return StopExecutionResponse(
                execution_id=execution_id,
                status=execution.status,
                message=f"Execution is not running (current status: {execution.status})",
                partial_results=execution.result
            )
        
        # Call the stop service method
        stop_result = await service.stop_execution(
            execution_id=execution_id,
            stop_type=request.stop_type,
            reason=request.reason,
            requested_by=group_context.group_email,
            preserve_partial_results=request.preserve_partial_results,
            db=db
        )
        
        return StopExecutionResponse(**stop_result)
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error stopping execution {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to stop execution: {str(e)}"
        )


@router.post("/{execution_id}/force-stop", response_model=StopExecutionResponse)
async def force_stop_execution(
    execution_id: str,
    group_context: GroupContextDep,
    db: SessionDep
):
    """
    Force stop a running execution immediately.
    
    This is a convenience endpoint that calls the stop endpoint with force=true.
    Use this when an execution is not responding to graceful stop.
    
    Args:
        execution_id: The ID of the execution to force stop
        group_context: Group context for access control
        db: Database session
        
    Returns:
        StopExecutionResponse with status
    """
    from src.schemas.execution import StopType
    
    request = StopExecutionRequest(
        stop_type=StopType.FORCE,
        reason="Force stop requested by user",
        preserve_partial_results=True
    )

    # Create service instance
    service = ExecutionService(session=db)

    return await stop_execution(
        execution_id=execution_id,
        request=request,
        service=service,
        group_context=group_context,
        db=db
    )


@router.get("/{execution_id}/status", response_model=ExecutionStatusResponse)
async def get_execution_status_simple(
    execution_id: str,
    group_context: GroupContextDep,
    db: SessionDep
):
    """
    Get the current status of an execution.
    
    This endpoint returns detailed status information including whether
    the execution is currently being stopped.
    
    Args:
        execution_id: The ID of the execution
        group_context: Group context for access control
        db: Database session
        
    Returns:
        ExecutionStatusResponse with current status and progress
    """
    try:
        # Get the execution from database
        stmt = select(ExecutionHistory).where(
            ExecutionHistory.job_id == execution_id,
            ExecutionHistory.group_id == group_context.primary_group_id
        )
        result = await db.execute(stmt)
        execution = result.scalar_one_or_none()
        
        if not execution:
            raise HTTPException(
                status_code=404,
                detail=f"Execution {execution_id} not found"
            )
        
        # Build progress information if execution is running or stopping
        progress = None
        if execution.status in ["RUNNING", "STOPPING"]:
            # Get task status information
            from src.models.execution_history import TaskStatus
            task_stmt = select(TaskStatus).where(
                TaskStatus.job_id == execution_id
            )
            task_result = await db.execute(task_stmt)
            tasks = task_result.scalars().all()
            
            if tasks:
                completed_tasks = [t for t in tasks if t.status == "completed"]
                running_tasks = [t for t in tasks if t.status == "running"]
                progress = {
                    "total_tasks": len(tasks),
                    "completed_tasks": len(completed_tasks),
                    "running_tasks": len(running_tasks),
                    "current_task": running_tasks[0].task_id if running_tasks else None
                }
        
        return ExecutionStatusResponse(
            execution_id=execution_id,
            status=execution.status,
            is_stopping=execution.is_stopping if hasattr(execution, 'is_stopping') else False,
            stopped_at=execution.stopped_at if hasattr(execution, 'stopped_at') else None,
            stop_reason=execution.stop_reason if hasattr(execution, 'stop_reason') else None,
            progress=progress
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting execution status {execution_id}: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get execution status: {str(e)}"
        ) 