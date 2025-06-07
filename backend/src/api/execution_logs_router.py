"""
API router for execution logs endpoints.

This module provides endpoints for real-time execution log streaming
and retrieving historical execution logs.
"""

from typing import List, Dict, Annotated
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query, Depends

from src.core.logger import LoggerManager
from src.services.execution_logs_service import execution_logs_service
from src.schemas.execution_logs import ExecutionLogResponse, ExecutionLogsResponse
from src.core.dependencies import GroupContextDep

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

# Create router for WebSocket endpoints
logs_router = APIRouter(
    prefix="/logs",
    tags=["logs"],
)

# Create a router for the runs API to match frontend expectations
runs_router = APIRouter(
    prefix="/runs",
    tags=["runs"],
)

@logs_router.websocket("/executions/{execution_id}/stream")
async def websocket_execution_logs(websocket: WebSocket, execution_id: str):
    """
    WebSocket endpoint for streaming execution logs.
    
    This endpoint allows clients to connect via WebSocket and receive
    real-time updates about execution progress. For tenant isolation,
    the tenant context should be passed as a query parameter.
    """
    try:
        # Extract group information from query parameters for WebSocket
        query_params = websocket.query_params
        tenant_email = query_params.get('tenant_email')  # Keep for backward compatibility
        
        # Create a basic group context from query params
        # Note: WebSocket doesn't use standard headers, so we get group info from query params
        from src.utils.user_context import GroupContext
        group_context = await GroupContext.from_email(tenant_email) if tenant_email else GroupContext()
        
        # Connect to the WebSocket with group context
        await execution_logs_service.connect_with_group(websocket, execution_id, group_context)
        logger.info(f"WebSocket connection established for execution {execution_id} (group: {group_context.primary_group_id})")
        
        # Keep the connection alive until disconnect
        while True:
            try:
                # Wait for any client messages (typically ping/pong or close)
                await websocket.receive_text()
            except WebSocketDisconnect:
                logger.info(f"WebSocket disconnected for execution {execution_id}")
                break
    except Exception as e:
        logger.error(f"WebSocket error for execution {execution_id}: {e}")
    finally:
        # Ensure connection is properly cleaned up
        await execution_logs_service.disconnect(websocket, execution_id)

@logs_router.get("/executions/{execution_id}", response_model=List[ExecutionLogResponse])
async def get_execution_logs(
    execution_id: str,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical execution logs for the current tenant.
    
    This endpoint allows retrieval of past logs for a specific execution
    belonging to the current tenant.
    
    Args:
        execution_id: ID of the execution to get logs for
        group_context: Group context from headers
        limit: Maximum number of logs to return
        offset: Number of logs to skip
        
    Returns:
        List of execution logs with their timestamps
    """
    try:
        logs = await execution_logs_service.get_execution_logs_by_group(execution_id, group_context, limit, offset)
        return logs
    except Exception as e:
        logger.error(f"Error fetching execution logs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch execution logs: {str(e)}")

@runs_router.get("/{run_id}/outputs", response_model=ExecutionLogsResponse)
async def get_run_logs(
    run_id: str,
    group_context: GroupContextDep,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical logs for a specific run within the current tenant.
    
    This endpoint matches the frontend expectation for the URL pattern.
    It delegates to the execution logs service with tenant filtering.
    
    Args:
        run_id: ID of the run to get logs for
        group_context: Group context from headers
        limit: Maximum number of logs to return
        offset: Number of logs to skip
        
    Returns:
        Dictionary with a list of run logs with their timestamps
    """
    try:
        logs = await execution_logs_service.get_execution_logs_by_group(run_id, group_context, limit, offset)
        return ExecutionLogsResponse(logs=logs)
    except Exception as e:
        logger.error(f"Error fetching run logs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch run logs: {str(e)}")

# Export the send_execution_log function for use in other modules
async def send_execution_log(execution_id: str, message: str):
    """
    Send an execution log message to all connected clients.
    
    This function can be called from other parts of the application
    to broadcast execution logs to WebSocket clients.
    
    Args:
        execution_id: ID of the execution the log belongs to
        message: Content of the log message
    """
    await execution_logs_service.broadcast_to_execution(execution_id, message) 