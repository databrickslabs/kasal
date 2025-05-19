"""
API router for execution logs endpoints.

This module provides endpoints for real-time execution log streaming
and retrieving historical execution logs.
"""

from typing import List, Dict
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, HTTPException, Query

from src.core.logger import LoggerManager
from src.services.execution_logs_service import execution_logs_service
from src.schemas.execution_logs import ExecutionLogResponse, ExecutionLogsResponse

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
    real-time updates about execution progress.
    """
    try:
        # Connect to the WebSocket
        await execution_logs_service.connect(websocket, execution_id)
        logger.info(f"WebSocket connection established for execution {execution_id}")
        
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
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical execution logs.
    
    This endpoint allows retrieval of past logs for a specific execution.
    
    Args:
        execution_id: ID of the execution to get logs for
        limit: Maximum number of logs to return
        offset: Number of logs to skip
        
    Returns:
        List of execution logs with their timestamps
    """
    try:
        logs = await execution_logs_service.get_execution_logs(execution_id, limit, offset)
        return logs
    except Exception as e:
        logger.error(f"Error fetching execution logs: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to fetch execution logs: {str(e)}")

@runs_router.get("/{run_id}/outputs", response_model=ExecutionLogsResponse)
async def get_run_logs(
    run_id: str,
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
):
    """
    Get historical logs for a specific run.
    
    This endpoint matches the frontend expectation for the URL pattern.
    It simply delegates to the execution logs service.
    
    Args:
        run_id: ID of the run to get logs for
        limit: Maximum number of logs to return
        offset: Number of logs to skip
        
    Returns:
        Dictionary with a list of run logs with their timestamps
    """
    try:
        logs = await execution_logs_service.get_execution_logs(run_id, limit, offset)
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