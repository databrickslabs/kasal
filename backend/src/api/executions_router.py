"""
API router for execution-related operations.

This module provides API endpoints for creating and managing executions
of crews and flows, as well as utility operations like name generation.
"""

import logging
import asyncio
from fastapi import APIRouter, HTTPException, Depends, BackgroundTasks
import uuid
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, UTC
from sqlalchemy import select
from sqlalchemy import desc

from src.core.logger import LoggerManager
from src.db.session import get_db
from src.models.execution_history import ExecutionHistory
from src.schemas.execution import (
    CrewConfig, 
    ExecutionStatus, 
    ExecutionResponse, 
    ExecutionCreateResponse,
    ExecutionNameGenerationRequest,
    ExecutionNameGenerationResponse
)
from src.services.execution_service import ExecutionService
from src.services.flow_service import FlowService

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().crew

# Create router
router = APIRouter(
    prefix="/executions",
    tags=["executions"],
)

@router.post("", response_model=ExecutionCreateResponse)
async def create_execution(
    config: CrewConfig,
    background_tasks: BackgroundTasks, 
    db: AsyncSession = Depends(get_db)
):
    """
    Create a new execution.
    
    Args:
        config: Configuration for the execution
        background_tasks: FastAPI background tasks
        db: Database session
        
    Returns:
        Dict with execution_id, status, and run_name
    """
    try:
        # Process flow_id if present
        if hasattr(config, 'flow_id') and config.flow_id:
            logger.info(f"Executing flow with ID: {config.flow_id}")
            
            # Convert string to UUID if necessary
            if isinstance(config.flow_id, str):
                try:
                    # Validate that it's a proper UUID
                    config.flow_id = uuid.UUID(config.flow_id)
                    logger.info(f"Converted flow_id to UUID: {config.flow_id}")
                except ValueError:
                    logger.error(f"Invalid flow_id format: {config.flow_id}")
                    raise ValueError(f"Invalid flow_id format: {config.flow_id}. Must be a valid UUID.")
            
            # Verify the flow exists in database
            flow_service = FlowService(db)
            try:
                flow = await flow_service.get_flow(config.flow_id)
                logger.info(f"Found flow in database: {flow.name} ({flow.id})")
            except HTTPException as he:
                if he.status_code == 404:
                    logger.error(f"Flow with ID {config.flow_id} not found")
                    raise ValueError(f"Flow with ID {config.flow_id} not found")
                raise
        
        # Create service instance
        execution_service = ExecutionService()
        
        # Delegate all business logic to the service
        result = await execution_service.create_execution(
            config=config,
            background_tasks=background_tasks
        )
        
        # Return the result as an API response
        return ExecutionCreateResponse(**result)
        
    except ValueError as e:
        # Handle validation errors with 400 status
        logger.warning(f"Validation error: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Handle other errors with 500 status
        logger.error(f"Error creating execution: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{execution_id}", response_model=ExecutionResponse)
async def get_execution_status(execution_id: str, db: AsyncSession = Depends(get_db)):
    """
    Get the status of a specific execution.
    
    Args:
        execution_id: ID of the execution to get status for
        
    Returns:
        ExecutionResponse with execution details
    """
    # Use the service method to get execution data
    execution_data = await ExecutionService.get_execution_status(execution_id)
    
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
    
    # Return the execution data
    return ExecutionResponse(**execution_data)


@router.get("", response_model=list[ExecutionResponse])
async def list_executions():
    """
    List all executions.
    
    Returns:
        List of ExecutionResponse objects
    """
    # Use the service's list_executions method
    executions_list = await ExecutionService.list_executions()
    
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
    db: AsyncSession = Depends(get_db)
):
    """
    Generate a descriptive name for an execution based on agents and tasks configuration.
    
    This endpoint analyzes the given agent and task configurations and generates
    a short, memorable name (2-4 words) that captures the essence of the execution.
    """
    execution_service = ExecutionService()
    return await execution_service.generate_execution_name(request)


@router.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"} 