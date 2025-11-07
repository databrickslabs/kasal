"""
Databricks Knowledge Source API Router
"""
from typing import Dict, Any, List, Annotated
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import JSONResponse
import json
import logging

from src.core.dependencies import SessionDep, GroupContextDep
from src.services.databricks_knowledge_service import DatabricksKnowledgeService

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/databricks/knowledge",
    tags=["databricks_knowledge"],
    responses={404: {"description": "Not found"}},
)


def get_databricks_knowledge_service(
    session: SessionDep,
    group_context: GroupContextDep
) -> DatabricksKnowledgeService:
    """
    Get a properly initialized DatabricksKnowledgeService instance.

    Args:
        session: Database session from dependency injection
        group_context: Group context for multi-tenant filtering

    Returns:
        Initialized DatabricksKnowledgeService with all dependencies
    """
    # Get group_id from context - use first group ID or default
    group_id = group_context.group_ids[0] if group_context and group_context.group_ids else "default"
    created_by_email = group_context.group_email if group_context else None
    user_token = group_context.access_token if group_context else None

    # Create service and pass session to it
    return DatabricksKnowledgeService(
        session=session,
        group_id=group_id,
        created_by_email=created_by_email,
        user_token=user_token
    )


@router.post("/upload/{execution_id}")
async def upload_knowledge_file(
    execution_id: str,
    request: Request,
    service: Annotated[DatabricksKnowledgeService, Depends(get_databricks_knowledge_service)],
    group_context: GroupContextDep,
    file: UploadFile = File(...),
    volume_config: str = Form(...),
    agent_ids: str = Form("[]")  # JSON array of agent IDs
) -> Dict[str, Any]:
    """
    Upload a file to Databricks Volume for knowledge source.

    Args:
        execution_id: Execution ID for scoping the file
        request: FastAPI request object (for extracting user token)
        file: The uploaded file
        volume_config: JSON string containing volume configuration
        agent_ids: JSON array of agent IDs that can access this knowledge source
        group_context: Group context for multi-tenant operations

    Returns:
        Upload response with file path and metadata
    """
    logger.info(f"[API] 🚀 UPLOAD REQUEST RECEIVED!")
    logger.info(f"[API] Execution ID: {execution_id}")
    logger.info(f"[API] File: {file.filename} ({file.content_type})")
    logger.info(f"[API] Volume config: {volume_config}")
    logger.info(f"[API] Agent IDs raw: '{agent_ids}' (type: {type(agent_ids)})")
    logger.info(f"[API] Group context: {group_context}")

    try:
        # Extract user token for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(request)

        if user_token:
            logger.info("Found user token for OBO authentication")

        # Parse volume configuration
        config = json.loads(volume_config)

        # Parse agent_ids
        logger.info(f"[API] 🔍 AGENT IDS DEBUG: raw='{agent_ids}', empty_check={not agent_ids}, none_check={agent_ids is None}")
        parsed_agent_ids = json.loads(agent_ids) if agent_ids else []
        logger.info(f"[API] ✅ Parsed agent IDs: {parsed_agent_ids} (length: {len(parsed_agent_ids)})")

        # Upload file with user token
        result = await service.upload_knowledge_file(
            file=file,
            execution_id=execution_id,
            group_id=group_context.group_ids[0] if group_context and group_context.group_ids else "default",
            volume_config=config,
            agent_ids=parsed_agent_ids,  # Pass agent IDs for access control
            user_token=user_token  # Pass user token for OBO
        )
        
        return result
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid volume configuration: {e}")
        raise HTTPException(status_code=400, detail="Invalid volume configuration")
    except Exception as e:
        logger.error(f"Error uploading knowledge file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/browse/{volume_path:path}")
async def browse_volume_files(
    volume_path: str,
    request: Request,
    service: Annotated[DatabricksKnowledgeService, Depends(get_databricks_knowledge_service)],
    group_context: GroupContextDep
) -> List[Dict[str, Any]]:
    """
    Browse files in a Databricks Volume directory.
    
    Args:
        volume_path: Path within the volume to browse (e.g., "catalog.schema.volume/path")
        request: FastAPI request object (for extracting user token)
        service: DatabricksKnowledgeService instance
        group_context: Group context for multi-tenant operations
    
    Returns:
        List of files and directories with metadata
    """
    try:
        # Extract user token for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(request)
        
        if user_token:
            logger.info("Found user token for OBO authentication")
        
        # Browse volume files
        files = await service.browse_volume_files(
            volume_path=volume_path,
            group_id=group_context.group_ids[0] if group_context and group_context.group_ids else "default",
            user_token=user_token  # Pass user token for OBO
        )
        
        return files
        
    except Exception as e:
        logger.error(f"Error browsing volume files: {e}")
        raise HTTPException(status_code=500, detail=str(e))



@router.get("/list/{execution_id}")
async def list_knowledge_files(
    execution_id: str,
    service: Annotated[DatabricksKnowledgeService, Depends(get_databricks_knowledge_service)],
    group_context: GroupContextDep
) -> List[Dict[str, Any]]:
    """
    List all knowledge files for a specific execution.
    
    Args:
        execution_id: Execution ID to list files for
        group_context: Group context for multi-tenant operations
    
    Returns:
        List of files with metadata
    """
    try:
        # List files
        files = await service.list_knowledge_files(
            execution_id=execution_id,
            group_id=group_context.group_ids[0] if group_context and group_context.group_ids else "default"
        )
        
        return files
        
    except Exception as e:
        logger.error(f"Error listing knowledge files: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/delete/{execution_id}/{filename}")
async def delete_knowledge_file(
    execution_id: str,
    filename: str,
    service: Annotated[DatabricksKnowledgeService, Depends(get_databricks_knowledge_service)],
    group_context: GroupContextDep
) -> Dict[str, str]:
    """
    Delete a knowledge file from Databricks Volume.
    
    Args:
        execution_id: Execution ID of the file
        filename: Name of the file to delete
        group_context: Group context for multi-tenant operations
    
    Returns:
        Deletion confirmation
    """
    try:
        # Delete file
        result = await service.delete_knowledge_file(
            execution_id=execution_id,
            group_id=group_context.group_ids[0] if group_context and group_context.group_ids else "default",
            filename=filename
        )
        
        return {"status": "success", "message": f"File {filename} deleted successfully"}
        
    except Exception as e:
        logger.error(f"Error deleting knowledge file: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/select-from-volume/{execution_id}")
async def select_volume_file(
    execution_id: str,
    request: Request,
    service: Annotated[DatabricksKnowledgeService, Depends(get_databricks_knowledge_service)],
    group_context: GroupContextDep,
    file_path: str = Form(...),
    selected_agents: str = Form(default="[]")
) -> Dict[str, Any]:
    """
    Select an existing file from Databricks Volume for knowledge source.
    
    Args:
        execution_id: Execution ID for scoping
        request: FastAPI request object (for extracting user token)
        file_path: Full path to the file in Databricks Volume
        selected_agents: JSON string containing list of agent IDs
        service: DatabricksKnowledgeService instance
        group_context: Group context for multi-tenant operations
    
    Returns:
        File metadata and registration confirmation
    """
    try:
        # Extract user token for OBO authentication
        from src.utils.databricks_auth import extract_user_token_from_request
        user_token = extract_user_token_from_request(request)
        
        # Parse selected agents
        agents = json.loads(selected_agents) if selected_agents else []
        
        # Get group_id from context
        group_id = group_context.group_ids[0] if group_context and group_context.group_ids else "default"
        
        # Register the selected file for this execution
        logger.info(f"Registering volume file: {file_path} for execution: {execution_id}")
        logger.info(f"Selected agents: {agents}")
        
        # Extract filename from path
        filename = file_path.split('/')[-1] if '/' in file_path else file_path
        
        # Return metadata about the selected file
        return {
            "status": "success",
            "path": file_path,
            "filename": filename,
            "execution_id": execution_id,
            "group_id": group_id,
            "selected_agents": agents,
            "message": f"File {filename} selected from volume successfully"
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"Invalid selected_agents JSON: {e}")
        raise HTTPException(status_code=400, detail="Invalid selected agents data")
    except Exception as e:
        logger.error(f"Error selecting volume file: {e}")
        raise HTTPException(status_code=500, detail=str(e))