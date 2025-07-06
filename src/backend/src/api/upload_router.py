import logging
from typing import List, Optional
from fastapi import APIRouter, File, UploadFile, HTTPException, Depends, Query
from fastapi.responses import JSONResponse

from src.schemas.upload import FileResponse, MultiFileResponse, FileCheckResponse, FileCheckNotFoundResponse, FileListResponse
from src.services.upload_service import UploadService

# Create router instance
router = APIRouter(
    prefix="/upload",
    tags=["uploads"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

# Create service instance
upload_service = UploadService()


@router.post("/knowledge", response_model=FileResponse, status_code=201)
async def upload_knowledge_file(
    file: UploadFile = File(...),
) -> FileResponse:
    """
    Upload a knowledge file to be used as a knowledge source.
    
    Args:
        file: The file to upload
        
    Returns:
        FileResponse with file metadata
    """
    logger.info(f"Uploading knowledge file: {file.filename}")
    try:
        response = await upload_service.upload_file(file)
        logger.info(f"Knowledge file uploaded successfully: {file.filename}")
        return response
    except HTTPException as e:
        logger.warning(f"Knowledge file upload failed: {str(e)}")
        raise


@router.post("/knowledge/multi", response_model=MultiFileResponse, status_code=201)
async def upload_multiple_knowledge_files(
    files: List[UploadFile] = File(...),
) -> MultiFileResponse:
    """
    Upload multiple knowledge files in a single request.
    
    Args:
        files: List of files to upload
        
    Returns:
        MultiFileResponse with file metadata list
    """
    logger.info(f"Uploading {len(files)} knowledge files")
    try:
        response = await upload_service.upload_multiple_files(files)
        logger.info(f"Multiple knowledge files uploaded successfully: {len(files)} files")
        return response
    except HTTPException as e:
        logger.warning(f"Multiple knowledge files upload failed: {str(e)}")
        raise


@router.get("/knowledge/check", response_model=FileCheckResponse)
async def check_knowledge_file(
    filename: str = Query(..., description="Name of the file to check"),
) -> FileCheckResponse | FileCheckNotFoundResponse:
    """
    Check if a knowledge file exists and get its metadata.
    
    Args:
        filename: Name of the file to check
        
    Returns:
        FileCheckResponse with file metadata if file exists,
        FileCheckNotFoundResponse if file does not exist
    """
    logger.info(f"Checking knowledge file: {filename}")
    try:
        response = await upload_service.check_file(filename)
        if isinstance(response, FileCheckResponse):
            logger.info(f"Knowledge file exists: {filename}")
        else:
            logger.info(f"Knowledge file does not exist: {filename}")
        return response
    except HTTPException as e:
        logger.warning(f"Knowledge file check failed: {str(e)}")
        raise


@router.get("/knowledge/list", response_model=FileListResponse)
async def list_knowledge_files() -> FileListResponse:
    """
    List all knowledge files in the uploads directory.
    
    Returns:
        FileListResponse with file metadata list
    """
    logger.info("Listing knowledge files")
    try:
        response = await upload_service.list_files()
        logger.info(f"Listed {len(response.files)} knowledge files")
        return response
    except HTTPException as e:
        logger.warning(f"Knowledge files listing failed: {str(e)}")
        raise 