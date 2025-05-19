from typing import Annotated, List, Dict, Any, Optional
import logging

from fastapi import APIRouter, Depends, HTTPException, Query, Body, status
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import get_db
from src.services.uc_tool_service import UCToolService
from src.schemas.uc_tool import UCToolSchema, UCToolListResponse

# Create router instance
router = APIRouter(
    prefix="/uc-tools",
    tags=["unity-catalog"],
    responses={404: {"description": "Not found"}},
)

# Set up logger
logger = logging.getLogger(__name__)

# Create service dependency
async def get_uc_tool_service(db: AsyncSession = Depends(get_db)) -> UCToolService:
    return UCToolService(db)

@router.get("/", response_model=UCToolListResponse)
async def get_uc_tools(
    service: Annotated[UCToolService, Depends(get_uc_tool_service)]
) -> UCToolListResponse:
    """
    Get available Unity Catalog tools.
    
    Returns:
        List of UC tools with count
    """
    try:
        response = await service.get_all_uc_tools()
        return response
    except Exception as e:
        logger.error(f"Error getting UC tools: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        ) 