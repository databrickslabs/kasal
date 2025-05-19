from typing import Annotated, Dict, Any
import logging

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, BackgroundTasks, status
from fastapi.responses import FileResponse, JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy.ext.asyncio import AsyncSession

from src.config.settings import settings
from src.db.session import get_sync_db, get_db
from src.services.db_management_service import DBManagementService

router = APIRouter(
    prefix="/db",
    tags=["database management"],
    responses={404: {"description": "Not found"}},
)

logger = logging.getLogger(__name__)

# Create a singleton service instance
db_management_service = DBManagementService(settings.DB_FILE_PATH)


@router.get("/export")
async def export_database(background_tasks: BackgroundTasks) -> FileResponse:
    """
    Export the SQLite database file for backup purposes.
    
    Args:
        background_tasks: FastAPI background tasks for cleanup
        
    Returns:
        The database file for download
    """
    try:
        result = await db_management_service.export_database(background_tasks)
        
        return FileResponse(
            path=result["path"],
            filename=result["filename"],
            media_type="application/octet-stream",
            background=background_tasks
        )
    except Exception as e:
        logger.error(f"Error exporting database: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/import", status_code=status.HTTP_200_OK)
async def import_database(
    file: UploadFile = File(...),
    db: Annotated[Session, Depends(get_sync_db)] = None,
) -> Dict[str, str]:
    """
    Import a SQLite database file to replace the current database.
    
    Args:
        file: The database file to import
        db: Database session
        
    Returns:
        Success message
    """
    try:
        result = await db_management_service.import_database(file, db)
        return result
    except Exception as e:
        logger.error(f"Error importing database: {e}")
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        await file.close()


@router.get("/status", response_model=Dict[str, Any])
async def get_database_status(
    db: AsyncSession = Depends(get_db)
) -> Dict[str, Any]:
    """
    Get the current status of the database.
    
    Returns:
        Dictionary containing database status information
    """
    try:
        service = DBManagementService(settings.DB_FILE_PATH)
        status = await service.get_database_status()
        return status
    except Exception as e:
        logger.error(f"Error getting database status: {e}")
        raise HTTPException(status_code=500, detail=str(e)) 