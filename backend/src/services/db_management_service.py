import os
import shutil
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional, Callable

from fastapi import UploadFile, BackgroundTasks, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class DBManagementService:
    """
    Service for database file management operations.
    """
    
    def __init__(self, db_path: str):
        """
        Initialize the service with the database path.
        
        Args:
            db_path: Path to the SQLite database file
        """
        self.db_path = Path(db_path)
        self.backup_dir = Path("./tmp")
        self.backup_dir.mkdir(exist_ok=True)
    
    async def export_database(self, background_tasks: BackgroundTasks) -> Dict[str, any]:
        """
        Export the SQLite database file for backup purposes.
        
        Args:
            background_tasks: FastAPI background tasks for cleanup
            
        Returns:
            Dictionary with path and filename for the exported database
            
        Raises:
            HTTPException: If the database file cannot be exported
        """
        try:
            if not self.db_path.exists():
                raise HTTPException(status_code=404, detail="Database file not found")
            
            # Create a temporary copy of the database to avoid locking issues
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"crewai_backup_{timestamp}.db"
            backup_path = self.backup_dir / backup_filename
            
            # Copy the database file
            shutil.copy2(self.db_path, backup_path)
            
            # Schedule cleanup of the backup file after it has been sent
            def cleanup_backup():
                try:
                    if backup_path.exists():
                        os.unlink(backup_path)
                        logger.info(f"Deleted temporary backup file: {backup_path}")
                except Exception as e:
                    logger.error(f"Failed to delete temporary backup file: {e}")
            
            background_tasks.add_task(cleanup_backup)
            
            return {
                "path": str(backup_path),
                "filename": backup_filename
            }
            
        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Database export error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to export database: {str(e)}")
    
    async def import_database(self, file: UploadFile, db_session: Session) -> Dict[str, str]:
        """
        Import a SQLite database file to replace the current database.
        
        Args:
            file: Uploaded database file
            db_session: SQLAlchemy database session
            
        Returns:
            Dictionary with success message
            
        Raises:
            HTTPException: If the database file cannot be imported
        """
        try:
            # Close the current database session
            db_session.close()
            
            # Create a backup of the current database
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            original_backup = self.backup_dir / f"crewai_original_{timestamp}.db"
            
            # Create backup of original DB if it exists
            if self.db_path.exists():
                shutil.copy2(self.db_path, original_backup)
                logger.info(f"Created backup of original database at {original_backup}")
            
            # Save the uploaded file to replace the database
            try:
                # Create a temporary file for the upload
                upload_path = self.backup_dir / f"crewai_upload_{timestamp}.db"
                
                with open(upload_path, "wb") as buffer:
                    shutil.copyfileobj(file.file, buffer)
                
                # Replace the database file
                shutil.copy2(upload_path, self.db_path)
                
                # Clean up the temporary file
                if upload_path.exists():
                    os.unlink(upload_path)
                    
                return {"message": "Database imported successfully"}
                
            except Exception as import_error:
                # If import fails, restore the original database
                if original_backup.exists():
                    shutil.copy2(original_backup, self.db_path)
                    logger.warning(f"Restored original database after import failure")
                
                raise import_error
            
        except Exception as e:
            logger.error(f"Database import error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to import database: {str(e)}")
    
    async def get_database_status(self) -> Dict[str, any]:
        """
        Get information about the current database.
        
        Returns:
            Dictionary with database status information
        """
        try:
            if not self.db_path.exists():
                return {
                    "exists": False,
                    "size": 0,
                    "path": str(self.db_path),
                    "last_modified": None
                }
            
            # Get database file stats
            stats = self.db_path.stat()
            
            return {
                "exists": True,
                "size": stats.st_size,
                "size_human": f"{stats.st_size / (1024*1024):.2f} MB",
                "path": str(self.db_path),
                "last_modified": datetime.fromtimestamp(stats.st_mtime).isoformat()
            }
        
        except Exception as e:
            logger.error(f"Database status error: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to get database status: {str(e)}") 