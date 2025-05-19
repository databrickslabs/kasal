"""
Repository for the data_processing table.

This module provides database access functions for the data_processing table.
"""

from typing import Optional, List
import logging
from sqlalchemy import select, insert, update, delete, func, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.data_processing import DataProcessing

class DataProcessingRepository(BaseRepository):
    """Repository for data_processing table operations."""
    
    def __init__(self, session: Optional[AsyncSession] = None, sync_session: Optional[Session] = None):
        """
        Initialize the repository with a session.
        
        Args:
            session: Async SQLAlchemy session
            sync_session: Sync SQLAlchemy session
        """
        # Initialize model first
        self.model = DataProcessing
        
        # Call parent constructor with model and session
        # Note that BaseRepository expects (model, session) as parameters
        if session:
            super().__init__(self.model, session)
        else:
            # Initialize self.session to None since BaseRepository requires it
            self.session = None
        
        # Explicitly set sync_session attribute for sync operations
        self.sync_session = sync_session
    
    async def find_by_che_number(self, che_number: str) -> Optional[DataProcessing]:
        """
        Find a record by its che_number.
        
        Args:
            che_number: The che_number to look for
            
        Returns:
            DataProcessing record if found, None otherwise
        """
        if not self.session:
            raise ValueError("Session not provided")
            
        query = select(self.model).where(self.model.che_number == che_number)
        result = await self.session.execute(query)
        return result.scalars().first()
    
    def find_by_che_number_sync(self, che_number: str) -> Optional[DataProcessing]:
        """
        Find a record by its che_number (synchronous version).
        
        Args:
            che_number: The che_number to look for
            
        Returns:
            DataProcessing record if found, None otherwise
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        query = select(self.model).where(self.model.che_number == che_number)
        result = self.sync_session.execute(query)
        return result.scalars().first()
    
    async def update_processed_status(self, che_number: str, processed: bool) -> bool:
        """
        Update the processed status of a record.
        
        Args:
            che_number: The che_number of the record to update
            processed: The new processed status
            
        Returns:
            True if record was updated, False otherwise
        """
        if not self.session:
            raise ValueError("Session not provided")
            
        stmt = (
            update(self.model)
            .where(self.model.che_number == che_number)
            .values(processed=processed)
        )
        
        result = await self.session.execute(stmt)
        return result.rowcount > 0
    
    def update_processed_status_sync(self, che_number: str, processed: bool) -> bool:
        """
        Update the processed status of a record (synchronous version).
        
        Args:
            che_number: The che_number of the record to update
            processed: The new processed status
            
        Returns:
            True if record was updated, False otherwise
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        stmt = (
            update(self.model)
            .where(self.model.che_number == che_number)
            .values(processed=processed)
        )
        
        result = self.sync_session.execute(stmt)
        return result.rowcount > 0
    
    async def count_unprocessed_records(self) -> int:
        """
        Count records with processed=false.
        
        Returns:
            Number of unprocessed records
        """
        if not self.session:
            raise ValueError("Session not provided")
            
        query = select(func.count()).select_from(self.model).where(
            self.model.processed == False
        )
        result = await self.session.execute(query)
        return result.scalar() or 0
    
    def count_unprocessed_records_sync(self) -> int:
        """
        Count records with processed=false (synchronous version).
        
        Returns:
            Number of unprocessed records
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        query = select(func.count()).select_from(self.model).where(
            self.model.processed == False
        )
        result = self.sync_session.execute(query)
        return result.scalar() or 0
        
    def count_total_records_sync(self) -> int:
        """
        Count total number of records in the data_processing table.
        
        Returns:
            Total number of records
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        query = select(func.count()).select_from(self.model)
        result = self.sync_session.execute(query)
        return result.scalar() or 0
        
    def count_null_company_name_sync(self) -> int:
        """
        Count records with null company_name.
        
        Returns:
            Number of records with null company_name
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        query = select(func.count()).select_from(self.model).where(
            self.model.company_name.is_(None)
        )
        result = self.sync_session.execute(query)
        return result.scalar() or 0
        
    def create_record_sync(self, che_number: str, processed: bool = False, company_name: Optional[str] = None) -> DataProcessing:
        """
        Create a new data processing record.
        
        Args:
            che_number: CHE number for the record
            processed: Whether the record has been processed
            company_name: Name of the company
            
        Returns:
            The created DataProcessing instance
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        # Create a new record
        record = DataProcessing(
            che_number=che_number,
            processed=processed,
            company_name=company_name
        )
        
        # Add to session
        self.sync_session.add(record)
        self.sync_session.flush()
        
        return record
        
    def create_table_if_not_exists_sync(self) -> bool:
        """
        Create the data_processing table if it doesn't exist yet.
        
        Returns:
            True if successful
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")
            
        # Define the SQL for creating the table
        create_table_sql = text("""
        CREATE TABLE IF NOT EXISTS data_processing (
            id SERIAL PRIMARY KEY,
            che_number VARCHAR(255) UNIQUE NOT NULL,
            processed BOOLEAN NOT NULL DEFAULT FALSE,
            company_name VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        
        # Execute the SQL
        self.sync_session.execute(create_table_sql)
        
        return True