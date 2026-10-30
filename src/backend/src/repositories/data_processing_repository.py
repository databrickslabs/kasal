"""
Repository for the data_processing table.

This module provides database access functions for the data_processing table.
"""

from typing import Optional, cast

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from src.core.base_repository import BaseRepository
from src.models.data_processing import DataProcessing


class DataProcessingRepository(BaseRepository):
    """Repository for data_processing table operations."""

    def __init__(
        self,
        session: Optional[AsyncSession] = None,
        sync_session: Optional[Session] = None,
    ):
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
            # A sync-only instance has no async session; every async method
            # checks `if not self.session` before using it.
            self.session = cast(AsyncSession, None)

        # Explicitly set sync_session attribute for sync operations
        self.sync_session = sync_session

    def count_unprocessed_records_sync(self) -> int:
        """
        Count records with processed=false (synchronous version).

        Returns:
            Number of unprocessed records
        """
        if not self.sync_session:
            raise ValueError("Sync session not provided")

        query = (
            select(func.count())
            .select_from(self.model)
            .where(self.model.processed == False)
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

    def create_record_sync(
        self,
        che_number: str,
        processed: bool = False,
        company_name: Optional[str] = None,
    ) -> DataProcessing:
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
            che_number=che_number, processed=processed, company_name=company_name
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
