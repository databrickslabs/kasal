"""
Data Processing model.

This module defines the SQLAlchemy model for the data_processing table.
"""

from datetime import datetime
from sqlalchemy import Column, Integer, String, Boolean, DateTime

from src.db.base import Base

class DataProcessing(Base):
    """
    SQLAlchemy model for data processing records.
    """
    __tablename__ = "data_processing"
    
    id = Column(Integer, primary_key=True, index=True)
    che_number = Column(String, unique=True, index=True, nullable=False)
    processed = Column(Boolean, default=False, nullable=False)
    company_name = Column(String, nullable=True)
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    def __init__(self, **kwargs):
        """
        Initialize a data processing record.
        
        Args:
            **kwargs: Keyword arguments for model fields
        """
        super(DataProcessing, self).__init__(**kwargs)
        if self.processed is None:
            self.processed = False
    
    def __repr__(self):
        """String representation of the model."""
        return f"<DataProcessing(id={self.id}, che_number={self.che_number}, processed={self.processed}, company_name={self.company_name})>" 