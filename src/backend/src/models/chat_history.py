from datetime import datetime
from sqlalchemy import Column, String, Text, DateTime, JSON, Index
from typing import Optional
from uuid import uuid4

from src.db.base import Base

def generate_uuid():
    return str(uuid4())


class ChatHistory(Base):
    """
    ChatHistory model for tracking chat conversations in the workflow designer.
    Enhanced with group isolation for multi-group deployments.
    """
    
    __tablename__ = "chat_history"
    
    id = Column(String, primary_key=True, default=generate_uuid)
    session_id = Column(String, nullable=False, index=True)  # Group related messages
    user_id = Column(String, nullable=False, index=True)    # User identifier
    message_type = Column(String, nullable=False)           # 'user' or 'assistant'
    content = Column(Text, nullable=False)                  # Message content
    intent = Column(String, nullable=True)                  # Detected intent (generate_agent, etc.)
    confidence = Column(String, nullable=True)              # Confidence score as string
    generation_result = Column(JSON, nullable=True)         # Generated agent/task/crew data
    timestamp = Column(DateTime, default=datetime.utcnow, nullable=False)  # Timezone-naive UTC
    
    # Multi-group fields (REQUIRED for all models)
    group_id = Column(String(100), index=True, nullable=True)  # Group isolation
    tenant_id = Column(String(100), index=True, nullable=True)  # Legacy compatibility
    group_email = Column(String(255), nullable=True)           # Creator email for audit
    tenant_email = Column(String(255), nullable=True)          # Legacy compatibility
    
    # Database indexes for performance
    __table_args__ = (
        Index('idx_chat_history_session_timestamp', 'session_id', 'timestamp'),
        Index('idx_chat_history_user_timestamp', 'user_id', 'timestamp'),
        Index('idx_chat_history_group_timestamp', 'group_id', 'timestamp'),
    )