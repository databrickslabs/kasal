from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Enum, Boolean, ForeignKey, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy import Enum as SQLAlchemyEnum
import sqlalchemy as sa
from src.db.base import Base
from uuid import uuid4
from src.models.enums import UserRole, UserStatus, IdentityProviderType

def generate_uuid():
    return str(uuid4())

class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=generate_uuid)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    display_name = Column(String, nullable=True)  # Moved from UserProfile
    # hashed_password removed - using OAuth proxy authentication
    role = Column(SQLAlchemyEnum(UserRole, name="user_role_enum"), default=UserRole.REGULAR)
    status = Column(SQLAlchemyEnum(UserStatus, name="user_status_enum"), default=UserStatus.ACTIVE)

    # New user-level permission fields
    is_system_admin = Column(Boolean, default=False, nullable=False)
    is_personal_workspace_manager = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    last_login = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    refresh_tokens = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")
    # Complex auth relationships removed - using simplified group-based roles
    # UserProfile removed - display_name moved to User model


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    token = Column(String, nullable=False, unique=True)  # Hashed token
    expires_at = Column(DateTime(timezone=True), nullable=False)
    is_revoked = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    
    # Relationships
    user = relationship("User", back_populates="refresh_tokens")


# ExternalIdentity model removed - simplified auth system


# Complex RBAC models removed - using simplified group-based roles instead


# IdentityProvider model removed - simplified auth system


# All complex RBAC models removed - using simplified group-based roles