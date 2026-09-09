from datetime import datetime, timezone
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import Boolean, Column, DateTime
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import String

from src.db.base import Base
from src.models.enums import UserRole, UserStatus


def generate_uuid():
    return str(uuid4())


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=generate_uuid)
    username = Column(String, unique=True, index=True, nullable=False)
    email = Column(String, unique=True, index=True, nullable=False)
    # The personal workspace's id — allocated ONCE and never derived at request
    # time. The derivation collapsed '@', '.', '-' and '+' to '_', so
    # alice.smith@ and alice-smith@ shared a workspace (audit F06 / R2-06).
    # Unique; NULL only until the startup heal or the first login assigns it.
    personal_group_id = Column(sa.String, unique=True, nullable=True, index=True)
    display_name = Column(String, nullable=True)  # Moved from UserProfile
    # hashed_password removed - using OAuth proxy authentication
    role = Column(
        SQLAlchemyEnum(UserRole, name="user_role_enum"), default=UserRole.REGULAR
    )
    status = Column(
        SQLAlchemyEnum(UserStatus, name="user_status_enum"), default=UserStatus.ACTIVE
    )

    # New user-level permission fields
    is_system_admin = Column(Boolean, default=False, nullable=False)
    is_personal_workspace_manager = Column(Boolean, default=False, nullable=False)

    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
    )
    last_login = Column(DateTime(timezone=True), nullable=True)

    # Complex auth relationships removed - using Databricks Apps proxy authentication
    # UserProfile removed - display_name moved to User model


# ExternalIdentity model removed - simplified auth system


# Complex RBAC models removed - using simplified group-based roles instead


# IdentityProvider model removed - simplified auth system


# All complex RBAC models removed - using simplified group-based roles
