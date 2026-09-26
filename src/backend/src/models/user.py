from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy import Boolean, DateTime
from sqlalchemy import Enum as SQLAlchemyEnum
from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column

from src.db.base import Base
from src.models.enums import UserRole, UserStatus


def generate_uuid():
    return str(uuid4())


class User(Base):
    __tablename__ = "users"

    id: Mapped[str] = mapped_column(String, primary_key=True, default=generate_uuid)
    username: Mapped[str] = mapped_column(
        String, unique=True, index=True, nullable=False
    )
    email: Mapped[str] = mapped_column(String, unique=True, index=True, nullable=False)
    # The personal workspace's id — allocated ONCE and never derived at request
    # time. The derivation collapsed '@', '.', '-' and '+' to '_', so
    # alice.smith@ and alice-smith@ shared a workspace (audit F06 / R2-06).
    # Unique; NULL only until the startup heal or the first login assigns it.
    personal_group_id: Mapped[Optional[str]] = mapped_column(
        sa.String, unique=True, nullable=True, index=True
    )
    display_name: Mapped[Optional[str]] = mapped_column(
        String, nullable=True
    )  # Moved from UserProfile
    # hashed_password removed - using OAuth proxy authentication
    role: Mapped[UserRole] = mapped_column(
        SQLAlchemyEnum(UserRole, name="user_role_enum"),
        default=UserRole.REGULAR,
        nullable=True,
    )
    status: Mapped[UserStatus] = mapped_column(
        SQLAlchemyEnum(UserStatus, name="user_status_enum"),
        default=UserStatus.ACTIVE,
        nullable=True,
    )

    # New user-level permission fields
    is_system_admin: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )
    is_personal_workspace_manager: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        nullable=True,
    )
    last_login: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Complex auth relationships removed - using Databricks Apps proxy authentication
    # UserProfile removed - display_name moved to User model


# ExternalIdentity model removed - simplified auth system


# Complex RBAC models removed - using simplified group-based roles instead


# IdentityProvider model removed - simplified auth system


# All complex RBAC models removed - using simplified group-based roles
