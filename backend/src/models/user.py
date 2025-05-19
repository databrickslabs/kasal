from datetime import datetime, timezone
from sqlalchemy import Column, String, DateTime, Enum, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy import Enum as SQLAlchemyEnum
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
    hashed_password = Column(String, nullable=False)
    role = Column(SQLAlchemyEnum(UserRole, name="user_role_enum"), default=UserRole.REGULAR)
    status = Column(SQLAlchemyEnum(UserStatus, name="user_status_enum"), default=UserStatus.ACTIVE)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    last_login = Column(DateTime(timezone=True), nullable=True)

    # Relationships
    profile = relationship("UserProfile", back_populates="user", uselist=False, cascade="all, delete-orphan")
    refresh_tokens = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")
    external_identities = relationship("ExternalIdentity", back_populates="user", cascade="all, delete-orphan")


class UserProfile(Base):
    __tablename__ = "user_profiles"

    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"), unique=True)
    display_name = Column(String, nullable=True)
    avatar_url = Column(String, nullable=True)
    preferences = Column(String, nullable=True)  # JSON string of preferences
    
    # Relationships
    user = relationship("User", back_populates="profile")


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


class ExternalIdentity(Base):
    __tablename__ = "external_identities"

    id = Column(String, primary_key=True, default=generate_uuid)
    user_id = Column(String, ForeignKey("users.id", ondelete="CASCADE"))
    provider = Column(String, nullable=False)  # OAuth provider name
    provider_user_id = Column(String, nullable=False)  # User ID in the external system
    email = Column(String, nullable=True)
    profile_data = Column(String, nullable=True)  # JSON string of profile data
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    last_login = Column(DateTime(timezone=True), nullable=True)
    
    # Relationships
    user = relationship("User", back_populates="external_identities")
    
    __table_args__ = (
        # Unique constraint to prevent duplicate provider identity
        {'unique_constraint': ['provider', 'provider_user_id']}
    )


class Role(Base):
    __tablename__ = "roles"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc))
    
    # Relationships
    role_privileges = relationship("RolePrivilege", back_populates="role", cascade="all, delete-orphan")


class Privilege(Base):
    __tablename__ = "privileges"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False, unique=True)  # Format: "resource:action"
    description = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    role_privileges = relationship("RolePrivilege", back_populates="privilege", cascade="all, delete-orphan")


class RolePrivilege(Base):
    __tablename__ = "role_privileges"

    id = Column(String, primary_key=True, default=generate_uuid)
    role_id = Column(String, ForeignKey("roles.id", ondelete="CASCADE"))
    privilege_id = Column(String, ForeignKey("privileges.id", ondelete="CASCADE"))
    
    # Relationships
    role = relationship("Role", back_populates="role_privileges")
    privilege = relationship("Privilege", back_populates="role_privileges")
    
    __table_args__ = (
        # Unique constraint to prevent duplicate role-privilege pairs
        {'unique_constraint': ['role_id', 'privilege_id']}
    )


class IdentityProvider(Base):
    __tablename__ = "identity_providers"

    id = Column(String, primary_key=True, default=generate_uuid)
    name = Column(String, nullable=False)
    type = Column(SQLAlchemyEnum(IdentityProviderType, name="identity_provider_type_enum"), nullable=False)
    config = Column(String, nullable=False)  # JSON configuration
    enabled = Column(Boolean, default=True)
    is_default = Column(Boolean, default=False)
    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc), onupdate=datetime.now(timezone.utc)) 