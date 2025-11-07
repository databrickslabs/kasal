from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
import json
import jwt
from jwt.exceptions import PyJWTError as JWTError
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from src.config import settings
from src.db.session import get_db
from src.models.user import User, RefreshToken
from src.repositories.user_repository import (
    UserRepository, RefreshTokenRepository
)
from src.schemas.user import UserCreate, UserInDB, UserRole, TokenData
from src.core.logger import LoggerManager

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Initialize logger
logger = LoggerManager.get_instance().system

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verify a password against its hash"""
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """Hash a password"""
    return pwd_context.hash(password)

def create_access_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Create a new JWT access token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES))
    to_encode.update({"exp": expire.timestamp()})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return encoded_jwt

def create_refresh_token(data: Dict[str, Any], expires_delta: Optional[timedelta] = None) -> str:
    """Create a new JWT refresh token"""
    to_encode = data.copy()
    expire = datetime.utcnow() + (expires_delta or timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS))
    to_encode.update({"exp": expire.timestamp()})
    encoded_jwt = jwt.encode(to_encode, settings.JWT_REFRESH_SECRET_KEY, algorithm=settings.JWT_ALGORITHM)
    return encoded_jwt

def decode_token(token: str, secret_key: str) -> Dict[str, Any]:
    """Decode a JWT token"""
    return jwt.decode(token, secret_key, algorithms=[settings.JWT_ALGORITHM])

def get_refresh_token_hash(token: str) -> str:
    """Hash a refresh token for storage"""
    return pwd_context.hash(token)

def verify_refresh_token(plain_token: str, hashed_token: str) -> bool:
    """Verify a refresh token against its hash"""
    return pwd_context.verify(plain_token, hashed_token)


class AuthService:
    """Service for authentication operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.user_repo = UserRepository(User, session)
        self.refresh_token_repo = RefreshTokenRepository(RefreshToken, session)
        # External identity and identity provider repos removed - using simplified auth
    

    async def authenticate_user(self, username_or_email: str, password: str) -> Optional[User]:
        """Authenticate a user by username/email and password"""
        user = await self.user_repo.get_by_username_or_email(username_or_email)
        if not user:
            return None
        if not verify_password(password, user.hashed_password):
            return None

        # Update last login time
        await self.user_repo.update_last_login(user.id)

        # Check if this is the first user logging in when no groups exist
        await self._handle_first_user_admin_setup(user)

        return user
    
    async def register_user(self, user_data: UserCreate) -> User:
        """Register a new user"""
        # Check if username or email already exists
        existing_username = await self.user_repo.get_by_username(user_data.username)
        if existing_username:
            raise ValueError("Username already registered")
        
        existing_email = await self.user_repo.get_by_email(user_data.email)
        if existing_email:
            raise ValueError("Email already registered")
        
        # Create user
        hashed_password = get_password_hash(user_data.password)
        user_dict = user_data.model_dump()
        user_dict.pop("password")
        user_dict["hashed_password"] = hashed_password
        user_dict["role"] = UserRole.REGULAR  # Default role for new users
        
        user = await self.user_repo.create(user_dict)

        return user
    
    async def create_user_tokens(self, user: User) -> Dict[str, str]:
        """Create access and refresh tokens for a user"""
        # Create access token
        access_token_data = {"sub": user.id, "role": user.role}
        access_token = create_access_token(access_token_data)
        
        # Create refresh token
        refresh_token_data = {"sub": user.id}
        refresh_token = create_refresh_token(refresh_token_data)
        
        # Store refresh token in database
        token_hash = get_refresh_token_hash(refresh_token)
        expires_at = datetime.utcnow() + timedelta(days=settings.REFRESH_TOKEN_EXPIRE_DAYS)
        token_data = {
            "user_id": user.id,
            "token": token_hash,
            "expires_at": expires_at,
        }
        await self.refresh_token_repo.create(token_data)
        
        return {
            "access_token": access_token,
            "refresh_token": refresh_token,
            "token_type": "bearer",
        }
    
    async def refresh_access_token(self, refresh_token: str) -> Optional[Dict[str, str]]:
        """Refresh an access token using a refresh token"""
        try:
            # Decode token without verification to get user ID
            payload = jwt.decode(
                refresh_token, 
                options={"verify_signature": False}
            )
            user_id = payload.get("sub")
            if not user_id:
                return None
            
            # Get stored token
            current_time = datetime.utcnow()
            stored_tokens = await self.refresh_token_repo.get_all(
                filters={"user_id": user_id, "is_revoked": False}
            )
            
            valid_token = None
            for token in stored_tokens:
                if token.expires_at > current_time and verify_refresh_token(refresh_token, token.token):
                    valid_token = token
                    break
            
            if not valid_token:
                return None
            
            # Verify token with secret
            payload = decode_token(refresh_token, settings.JWT_REFRESH_SECRET_KEY)
            
            # Get user
            user = await self.user_repo.get(user_id)
            if not user:
                return None
            
            # Create new access token
            access_token_data = {"sub": user.id, "role": user.role}
            access_token = create_access_token(access_token_data)
            
            return {
                "access_token": access_token,
                "refresh_token": refresh_token,  # Return same refresh token
                "token_type": "bearer",
            }
            
        except JWTError:
            return None
    
    async def revoke_refresh_token(self, refresh_token: str) -> bool:
        """Revoke a refresh token"""
        try:
            # Decode token without verification to get user ID
            payload = jwt.decode(
                refresh_token, 
                options={"verify_signature": False}
            )
            user_id = payload.get("sub")
            if not user_id:
                return False
            
            # Get stored tokens
            stored_tokens = await self.refresh_token_repo.get_all(
                filters={"user_id": user_id, "is_revoked": False}
            )
            
            for token in stored_tokens:
                if verify_refresh_token(refresh_token, token.token):
                    await self.refresh_token_repo.revoke_token(token.token)
                    return True
            
            return False
            
        except JWTError:
            return False
    
    async def revoke_all_user_tokens(self, user_id: str) -> None:
        """Revoke all refresh tokens for a user"""
        await self.refresh_token_repo.revoke_all_for_user(user_id)
    

    async def _handle_first_user_admin_setup(self, user: User) -> None:
        """
        Check if this is the first user when no groups exist.
        If so, create the first admin group and assign the user as admin.
        """
        try:
            # Import services here to avoid circular imports
            from src.services.group_service import GroupService
            from src.services.user_service import UserService

            # Initialize services
            group_service = GroupService(self.session)
            user_service = UserService(self.session)

            # Check if any groups exist
            total_groups = await group_service.get_total_group_count()

            if total_groups == 0:
                logger.info(f"No groups exist. Creating first admin group for user {user.email}")

                # Create the first admin group and assign the user as admin
                group, group_user = await group_service.create_first_admin_group_for_user(user)

                # Also update the user's global role to ADMIN if it's not already
                if user.role != UserRole.ADMIN:
                    await user_service.assign_role(user.id, UserRole.ADMIN)
                    logger.info(f"Updated user {user.email} global role to ADMIN")

        except Exception as e:
            # Log the error but don't fail authentication
            logger.error(f"Error during first user admin setup: {e}")
            # Don't raise the exception - allow authentication to continue 