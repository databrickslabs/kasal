from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List, Tuple
import json
import jwt
from jwt.exceptions import PyJWTError as JWTError
from passlib.context import CryptContext
from sqlalchemy.ext.asyncio import AsyncSession

from src.config import settings
from src.db.session import get_db
from src.models.user import User, RefreshToken, UserProfile, ExternalIdentity, IdentityProvider
from src.repositories.user_repository import (
    UserRepository, RefreshTokenRepository, UserProfileRepository,
    ExternalIdentityRepository, IdentityProviderRepository
)
from src.schemas.user import UserCreate, UserInDB, UserRole, TokenData

# Password hashing
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

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
        self.user_profile_repo = UserProfileRepository(UserProfile, session)
        self.external_identity_repo = ExternalIdentityRepository(ExternalIdentity, session)
        self.identity_provider_repo = IdentityProviderRepository(IdentityProvider, session)
    
    async def authenticate_user(self, username_or_email: str, password: str) -> Optional[User]:
        """Authenticate a user by username/email and password"""
        user = await self.user_repo.get_by_username_or_email(username_or_email)
        if not user:
            return None
        if not verify_password(password, user.hashed_password):
            return None
        
        # Update last login time
        await self.user_repo.update_last_login(user.id)
        
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
        user_dict = user_data.dict()
        user_dict.pop("password")
        user_dict["hashed_password"] = hashed_password
        user_dict["role"] = UserRole.REGULAR  # Default role for new users
        
        user = await self.user_repo.create(user_dict)
        
        # Create default empty profile
        profile_data = {
            "user_id": user.id,
            "display_name": user.username,  # Use username as initial display name
        }
        await self.user_profile_repo.create(profile_data)
        
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
    
    async def authenticate_with_external_provider(
        self, provider: str, provider_user_id: str, email: str, profile_data: Dict[str, Any] = None
    ) -> Tuple[User, bool]:
        """
        Authenticate a user with an external identity provider
        Returns a tuple of (user, is_new_user)
        """
        # Check if external identity exists
        external_id = await self.external_identity_repo.get_by_provider_and_id(provider, provider_user_id)
        
        if external_id:
            # Update last login
            await self.external_identity_repo.update_last_login(external_id.id)
            
            # Get the user
            user = await self.user_repo.get(external_id.user_id)
            if not user:
                raise ValueError("User not found for external identity")
            
            # Update user's last login
            await self.user_repo.update_last_login(user.id)
            
            return user, False
            
        else:
            # Check if user with this email exists
            user = await self.user_repo.get_by_email(email)
            
            if user:
                # Link external identity to existing user
                external_id_data = {
                    "user_id": user.id,
                    "provider": provider,
                    "provider_user_id": provider_user_id,
                    "email": email,
                    "profile_data": json.dumps(profile_data) if profile_data else None,
                }
                await self.external_identity_repo.create(external_id_data)
                
                # Update user's last login
                await self.user_repo.update_last_login(user.id)
                
                return user, False
                
            else:
                # Create new user with external identity
                # Generate random username base from email
                username_base = email.split("@")[0]
                username = username_base
                i = 1
                
                # Check if username exists, if so, append number
                while await self.user_repo.get_by_username(username):
                    username = f"{username_base}{i}"
                    i += 1
                
                # Create user with random password (not used)
                import secrets
                random_password = secrets.token_urlsafe(32)
                hashed_password = get_password_hash(random_password)
                
                user_data = {
                    "username": username,
                    "email": email,
                    "hashed_password": hashed_password,
                    "role": UserRole.REGULAR,  # Default role for new users
                }
                
                user = await self.user_repo.create(user_data)
                
                # Create profile
                display_name = None
                if profile_data:
                    # Try to extract name from profile data
                    display_name = profile_data.get("name") or profile_data.get("displayName")
                
                profile_data = {
                    "user_id": user.id,
                    "display_name": display_name or username,
                    "avatar_url": profile_data.get("picture") if profile_data else None,
                }
                await self.user_profile_repo.create(profile_data)
                
                # Create external identity
                external_id_data = {
                    "user_id": user.id,
                    "provider": provider,
                    "provider_user_id": provider_user_id,
                    "email": email,
                    "profile_data": json.dumps(profile_data) if profile_data else None,
                }
                await self.external_identity_repo.create(external_id_data)
                
                return user, True 