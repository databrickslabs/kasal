from typing import List, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy.ext.asyncio import AsyncSession
import jwt
from jwt.exceptions import PyJWTError as JWTError

from src.config import settings
from src.db.session import get_db
from src.services.user_service import UserService
from src.models.user import User
from src.schemas.user import TokenData

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    session: AsyncSession = Depends(get_db)
) -> User:
    """
    Get the current user from the token.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    
    try:
        # Decode JWT
        payload = jwt.decode(
            token, 
            settings.JWT_SECRET_KEY, 
            algorithms=[settings.JWT_ALGORITHM]
        )
        user_id: str = payload.get("sub")
        if user_id is None:
            raise credentials_exception
        
        token_data = TokenData(
            sub=user_id,
            role=payload.get("role"),
            exp=payload.get("exp")
        )
    except JWTError:
        raise credentials_exception
    
    # Get user from database
    user_service = UserService(session)
    user = await user_service.get_user(token_data.sub)
    if user is None:
        raise credentials_exception
    
    return user

async def get_current_active_user(
    current_user: User = Depends(get_current_user)
) -> User:
    """
    Get the current user and make sure they are active.
    """
    if current_user.status != "active":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Inactive user account"
        )
    return current_user

def check_user_role(allowed_roles: List[str]):
    """
    Check if the current user has one of the allowed roles.
    This is a dependency factory that creates a dependency.
    """
    async def _check_role(current_user: User = Depends(get_current_active_user)) -> User:
        if current_user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Not enough permissions"
            )
        return current_user
    
    return _check_role

def check_permission(permission: str):
    """
    Check if the current user has the required permission.
    This is more granular than role-based checks.
    """
    async def _check_permission(
        current_user: User = Depends(get_current_active_user),
        session: AsyncSession = Depends(get_db)
    ) -> User:
        # Admin always has all permissions
        if current_user.role == "admin":
            return current_user
        
        # For more granular permission checks, get the user's role and check if it has the permission
        # This would require loading the role with its permissions from the database
        
        # For now, using a simple role-based check
        resource, action = permission.split(":")
        
        # Technical users can perform most operations except user management
        if current_user.role == "technical" and resource != "user" and resource != "system":
            return current_user
        
        # Regular users can only execute and read certain resources
        if current_user.role == "regular" and action in ["read", "execute"]:
            if resource in ["agent", "task", "flow", "crew"]:
                return current_user
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not enough permissions: {permission} required"
        )
    
    return _check_permission

def check_any_permission(permissions: List[str]):
    """
    Check if the current user has any of the required permissions.
    """
    async def _check_any_permission(
        current_user: User = Depends(get_current_active_user),
        session: AsyncSession = Depends(get_db)
    ) -> User:
        # Admin always has all permissions
        if current_user.role == "admin":
            return current_user
        
        for permission in permissions:
            try:
                result = await check_permission(permission)(current_user, session)
                return result
            except HTTPException:
                continue
        
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Not enough permissions: one of {permissions} required"
        )
    
    return _check_any_permission 