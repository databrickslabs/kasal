from datetime import datetime
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, HTTPException, status, Cookie, Response, Request
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy.ext.asyncio import AsyncSession
import jwt
from jwt.exceptions import PyJWTError as JWTError

from src.config import settings
from src.db.session import get_db
from src.schemas.user import (
    UserCreate, UserInDB, Token, UserLogin, PasswordReset, 
    PasswordResetRequest, PasswordChange, OAuthAuthorize, OAuthCallback
)
from src.services.auth_service import AuthService, decode_token
from src.services.user_service import UserService
from src.models.user import User
from src.dependencies.auth import get_current_user, get_current_active_user

router = APIRouter(
    prefix="/auth",
    tags=["authentication"],
    responses={401: {"description": "Unauthorized"}},
)

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="auth/login")

@router.post("/register", response_model=UserInDB, status_code=status.HTTP_201_CREATED)
async def register_user(
    user_data: UserCreate,
    session: AsyncSession = Depends(get_db)
):
    """Register a new user"""
    auth_service = AuthService(session)
    
    try:
        user = await auth_service.register_user(user_data)
        return user
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )

@router.post("/login", response_model=Token)
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_db),
    response: Response = None,
):
    """OAuth2 compatible token login, get an access token for future requests"""
    auth_service = AuthService(session)
    
    user = await auth_service.authenticate_user(form_data.username, form_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    tokens = await auth_service.create_user_tokens(user)
    
    # Set refresh token as httpOnly cookie if response is provided
    if response:
        cookie_max_age = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60  # in seconds
        response.set_cookie(
            key="refresh_token",
            value=tokens["refresh_token"],
            httponly=True,
            max_age=cookie_max_age,
            secure=settings.COOKIE_SECURE,  # True in production
            samesite="lax",
        )
    
    return tokens

@router.post("/login/alternative", response_model=Token)
async def login_with_json(
    login_data: UserLogin,
    session: AsyncSession = Depends(get_db),
    response: Response = None,
):
    """JSON-based login endpoint, alternative to OAuth2 form-based login"""
    auth_service = AuthService(session)
    
    user = await auth_service.authenticate_user(login_data.username_or_email, login_data.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    tokens = await auth_service.create_user_tokens(user)
    
    # Set refresh token as httpOnly cookie if response is provided
    if response:
        cookie_max_age = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60  # in seconds
        response.set_cookie(
            key="refresh_token",
            value=tokens["refresh_token"],
            httponly=True,
            max_age=cookie_max_age,
            secure=settings.COOKIE_SECURE,  # True in production
            samesite="lax",
        )
    
    return tokens

@router.post("/refresh-token", response_model=Token)
async def refresh_token(
    refresh_token: str = Cookie(None),
    token_in_body: Dict[str, str] = None,
    session: AsyncSession = Depends(get_db),
    response: Response = None,
):
    """Refresh an access token using a refresh token"""
    # Get refresh token from cookie or request body
    token = refresh_token
    if not token and token_in_body:
        token = token_in_body.get("refresh_token")
    
    if not token:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token required",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    auth_service = AuthService(session)
    new_tokens = await auth_service.refresh_access_token(token)
    
    if not new_tokens:
        # Clear invalid cookie if it exists
        if refresh_token and response:
            response.delete_cookie(key="refresh_token")
            
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired refresh token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    
    # Update cookie if needed
    if response:
        cookie_max_age = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60  # in seconds
        response.set_cookie(
            key="refresh_token",
            value=new_tokens["refresh_token"],
            httponly=True,
            max_age=cookie_max_age,
            secure=settings.COOKIE_SECURE,  # True in production
            samesite="lax",
        )
    
    return new_tokens

@router.post("/logout", status_code=status.HTTP_204_NO_CONTENT)
async def logout(
    refresh_token: str = Cookie(None),
    token_in_body: Dict[str, str] = None,
    session: AsyncSession = Depends(get_db),
    response: Response = None,
    current_user: User = Depends(get_current_user),
):
    """Logout a user by revoking their refresh token"""
    auth_service = AuthService(session)
    
    # Get refresh token from cookie or request body
    token = refresh_token
    if not token and token_in_body:
        token = token_in_body.get("refresh_token")
    
    # If token exists, revoke it
    if token:
        await auth_service.revoke_refresh_token(token)
    
    # In any case, clear the cookie
    if response:
        response.delete_cookie(key="refresh_token")
    
    # Return no content
    return None

@router.post("/password-reset-request", status_code=status.HTTP_202_ACCEPTED)
async def request_password_reset(
    reset_request: PasswordResetRequest,
    session: AsyncSession = Depends(get_db),
):
    """Request a password reset token"""
    # This would normally send an email with a reset link
    # For now, just return accepted without doing anything
    return {"message": "If the email is registered, a password reset link has been sent"}

@router.post("/password-reset", status_code=status.HTTP_200_OK)
async def reset_password(
    reset_data: PasswordReset,
    session: AsyncSession = Depends(get_db),
):
    """Reset a password using a token"""
    # This would normally verify the token and reset the password
    # For now, just return a placeholder response
    return {"message": "Password has been reset"}

@router.post("/password-change", status_code=status.HTTP_200_OK)
async def change_password(
    password_data: PasswordChange,
    current_user: User = Depends(get_current_active_user),
    session: AsyncSession = Depends(get_db),
):
    """Change a user's password"""
    auth_service = AuthService(session)
    user_service = UserService(session)
    
    # Verify current password
    user = await auth_service.authenticate_user(current_user.username, password_data.current_password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Current password is incorrect",
        )
    
    # Update password
    await user_service.update_password(current_user.id, password_data.new_password)
    
    # Revoke all refresh tokens for security
    await auth_service.revoke_all_user_tokens(current_user.id)
    
    return {"message": "Password has been changed"}

@router.get("/oauth/{provider}/authorize")
async def oauth_authorize(
    provider: str,
    redirect_uri: Optional[str] = None,
    session: AsyncSession = Depends(get_db),
):
    """Get OAuth authorization URL"""
    # This would normally return or redirect to the OAuth provider's auth URL
    # For simplicity, returning a placeholder response
    auth_url = f"https://{provider}.example.com/authorize?redirect_uri={redirect_uri or 'default'}"
    return {"auth_url": auth_url}

@router.post("/oauth/{provider}/callback", response_model=Token)
async def oauth_callback(
    provider: str,
    code: str,
    state: Optional[str] = None,
    redirect_uri: Optional[str] = None,
    session: AsyncSession = Depends(get_db),
    response: Response = None,
):
    """Handle OAuth callback and create/login user"""
    # This would normally exchange the code for tokens and user info
    # For simplicity, returning a placeholder token
    # In a real implementation, this would:
    # 1. Exchange code for token with provider
    # 2. Get user info from provider
    # 3. Create or log in user
    # 4. Generate our own tokens
    
    # Placeholder for demo
    mock_access_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJvYXV0aC11c2VyLWlkIiwicm9sZSI6InJlZ3VsYXIifQ.signature"
    mock_refresh_token = "mock_refresh_token_for_oauth"
    
    if response:
        cookie_max_age = settings.REFRESH_TOKEN_EXPIRE_DAYS * 24 * 60 * 60  # in seconds
        response.set_cookie(
            key="refresh_token",
            value=mock_refresh_token,
            httponly=True,
            max_age=cookie_max_age,
            secure=settings.COOKIE_SECURE,
            samesite="lax",
        )
    
    return {
        "access_token": mock_access_token,
        "refresh_token": mock_refresh_token,
        "token_type": "bearer",
    } 