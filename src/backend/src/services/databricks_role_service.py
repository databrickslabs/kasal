"""
Service for managing Databricks-based role assignments using RBAC.
Integrates with Databricks app permissions to automatically assign admin roles.
"""
import os
import json
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import aiohttp
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_service import BaseService
from src.models.user import User, Role, UserRole
from src.repositories.user_repository import UserRepository, RoleRepository, UserRoleRepository
from src.services.user_service import UserService

logger = logging.getLogger(__name__)


class DatabricksRoleService(BaseService):
    """
    Service for managing role assignments based on Databricks app permissions using RBAC.
    
    This service:
    1. Fetches permissions from Databricks using the app name
    2. Identifies users with 'Can Manage' permission
    3. Assigns admin roles to those users using proper RBAC
    4. Provides fallback for local development
    """
    
    def __init__(self, session: AsyncSession):
        super().__init__(session)
        self.user_repository = UserRepository(User, session)
        self.role_repository = RoleRepository(Role, session)
        self.user_role_repository = UserRoleRepository(UserRole, session)
        self.user_service = UserService(session)
        
        # Databricks configuration
        self.app_name = os.getenv("DATABRICKS_APP_NAME")
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")
        self.is_local_dev = os.getenv("ENVIRONMENT", "development").lower() in ("development", "dev", "local")
        
    async def sync_admin_roles(self) -> Dict[str, Any]:
        """
        Synchronize admin role assignments based on Databricks app permissions.
        
        Returns:
            Dict with sync results including assigned users and any errors
        """
        logger.info("Starting Databricks admin role synchronization...")
        
        try:
            # Get admin emails from Databricks or fallback
            admin_emails = await self.get_databricks_app_managers()
            
            if not admin_emails:
                logger.warning("No admin emails found")
                return {"success": False, "error": "No admin emails found"}
            
            # Get or create admin role
            admin_role = await self._get_or_create_admin_role()
            
            # Process each admin email
            results = {
                "success": True,
                "admin_emails": admin_emails,
                "processed_users": [],
                "errors": []
            }
            
            for email in admin_emails:
                try:
                    user_result = await self._process_admin_user(email, admin_role)
                    results["processed_users"].append(user_result)
                except Exception as e:
                    error_msg = f"Error processing admin user {email}: {str(e)}"
                    logger.error(error_msg)
                    results["errors"].append(error_msg)
            
            logger.info(f"Admin role synchronization completed. Processed {len(results['processed_users'])} users")
            return results
            
        except Exception as e:
            logger.error(f"Failed to sync admin roles: {e}")
            return {"success": False, "error": str(e)}
    
    async def get_databricks_app_managers(self, user_token: Optional[str] = None) -> List[str]:
        """
        Get emails of users with 'Can Manage' permission for the Databricks app.
        
        Args:
            user_token: Optional user access token for OBO authentication
        
        Returns:
            List of email addresses
        """
        if self.is_local_dev:
            logger.info("Running in local development mode - using fallback admin detection")
            return self._get_fallback_admins()
            
        if not all([self.app_name, self.databricks_host]):
            logger.error("Databricks configuration incomplete in production mode")
            logger.error(f"app_name: {self.app_name}, databricks_host: {self.databricks_host}")
            # Return empty list instead of raising exception
            return []
            
        try:
            manager_emails = await self._fetch_databricks_permissions(user_token)
            logger.info(f"get_databricks_app_managers returning: {manager_emails}")
            return manager_emails
        except Exception as e:
            logger.error(f"Failed to fetch Databricks permissions: {e}")
            # Return empty list for safety instead of raising exception
            return []
    
    def _get_fallback_admins(self) -> List[str]:
        """
        Fallback method for local development ONLY.
        Returns admins based on environment variables or default patterns.
        
        SECURITY: This method should NEVER be called in production.
        """
        # SECURITY CHECK: Ensure we're in development mode
        if not self.is_local_dev:
            logger.error("SECURITY: _get_fallback_admins called in production mode!")
            raise Exception("SECURITY: Fallback admin method cannot be used in production")
        
        # Check for explicitly defined admin emails
        admin_emails_env = os.getenv("ADMIN_EMAILS", "")
        if admin_emails_env:
            emails = [email.strip() for email in admin_emails_env.split(",") if email.strip()]
            logger.info(f"Using admin emails from ADMIN_EMAILS env var: {emails}")
            return emails
        
        # Check for developer email
        dev_email = os.getenv("DEVELOPER_EMAIL", "")
        if dev_email:
            logger.info(f"Using developer email as admin: {dev_email}")
            return [dev_email]
        
        # Default mock users from the frontend DeveloperMode.tsx for testing
        # These match the users available in the frontend mock user selector
        mock_admin_emails = [
            # Mock users from frontend - one admin from each tenant for testing
            "alice@acme-corp.com",        # Alice (Acme Corp)
            "bob@tech-startup.io",        # Bob (Tech Startup) 
            "charlie@big-enterprise.com", # Charlie (Big Enterprise)
            
            # Additional common development patterns
            "admin@localhost",
            "admin@example.com",
            "developer@localhost", 
            "test@example.com"
        ]
        
        logger.info(f"Using default mock admin emails for development: {mock_admin_emails}")
        return mock_admin_emails
    
    async def _fetch_databricks_permissions(self, user_token: Optional[str] = None) -> List[str]:
        """
        Fetch permissions from Databricks API using service principal authentication.
        
        Args:
            user_token: Optional user access token (not used for permissions API)
            
        Returns:
            List of email addresses with 'Can Manage' permission
        """
        # Ensure the host has https:// protocol
        databricks_host = self.databricks_host
        if not databricks_host.startswith(('http://', 'https://')):
            databricks_host = f"https://{databricks_host}"
        url = f"{databricks_host.rstrip('/')}/api/2.0/permissions/apps/{self.app_name}"
        
        # For checking app permissions, we MUST use service principal auth
        # The user token (X-Forwarded-Access-Token) is scoped and cannot call the permissions API
        
        # Check if we have service principal credentials
        client_id = os.getenv("DATABRICKS_CLIENT_ID")
        client_secret = os.getenv("DATABRICKS_CLIENT_SECRET")
        
        if client_id and client_secret:
            # Use service principal OAuth authentication
            logger.info("Using service principal OAuth for permissions check")
            try:
                # Get OAuth token using client credentials flow
                oauth_url = f"{databricks_host.rstrip('/')}/oidc/v1/token"
                
                async with aiohttp.ClientSession() as oauth_session:
                    data = {
                        "grant_type": "client_credentials",
                        "client_id": client_id,
                        "client_secret": client_secret,
                        "scope": "all-apis"
                    }
                    
                    async with oauth_session.post(oauth_url, data=data) as oauth_response:
                        if oauth_response.status == 200:
                            oauth_data = await oauth_response.json()
                            access_token = oauth_data.get("access_token")
                            
                            if access_token:
                                headers = {
                                    "Authorization": f"Bearer {access_token}",
                                    "Content-Type": "application/json"
                                }
                                logger.info("Successfully obtained service principal OAuth token")
                            else:
                                logger.error("No access token in OAuth response")
                                return []
                        else:
                            error_text = await oauth_response.text()
                            logger.error(f"OAuth request failed with status {oauth_response.status}: {error_text}")
                            return []
                            
            except Exception as e:
                logger.error(f"Error getting service principal OAuth token: {e}")
                return []
        else:
            # Try to get auth headers from databricks_auth utility
            # This will try various authentication methods
            from src.utils.databricks_auth import get_databricks_auth_headers
            headers, error = await get_databricks_auth_headers()
            
            if error or not headers:
                # Fallback to environment token if available
                if self.databricks_token:
                    headers = {
                        "Authorization": f"Bearer {self.databricks_token}",
                        "Content-Type": "application/json"
                    }
                    logger.info(f"Using environment token for permissions check")
                else:
                    logger.error(f"No authentication available for permissions check - error: {error}")
                    logger.warning("Cannot verify permissions - no service principal credentials or valid token")
                    return []
            else:
                logger.info(f"Using databricks_auth headers for permissions check")
        
        logger.info(f"Fetching Databricks app permissions for app: {self.app_name} from URL: {url}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                logger.info(f"API Response status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    logger.info(f"Permissions response: {data}")
                    users = self._extract_manage_users(data)
                    logger.info(f"Extracted users with CAN_MANAGE: {users}")
                    return users
                elif response.status == 404:
                    logger.warning(f"Databricks app '{self.app_name}' not found (404)")
                    return []
                else:
                    error_text = await response.text()
                    logger.error(f"Databricks API error {response.status}: {error_text}")
                    # Don't raise exception, return empty list for safety
                    return []
    
    def _extract_manage_users(self, permissions_data: Dict[str, Any]) -> List[str]:
        """
        Extract users with 'Can Manage' permission from Databricks response.
        
        Args:
            permissions_data: Response from Databricks permissions API
            
        Returns:
            List of email addresses
        """
        manage_users = []
        
        # Log the entire response structure for debugging
        logger.info(f"Full permissions response structure: {json.dumps(permissions_data, indent=2)}")
        
        # The structure may vary, but typically:
        # permissions_data = {
        #   "object_id": "...",
        #   "object_type": "...", 
        #   "access_control_list": [
        #     {
        #       "user_name": "user@example.com",
        #       "all_permissions": [
        #         {"permission_level": "CAN_MANAGE"}
        #       ]
        #     }
        #   ]
        # }
        
        access_control_list = permissions_data.get("access_control_list", [])
        logger.info(f"Access control list has {len(access_control_list)} entries")
        
        for acl_entry in access_control_list:
            user_email = acl_entry.get("user_name")
            group_name = acl_entry.get("group_name")
            
            logger.info(f"Processing ACL entry - user: {user_email}, group: {group_name}")
            
            if not user_email:
                logger.info(f"Skipping entry with no user_name (might be group: {group_name})")
                continue
                
            permissions = acl_entry.get("all_permissions", [])
            logger.info(f"User {user_email} has permissions: {permissions}")
            
            for perm in permissions:
                if perm.get("permission_level") == "CAN_MANAGE":
                    manage_users.append(user_email)
                    logger.info(f"Added {user_email} to manage_users list")
                    break
        
        logger.info(f"Final list - Found {len(manage_users)} users with CAN_MANAGE permission: {manage_users}")
        return manage_users
    
    async def _get_or_create_admin_role(self) -> Role:
        """
        Get or create the admin role.
        
        Returns:
            The admin Role object
        """
        admin_role = await self.role_repository.get_by_name("admin")
        
        if not admin_role:
            logger.warning("Admin role not found - this should have been created by role seeder")
            raise Exception("Admin role not found. Please run role seeder first.")
        
        return admin_role
    
    async def _process_admin_user(self, email: str, admin_role: Role) -> Dict[str, Any]:
        """
        Process a single admin user - create user if needed and assign admin role.
        
        Args:
            email: User's email address
            admin_role: The admin Role object
            
        Returns:
            Dict with processing results
        """
        result = {
            "email": email,
            "user_created": False,
            "role_assigned": False,
            "already_admin": False
        }
        
        try:
            # Get or create user
            user = await self.user_repository.get_by_email(email)
            
            if not user:
                # Create user - in a real system, this might be handled differently
                # For now, we'll create a placeholder user that will be completed on first login
                user = await self._create_placeholder_user(email)
                result["user_created"] = True
                logger.info(f"Created placeholder user for admin: {email}")
            
            # Check if user already has admin role
            has_admin_role = await self.user_role_repository.has_role(user.id, "admin")
            
            if has_admin_role:
                result["already_admin"] = True
                logger.info(f"User {email} already has admin role")
            else:
                # Assign admin role to user
                await self.user_role_repository.assign_role(
                    user_id=user.id,
                    role_id=admin_role.id,
                    assigned_by="databricks_sync"
                )
                # Flush changes - commit is handled by UnitOfWork
                await self.session.flush()
                result["role_assigned"] = True
                logger.info(f"Assigned admin role to user {email}")
            
        except Exception as e:
            error_msg = f"Error processing admin user {email}: {str(e)}"
            logger.error(error_msg)
            result["error"] = error_msg
        
        return result
    
    async def _create_placeholder_user(self, email: str) -> User:
        """
        Create a placeholder user for an admin identified from Databricks.
        This user will be completed when they first log in.
        
        Args:
            email: User's email address
            
        Returns:
            Created User object
        """
        from src.models.enums import UserRole, UserStatus
        
        # Extract username from email
        username = email.split("@")[0]
        
        # Create placeholder user
        user = User(
            username=username,
            email=email,
            hashed_password="placeholder_password",  # Will be set on first login
            role=UserRole.ADMIN,  # Legacy role field
            status=UserStatus.ACTIVE
        )
        
        self.session.add(user)
        await self.session.flush()  # Get the user ID
        
        return user
    
    async def check_user_admin_access(self, user_id: str) -> bool:
        """
        Check if a user has admin access using RBAC.
        
        Args:
            user_id: User's ID
            
        Returns:
            True if user has admin role, False otherwise
        """
        try:
            return await self.user_role_repository.has_role(user_id, "admin")
        except Exception as e:
            logger.error(f"Error checking admin access for user {user_id}: {e}")
            return False
    
    async def check_user_privilege(self, user_id: str, privilege_name: str) -> bool:
        """
        Check if a user has a specific privilege using RBAC.
        
        Args:
            user_id: User's ID
            privilege_name: Name of the privilege to check
            
        Returns:
            True if user has the privilege, False otherwise
        """
        try:
            return await self.user_role_repository.has_privilege(user_id, privilege_name)
        except Exception as e:
            logger.error(f"Error checking privilege {privilege_name} for user {user_id}: {e}")
            return False
    
    async def get_user_roles(self, user_id: str) -> List[Role]:
        """
        Get all roles assigned to a user.
        
        Args:
            user_id: User's ID
            
        Returns:
            List of Role objects
        """
        try:
            return await self.user_role_repository.get_user_roles(user_id)
        except Exception as e:
            logger.error(f"Error getting roles for user {user_id}: {e}")
            return []
    
    async def get_user_privileges(self, user_id: str) -> List[str]:
        """
        Get all privilege names for a user based on their roles.
        
        Args:
            user_id: User's ID
            
        Returns:
            List of privilege names
        """
        try:
            privileges = await self.user_role_repository.get_user_privileges(user_id)
            return [privilege.name for privilege in privileges]
        except Exception as e:
            logger.error(f"Error getting privileges for user {user_id}: {e}")
            return []