"""
Seed the roles and privileges tables with default data and Databricks integration for Admin role.
"""
import os
import json
import logging
import asyncio
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
import aiohttp

from src.db.session import async_session_factory, SessionLocal
from src.models.user import Role, Privilege, RolePrivilege
from src.models.privileges import Privileges, DefaultRoles

# Configure logging
logger = logging.getLogger(__name__)

# Use centralized privilege definitions
DEFAULT_PRIVILEGES = Privileges.get_all_privileges()

# Use centralized role definitions  
DEFAULT_ROLES = DefaultRoles.get_all_roles()


class DatabricksPermissionChecker:
    """Helper class to check Databricks app permissions"""
    
    def __init__(self):
        self.app_name = os.getenv("DATABRICKS_APP_NAME")
        self.databricks_host = os.getenv("DATABRICKS_HOST")
        self.databricks_token = os.getenv("DATABRICKS_TOKEN")
        self.is_local_dev = os.getenv("ENVIRONMENT", "development").lower() in ("development", "dev", "local")
        
    async def get_app_managers(self):
        """
        Get users with 'Can Manage' permission for the Databricks app.
        Returns a list of user emails.
        """
        if self.is_local_dev:
            logger.info("Running in local development mode - using fallback admin detection")
            return self._get_fallback_admins()
            
        if not all([self.app_name, self.databricks_host, self.databricks_token]):
            if not self.is_local_dev:
                logger.error("Databricks configuration incomplete in production mode")
                raise Exception("Databricks configuration incomplete. Required: DATABRICKS_APP_NAME, DATABRICKS_HOST, DATABRICKS_TOKEN")
            logger.warning("Databricks configuration incomplete - falling back to local development mode")
            return self._get_fallback_admins()
            
        try:
            return await self._fetch_databricks_permissions()
        except Exception as e:
            logger.error(f"Failed to fetch Databricks permissions: {e}")
            # SECURITY: Never fallback in production - this would be a security risk
            if not self.is_local_dev:
                raise Exception(f"Failed to fetch Databricks permissions in production: {str(e)}")
            logger.info("Falling back to local development mode")
            return self._get_fallback_admins()
    
    def _get_fallback_admins(self):
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
    
    async def _fetch_databricks_permissions(self):
        """
        Fetch permissions from Databricks API.
        """
        url = f"{self.databricks_host.rstrip('/')}/api/2.0/workspace/apps/{self.app_name}/permissions"
        headers = {
            "Authorization": f"Bearer {self.databricks_token}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"Fetching Databricks app permissions for app: {self.app_name}")
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return self._extract_manage_users(data)
                else:
                    error_text = await response.text()
                    raise Exception(f"Databricks API error {response.status}: {error_text}")
    
    def _extract_manage_users(self, permissions_data):
        """
        Extract users with 'Can Manage' permission from Databricks response.
        """
        manage_users = []
        
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
        
        for acl_entry in access_control_list:
            user_email = acl_entry.get("user_name")
            if not user_email:
                continue
                
            permissions = acl_entry.get("all_permissions", [])
            for perm in permissions:
                if perm.get("permission_level") == "CAN_MANAGE":
                    manage_users.append(user_email)
                    break
        
        logger.info(f"Found {len(manage_users)} users with CAN_MANAGE permission: {manage_users}")
        return manage_users


async def seed_privileges(session: AsyncSession):
    """Seed default privileges."""
    logger.info("Seeding privileges...")
    
    # Get existing privileges
    result = await session.execute(select(Privilege.name))
    existing_privileges = set(result.scalars().all())
    
    privileges_added = 0
    privileges_updated = 0
    
    for name, description in DEFAULT_PRIVILEGES:
        if name not in existing_privileges:
            privilege = Privilege(
                name=name,
                description=description,
                created_at=datetime.utcnow()
            )
            session.add(privilege)
            privileges_added += 1
        else:
            # Update existing privilege description
            result = await session.execute(
                select(Privilege).filter(Privilege.name == name)
            )
            existing_privilege = result.scalars().first()
            if existing_privilege and existing_privilege.description != description:
                existing_privilege.description = description
                privileges_updated += 1
    
    await session.commit()
    logger.info(f"Privileges seeding: Added {privileges_added}, Updated {privileges_updated}")


async def seed_roles(session: AsyncSession):
    """Seed default roles with their privileges."""
    logger.info("Seeding roles...")
    
    # Get all privileges for mapping
    result = await session.execute(select(Privilege))
    all_privileges = {priv.name: priv for priv in result.scalars().all()}
    
    # Get existing roles
    result = await session.execute(
        select(Role).options(selectinload(Role.role_privileges))
    )
    existing_roles = {role.name: role for role in result.scalars().all()}
    
    roles_added = 0
    roles_updated = 0
    
    for role_name, role_data in DEFAULT_ROLES.items():
        if role_name not in existing_roles:
            # Create new role
            role = Role(
                name=role_name,
                description=role_data["description"],
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            session.add(role)
            await session.flush()  # Get the role ID
            
            # Add privileges to role
            for privilege_name in role_data["privileges"]:
                if privilege_name in all_privileges:
                    role_privilege = RolePrivilege(
                        role_id=role.id,
                        privilege_id=all_privileges[privilege_name].id
                    )
                    session.add(role_privilege)
            
            roles_added += 1
        else:
            # Update existing role
            role = existing_roles[role_name]
            if role.description != role_data["description"]:
                role.description = role_data["description"]
                role.updated_at = datetime.utcnow()
                roles_updated += 1
            
            # Update role privileges (simplified - could be more sophisticated)
            existing_privilege_names = {
                rp.privilege.name for rp in role.role_privileges
            }
            required_privilege_names = set(role_data["privileges"])
            
            # Add missing privileges
            for privilege_name in required_privilege_names - existing_privilege_names:
                if privilege_name in all_privileges:
                    role_privilege = RolePrivilege(
                        role_id=role.id,
                        privilege_id=all_privileges[privilege_name].id
                    )
                    session.add(role_privilege)
            
            # Remove extra privileges (only for admin role to prevent accidental removal)
            if role_name != "admin":
                for rp in role.role_privileges:
                    if rp.privilege.name not in required_privilege_names:
                        await session.delete(rp)
    
    await session.commit()
    logger.info(f"Roles seeding: Added {roles_added}, Updated {roles_updated}")


async def setup_databricks_admins():
    """
    Set up admin role assignments based on Databricks app permissions.
    This function will be called by other services to update admin memberships.
    """
    logger.info("Setting up Databricks-based admin role assignments...")
    
    try:
        permission_checker = DatabricksPermissionChecker()
        admin_emails = await permission_checker.get_app_managers()
        
        if not admin_emails:
            logger.warning("No admin emails found from Databricks or fallback methods")
            return
        
        # This functionality would be implemented by the user/tenant service
        # to actually assign users to the admin role based on their emails
        logger.info(f"Admin emails identified: {admin_emails}")
        logger.info("Admin role assignment should be handled by user/tenant services")
        
        return admin_emails
        
    except Exception as e:
        logger.error(f"Error setting up Databricks admins: {e}")
        raise


async def seed_async():
    """Seed roles and privileges into the database using async session."""
    logger.info("Seeding roles and privileges table (async)...")
    
    try:
        async with async_session_factory() as session:
            # Seed privileges first
            await seed_privileges(session)
            
            # Then seed roles with their privileges
            await seed_roles(session)
            
        # Set up Databricks admin assignments
        await setup_databricks_admins()
        
        logger.info("Roles and privileges seeding completed successfully")
        
    except Exception as e:
        logger.error(f"Error in roles seeding: {e}")
        raise


def seed_sync():
    """Seed roles and privileges into the database using sync session (not implemented)."""
    logger.info("Sync seeding not implemented for roles - use async version")
    raise NotImplementedError("Roles seeding requires async operations")


# Main entry point for seeding - can be called directly or by seed_runner
async def seed():
    """Main entry point for seeding roles and privileges."""
    logger.info("Roles seed function called")
    try:
        logger.info("Attempting to call seed_async in roles.py")
        await seed_async()
        logger.info("Roles seed_async completed successfully")
    except Exception as e:
        logger.error(f"Error in roles seed function: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise


# For direct external calls
if __name__ == "__main__":
    asyncio.run(seed())