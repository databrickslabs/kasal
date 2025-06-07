"""
Tenant service for managing multi-tenant isolation.

This service handles automatic tenant creation and user management
for the simple multi-tenant foundation that can later evolve into
Unity Catalog integration.
"""
from datetime import datetime
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from src.models.tenant import Tenant, TenantUser
from src.models.enums import TenantStatus, TenantUserRole, TenantUserStatus, UserRole, UserStatus
from src.models.user import User
from src.utils.user_context import TenantContext
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().system


class TenantService:
    """Service for managing tenants and tenant users."""
    
    def __init__(self, session: AsyncSession):
        self.session = session
    
    async def ensure_tenant_exists(self, tenant_context: TenantContext) -> Optional[Tenant]:
        """
        Ensure tenant exists, creating it automatically if needed.
        
        This is the core auto-tenant creation logic for Databricks Apps deployment.
        When a user with a new email domain accesses the system, we automatically
        create a tenant for their organization.
        
        Args:
            tenant_context: Context with tenant_id and email_domain
            
        Returns:
            Tenant: The existing or newly created tenant
        """
        if not tenant_context.tenant_id or not tenant_context.email_domain:
            logger.warning("Cannot create tenant: missing tenant_id or email_domain")
            return None
        
        # Check if tenant already exists
        stmt = select(Tenant).where(Tenant.id == tenant_context.tenant_id)
        result = await self.session.execute(stmt)
        tenant = result.scalar_one_or_none()
        
        if tenant:
            logger.debug(f"Tenant {tenant_context.tenant_id} already exists")
            return tenant
        
        # Auto-create tenant
        tenant = Tenant(
            id=tenant_context.tenant_id,
            name=self._generate_tenant_name(tenant_context.email_domain),
            email_domain=tenant_context.email_domain,
            status=TenantStatus.ACTIVE,
            description=f"Auto-created tenant for {tenant_context.email_domain}",
            auto_created=True,
            created_by_email=tenant_context.tenant_email,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.session.add(tenant)
        await self.session.commit()
        
        logger.info(f"Auto-created tenant {tenant_context.tenant_id} for domain {tenant_context.email_domain}")
        return tenant
    
    async def ensure_tenant_user_exists(
        self, 
        tenant_context: TenantContext, 
        user_id: str
    ) -> Optional[TenantUser]:
        """
        Ensure tenant user association exists, creating it automatically if needed.
        
        Args:
            tenant_context: Context with tenant information
            user_id: User ID to associate with tenant
            
        Returns:
            TenantUser: The existing or newly created tenant user association
        """
        if not tenant_context.tenant_id:
            return None
        
        # Check if tenant user already exists
        stmt = select(TenantUser).where(
            TenantUser.tenant_id == tenant_context.tenant_id,
            TenantUser.user_id == user_id
        )
        result = await self.session.execute(stmt)
        tenant_user = result.scalar_one_or_none()
        
        if tenant_user:
            return tenant_user
        
        # Auto-create tenant user association
        tenant_user = TenantUser(
            id=f"{tenant_context.tenant_id}_{user_id}",
            tenant_id=tenant_context.tenant_id,
            user_id=user_id,
            role=TenantUserRole.USER,  # Default role
            status=TenantUserStatus.ACTIVE,
            joined_at=datetime.utcnow(),
            auto_created=True,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.session.add(tenant_user)
        await self.session.commit()
        
        logger.info(f"Auto-created tenant user association for {user_id} in tenant {tenant_context.tenant_id}")
        return tenant_user
    
    async def get_tenant_by_email_domain(self, email_domain: str) -> Optional[Tenant]:
        """
        Get tenant by email domain.
        
        Args:
            email_domain: Email domain to look up
            
        Returns:
            Tenant: Tenant for the email domain, if exists
        """
        stmt = select(Tenant).where(Tenant.email_domain == email_domain)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
    
    async def get_user_tenants(self, user_id: str) -> list[Tenant]:
        """
        Get all tenants a user belongs to.
        
        Args:
            user_id: User ID to look up
            
        Returns:
            list[Tenant]: List of tenants the user belongs to
        """
        stmt = (
            select(Tenant)
            .join(TenantUser, Tenant.id == TenantUser.tenant_id)
            .where(
                TenantUser.user_id == user_id,
                TenantUser.status == TenantUserStatus.ACTIVE,
                Tenant.status == TenantStatus.ACTIVE
            )
        )
        result = await self.session.execute(stmt)
        return result.scalars().all()
    
    async def create_tenant(
        self,
        name: str,
        email_domain: str,
        description: str = None,
        created_by_email: str = None
    ) -> Tenant:
        """
        Create a new tenant manually.
        
        Args:
            name: Human-readable tenant name
            email_domain: Email domain or identifier
            description: Optional description
            created_by_email: Email of creator
            
        Returns:
            Tenant: Created tenant
        """
        # Generate tenant ID from email domain
        tenant_id = Tenant.generate_tenant_id(email_domain)
        
        # Check if tenant already exists
        stmt = select(Tenant).where(Tenant.id == tenant_id)
        result = await self.session.execute(stmt)
        existing_tenant = result.scalar_one_or_none()
        
        if existing_tenant:
            raise ValueError(f"Tenant with domain '{email_domain}' already exists")
        
        # Create new tenant
        tenant = Tenant(
            id=tenant_id,
            name=name,
            email_domain=email_domain,
            status=TenantStatus.ACTIVE,
            description=description,
            auto_created=False,
            created_by_email=created_by_email,
            created_at=datetime.utcnow(),
            updated_at=datetime.utcnow()
        )
        
        self.session.add(tenant)
        await self.session.commit()
        
        logger.info(f"Created tenant {tenant_id} manually")
        return tenant
    
    async def list_tenants(self, skip: int = 0, limit: int = 100) -> list[Tenant]:
        """
        List all tenants.
        
        Args:
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            list[Tenant]: List of tenants
        """
        stmt = select(Tenant).offset(skip).limit(limit).order_by(Tenant.created_at.desc())
        result = await self.session.execute(stmt)
        return result.scalars().all()
    
    async def get_tenant_by_id(self, tenant_id: str) -> Optional[Tenant]:
        """
        Get tenant by ID.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            Tenant: Tenant if found, None otherwise
        """
        stmt = select(Tenant).where(Tenant.id == tenant_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
    
    async def update_tenant(
        self,
        tenant_id: str,
        **updates
    ) -> Tenant:
        """
        Update a tenant.
        
        Args:
            tenant_id: Tenant ID to update
            **updates: Fields to update
            
        Returns:
            Tenant: Updated tenant
        """
        stmt = select(Tenant).where(Tenant.id == tenant_id)
        result = await self.session.execute(stmt)
        tenant = result.scalar_one_or_none()
        
        if not tenant:
            raise ValueError(f"Tenant {tenant_id} not found")
        
        # Update fields
        for field, value in updates.items():
            if hasattr(tenant, field):
                setattr(tenant, field, value)
        
        tenant.updated_at = datetime.utcnow()
        await self.session.commit()
        
        return tenant
    
    async def get_tenant_user_count(self, tenant_id: str) -> int:
        """
        Get count of users in a tenant.
        
        Args:
            tenant_id: Tenant ID
            
        Returns:
            int: Number of users in tenant
        """
        from sqlalchemy import func
        
        stmt = select(func.count(TenantUser.id)).where(
            TenantUser.tenant_id == tenant_id,
            TenantUser.status == TenantUserStatus.ACTIVE
        )
        result = await self.session.execute(stmt)
        return result.scalar() or 0
    
    async def list_tenant_users(
        self, 
        tenant_id: str, 
        skip: int = 0, 
        limit: int = 100
    ) -> list[TenantUser]:
        """
        List users in a tenant.
        
        Args:
            tenant_id: Tenant ID
            skip: Number of records to skip
            limit: Maximum number of records to return
            
        Returns:
            list[TenantUser]: List of tenant users
        """
        stmt = (
            select(TenantUser)
            .where(TenantUser.tenant_id == tenant_id)
            .offset(skip)
            .limit(limit)
            .order_by(TenantUser.joined_at.desc())
        )
        result = await self.session.execute(stmt)
        
        # Enhance with actual user emails
        tenant_users = result.scalars().all()
        
        # Join with User table to get actual emails
        enhanced_results = []
        for tenant_user in tenant_users:
            # Get the actual user email
            user_stmt = select(User).where(User.id == tenant_user.user_id)
            user_result = await self.session.execute(user_stmt)
            user = user_result.scalar_one_or_none()
            
            email = user.email if user else f"{tenant_user.user_id}@databricks.com"
            enhanced_results.append((tenant_user, email))
        
        return enhanced_results
    
    async def assign_user_to_tenant(
        self,
        tenant_id: str,
        user_email: str,
        role: TenantUserRole = TenantUserRole.USER,
        assigned_by_email: str = None
    ) -> TenantUser:
        """
        Assign a user to a tenant manually.
        
        Args:
            tenant_id: Tenant ID
            user_email: User email
            role: Role to assign
            assigned_by_email: Email of admin assigning user
            
        Returns:
            TenantUser: Created or updated tenant user
        """
        # Generate user_id from email (simple approach)
        user_id = user_email.split('@')[0]
        
        # Ensure User record exists
        user_stmt = select(User).where(User.email == user_email)
        user_result = await self.session.execute(user_stmt)
        user = user_result.scalar_one_or_none()
        
        if not user:
            # Create a basic User record
            from uuid import uuid4
            user = User(
                id=str(uuid4()),
                username=user_id,
                email=user_email,
                hashed_password="",  # No password for tenant-assigned users
                role=UserRole.REGULAR,
                status=UserStatus.ACTIVE,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            self.session.add(user)
            await self.session.flush()  # Get the ID without committing
        
        # Use the actual user ID
        actual_user_id = user.id
        
        # Check if association already exists
        stmt = select(TenantUser).where(
            TenantUser.tenant_id == tenant_id,
            TenantUser.user_id == actual_user_id
        )
        result = await self.session.execute(stmt)
        tenant_user = result.scalar_one_or_none()
        
        if tenant_user:
            # Update existing association
            tenant_user.role = role
            tenant_user.status = TenantUserStatus.ACTIVE
            tenant_user.updated_at = datetime.utcnow()
        else:
            # Create new association
            tenant_user = TenantUser(
                id=f"{tenant_id}_{actual_user_id}",
                tenant_id=tenant_id,
                user_id=actual_user_id,
                role=role,
                status=TenantUserStatus.ACTIVE,
                joined_at=datetime.utcnow(),
                auto_created=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            self.session.add(tenant_user)
        
        await self.session.commit()
        
        logger.info(f"Assigned user {user_email} to tenant {tenant_id} with role {role}")
        return tenant_user, user_email
    
    async def update_tenant_user(
        self,
        tenant_id: str,
        user_id: str,
        **updates
    ) -> TenantUser:
        """
        Update a tenant user.
        
        Args:
            tenant_id: Tenant ID
            user_id: User ID
            **updates: Fields to update
            
        Returns:
            TenantUser: Updated tenant user
        """
        stmt = select(TenantUser).where(
            TenantUser.tenant_id == tenant_id,
            TenantUser.user_id == user_id
        )
        result = await self.session.execute(stmt)
        tenant_user = result.scalar_one_or_none()
        
        if not tenant_user:
            raise ValueError(f"User {user_id} not found in tenant {tenant_id}")
        
        # Update fields
        for field, value in updates.items():
            if hasattr(tenant_user, field):
                setattr(tenant_user, field, value)
        
        tenant_user.updated_at = datetime.utcnow()
        await self.session.commit()
        
        return tenant_user
    
    async def remove_user_from_tenant(
        self,
        tenant_id: str,
        user_id: str
    ):
        """
        Remove a user from a tenant.
        
        Args:
            tenant_id: Tenant ID
            user_id: User ID
        """
        stmt = select(TenantUser).where(
            TenantUser.tenant_id == tenant_id,
            TenantUser.user_id == user_id
        )
        result = await self.session.execute(stmt)
        tenant_user = result.scalar_one_or_none()
        
        if not tenant_user:
            raise ValueError(f"User {user_id} not found in tenant {tenant_id}")
        
        await self.session.delete(tenant_user)
        await self.session.commit()
        
        logger.info(f"Removed user {user_id} from tenant {tenant_id}")

    def _generate_tenant_name(self, email_domain: str) -> str:
        """
        Generate a human-readable tenant name from email domain.
        
        Examples:
        - "acme-corp.com" -> "Acme Corp"
        - "example.org" -> "Example"
        - "big-company.co.uk" -> "Big Company"
        """
        # Remove TLD and convert to title case
        name_part = email_domain.split('.')[0]
        # Replace hyphens and underscores with spaces
        name_part = name_part.replace('-', ' ').replace('_', ' ')
        # Title case
        return name_part.title()