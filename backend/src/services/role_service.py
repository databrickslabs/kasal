from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import Role, Privilege, RolePrivilege
from src.repositories.user_repository import RoleRepository, PrivilegeRepository, RolePrivilegeRepository
from src.schemas.user import RoleCreate, RoleUpdate

class RoleService:
    """Service for role management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.role_repo = RoleRepository(Role, session)
        self.privilege_repo = PrivilegeRepository(Privilege, session)
        self.role_privilege_repo = RolePrivilegeRepository(RolePrivilege, session)
    
    async def get_role(self, role_id: str) -> Optional[Role]:
        """Get a role by ID"""
        return await self.role_repo.get(role_id)
    
    async def get_role_with_privileges(self, role_id: str) -> Optional[Dict[str, Any]]:
        """Get a role with its privileges"""
        role = await self.role_repo.get_with_privileges(role_id)
        if not role:
            return None
        
        # Convert to dict with privileges
        privileges = getattr(role, "privileges", [])
        
        role_dict = {
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "created_at": role.created_at,
            "updated_at": role.updated_at,
            "privileges": privileges
        }
        
        return role_dict
    
    async def get_roles(self, skip: int = 0, limit: int = 100) -> List[Role]:
        """Get a list of roles"""
        return await self.role_repo.get_all(skip=skip, limit=limit)
    
    async def create_role(self, role_data: RoleCreate) -> Dict[str, Any]:
        """Create a new role with privileges"""
        # Check if role name already exists
        existing_role = await self.role_repo.get_by_name(role_data.name)
        if existing_role:
            raise ValueError(f"Role with name '{role_data.name}' already exists")
        
        # Create role
        role_dict = role_data.dict(exclude={"privileges"})
        role = await self.role_repo.create(role_dict)
        
        # Process privileges
        privileges = []
        if role_data.privileges:
            for privilege_name in role_data.privileges:
                # Get or create privilege
                privilege = await self.privilege_repo.get_by_name(privilege_name)
                if not privilege:
                    privilege = await self.privilege_repo.create({"name": privilege_name})
                
                # Create role-privilege mapping
                role_privilege_data = {
                    "role_id": role.id,
                    "privilege_id": privilege.id
                }
                await self.role_privilege_repo.create(role_privilege_data)
                
                privileges.append(privilege)
        
        # Return role with privileges
        role_dict = {
            "id": role.id,
            "name": role.name,
            "description": role.description,
            "created_at": role.created_at,
            "updated_at": role.updated_at,
            "privileges": privileges
        }
        
        return role_dict
    
    async def update_role(self, role_id: str, role_data: RoleUpdate) -> Optional[Dict[str, Any]]:
        """Update a role and its privileges"""
        # Check if role exists
        role = await self.role_repo.get(role_id)
        if not role:
            return None
        
        # Prepare update data
        update_data = {}
        if role_data.name is not None:
            # Check if new name already exists
            if role_data.name != role.name:
                existing_role = await self.role_repo.get_by_name(role_data.name)
                if existing_role:
                    raise ValueError(f"Role with name '{role_data.name}' already exists")
            update_data["name"] = role_data.name
        
        if role_data.description is not None:
            update_data["description"] = role_data.description
        
        # Update role
        if update_data:
            await self.role_repo.update(role_id, update_data)
        
        # Update privileges if provided
        if role_data.privileges is not None:
            # Remove all existing role-privilege mappings
            await self.role_privilege_repo.delete_by_role_id(role_id)
            
            # Create new mappings
            for privilege_name in role_data.privileges:
                # Get or create privilege
                privilege = await self.privilege_repo.get_by_name(privilege_name)
                if not privilege:
                    privilege = await self.privilege_repo.create({"name": privilege_name})
                
                # Create role-privilege mapping
                role_privilege_data = {
                    "role_id": role_id,
                    "privilege_id": privilege.id
                }
                await self.role_privilege_repo.create(role_privilege_data)
        
        # Return updated role with privileges
        return await self.get_role_with_privileges(role_id)
    
    async def delete_role(self, role_id: str) -> bool:
        """Delete a role"""
        # Check if role exists
        role = await self.role_repo.get(role_id)
        if not role:
            return False
        
        # Delete role (cascade will delete mappings)
        await self.role_repo.delete(role_id)
        
        return True
    
    async def check_role_has_privilege(self, role_id: str, privilege_name: str) -> bool:
        """Check if a role has a specific privilege"""
        # Get privilege by name
        privilege = await self.privilege_repo.get_by_name(privilege_name)
        if not privilege:
            return False
        
        # Get role with privileges
        role = await self.role_repo.get_with_privileges(role_id)
        if not role:
            return False
        
        # Check if privilege is in role's privileges
        privileges = getattr(role, "privileges", [])
        return any(p.id == privilege.id for p in privileges) 