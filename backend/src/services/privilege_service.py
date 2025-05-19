from typing import List, Optional, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.user import Privilege
from src.repositories.user_repository import PrivilegeRepository
from src.schemas.user import PrivilegeCreate, PrivilegeUpdate

class PrivilegeService:
    """Service for privilege management operations"""
    
    def __init__(self, session: AsyncSession):
        self.session = session
        self.privilege_repo = PrivilegeRepository(Privilege, session)
    
    async def get_privilege(self, privilege_id: str) -> Optional[Privilege]:
        """Get a privilege by ID"""
        return await self.privilege_repo.get(privilege_id)
    
    async def get_privilege_by_name(self, name: str) -> Optional[Privilege]:
        """Get a privilege by name"""
        return await self.privilege_repo.get_by_name(name)
    
    async def get_privileges(self, skip: int = 0, limit: int = 100) -> List[Privilege]:
        """Get a list of privileges"""
        return await self.privilege_repo.get_all(skip=skip, limit=limit)
    
    async def create_privilege(self, privilege_data: PrivilegeCreate) -> Privilege:
        """Create a new privilege"""
        # Check if privilege name already exists
        existing_privilege = await self.privilege_repo.get_by_name(privilege_data.name)
        if existing_privilege:
            raise ValueError(f"Privilege with name '{privilege_data.name}' already exists")
        
        # Create privilege
        privilege_dict = privilege_data.dict()
        privilege = await self.privilege_repo.create(privilege_dict)
        
        return privilege
    
    async def update_privilege(self, privilege_id: str, privilege_data: PrivilegeUpdate) -> Optional[Privilege]:
        """Update a privilege"""
        # Check if privilege exists
        privilege = await self.privilege_repo.get(privilege_id)
        if not privilege:
            return None
        
        # Prepare update data
        update_data = {}
        if privilege_data.name is not None:
            # Check if new name already exists
            if privilege_data.name != privilege.name:
                existing_privilege = await self.privilege_repo.get_by_name(privilege_data.name)
                if existing_privilege:
                    raise ValueError(f"Privilege with name '{privilege_data.name}' already exists")
            update_data["name"] = privilege_data.name
        
        if privilege_data.description is not None:
            update_data["description"] = privilege_data.description
        
        # Update privilege
        if update_data:
            await self.privilege_repo.update(privilege_id, update_data)
        
        # Return updated privilege
        return await self.privilege_repo.get(privilege_id)
    
    async def delete_privilege(self, privilege_id: str) -> bool:
        """Delete a privilege"""
        # Check if privilege exists
        privilege = await self.privilege_repo.get(privilege_id)
        if not privilege:
            return False
        
        # Check if privilege is in use by any roles
        # This would ideally be handled by the database with foreign key constraints
        # But we'll add an explicit check here for safety
        role_privileges = await self.privilege_repo.get_role_privileges_by_privilege(privilege_id)
        if role_privileges:
            raise ValueError(f"Cannot delete privilege because it's being used by {len(role_privileges)} roles")
        
        # Delete privilege
        await self.privilege_repo.delete(privilege_id)
        
        return True 