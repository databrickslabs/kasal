from typing import List, Optional, Any
import logging

from src.repositories.template_repository import TemplateRepository
from src.models.template import PromptTemplate
from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate
from src.seeds.prompt_templates import DEFAULT_TEMPLATES
from src.utils.user_context import GroupContext


# Configure logging
logger = logging.getLogger(__name__)


class TemplateService:
    """
    Service for PromptTemplate with business logic.
    Handles prompt template management and operations.
    Uses dependency injection for better testability and modularity.
    """

    def __init__(self, session: Any):
        """
        Initialize the service with session.
        Uses dependency injection pattern for clean architecture.

        Args:
            session: Database session from dependency injection
        """
        self.session = session
        self.repository = TemplateRepository(session)
    
    # Removed factory method - using dependency injection instead
    
    async def find_all_templates(self) -> List[PromptTemplate]:
        """
        Find all prompt templates.

        Returns:
            List of prompt templates
        """
        return await self.find_all()
    
    async def find_all_templates_for_group(self, group_context: GroupContext) -> List[PromptTemplate]:
        """
        Find all prompt templates for a specific group.

        Args:
            group_context: Group context with group IDs

        Returns:
            List of prompt templates for the group
        """
        return await self.find_by_group(group_context)
            
    async def find_all(self) -> List[PromptTemplate]:
        """
        Find all prompt templates.
        
        Returns:
            List of prompt templates
        """
        return await self.repository.find_active_templates()
    
    async def find_by_group(self, group_context: GroupContext) -> List[PromptTemplate]:
        """
        Find templates by group.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            List of templates for the group
        """
        if not group_context or not group_context.group_ids:
            return []
        
        all_templates = await self.repository.find_active_templates()
        # Filter templates by group_id
        return [
            template for template in all_templates
            if template.group_id in group_context.group_ids
        ]
    
    async def get_template_by_id(self, id: int) -> Optional[PromptTemplate]:
        """
        Get a prompt template by ID.

        Args:
            id: ID of the template to get

        Returns:
            PromptTemplate if found, else None
        """
        return await self.get(id)
    
    async def get_template_with_group_check(self, id: int, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Get a template by ID with group verification.

        Args:
            id: ID of the template to get
            group_context: Group context with group IDs

        Returns:
            PromptTemplate if found and authorized, else None
        """
        return await self.get_with_group_check(id, group_context)
    
    async def get(self, id: int) -> Optional[PromptTemplate]:
        """
        Get a prompt template by ID.
        
        Args:
            id: ID of the template to get
            
        Returns:
            PromptTemplate if found, else None
        """
        return await self.repository.get(id)
    
    async def get_with_group_check(self, id: int, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Get a template with group verification.
        
        Args:
            id: ID of the template to get
            group_context: Group context with group IDs
            
        Returns:
            PromptTemplate if found and authorized, else None
        """
        template = await self.repository.get(id)
        if template and group_context and group_context.group_ids:
            if template.group_id not in group_context.group_ids:
                return None  # Return None to trigger 404, not 403
        return template
    
    async def find_template_by_name(self, name: str) -> Optional[PromptTemplate]:
        """
        Find a prompt template by name.

        Args:
            name: Name to search for

        Returns:
            PromptTemplate if found, else None
        """
        return await self.find_by_name(name)
    
    async def find_template_by_name_with_group(self, name: str, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Find a template by name with group verification.

        Args:
            name: Name to search for
            group_context: Group context with group IDs

        Returns:
            PromptTemplate if found and authorized, else None
        """
        return await self.find_by_name_with_group_check(name, group_context)
    
    async def find_by_name(self, name: str) -> Optional[PromptTemplate]:
        """
        Find a prompt template by name.
        
        Args:
            name: Name to search for
            
        Returns:
            PromptTemplate if found, else None
        """
        return await self.repository.find_by_name(name)
    
    async def find_by_name_with_group_check(self, name: str, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Find a template by name with group verification.
        
        Args:
            name: Name to search for
            group_context: Group context with group IDs
            
        Returns:
            PromptTemplate if found and authorized, else None
        """
        template = await self.repository.find_by_name(name)
        if template and group_context and group_context.group_ids:
            if template.group_id not in group_context.group_ids:
                return None  # Return None to trigger 404, not 403
        return template
    
    async def create_new_template(self, template_data: PromptTemplateCreate) -> PromptTemplate:
        """
        Create a new prompt template.

        Args:
            template_data: Data for the new template

        Returns:
            Created PromptTemplate
        """
        template = await self.create_template(template_data)
        # Repository handles flush, session handles commit
        return template
    
    async def create_template_with_group(self, template_data: PromptTemplateCreate, group_context: GroupContext) -> PromptTemplate:
        """
        Create a new template with group assignment.

        Args:
            template_data: Data for the new template
            group_context: Group context with group IDs

        Returns:
            Created PromptTemplate
        """
        template = await self.create_with_group(template_data, group_context)
        # Repository handles flush, session handles commit
        return template
    
    async def create_template(self, template_data: PromptTemplateCreate) -> PromptTemplate:
        """
        Create a new prompt template.
        
        Args:
            template_data: Data for the new template
            
        Returns:
            Created PromptTemplate
        """
        template_dict = template_data.model_dump()
        return await self.repository.create(template_dict)
    
    async def create_with_group(self, template_data: PromptTemplateCreate, group_context: GroupContext) -> PromptTemplate:
        """
        Create a template with group assignment.
        
        Args:
            template_data: Data for the new template
            group_context: Group context with group IDs
            
        Returns:
            Created PromptTemplate
        """
        template_dict = template_data.model_dump()
        
        # Add group information
        if group_context and group_context.is_valid():
            template_dict['group_id'] = group_context.primary_group_id
            template_dict['created_by_email'] = group_context.group_email
        
        return await self.repository.create(template_dict)
    
    # Removed UoW-based class method - use instance method instead
    
    # Removed UoW-based class method - use instance method instead
    
    async def update_template(self, id: int, template_data: PromptTemplateUpdate) -> Optional[PromptTemplate]:
        """
        Update an existing prompt template.
        
        Args:
            id: ID of the template to update
            template_data: Updated data for the template
            
        Returns:
            Updated PromptTemplate if found, else None
        """
        update_data = template_data.model_dump(exclude_unset=True)
        return await self.repository.update_template(id, update_data)
    
    async def update_with_group_check(self, id: int, template_data: PromptTemplateUpdate, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Update a template with group verification.
        
        Args:
            id: ID of the template to update
            template_data: Updated data for the template
            group_context: Group context with group IDs
            
        Returns:
            Updated PromptTemplate if found and authorized, else None
        """
        # First check if template exists and belongs to group
        template = await self.get_with_group_check(id, group_context)
        if not template:
            return None
        
        update_data = template_data.model_dump(exclude_unset=True)
        return await self.repository.update_template(id, update_data)
    
    # Removed UoW-based class method - use instance method instead
    
    # Removed UoW-based class method - use instance method instead
    
    async def delete_template(self, id: int) -> bool:
        """
        Delete a prompt template.
        
        Args:
            id: ID of the template to delete
            
        Returns:
            True if deleted, False if not found
        """
        return await self.repository.delete(id)
    
    async def delete_with_group_check(self, id: int, group_context: GroupContext) -> bool:
        """
        Delete a template with group verification.
        
        Args:
            id: ID of the template to delete
            group_context: Group context with group IDs
            
        Returns:
            True if deleted, False if not found or not authorized
        """
        # First check if template exists and belongs to group
        template = await self.get_with_group_check(id, group_context)
        if not template:
            return False
        
        return await self.repository.delete(id)
    
    # Removed UoW-based class method - use instance method instead
    
    # Removed UoW-based class method - use instance method instead
    
    async def delete_all_templates(self) -> int:
        """
        Delete all prompt templates.
        
        Returns:
            Number of templates deleted
        """
        return await self.repository.delete_all()
    
    async def delete_all_for_group_internal(self, group_context: GroupContext) -> int:
        """
        Delete all templates for a specific group.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            Number of templates deleted
        """
        if not group_context or not group_context.group_ids:
            return 0
        
        # Find all templates for the group
        templates = await self.find_by_group(group_context)
        
        # Delete each template
        count = 0
        for template in templates:
            if await self.repository.delete(template.id):
                count += 1
        
        return count
    
    # Removed UoW-based class method - use instance method instead
    
    # Removed UoW-based class method - use instance method instead
    
    async def reset_templates(self) -> int:
        """
        Reset templates to default values.
        
        Returns:
            Number of templates reset
        """
        # Delete all existing templates
        await self.delete_all_templates()
        
        # Create default templates
        count = 0
        for template in DEFAULT_TEMPLATES:
            await self.create_template(PromptTemplateCreate(**template))
            count += 1
            
        return count
    
    async def reset_templates_with_group(self, group_context: GroupContext) -> int:
        """
        Reset templates to default values for a specific group.
        Uses upsert pattern to avoid constraint violations.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            Number of templates reset
        """
        count = 0
        
        # Process each default template using upsert pattern
        for template_data in DEFAULT_TEMPLATES:
            try:
                # First, check if template already exists by name
                existing_template = await self.find_by_name(template_data['name'])
                
                if existing_template:
                    # Template exists - update it with new content and group assignment
                    update_data = {
                        'description': template_data['description'],
                        'template': template_data['template'], 
                        'is_active': template_data['is_active']
                    }
                    
                    # Add group information if context is valid
                    if group_context and group_context.is_valid():
                        update_data['group_id'] = group_context.primary_group_id
                        update_data['created_by_email'] = group_context.group_email
                    
                    await self.repository.update_template(existing_template.id, update_data)
                    logger.debug(f"Updated existing template: {template_data['name']}")
                    count += 1
                else:
                    # Template doesn't exist - create new one
                    template_create = PromptTemplateCreate(**template_data)
                    await self.create_with_group(template_create, group_context)
                    logger.debug(f"Created new template: {template_data['name']}")
                    count += 1
                    
            except Exception as e:
                logger.error(f"Failed to reset template {template_data['name']}: {str(e)}")
                # Continue with other templates rather than failing completely
                continue
            
        return count
    
    async def _get_template_content_instance(self, name: str, default_template: str = None) -> str:
        """
        Get the content of a template by name (instance method).

        Args:
            name: Name of the template
            default_template: Default template to use if not found

        Returns:
            Template content
        """
        try:
            template = await self.find_by_name(name)
            if template:
                return template.template
            elif default_template:
                return default_template
            else:
                logger.warning(f"No template found for name: {name}")
                return ""
        except Exception as e:
            logger.error(f"Error getting template content: {str(e)}")
            if default_template:
                return default_template
            return ""

    @staticmethod
    async def get_template_content(name: str, default_template: str = None) -> str:
        """
        Static method for backward compatibility.
        Other services call this directly without instantiating the service.

        Args:
            name: Name of the template
            default_template: Default template to use if not found

        Returns:
            Template content
        """
        # Import here to avoid circular imports
        from src.db.database_router import get_smart_db_session

        async for session in get_smart_db_session():
            service = TemplateService(session)
            return await service._get_template_content_instance(name, default_template) 