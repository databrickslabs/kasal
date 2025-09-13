from typing import List, Optional
import logging


from src.repositories.template_repository import TemplateRepository
from src.models.template import PromptTemplate
from src.schemas.template import PromptTemplateCreate, PromptTemplateUpdate
from src.seeds.prompt_templates import DEFAULT_TEMPLATES
from src.core.unit_of_work import UnitOfWork
from src.utils.user_context import GroupContext


# Configure logging
logger = logging.getLogger(__name__)


class TemplateService:
    """
    Service for PromptTemplate with business logic.
    Handles prompt template management and operations.
    """
    
    def __init__(self, repository: TemplateRepository):
        """
        Initialize the service with repository.
        
        Args:
            repository: Repository for data access
        """
        self.repository = repository
    
    @classmethod
    def create(cls) -> 'TemplateService':
        """
        Factory method to create a properly configured instance of the service.
        
        This method abstracts the creation of dependencies while maintaining
        proper separation of concerns.
        
        Returns:
            An instance of TemplateService with all required dependencies
        """
        from src.db.session import SessionLocal
        session = SessionLocal()
        repository = TemplateRepository(session)
        return cls(repository=repository)
    
    @classmethod
    async def find_all_templates(cls) -> List[PromptTemplate]:
        """
        Find all prompt templates using UnitOfWork pattern.
        
        Returns:
            List of prompt templates
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.find_all()
    
    @classmethod
    async def find_all_templates_for_group(cls, group_context: GroupContext) -> List[PromptTemplate]:
        """
        Find all prompt templates for a specific group.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            List of prompt templates for the group
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.find_by_group(group_context)
            
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
    
    @classmethod
    async def get_template_by_id(cls, id: int) -> Optional[PromptTemplate]:
        """
        Get a prompt template by ID using UnitOfWork pattern.
        
        Args:
            id: ID of the template to get
            
        Returns:
            PromptTemplate if found, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.get(id)
    
    @classmethod
    async def get_template_with_group_check(cls, id: int, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Get a template by ID with group verification.
        
        Args:
            id: ID of the template to get
            group_context: Group context with group IDs
            
        Returns:
            PromptTemplate if found and authorized, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.get_with_group_check(id, group_context)
    
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
    
    @classmethod
    async def find_template_by_name(cls, name: str) -> Optional[PromptTemplate]:
        """
        Find a prompt template by name using UnitOfWork pattern.
        
        Args:
            name: Name to search for
            
        Returns:
            PromptTemplate if found, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.find_by_name(name)
    
    @classmethod
    async def find_template_by_name_with_group(cls, name: str, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Find a template by name with group verification.
        
        Args:
            name: Name to search for
            group_context: Group context with group IDs
            
        Returns:
            PromptTemplate if found and authorized, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            return await service.find_by_name_with_group_check(name, group_context)
    
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
    
    @classmethod
    async def create_new_template(cls, template_data: PromptTemplateCreate) -> PromptTemplate:
        """
        Create a new prompt template using UnitOfWork pattern.
        
        Args:
            template_data: Data for the new template
            
        Returns:
            Created PromptTemplate
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            template = await service.create_template(template_data)
            await uow.commit()
            return template
    
    @classmethod
    async def create_template_with_group(cls, template_data: PromptTemplateCreate, group_context: GroupContext) -> PromptTemplate:
        """
        Create a new template with group assignment.
        
        Args:
            template_data: Data for the new template
            group_context: Group context with group IDs
            
        Returns:
            Created PromptTemplate
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            template = await service.create_with_group(template_data, group_context)
            await uow.commit()
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
    
    @classmethod
    async def update_existing_template(cls, id: int, template_data: PromptTemplateUpdate) -> Optional[PromptTemplate]:
        """
        Update an existing prompt template using UnitOfWork pattern.
        
        Args:
            id: ID of the template to update
            template_data: Updated data for the template
            
        Returns:
            Updated PromptTemplate if found, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            template = await service.update_template(id, template_data)
            if template:
                await uow.commit()
            return template
    
    @classmethod
    async def update_template_with_group_check(cls, id: int, template_data: PromptTemplateUpdate, group_context: GroupContext) -> Optional[PromptTemplate]:
        """
        Update a template with group verification.
        
        Args:
            id: ID of the template to update
            template_data: Updated data for the template
            group_context: Group context with group IDs
            
        Returns:
            Updated PromptTemplate if found and authorized, else None
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            template = await service.update_with_group_check(id, template_data, group_context)
            if template:
                await uow.commit()
            return template
    
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
    
    @classmethod
    async def delete_template_by_id(cls, id: int) -> bool:
        """
        Delete a prompt template using UnitOfWork pattern.
        
        Args:
            id: ID of the template to delete
            
        Returns:
            True if deleted, False if not found
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            deleted = await service.delete_template(id)
            if deleted:
                await uow.commit()
            return deleted
    
    @classmethod
    async def delete_template_with_group_check(cls, id: int, group_context: GroupContext) -> bool:
        """
        Delete a template with group verification.
        
        Args:
            id: ID of the template to delete
            group_context: Group context with group IDs
            
        Returns:
            True if deleted, False if not found or not authorized
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            deleted = await service.delete_with_group_check(id, group_context)
            if deleted:
                await uow.commit()
            return deleted
    
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
    
    @classmethod
    async def delete_all_templates_service(cls) -> int:
        """
        Delete all prompt templates using UnitOfWork pattern.
        
        Returns:
            Number of templates deleted
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            count = await service.delete_all_templates()
            await uow.commit()
            return count
    
    @classmethod
    async def delete_all_for_group(cls, group_context: GroupContext) -> int:
        """
        Delete all templates for a specific group.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            Number of templates deleted
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            count = await service.delete_all_for_group_internal(group_context)
            await uow.commit()
            return count
    
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
    
    @classmethod
    async def reset_templates_service(cls) -> int:
        """
        Reset templates to default values using UnitOfWork pattern.
        
        Returns:
            Number of templates reset
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            count = await service.reset_templates()
            await uow.commit()
            return count
    
    @classmethod
    async def reset_templates_for_group(cls, group_context: GroupContext) -> int:
        """
        Reset templates to default values for a specific group.
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            Number of templates reset
        """
        async with UnitOfWork() as uow:
            service = cls(uow.template_repository)
            count = await service.reset_templates_with_group(group_context)
            await uow.commit()
            return count
    
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
        
        Args:
            group_context: Group context with group IDs
            
        Returns:
            Number of templates reset
        """
        # Delete all existing templates for the group
        await self.delete_all_for_group_internal(group_context)
        
        # Create default templates for the group
        count = 0
        for template_data in DEFAULT_TEMPLATES:
            template_create = PromptTemplateCreate(**template_data)
            await self.create_with_group(template_create, group_context)
            count += 1
            
        return count
    
    @classmethod
    async def get_template_content(cls, name: str, default_template: str = None) -> str:
        """
        Get the content of a template by name.
        
        Args:
            name: Name of the template
            default_template: Default template to use if not found
            
        Returns:
            Template content
        """
        try:
            async with UnitOfWork() as uow:
                service = cls(uow.template_repository)
                template = await service.find_by_name(name)
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