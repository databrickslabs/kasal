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
        Return a flat list of templates, one per base name, preferring the
        current group's row (same name) when it exists; otherwise the global row.
        Also ensures all default names exist (lazy base upsert from seeds) so the
        UI always shows the fixed set of prompt names.
        """
        all_templates = await self.repository.find_active_templates()
        by_name: dict[str, PromptTemplate] = {}
        primary_gid = group_context.primary_group_id if group_context else None

        # Index existing rows by (name, group_id)
        existing_by_key: dict[tuple[str, str | None], PromptTemplate] = {}
        for t in all_templates:
            existing_by_key[(t.name, t.group_id)] = t

        # Ensure base rows exist for all DEFAULT_TEMPLATES
        for seed in DEFAULT_TEMPLATES:
            name = seed['name']
            base_key = (name, None)
            if base_key not in existing_by_key:
                try:
                    created = await self.repository.create({
                        'name': name,
                        'description': seed.get('description'),
                        'template': seed.get('template', ''),
                        'is_active': seed.get('is_active', True),
                        'group_id': None,
                        'created_by_email': None,
                    })
                    existing_by_key[base_key] = created
                except Exception:
                    # If unique constraint or race, ignore; we'll pick up after
                    pass

        # Rebuild all_templates from index to include any lazy inserts
        all_templates = list(existing_by_key.values())

        # First pass: base as default
        for t in all_templates:
            if t.group_id is None and t.name not in by_name:
                by_name[t.name] = t
        # Second pass: prefer current group row if present
        if primary_gid:
            for t in all_templates:
                if t.group_id == primary_gid:
                    by_name[t.name] = t

        return list(by_name.values())

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

        Global templates (group_id is None) are visible to all groups.
        """
        template = await self.repository.get(id)
        if not template:
            return None
        # Base/global template is visible to all
        if template.group_id is None:
            return template
        # Group-scoped template must match one of the user's groups
        if group_context and group_context.group_ids and template.group_id in group_context.group_ids:
            return template
        return None

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
        Find a template by name with group semantics:
        - Prefer the current group's same-name row
        - Else return the global/base row (group_id is NULL)
        - If neither exists, lazily create the base from DEFAULT_TEMPLATES (if defined)
        """
        gid = group_context.primary_group_id if group_context else None
        if gid:
            grp = await self.repository.find_by_name_and_group(name, gid)
            if grp:
                return grp
        # fallback to base
        base = await self.repository.find_by_name_and_group(name, None)
        if base:
            return base
        # lazily seed base from DEFAULT_TEMPLATES if available
        try:
            seed = next((t for t in DEFAULT_TEMPLATES if t['name'] == name), None)
            if seed:
                created = await self.repository.create({
                    'name': seed['name'],
                    'description': seed.get('description'),
                    'template': seed.get('template', ''),
                    'is_active': seed.get('is_active', True),
                    'group_id': None,
                    'created_by_email': None,
                })
                return created
        except Exception:
            pass
        return None

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
        Update semantics aligned with flat list and same-name overrides:
        - If the row belongs to the current group, update it in place
        - Else, upsert a same-name row scoped to the current group_id
          (do not mutate the base/other-group record)
        """
        original = await self.repository.get(id)
        if not original:
            return None

        current_group_id = group_context.primary_group_id if group_context else None
        current_email = group_context.group_email if group_context else None

        # If record already belongs to this group, update it directly
        if current_group_id and original.group_id == current_group_id:
            update_data = template_data.model_dump(exclude_unset=True)
            update_data.pop('name', None)  # prevent cross-scope renames
            return await self.repository.update_template(id, update_data)

        # Otherwise, upsert the group-scoped row with the same name
        if not current_group_id:
            return None

        existing_group_row = await self.repository.find_by_name_and_group(original.name, current_group_id)

        incoming = template_data.model_dump(exclude_unset=True)
        incoming.pop('name', None)

        if existing_group_row:
            return await self.repository.update_template(existing_group_row.id, incoming)
        else:
            create_payload = {
                'name': original.name,
                'description': incoming.get('description', original.description),
                'template': incoming.get('template', original.template),
                'is_active': incoming.get('is_active', True),
                'group_id': current_group_id,
                'created_by_email': current_email,
            }
            return await self.repository.create(create_payload)

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
        Reset templates to default values for a specific group WITHOUT
        changing global ownership. Keeps defaults global/visible to all.

        Uses upsert pattern on the base templates only (no group_id writes).
        """
        count = 0

        for template_data in DEFAULT_TEMPLATES:
            try:
                # Always resolve against base/global row
                existing_template = await self.repository.find_by_name_and_group(template_data['name'], None)

                if existing_template:
                    # Update base/default content only; do NOT set group_id/created_by_email
                    update_data = {
                        'description': template_data['description'],
                        'template': template_data['template'],
                        'is_active': template_data['is_active']
                    }
                    await self.repository.update_template(existing_template.id, update_data)
                    logger.debug(f"Updated base template: {template_data['name']}")
                    count += 1
                else:
                    # Create new base template (no group assignment)
                    template_create = PromptTemplateCreate(**template_data)
                    await self.create_template(template_create)
                    logger.debug(f"Created base template: {template_data['name']}")
                    count += 1
            except Exception as e:
                logger.error(f"Failed to reset template {template_data['name']}: {str(e)}")
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

    async def get_template_content(self, name: str, default_template: str = None) -> str:
        """
        Get the content of a template by name.

        Args:
            name: Name of the template
            default_template: Default template to use if not found

        Returns:
            Template content
        """
        return await self._get_template_content_instance(name, default_template)

    async def _get_effective_template_content_instance(self, name: str, group_context: GroupContext) -> str:
        """
        Get effective template content for current group: prefer the group's
        same-name row; if absent, fall back to the global/base row.
        """
        try:
            gid = group_context.primary_group_id if group_context else None
            if gid:
                grp = await self.repository.find_by_name_and_group(name, gid)
                if grp and grp.template:
                    return grp.template
            base = await self.repository.find_by_name_and_group(name, None)
            return base.template if base and base.template else ""
        except Exception as e:
            logger.error(f"Error resolving effective template for {name}: {e}")
            return ""

    @staticmethod
    async def get_effective_template_content(name: str, group_context: GroupContext) -> str:
        """
        Static helper to retrieve composed template content for the current group/user.
        """
        from src.db.database_router import get_smart_db_session
        async for session in get_smart_db_session():
            service = TemplateService(session)
            return await service._get_effective_template_content_instance(name, group_context)

