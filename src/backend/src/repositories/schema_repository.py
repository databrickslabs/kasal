import json
from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.base_repository import BaseRepository
from src.models.schema import Schema


class SchemaRepository(BaseRepository[Schema]):
    """
    Repository for Schema model with custom query methods.
    Inherits base CRUD operations from BaseRepository.
    """

    def __init__(self, session: AsyncSession):
        """
        Initialize the repository with session.

        Args:
            session: SQLAlchemy async session
        """
        super().__init__(Schema, session)

    async def find_by_name(self, name: str) -> Optional[Schema]:
        """
        Find a schema by name.

        Args:
            name: Schema name to search for

        Returns:
            Schema if found, else None
        """
        query = select(self.model).where(self.model.name == name)
        result = await self.session.execute(query)
        return result.scalars().first()

    async def find_by_type(self, schema_type: str) -> List[Schema]:
        """
        Find schemas by type.

        Args:
            schema_type: Schema type to filter by

        Returns:
            List of schemas with the specified type
        """
        query = select(self.model).where(self.model.schema_type == schema_type)
        result = await self.session.execute(query)
        return list(result.scalars().all())

    async def create(self, data: Dict[str, Any]) -> Schema:
        """
        Create a new schema with improved JSON handling.

        Args:
            data: Dictionary of schema attributes

        Returns:
            Created Schema instance
        """
        # Handle JSON serialization of certain fields if needed
        self._sanitize_json_data(data)

        # Create schema
        schema = await super().create(data)
        return schema

    async def update(self, id: int, data: Dict[str, Any]) -> Optional[Schema]:
        """
        Update a schema with improved JSON handling.

        Args:
            id: Schema ID
            data: Dictionary of schema attributes to update

        Returns:
            Updated Schema instance if found, else None
        """
        # Handle JSON serialization of certain fields if needed
        self._sanitize_json_data(data)

        # Update schema
        schema = await super().update(id, data)
        return schema

    def _sanitize_json_data(self, data: Dict[str, Any]) -> None:
        """
        Ensure JSON fields are properly formatted.

        Args:
            data: Dictionary of schema attributes
        """
        # Convert string representations to proper JSON objects for fields that should be JSON
        for field in [
            "schema_definition",
            "field_descriptions",
            "example_data",
            "keywords",
            "tools",
        ]:
            if field in data and isinstance(data[field], str):
                try:
                    data[field] = json.loads(data[field])
                except (json.JSONDecodeError, TypeError):
                    # Set appropriate defaults for invalid JSON
                    if field in ["keywords", "tools"]:
                        data[field] = []
                    elif field in ["field_descriptions"]:
                        data[field] = {}
                    elif field == "schema_definition":
                        data[field] = {}
                    # Leave example_data as is if invalid, since it's optional
