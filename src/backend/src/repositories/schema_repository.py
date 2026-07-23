from typing import List, Optional, Dict, Any, Union
import json

from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import cast
from sqlalchemy.dialects.postgresql import JSONB

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
    
    def find_by_name_sync(self, name: str) -> Optional[Schema]:
        """
        Find a schema by name (synchronous version).
        
        Args:
            name: Schema name to search for
            
        Returns:
            Schema if found, else None
        """
        # Check if we have a synchronous session
        if hasattr(self.session, 'execute'):
            # Using a synchronous session
            query = select(self.model).where(self.model.name == name)
            result = self.session.execute(query)
            return result.scalars().first()
        else:
            # Log a warning if we're using an async session with sync method
            import logging
            logging.getLogger(__name__).warning(
                "find_by_name_sync called with an async session, this may not work as expected"
            )
            # Create a sync version of the query
            query = select(self.model).where(self.model.name == name)
            # Execute synchronously (will only work if session supports sync execution)
            result = self.session.execute(query)
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
    
    async def find_by_keyword(self, keyword: str) -> List[Schema]:
        """
        Find schemas that contain a specific keyword.
        
        Args:
            keyword: Keyword to search for
            
        Returns:
            List of schemas with the specified keyword
        """
        # Use database-specific json containment operators
        # This handles both string arrays and JSON arrays
        json_keyword = json.dumps(keyword)
        query = select(self.model).where(
            or_(
                # Check if keywords contains the keyword as string
                self.model.keywords.contains([keyword]),
                # Check if keywords contains the keyword as json string
                cast(self.model.keywords, JSONB).contains(json_keyword),
                # Fallback for databases without JSONB support
                func.json_contains(self.model.keywords, json_keyword)
            )
        )
        
        try:
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            # Fallback to application-level filtering if database query fails
            import logging
            logging.getLogger(__name__).warning(f"Database JSON query failed: {str(e)}. Using application filtering.")
            
            query = select(self.model)
            result = await self.session.execute(query)
            schemas = list(result.scalars().all())
            
            # Filter at application level
            return [
                schema for schema in schemas 
                if schema.keywords and isinstance(schema.keywords, list) and keyword in schema.keywords
            ]
    
    async def find_by_tool(self, tool: str) -> List[Schema]:
        """
        Find schemas that are associated with a specific tool.
        
        Args:
            tool: Tool name to search for
            
        Returns:
            List of schemas associated with the specified tool
        """
        # Use database-specific json containment operators
        # This handles both string arrays and JSON arrays
        json_tool = json.dumps(tool)
        query = select(self.model).where(
            or_(
                # Check if tools contains the tool as string
                self.model.tools.contains([tool]),
                # Check if tools contains the tool as json string
                cast(self.model.tools, JSONB).contains(json_tool),
                # Fallback for databases without JSONB support
                func.json_contains(self.model.tools, json_tool)
            )
        )
        
        try:
            result = await self.session.execute(query)
            return list(result.scalars().all())
        except Exception as e:
            # Fallback to application-level filtering if database query fails
            import logging
            logging.getLogger(__name__).warning(f"Database JSON query failed: {str(e)}. Using application filtering.")
            
            query = select(self.model)
            result = await self.session.execute(query)
            schemas = list(result.scalars().all())
            
            # Filter at application level
            return [
                schema for schema in schemas 
                if schema.tools and isinstance(schema.tools, list) and tool in schema.tools
            ]
            
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
        for field in ['schema_definition', 'field_descriptions', 'example_data', 'keywords', 'tools']:
            if field in data and isinstance(data[field], str):
                try:
                    data[field] = json.loads(data[field])
                except (json.JSONDecodeError, TypeError):
                    # Set appropriate defaults for invalid JSON
                    if field in ['keywords', 'tools']:
                        data[field] = []
                    elif field in ['field_descriptions']:
                        data[field] = {}
                    elif field == 'schema_definition':
                        data[field] = {}
                    # Leave example_data as is if invalid, since it's optional 