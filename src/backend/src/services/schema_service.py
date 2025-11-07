from typing import List, Optional, Dict, Any
import logging
import json

from fastapi import HTTPException, status

from src.repositories.schema_repository import SchemaRepository
from src.schemas.schema import SchemaCreate, SchemaUpdate, SchemaResponse, SchemaListResponse

logger = logging.getLogger(__name__)

class SchemaService:
    """
    Service for Schema business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """
    
    def __init__(self, session=None, repository: SchemaRepository = None):
        """
        Initialize service with session or repository.

        Args:
            session: Database session for dependency injection
            repository: Schema repository (for backward compatibility)
        """
        if repository is not None:
            # Backward compatibility
            self.repository = repository
        elif session is not None:
            # Create repository with injected session
            self.repository = SchemaRepository(session)
        else:
            raise ValueError("Either session or repository must be provided")
    
    async def get_all_schemas(self) -> SchemaListResponse:
        """
        Get all schemas using repository injection.

        Returns:
            SchemaListResponse with list of all schemas and count
        """
        schemas = await self.repository.list()
        return SchemaListResponse(
            schemas=[SchemaResponse.model_validate(schema) for schema in schemas],
            count=len(schemas)
        )
    
    async def get_schema_by_name(self, name: str) -> SchemaResponse:
        """
        Get a schema by name using repository injection.

        Args:
            name: Name of the schema to retrieve

        Returns:
            SchemaResponse if found

        Raises:
            HTTPException: If schema not found
        """
        schema = await self.repository.find_by_name(name)
        if not schema:
            logger.warning(f"Schema with name '{name}' not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with name '{name}' not found"
            )
        return SchemaResponse.model_validate(schema)
    
    async def get_schemas_by_type(self, schema_type: str) -> SchemaListResponse:
        """
        Get schemas by type using repository injection.

        Args:
            schema_type: Type of schemas to retrieve

        Returns:
            SchemaListResponse with list of schemas of specified type and count
        """
        schemas = await self.repository.find_by_type(schema_type)
        return SchemaListResponse(
            schemas=[SchemaResponse.model_validate(schema) for schema in schemas],
            count=len(schemas)
        )
    
    async def create_schema(self, schema_data: SchemaCreate) -> SchemaResponse:
        """
        Create a new schema using repository injection.

        Args:
            schema_data: Schema data for creation

        Returns:
            SchemaResponse of the created schema

        Raises:
            HTTPException: If schema with same name already exists or JSON validation fails
        """
        # Check if schema with same name exists
        existing_schema = await self.repository.find_by_name(schema_data.name)
        if existing_schema:
            logger.warning(f"Schema with name '{schema_data.name}' already exists")
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Schema with name '{schema_data.name}' already exists"
            )

        # Handle legacy schema_json field if provided
        schema_dict = schema_data.model_dump()

        # Remove legacy_schema_json to prevent SQLAlchemy error
        if 'legacy_schema_json' in schema_dict:
            schema_dict.pop('legacy_schema_json')

        # Validate JSON fields
        try:
            self._validate_json_fields(schema_dict)
        except ValueError as e:
            logger.warning(f"JSON validation failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid JSON format: {str(e)}"
            )

        # Create schema
        try:
            schema = await self.repository.create(schema_dict)
            return SchemaResponse.model_validate(schema)
        except Exception as e:
            logger.error(f"Error creating schema: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error creating schema: {str(e)}"
            )
    
    async def update_schema(self, name: str, schema_data: SchemaUpdate) -> SchemaResponse:
        """
        Update an existing schema.

        Args:
            name: Name of schema to update
            schema_data: Schema data for update

        Returns:
            SchemaResponse of the updated schema

        Raises:
            HTTPException: If schema not found or JSON validation fails
        """
        # Check if schema exists
        schema = await self.repository.find_by_name(name)
        if not schema:
            logger.warning(f"Schema with name '{name}' not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with name '{name}' not found"
            )

        # Prepare update data
        update_data = schema_data.model_dump(exclude_unset=True, by_alias=True)

        # Handle legacy schema_json field if provided
        if 'schema_json' in update_data and update_data.get('schema_json') and 'schema_definition' not in update_data:
            update_data['schema_definition'] = update_data.pop('schema_json')

        # Remove legacy_schema_json to prevent SQLAlchemy error
        if 'legacy_schema_json' in update_data:
            update_data.pop('legacy_schema_json')

        # Validate JSON fields
        try:
            self._validate_json_fields(update_data)
        except ValueError as e:
            logger.warning(f"JSON validation failed: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid JSON format: {str(e)}"
            )

        # Update schema
        try:
            updated_schema = await self.repository.update(schema.id, update_data)
            # Repository handles commit internally for single operations
            return SchemaResponse.model_validate(updated_schema)
        except Exception as e:
            logger.error(f"Error updating schema: {str(e)}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Error updating schema: {str(e)}"
            )
    
    async def delete_schema(self, name: str) -> bool:
        """
        Delete a schema by name.

        Args:
            name: Name of schema to delete

        Returns:
            True if deleted successfully

        Raises:
            HTTPException: If schema not found
        """
        # Check if schema exists
        schema = await self.repository.find_by_name(name)
        if not schema:
            logger.warning(f"Schema with name '{name}' not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Schema with name '{name}' not found"
            )

        # Delete schema
        await self.repository.delete(schema.id)
        # Repository handles commit internally for single operations
        return True
    
    def _validate_json_fields(self, data: Dict[str, Any]) -> None:
        """
        Validate JSON fields in schema data.
        
        Args:
            data: Dictionary of schema data
            
        Raises:
            ValueError: If JSON validation fails
        """
        json_fields = {
            'schema_definition': 'Schema definition',
            'field_descriptions': 'Field descriptions',
            'example_data': 'Example data',
            'keywords': 'Keywords',
            'tools': 'Tools'
        }
        
        for field, label in json_fields.items():
            if field in data and data[field] is not None:
                value = data[field]
                
                # If it's a string, try to parse it as JSON
                if isinstance(value, str):
                    try:
                        data[field] = json.loads(value)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"{label} contains invalid JSON: {str(e)}")
                
                # Additional validation for array fields
                if field in ['keywords', 'tools'] and data[field] is not None:
                    if not isinstance(data[field], list):
                        data[field] = []  # Default to empty list
                
                # Additional validation for object fields
                if field in ['schema_definition', 'field_descriptions'] and data[field] is not None:
                    if not isinstance(data[field], dict):
                        raise ValueError(f"{label} must be a valid JSON object")
        
        # Schema definition must be a non-empty object if provided
        if 'schema_definition' in data and data['schema_definition'] is not None:
            if not data['schema_definition']:
                raise ValueError("Schema definition cannot be empty") 