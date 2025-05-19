from typing import List, Optional, Dict, Any
import logging
import json

from fastapi import HTTPException, status

from src.repositories.schema_repository import SchemaRepository
from src.schemas.schema import SchemaCreate, SchemaUpdate, SchemaResponse, SchemaListResponse
from src.core.unit_of_work import UnitOfWork

logger = logging.getLogger(__name__)

class SchemaService:
    """
    Service for Schema business logic and error handling.
    Acts as an intermediary between the API routers and the repository.
    """
    
    def __init__(self, repository: SchemaRepository):
        """
        Initialize service with repository.
        
        Args:
            repository: Schema repository
        """
        self.repository = repository
    
    @classmethod
    async def get_all_schemas(cls) -> SchemaListResponse:
        """
        Get all schemas using UnitOfWork pattern.
        
        Returns:
            SchemaListResponse with list of all schemas and count
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            schemas = await service.repository.list()
            return SchemaListResponse(
                schemas=[SchemaResponse.model_validate(schema) for schema in schemas],
                count=len(schemas)
            )
    
    @classmethod
    async def get_schema_by_name(cls, name: str) -> SchemaResponse:
        """
        Get a schema by name using UnitOfWork pattern.
        
        Args:
            name: Name of the schema to retrieve
            
        Returns:
            SchemaResponse if found
            
        Raises:
            HTTPException: If schema not found
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            schema = await service.repository.find_by_name(name)
            if not schema:
                logger.warning(f"Schema with name '{name}' not found")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schema with name '{name}' not found"
                )
            return SchemaResponse.model_validate(schema)
    
    @classmethod
    async def get_schemas_by_type(cls, schema_type: str) -> SchemaListResponse:
        """
        Get schemas by type using UnitOfWork pattern.
        
        Args:
            schema_type: Type of schemas to retrieve
            
        Returns:
            SchemaListResponse with list of schemas of specified type and count
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            schemas = await service.repository.find_by_type(schema_type)
            return SchemaListResponse(
                schemas=[SchemaResponse.model_validate(schema) for schema in schemas],
                count=len(schemas)
            )
    
    @classmethod
    async def create_schema(cls, schema_data: SchemaCreate) -> SchemaResponse:
        """
        Create a new schema using UnitOfWork pattern.
        
        Args:
            schema_data: Schema data for creation
            
        Returns:
            SchemaResponse of the created schema
            
        Raises:
            HTTPException: If schema with same name already exists or JSON validation fails
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            
            # Check if schema with same name exists
            existing_schema = await service.repository.find_by_name(schema_data.name)
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
                cls._validate_json_fields(schema_dict)
            except ValueError as e:
                logger.warning(f"JSON validation failed: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid JSON format: {str(e)}"
                )
            
            # Create schema
            try:
                schema = await service.repository.create(schema_dict)
                await uow.commit()
                return SchemaResponse.model_validate(schema)
            except Exception as e:
                logger.error(f"Error creating schema: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error creating schema: {str(e)}"
                )
    
    @classmethod
    async def update_schema(cls, name: str, schema_data: SchemaUpdate) -> SchemaResponse:
        """
        Update an existing schema using UnitOfWork pattern.
        
        Args:
            name: Name of schema to update
            schema_data: Schema data for update
            
        Returns:
            SchemaResponse of the updated schema
            
        Raises:
            HTTPException: If schema not found or JSON validation fails
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            
            # Check if schema exists
            schema = await service.repository.find_by_name(name)
            if not schema:
                logger.warning(f"Schema with name '{name}' not found for update")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schema with name '{name}' not found"
                )
            
            # Prepare update data
            update_data = schema_data.model_dump(exclude_unset=True)
            
            # Handle legacy schema_json field if provided
            if 'schema_json' in update_data and update_data.get('schema_json') and 'schema_definition' not in update_data:
                update_data['schema_definition'] = update_data.pop('schema_json')
            
            # Remove legacy_schema_json to prevent SQLAlchemy error
            if 'legacy_schema_json' in update_data:
                update_data.pop('legacy_schema_json')
            
            # Validate JSON fields
            try:
                cls._validate_json_fields(update_data)
            except ValueError as e:
                logger.warning(f"JSON validation failed: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid JSON format: {str(e)}"
                )
            
            # Update schema
            try:
                updated_schema = await service.repository.update(schema.id, update_data)
                await uow.commit()
                return SchemaResponse.model_validate(updated_schema)
            except Exception as e:
                logger.error(f"Error updating schema: {str(e)}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Error updating schema: {str(e)}"
                )
    
    @classmethod
    async def delete_schema(cls, name: str) -> bool:
        """
        Delete a schema by name using UnitOfWork pattern.
        
        Args:
            name: Name of schema to delete
            
        Returns:
            True if deleted successfully
            
        Raises:
            HTTPException: If schema not found
        """
        async with UnitOfWork() as uow:
            service = cls(uow.schema_repository)
            
            # Check if schema exists
            schema = await service.repository.find_by_name(name)
            if not schema:
                logger.warning(f"Schema with name '{name}' not found for deletion")
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Schema with name '{name}' not found"
                )
            
            # Delete schema
            await service.repository.delete(schema.id)
            await uow.commit()
            return True
    
    @staticmethod
    def _validate_json_fields(data: Dict[str, Any]) -> None:
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
            if not isinstance(data['schema_definition'], dict):
                raise ValueError("Schema definition must be a valid JSON object")
            if not data['schema_definition'] and field == 'schema_definition':
                raise ValueError("Schema definition cannot be empty") 