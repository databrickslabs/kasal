import logging
from typing import Dict, List, Optional

from fastapi import HTTPException

from src.repositories.powerbi_context_config_repository import (
    PowerBIBusinessMappingRepository,
    PowerBIFieldSynonymRepository
)
from src.schemas.powerbi_context_config import (
    PowerBIBusinessMappingCreate,
    PowerBIBusinessMappingUpdate,
    PowerBIBusinessMappingResponse,
    PowerBIFieldSynonymCreate,
    PowerBIFieldSynonymUpdate,
    PowerBIFieldSynonymResponse,
    PowerBIContextConfigBulkResponse,
    PowerBIContextConfigDict
)

# Set up logger
logger = logging.getLogger(__name__)


class PowerBIContextConfigService:
    """Service for managing PowerBI context configuration (business mappings and field synonyms)."""

    def __init__(self, session, group_id: str):
        """
        Initialize the service with session and group context.

        Args:
            session: SQLAlchemy async session
            group_id: Group ID for multi-tenant isolation
        """
        self.session = session
        self.group_id = group_id
        self.business_mapping_repo = PowerBIBusinessMappingRepository(session)
        self.field_synonym_repo = PowerBIFieldSynonymRepository(session)

    # ===== Business Mappings Operations =====

    async def create_business_mapping(
        self,
        semantic_model_id: str,
        mapping_data: PowerBIBusinessMappingCreate
    ) -> PowerBIBusinessMappingResponse:
        """
        Create a new business terminology mapping.

        Args:
            semantic_model_id: Power BI semantic model ID
            mapping_data: Business mapping creation data

        Returns:
            Created business mapping

        Raises:
            HTTPException: If mapping already exists or creation fails
        """
        try:
            # Check if mapping already exists
            existing = await self.business_mapping_repo.get_by_term(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id,
                natural_term=mapping_data.natural_term
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Business mapping for term '{mapping_data.natural_term}' already exists"
                )

            # Create mapping
            db_data = {
                "group_id": self.group_id,
                "semantic_model_id": semantic_model_id,
                **mapping_data.model_dump()
            }
            # Remove duplicate semantic_model_id from model_dump
            db_data.pop("semantic_model_id", None)
            db_data["semantic_model_id"] = semantic_model_id

            mapping = await self.business_mapping_repo.create(db_data)
            logger.info(f"Created business mapping: {mapping.natural_term} for model {semantic_model_id}")

            return PowerBIBusinessMappingResponse.model_validate(mapping)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating business mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to create business mapping: {str(e)}")

    async def update_business_mapping(
        self,
        mapping_id: int,
        mapping_data: PowerBIBusinessMappingUpdate
    ) -> PowerBIBusinessMappingResponse:
        """
        Update an existing business mapping.

        Args:
            mapping_id: Mapping ID
            mapping_data: Updated mapping data

        Returns:
            Updated business mapping

        Raises:
            HTTPException: If mapping not found or update fails
        """
        try:
            # Verify mapping exists and belongs to group
            existing = await self.business_mapping_repo.get(mapping_id)
            if not existing or existing.group_id != self.group_id:
                raise HTTPException(status_code=404, detail="Business mapping not found")

            # Update mapping
            update_dict = mapping_data.model_dump(exclude_unset=True)
            updated = await self.business_mapping_repo.update(mapping_id, update_dict)

            if not updated:
                raise HTTPException(status_code=404, detail="Business mapping not found")

            logger.info(f"Updated business mapping ID {mapping_id}")
            return PowerBIBusinessMappingResponse.model_validate(updated)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating business mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to update business mapping: {str(e)}")

    async def delete_business_mapping(self, mapping_id: int) -> bool:
        """
        Delete a business mapping.

        Args:
            mapping_id: Mapping ID

        Returns:
            True if deleted successfully

        Raises:
            HTTPException: If mapping not found or deletion fails
        """
        try:
            # Verify mapping exists and belongs to group
            existing = await self.business_mapping_repo.get(mapping_id)
            if not existing or existing.group_id != self.group_id:
                raise HTTPException(status_code=404, detail="Business mapping not found")

            # Delete mapping
            deleted = await self.business_mapping_repo.delete(mapping_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="Business mapping not found")

            logger.info(f"Deleted business mapping ID {mapping_id}")
            return True

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting business mapping: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to delete business mapping: {str(e)}")

    async def get_business_mappings(
        self,
        semantic_model_id: str
    ) -> List[PowerBIBusinessMappingResponse]:
        """
        Get all business mappings for a semantic model.

        Args:
            semantic_model_id: Power BI semantic model ID

        Returns:
            List of business mappings
        """
        try:
            mappings = await self.business_mapping_repo.get_by_model(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id
            )
            return [PowerBIBusinessMappingResponse.model_validate(m) for m in mappings]

        except Exception as e:
            logger.error(f"Error retrieving business mappings: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve business mappings: {str(e)}")

    # ===== Field Synonyms Operations =====

    async def create_field_synonym(
        self,
        semantic_model_id: str,
        synonym_data: PowerBIFieldSynonymCreate
    ) -> PowerBIFieldSynonymResponse:
        """
        Create a new field synonym.

        Args:
            semantic_model_id: Power BI semantic model ID
            synonym_data: Field synonym creation data

        Returns:
            Created field synonym

        Raises:
            HTTPException: If synonym already exists or creation fails
        """
        try:
            # Check if synonym already exists
            existing = await self.field_synonym_repo.get_by_field(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id,
                field_name=synonym_data.field_name
            )
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"Field synonym for field '{synonym_data.field_name}' already exists"
                )

            # Create synonym
            db_data = {
                "group_id": self.group_id,
                "semantic_model_id": semantic_model_id,
                **synonym_data.model_dump()
            }
            # Remove duplicate semantic_model_id from model_dump
            db_data.pop("semantic_model_id", None)
            db_data["semantic_model_id"] = semantic_model_id

            synonym = await self.field_synonym_repo.create(db_data)
            logger.info(f"Created field synonym: {synonym.field_name} for model {semantic_model_id}")

            return PowerBIFieldSynonymResponse.model_validate(synonym)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating field synonym: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to create field synonym: {str(e)}")

    async def update_field_synonym(
        self,
        synonym_id: int,
        synonym_data: PowerBIFieldSynonymUpdate
    ) -> PowerBIFieldSynonymResponse:
        """
        Update an existing field synonym.

        Args:
            synonym_id: Synonym ID
            synonym_data: Updated synonym data

        Returns:
            Updated field synonym

        Raises:
            HTTPException: If synonym not found or update fails
        """
        try:
            # Verify synonym exists and belongs to group
            existing = await self.field_synonym_repo.get(synonym_id)
            if not existing or existing.group_id != self.group_id:
                raise HTTPException(status_code=404, detail="Field synonym not found")

            # Update synonym
            update_dict = synonym_data.model_dump(exclude_unset=True)
            updated = await self.field_synonym_repo.update(synonym_id, update_dict)

            if not updated:
                raise HTTPException(status_code=404, detail="Field synonym not found")

            logger.info(f"Updated field synonym ID {synonym_id}")
            return PowerBIFieldSynonymResponse.model_validate(updated)

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating field synonym: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to update field synonym: {str(e)}")

    async def delete_field_synonym(self, synonym_id: int) -> bool:
        """
        Delete a field synonym.

        Args:
            synonym_id: Synonym ID

        Returns:
            True if deleted successfully

        Raises:
            HTTPException: If synonym not found or deletion fails
        """
        try:
            # Verify synonym exists and belongs to group
            existing = await self.field_synonym_repo.get(synonym_id)
            if not existing or existing.group_id != self.group_id:
                raise HTTPException(status_code=404, detail="Field synonym not found")

            # Delete synonym
            deleted = await self.field_synonym_repo.delete(synonym_id)
            if not deleted:
                raise HTTPException(status_code=404, detail="Field synonym not found")

            logger.info(f"Deleted field synonym ID {synonym_id}")
            return True

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting field synonym: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to delete field synonym: {str(e)}")

    async def get_field_synonyms(
        self,
        semantic_model_id: str
    ) -> List[PowerBIFieldSynonymResponse]:
        """
        Get all field synonyms for a semantic model.

        Args:
            semantic_model_id: Power BI semantic model ID

        Returns:
            List of field synonyms
        """
        try:
            synonyms = await self.field_synonym_repo.get_by_model(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id
            )
            return [PowerBIFieldSynonymResponse.model_validate(s) for s in synonyms]

        except Exception as e:
            logger.error(f"Error retrieving field synonyms: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve field synonyms: {str(e)}")

    # ===== Bulk Operations =====

    async def get_all_context_config(
        self,
        semantic_model_id: str
    ) -> PowerBIContextConfigBulkResponse:
        """
        Get all context configuration (mappings and synonyms) for a semantic model.

        Args:
            semantic_model_id: Power BI semantic model ID

        Returns:
            Bulk response with all mappings and synonyms
        """
        try:
            mappings = await self.get_business_mappings(semantic_model_id)
            synonyms = await self.get_field_synonyms(semantic_model_id)

            return PowerBIContextConfigBulkResponse(
                business_mappings=mappings,
                field_synonyms=synonyms
            )

        except Exception as e:
            logger.error(f"Error retrieving context configuration: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve context configuration: {str(e)}")

    async def get_context_config_dict(
        self,
        semantic_model_id: str
    ) -> PowerBIContextConfigDict:
        """
        Get context configuration in dictionary format (for tool integration).

        Args:
            semantic_model_id: Power BI semantic model ID

        Returns:
            Dictionary format compatible with powerbi_analysis_tool
        """
        try:
            mappings_dict = await self.business_mapping_repo.get_as_dict(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id
            )
            synonyms_dict = await self.field_synonym_repo.get_as_dict(
                group_id=self.group_id,
                semantic_model_id=semantic_model_id
            )

            return PowerBIContextConfigDict(
                business_mappings=mappings_dict if mappings_dict else None,
                field_synonyms=synonyms_dict if synonyms_dict else None
            )

        except Exception as e:
            logger.error(f"Error retrieving context configuration dict: {e}", exc_info=True)
            raise HTTPException(status_code=500, detail=f"Failed to retrieve context configuration: {str(e)}")
