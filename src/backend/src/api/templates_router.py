from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, HTTPException, status, Depends
import logging

from src.core.dependencies import GroupContextDep, SessionDep
from src.services.template_service import TemplateService
from src.models.template import PromptTemplate
from src.schemas.template import (
    PromptTemplateCreate,
    PromptTemplateUpdate,
    PromptTemplateResponse,
    TemplateListResponse,
    ResetResponse
)

router = APIRouter(
    prefix="/templates",
    tags=["templates"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)


def get_template_service(session: SessionDep) -> TemplateService:
    """
    Dependency provider for TemplateService.

    Creates service with session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        TemplateService instance with session
    """
    return TemplateService(session)


# Type alias for cleaner function signatures
TemplateServiceDep = Annotated[TemplateService, Depends(get_template_service)]


@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Health status
    """
    return {"status": "healthy"}


@router.get("", response_model=List[PromptTemplateResponse])
async def list_templates(
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Get all prompt templates for the current group.

    Uses dependency injection to get TemplateService with repository.

    Args:
        service: Injected TemplateService instance
        group_context: Group context from headers

    Returns:
        List of prompt templates for the current group
    """
    try:
        logger.info("API call: GET /templates")

        templates = await service.find_all_templates_for_group(group_context)
        logger.info(f"Retrieved {len(templates)} prompt templates")

        return templates
    except Exception as e:
        logger.error(f"Error retrieving prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{template_id}", response_model=PromptTemplateResponse)
async def get_template(
    template_id: int,
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Get a specific prompt template by ID with group isolation.

    Uses dependency injection to get TemplateService with repository.

    Args:
        template_id: ID of the template to get
        service: Injected TemplateService instance
        group_context: Group context from headers

    Returns:
        Prompt template if found and belongs to user's group

    Raises:
        HTTPException: If template not found or not authorized
    """
    try:
        logger.info(f"API call: GET /templates/{template_id}")

        template = await service.get_template_with_group_check(template_id, group_context)
        if not template:
            logger.warning(f"Template with ID {template_id} not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Prompt template not found"
            )
            
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving prompt template {template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/by-name/{name}", response_model=PromptTemplateResponse)
async def get_template_by_name(
    name: str,
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Get a specific prompt template by name with group isolation.

    Uses dependency injection to get TemplateService with repository.

    Args:
        name: Name of the template to get
        service: Injected TemplateService instance
        group_context: Group context from headers

    Returns:
        Prompt template if found and belongs to user's group

    Raises:
        HTTPException: If template not found or not authorized
    """
    try:
        logger.info(f"API call: GET /templates/by-name/{name}")

        template = await service.find_template_by_name_with_group(name, group_context)
        if not template:
            logger.warning(f"Template with name '{name}' not found")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Prompt template with name '{name}' not found"
            )
            
        return template
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error retrieving prompt template with name '{name}': {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("", response_model=PromptTemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(
    template: PromptTemplateCreate,
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Create a new prompt template with group isolation.

    Uses dependency injection to get TemplateService with repository.

    Args:
        template: Template data for creation
        service: Injected TemplateService instance
        group_context: Group context from headers

    Returns:
        Created prompt template

    Raises:
        HTTPException: If template with the same name already exists
    """
    try:
        logger.info(f"API call: POST /templates - Creating template '{template.name}'")
        
        created_template = await service.create_template_with_group(template, group_context)
        logger.info(f"Created new prompt template with name '{template.name}'")
        
        return created_template
    except ValueError as ve:
        # Value error indicates template name already exists
        logger.error(f"Template with name '{template.name}' already exists")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except Exception as e:
        logger.error(f"Error creating prompt template: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{template_id}", response_model=PromptTemplateResponse)
async def update_template(
    template_id: int,
    template: PromptTemplateUpdate,
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Update an existing prompt template with group isolation.
    
    Args:
        template_id: ID of the template to update
        template: Template data for update
        group_context: Group context from headers
        
    Returns:
        Updated prompt template
        
    Raises:
        HTTPException: If template not found or not authorized
    """
    try:
        logger.info(f"API call: PUT /templates/{template_id}")
        
        updated_template = await service.update_with_group_check(template_id, template, group_context)
        if not updated_template:
            logger.warning(f"Template with ID {template_id} not found for update")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Prompt template not found"
            )
            
        logger.info(f"Updated prompt template with ID {template_id}")
        return updated_template
    except ValueError as ve:
        # Value error indicates template name conflict
        logger.error(f"Name conflict when updating template: {str(ve)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(ve)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error updating prompt template {template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{template_id}", status_code=status.HTTP_200_OK)
async def delete_template(
    template_id: int,
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete a prompt template with group isolation.
    
    Args:
        template_id: ID of the template to delete
        group_context: Group context from headers
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If template not found or not authorized
    """
    try:
        logger.info(f"API call: DELETE /templates/{template_id}")
        
        deleted = await service.delete_with_group_check(template_id, group_context)
        if not deleted:
            logger.warning(f"Template with ID {template_id} not found for deletion")
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Prompt template not found"
            )
            
        logger.info(f"Deleted prompt template with ID {template_id}")
        return {"message": f"Prompt template with ID {template_id} deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting prompt template {template_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("", status_code=status.HTTP_200_OK)
async def delete_all_templates(
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Delete all prompt templates for the current group.
    
    Args:
        group_context: Group context from headers
        
    Returns:
        Success message with count of deleted templates
    """
    try:
        logger.info("API call: DELETE /templates")
        
        deleted_count = await service.delete_all_for_group_internal(group_context)
        logger.info(f"Deleted {deleted_count} prompt templates")
        
        return {
            "message": "All prompt templates deleted successfully",
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Error deleting all prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset", response_model=ResetResponse, status_code=status.HTTP_200_OK)
async def reset_templates(
    service: TemplateServiceDep,
    group_context: GroupContextDep,
):
    """
    Reset all prompt templates to default values for the current group.
    
    Args:
        group_context: Group context from headers
        
    Returns:
        Success message with count of reset templates
    """
    try:
        logger.info("API call: POST /templates/reset")
        
        reset_count = await service.reset_templates_with_group(group_context)
        logger.info(f"Reset {reset_count} prompt templates to default values")
        
        return {
            "message": f"Reset {reset_count} prompt templates to default values",
            "reset_count": reset_count
        }
    except Exception as e:
        logger.error(f"Error resetting prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 