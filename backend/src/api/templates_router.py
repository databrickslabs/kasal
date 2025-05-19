from typing import Annotated, List, Dict, Any

from fastapi import APIRouter, HTTPException, status
import logging

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


@router.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        Health status
    """
    return {"status": "healthy"}


@router.get("", response_model=List[PromptTemplateResponse])
async def list_templates():
    """
    Get all prompt templates.
    
    Returns:
        List of prompt templates
    """
    try:
        logger.info("API call: GET /templates")
        
        templates = await TemplateService.find_all_templates()
        logger.info(f"Retrieved {len(templates)} prompt templates")
        
        return templates
    except Exception as e:
        logger.error(f"Error retrieving prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{template_id}", response_model=PromptTemplateResponse)
async def get_template(template_id: int):
    """
    Get a specific prompt template by ID.
    
    Args:
        template_id: ID of the template to get
        
    Returns:
        Prompt template if found
        
    Raises:
        HTTPException: If template not found
    """
    try:
        logger.info(f"API call: GET /templates/{template_id}")
        
        template = await TemplateService.get_template_by_id(template_id)
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
async def get_template_by_name(name: str):
    """
    Get a specific prompt template by name.
    
    Args:
        name: Name of the template to get
        
    Returns:
        Prompt template if found
        
    Raises:
        HTTPException: If template not found
    """
    try:
        logger.info(f"API call: GET /templates/by-name/{name}")
        
        template = await TemplateService.find_template_by_name(name)
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
async def create_template(template: PromptTemplateCreate):
    """
    Create a new prompt template.
    
    Args:
        template: Template data for creation
        
    Returns:
        Created prompt template
        
    Raises:
        HTTPException: If template with the same name already exists
    """
    try:
        logger.info(f"API call: POST /templates - Creating template '{template.name}'")
        
        created_template = await TemplateService.create_new_template(template)
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
async def update_template(template_id: int, template: PromptTemplateUpdate):
    """
    Update an existing prompt template.
    
    Args:
        template_id: ID of the template to update
        template: Template data for update
        
    Returns:
        Updated prompt template
        
    Raises:
        HTTPException: If template not found or name conflict
    """
    try:
        logger.info(f"API call: PUT /templates/{template_id}")
        
        updated_template = await TemplateService.update_existing_template(template_id, template)
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
async def delete_template(template_id: int):
    """
    Delete a prompt template.
    
    Args:
        template_id: ID of the template to delete
        
    Returns:
        Success message
        
    Raises:
        HTTPException: If template not found
    """
    try:
        logger.info(f"API call: DELETE /templates/{template_id}")
        
        deleted = await TemplateService.delete_template_by_id(template_id)
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
async def delete_all_templates():
    """
    Delete all prompt templates.
    
    Returns:
        Success message with count of deleted templates
    """
    try:
        logger.info("API call: DELETE /templates")
        
        deleted_count = await TemplateService.delete_all_templates_service()
        logger.info(f"Deleted {deleted_count} prompt templates")
        
        return {
            "message": "All prompt templates deleted successfully",
            "deleted_count": deleted_count
        }
    except Exception as e:
        logger.error(f"Error deleting all prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/reset", response_model=ResetResponse, status_code=status.HTTP_200_OK)
async def reset_templates():
    """
    Reset all prompt templates to default values.
    
    Returns:
        Success message with count of reset templates
    """
    try:
        logger.info("API call: POST /templates/reset")
        
        reset_count = await TemplateService.reset_templates_service()
        logger.info(f"Reset {reset_count} prompt templates to default values")
        
        return {
            "message": f"Reset {reset_count} prompt templates to default values",
            "reset_count": reset_count
        }
    except Exception as e:
        logger.error(f"Error resetting prompt templates: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) 