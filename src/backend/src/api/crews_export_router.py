"""
API router for crew export and deployment operations.
"""

from typing import Annotated, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse, Response
import logging
import zipfile
import io

from src.core.dependencies import SessionDep, GroupContextDep
from src.core.permissions import check_role_in_context
from src.schemas.crew_export import (
    CrewExportRequest,
    CrewExportResponse,
    ExportFormat,
    DeploymentRequest,
    DeploymentResponse,
)
from src.services.crew_export_service import CrewExportService
from src.services.crew_deployment_service import CrewDeploymentService

router = APIRouter(
    prefix="/crews",
    tags=["crews-export"],
    responses={404: {"description": "Not found"}},
)

# Set up logging
logger = logging.getLogger(__name__)


async def get_export_service(session: SessionDep) -> CrewExportService:
    """
    Dependency provider for CrewExportService.

    Creates service with properly injected session following the pattern:
    Router → Service → Repository → DB

    Args:
        session: Database session from FastAPI DI

    Returns:
        CrewExportService instance with injected session
    """
    return CrewExportService(session=session)


async def get_deployment_service(session: SessionDep) -> CrewDeploymentService:
    """
    Dependency provider for CrewDeploymentService.

    Args:
        session: Database session from FastAPI DI

    Returns:
        CrewDeploymentService instance with injected session
    """
    return CrewDeploymentService(session=session)


# Type aliases for cleaner function signatures
ExportServiceDep = Annotated[CrewExportService, Depends(get_export_service)]
DeploymentServiceDep = Annotated[CrewDeploymentService, Depends(get_deployment_service)]


@router.post("/{crew_id}/export", response_model=CrewExportResponse, status_code=status.HTTP_200_OK)
async def export_crew(
    crew_id: str,
    request: CrewExportRequest,
    service: ExportServiceDep,
    group_context: GroupContextDep,
):
    """
    Export crew to Python project or Databricks notebook.
    Only Editors and Admins can export crews.

    **Export Formats:**
    - `python_project`: Complete Python project with multiple files (downloaded as .zip)
    - `databricks_notebook`: Single Databricks-compatible .ipynb notebook file

    **Options:**
    - `include_custom_tools`: Include custom tool implementations (default: true)
    - `include_comments`: Add explanatory comments (default: true)
    - `include_tests`: Include test files - python_project only (default: true)
    - `model_override`: Override default LLM model for all agents (optional)

    Args:
        crew_id: ID of the crew to export
        request: Export configuration
        service: Export service injected by dependency
        group_context: Group context from headers

    Returns:
        Export result with files/notebook and metadata
    """
    # Log the export request options for debugging
    logger.info(f"Export request for crew {crew_id}: format={request.export_format}, options={request.options}")

    # Check permissions - only editors and admins can export
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can export crews"
        )

    # Validate group context
    if not group_context or not group_context.is_valid():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid group context provided"
        )

    try:
        result = await service.export_crew(
            crew_id=crew_id,
            export_format=request.export_format,
            options=request.options,
            group_context=group_context
        )

        # Add download URL
        result['download_url'] = f"/api/crews/{crew_id}/export/download?format={request.export_format}"

        return CrewExportResponse(**result)

    except ValueError as e:
        logger.error(f"Crew not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Export failed for crew {crew_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Export failed: {str(e)}"
        )


@router.get("/{crew_id}/export/download")
async def download_export(
    crew_id: str,
    service: ExportServiceDep,
    group_context: GroupContextDep,
    format: ExportFormat = Query(..., description="Export format"),
    include_custom_tools: bool = Query(True, description="Include custom tool implementations"),
    include_comments: bool = Query(True, description="Add explanatory comments"),
    include_tracing: bool = Query(True, description="Include MLflow tracing"),
    include_evaluation: bool = Query(True, description="Include evaluation metrics"),
    include_deployment: bool = Query(True, description="Include deployment code"),
    model_override: str = Query(None, description="Override LLM model")
):
    """
    Download exported crew as file.
    Only Editors and Admins can download exports.

    **Returns:**
    - Python project: .zip archive containing project structure
    - Databricks notebook: .ipynb file ready for Databricks import

    **Usage:**
    1. Call /export endpoint to generate export
    2. Use the download_url from response to download file
    3. For Python Project: Extract .zip and follow README.md
    4. For Databricks Notebook: Import .ipynb into Databricks workspace

    Args:
        crew_id: ID of the crew to download
        service: Export service injected by dependency
        group_context: Group context from headers
        format: Export format (python_project or databricks_notebook)

    Returns:
        File download (zip or ipynb)
    """
    # Check permissions
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can download exports"
        )

    # Validate group context
    if not group_context or not group_context.is_valid():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid group context provided"
        )

    try:
        # Create options from query parameters
        from src.schemas.crew_export import ExportOptions
        export_options = ExportOptions(
            include_custom_tools=include_custom_tools,
            include_comments=include_comments,
            include_tracing=include_tracing,
            include_evaluation=include_evaluation,
            include_deployment=include_deployment,
            model_override=model_override if model_override else None
        )

        logger.info(f"Download request with options: {export_options}")

        # Generate export
        result = await service.export_crew(
            crew_id=crew_id,
            export_format=format,
            options=export_options,
            group_context=group_context
        )

        crew_name = result['crew_name']
        sanitized_name = crew_name.lower().replace(' ', '_')

        if format == ExportFormat.PYTHON_PROJECT:
            # Create zip archive
            zip_buffer = io.BytesIO()

            with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
                for file_info in result['files']:
                    file_path = f"{sanitized_name}/{file_info['path']}"
                    zip_file.writestr(file_path, file_info['content'])

            zip_buffer.seek(0)

            return StreamingResponse(
                zip_buffer,
                media_type='application/zip',
                headers={
                    'Content-Disposition': f'attachment; filename="{sanitized_name}_project.zip"'
                }
            )

        else:  # databricks_notebook
            # Return notebook as .ipynb file
            notebook_content = result['notebook_content']

            return Response(
                content=notebook_content,
                media_type='application/x-ipynb+json',
                headers={
                    'Content-Disposition': f'attachment; filename="{sanitized_name}.ipynb"'
                }
            )

    except ValueError as e:
        logger.error(f"Crew not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Download failed for crew {crew_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Download failed: {str(e)}"
        )


@router.post("/{crew_id}/deploy", response_model=DeploymentResponse, status_code=status.HTTP_200_OK)
async def deploy_crew(
    crew_id: str,
    request: DeploymentRequest,
    service: DeploymentServiceDep,
    group_context: GroupContextDep,
):
    """
    Deploy crew to Databricks Model Serving endpoint.
    Only Admins can deploy crews.

    **Deployment Process:**
    1. Wraps crew as MLflow PyFunc model
    2. Logs model to MLflow with dependencies
    3. Registers in Unity Catalog (if configured)
    4. Creates/updates Model Serving endpoint
    5. Returns endpoint URL for invocations

    **Configuration:**
    - `model_name`: Name for the registered model (required)
    - `endpoint_name`: Name for serving endpoint (defaults to model_name)
    - `workload_size`: Small, Medium, or Large (default: Small)
    - `scale_to_zero_enabled`: Enable auto-scaling to zero (default: true)
    - `unity_catalog_model`: Register in Unity Catalog (default: true)
    - `catalog_name`: Unity Catalog name (required if unity_catalog_model=true)
    - `schema_name`: Unity Catalog schema (required if unity_catalog_model=true)

    Args:
        crew_id: ID of the crew to deploy
        request: Deployment configuration
        service: Deployment service injected by dependency
        group_context: Group context from headers

    Returns:
        Deployment result with endpoint details
    """
    # Check permissions - only admins can deploy
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can deploy crews to Model Serving"
        )

    # Validate group context
    if not group_context or not group_context.is_valid():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid group context provided"
        )

    try:
        result = await service.deploy_to_model_serving(
            crew_id=crew_id,
            config=request.config,
            group_context=group_context
        )

        return result

    except ValueError as e:
        logger.error(f"Crew not found: {e}")
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(e)
        )
    except Exception as e:
        logger.error(f"Deployment failed for crew {crew_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Deployment failed: {str(e)}"
        )


@router.get("/{crew_id}/deployment/status")
async def get_deployment_status(
    crew_id: str,
    group_context: GroupContextDep,
    endpoint_name: str = Query(..., description="Model serving endpoint name")
) -> Dict[str, Any]:
    """
    Get status of a deployed endpoint.
    Only Editors and Admins can check deployment status.

    **Returns:**
    - Endpoint state (READY, NOT_READY, etc.)
    - Configuration details
    - Ready/target replica counts
    - Last update timestamp

    **States:**
    - `READY`: Endpoint is ready for serving
    - `NOT_READY`: Endpoint is starting up
    - `UPDATE_IN_PROGRESS`: Endpoint configuration is being updated
    - `UPDATE_FAILED`: Update failed

    Args:
        crew_id: ID of the crew
        group_context: Group context from headers
        endpoint_name: Name of the serving endpoint

    Returns:
        Endpoint status information
    """
    # Check permissions
    if not check_role_in_context(group_context, ["admin", "editor"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only editors and admins can check deployment status"
        )

    # Validate group context
    if not group_context or not group_context.is_valid():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid group context provided"
        )

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        endpoint = w.serving_endpoints.get(endpoint_name)

        return {
            "endpoint_name": endpoint_name,
            "state": endpoint.state.ready.value if endpoint.state and endpoint.state.ready else "UNKNOWN",
            "config_update": endpoint.state.config_update.value if endpoint.state and endpoint.state.config_update else None,
            "pending_config": endpoint.pending_config is not None,
            "ready_replicas": getattr(endpoint.state, 'ready_replicas', 0) if endpoint.state else 0,
            "target_replicas": getattr(endpoint.config, 'target_replicas', 0) if endpoint.config else 0,
            "creator": endpoint.creator,
            "creation_timestamp": endpoint.creation_timestamp,
            "last_updated_timestamp": endpoint.last_updated_timestamp,
        }

    except Exception as e:
        logger.error(f"Failed to get deployment status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get status: {str(e)}"
        )


@router.delete("/{crew_id}/deployment/{endpoint_name}")
async def delete_deployment(
    crew_id: str,
    endpoint_name: str,
    group_context: GroupContextDep,
) -> Dict[str, str]:
    """
    Delete a Model Serving endpoint.
    Only Admins can delete deployments.

    **Warning:** This operation is irreversible. The endpoint will be permanently deleted.

    Args:
        crew_id: ID of the crew
        endpoint_name: Name of the serving endpoint to delete
        group_context: Group context from headers

    Returns:
        Confirmation message
    """
    # Check permissions - only admins can delete deployments
    if not check_role_in_context(group_context, ["admin"]):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Only admins can delete deployments"
        )

    # Validate group context
    if not group_context or not group_context.is_valid():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="No valid group context provided"
        )

    try:
        from databricks.sdk import WorkspaceClient

        w = WorkspaceClient()
        w.serving_endpoints.delete(endpoint_name)

        logger.info(f"Deleted endpoint {endpoint_name} for crew {crew_id}")

        return {
            "message": f"Endpoint {endpoint_name} has been deleted successfully",
            "endpoint_name": endpoint_name
        }

    except Exception as e:
        logger.error(f"Failed to delete endpoint: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete endpoint: {str(e)}"
        )
