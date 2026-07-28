"""Databricks Vector Search memory backend endpoints.

Connection testing, endpoint/index provisioning and teardown, one-click setup,
and read access to the vector indexes backing memory.
"""

from typing import Any, Dict, Optional

from fastapi import APIRouter, Query, Request

from src.core.dependencies import GroupContextDep
from src.core.exceptions import BadRequestError, ForbiddenError
from src.core.permissions import is_workspace_admin
from src.schemas.memory_backend import DatabricksMemoryConfig
from src.utils.databricks_auth import extract_user_token_from_request

from .dependencies import MemoryBackendServiceDep, logger

router = APIRouter()


@router.get("/databricks/workspace-url")
async def get_workspace_url(
    service: MemoryBackendServiceDep,
    group_context: GroupContextDep,
) -> Dict[str, Any]:
    """
    Get the Databricks workspace URL from environment or configuration.

    Returns:
        Dict with workspace URL if available, or None
    """
    result = await service.get_workspace_url()
    return result


@router.post("/databricks/test-connection")
async def test_databricks_connection(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Test connection to Databricks Vector Search.

    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Connection test result
    """
    try:
        # Extract user token for OBO authentication
        user_token = extract_user_token_from_request(request)

        # Service is injected via dependency
        result = await service.test_databricks_connection(config, user_token)
        return result

    except Exception as e:
        logger.error(f"Error testing Databricks connection: {e}")
        return {
            "success": False,
            "message": f"Connection test failed: {str(e)}",
            "details": {"error": str(e)},
        }


@router.post("/databricks/indexes")
async def get_databricks_indexes(
    config: DatabricksMemoryConfig,
    request: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Get available indexes for a Databricks endpoint.

    Args:
        config: Databricks configuration
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        List of available indexes
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request)

    # Get indexes from the service
    result = await service.get_databricks_indexes(config, user_token)
    return result


@router.post("/databricks/create-index")
async def create_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Create a new Databricks Vector Search index.
    Only workspace admins can create Databricks indexes.

    Args:
        request: Request containing index creation parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Index creation result
    """
    # Check permissions - only workspace admins can create indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can create Databricks indexes")

    # Extract parameters
    try:
        config = DatabricksMemoryConfig(**request.get("config", {}))
    except Exception as e:
        raise BadRequestError(f"Invalid Databricks configuration: {str(e)}")

    index_type = request.get("index_type")
    catalog = request.get("catalog")
    schema = request.get("schema")
    table_name = request.get("table_name")
    primary_key = request.get("primary_key", "id")

    # Validate required parameters
    if not all([index_type, catalog, schema, table_name]):
        raise BadRequestError(
            "index_type, catalog, schema, and table_name are required"
        )

    if index_type not in ["short_term", "long_term", "entity", "document"]:
        raise BadRequestError(
            "index_type must be one of: short_term, long_term, entity, document"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Create the index
    result = await service.create_databricks_index(
        config=config,
        index_type=index_type,
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        primary_key=primary_key,
        user_token=user_token,
    )

    return result


@router.post("/databricks/one-click-setup")
async def one_click_databricks_setup(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    One-click setup for Databricks Vector Search.
    Creates all endpoints and indexes automatically.
    Only workspace admins can set up memory backend for their workspace.

    Args:
        request: Request containing workspace_url, catalog, and schema
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Setup result with created resources
    """
    # Check permissions - only workspace admins can set up memory backend
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can set up memory backend")

    # CRITICAL: Set UserContext for authentication system to access group_id
    # The authentication system needs group_id to look up PAT tokens from database
    from src.utils.user_context import UserContext

    UserContext.set_group_context(group_context)
    logger.info(
        f"[ONE-CLICK-SETUP] Set UserContext with group_id: {group_context.primary_group_id}"
    )

    # Get workspace URL from unified auth or user request
    workspace_url = request.get("workspace_url")

    # Try to get from unified auth first
    if not workspace_url:
        try:
            from src.utils.databricks_auth import get_auth_context

            auth = await get_auth_context()
            if auth and auth.workspace_url:
                workspace_url = auth.workspace_url
                logger.info(
                    f"Using workspace URL from unified {auth.auth_method} auth: {workspace_url}"
                )
        except Exception as e:
            logger.warning(f"Failed to get unified auth: {e}")

    if not workspace_url:
        raise BadRequestError(
            "workspace_url is required and not available from unified auth"
        )

    catalog = request.get("catalog", "ml")
    schema = request.get("schema", "agents")
    embedding_dimension = request.get(
        "embedding_dimension", 1024
    )  # Default to 1024 for databricks-gte-large-en

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Run one-click setup with user_id from group context
    logger.info(f"Starting one-click setup for group: {group_context.primary_group_id}")
    logger.info(
        f"Workspace URL: {workspace_url}, Catalog: {catalog}, Schema: {schema}, Embedding dimension: {embedding_dimension}"
    )

    result = await service.one_click_databricks_setup(
        workspace_url=workspace_url,
        catalog=catalog,
        schema=schema,
        embedding_dimension=embedding_dimension,
        user_token=user_token,
        group_id=group_context.primary_group_id,  # Pass group_id from group context
    )

    logger.info(f"One-click setup result: {result}")

    return result


@router.get("/databricks/verify-resources")
async def verify_databricks_resources(
    workspace_url: str,
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
    backend_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Verify which Databricks resources actually exist.

    Args:
        workspace_url: Databricks workspace URL
        group_context: Current group context
        service: Memory backend service
        backend_id: Optional backend ID to verify specific configuration

    Returns:
        Dict with existing resources information
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)
    logger.info(f"Extracted user token from request: {bool(user_token)}")

    # Get the configuration to check
    config = None
    if backend_id:
        # Get specific backend configuration
        config = await service.get_memory_backend(
            group_context.primary_group_id, backend_id
        )
        logger.info(f"Using specific backend config: {backend_id}")
    else:
        # Get default configuration
        config = await service.get_default_memory_backend(
            group_context.primary_group_id
        )
        logger.info(f"Using default backend config")

    # Use the service to verify resources
    result = await service.verify_databricks_resources(
        workspace_url, user_token, config
    )

    return result


@router.get("/databricks/endpoint-status")
async def get_databricks_endpoint_status(
    workspace_url: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Get the status of a Databricks Vector Search endpoint.

    Args:
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the endpoint to check
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dict with endpoint status information
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Use the service to get endpoint status
    result = await service.get_databricks_endpoint_status(
        workspace_url=workspace_url, endpoint_name=endpoint_name, user_token=user_token
    )

    return result


@router.delete("/databricks/index")
async def delete_databricks_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search index.
    Only workspace admins can delete Databricks indexes.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can delete Databricks indexes")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    index_name = request.get("index_name")
    endpoint_name = request.get("endpoint_name")

    # Validate required parameters
    if not all([workspace_url, index_name, endpoint_name]):
        raise BadRequestError(
            "workspace_url, index_name, and endpoint_name are required"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Delete the index
    result = await service.delete_databricks_index(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        user_token=user_token,
    )

    return result


@router.delete("/databricks/endpoint")
async def delete_databricks_endpoint(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Delete a Databricks Vector Search endpoint.
    Only workspace admins can delete Databricks endpoints.

    Args:
        request: Request containing deletion parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Deletion result
    """
    # Check permissions - only workspace admins can delete endpoints
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can delete Databricks endpoints")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    endpoint_name = request.get("endpoint_name")

    # Validate required parameters
    if not all([workspace_url, endpoint_name]):
        raise BadRequestError("workspace_url and endpoint_name are required")

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Delete the endpoint
    result = await service.delete_databricks_endpoint(
        workspace_url=workspace_url, endpoint_name=endpoint_name, user_token=user_token
    )

    return result


@router.get("/databricks/index-info")
async def get_index_info(
    workspace_url: str,
    index_name: str,
    endpoint_name: str,
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Get information about a Databricks Vector Search index including document count.

    Args:
        workspace_url: Databricks workspace URL
        index_name: Full index name (catalog.schema.table)
        endpoint_name: Endpoint name that hosts the index
        group_context: Current group context
        service: Memory backend service

    Returns:
        Index information including document count
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Get index info
    result = await service.get_index_info(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        user_token=user_token,
    )

    return result


@router.post("/databricks/empty-index")
async def empty_index(
    request: Dict[str, Any],
    req: Request,
    group_context: GroupContextDep,
    service: MemoryBackendServiceDep,
) -> Dict[str, Any]:
    """
    Empty a Databricks Vector Search index by deleting and recreating it.
    Only workspace admins can empty Databricks indexes.

    Args:
        request: Request containing index parameters
        req: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Operation result
    """
    # Check permissions - only workspace admins can empty indexes
    if not is_workspace_admin(group_context):
        raise ForbiddenError("Only workspace admins can empty Databricks indexes")

    # Extract parameters
    workspace_url = request.get("workspace_url")
    index_name = request.get("index_name")
    endpoint_name = request.get("endpoint_name")
    index_type = request.get("index_type")
    embedding_dimension = request.get("embedding_dimension", 1024)

    # Validate required parameters
    if not all([workspace_url, index_name, endpoint_name, index_type]):
        raise BadRequestError(
            "workspace_url, index_name, endpoint_name, and index_type are required"
        )

    if index_type not in ["short_term", "long_term", "entity", "document"]:
        raise BadRequestError(
            "index_type must be one of: short_term, long_term, entity, document"
        )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(req)

    # Empty the index
    result = await service.empty_index(
        workspace_url=workspace_url,
        index_name=index_name,
        endpoint_name=endpoint_name,
        index_type=index_type,
        embedding_dimension=embedding_dimension,
        user_token=user_token,
    )

    return result


@router.get("/databricks/index-documents")
async def get_index_documents(
    index_name: str = Query(
        ..., description="Full name of the index (catalog.schema.index)"
    ),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    index_type: Optional[str] = Query(
        None, description="Type of index (short_term, long_term, entity, document)"
    ),
    backend_id: Optional[str] = Query(None, description="Backend configuration ID"),
    limit: int = Query(30, description="Maximum number of documents to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: MemoryBackendServiceDep = None,
) -> Dict[str, Any]:
    """
    Retrieve documents from any Databricks Vector Search index.

    This endpoint fetches the most recent documents from a specified index
    for viewing and inspection purposes.

    Args:
        index_name: Full name of the index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        index_type: Type of index (short_term, long_term, entity, document)
        backend_id: Backend configuration ID to retrieve embedding dimension
        limit: Maximum number of documents to return (default: 30)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dictionary containing documents and metadata
    """
    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request) if request else None

    # Get embedding dimension from backend config if backend_id is provided
    embedding_dimension = 1024  # Default
    if backend_id and group_context:
        try:
            backend = await service.get_memory_backend(
                group_context.primary_group_id, backend_id
            )
            if backend and backend.databricks_config:
                db_config = backend.databricks_config
                if hasattr(db_config, "embedding_dimension"):
                    embedding_dimension = db_config.embedding_dimension or 1024
                elif isinstance(db_config, dict):
                    embedding_dimension = db_config.get("embedding_dimension", 1024)
                else:
                    embedding_dimension = 1024
                logger.info(
                    f"Retrieved embedding dimension {embedding_dimension} from backend config {backend_id}"
                )
        except Exception as e:
            logger.warning(
                f"Could not retrieve embedding dimension from backend config: {e}"
            )

    # Get documents from the service
    result = await service.get_index_documents(
        workspace_url=workspace_url,
        endpoint_name=endpoint_name,
        index_name=index_name,
        index_type=index_type,
        embedding_dimension=embedding_dimension,
        limit=limit,
        user_token=user_token,
    )

    return result


@router.get("/databricks/entity-data")
async def get_entity_data(
    index_name: str = Query(..., description="Name of the entity memory index"),
    workspace_url: str = Query(..., description="Databricks workspace URL"),
    endpoint_name: str = Query(..., description="Vector Search endpoint name"),
    embedding_dimension: int = Query(
        1024, description="Dimension of embedding vectors"
    ),
    limit: int = Query(100, description="Maximum number of entities to return"),
    request: Request = None,
    group_context: GroupContextDep = None,
    service: MemoryBackendServiceDep = None,
) -> Dict[str, Any]:
    """
    Retrieve entity data from the entity memory index for visualization.

    This endpoint fetches entities and their relationships from the Databricks
    Vector Search entity memory index and formats them for graph visualization.

    Args:
        index_name: Full name of the entity memory index (catalog.schema.index)
        workspace_url: Databricks workspace URL
        endpoint_name: Name of the Vector Search endpoint
        embedding_dimension: Dimension of embedding vectors (default: 1024)
        limit: Maximum number of entities to return (default: 100)
        request: FastAPI request for extracting user token
        group_context: Current group context
        service: Memory backend service

    Returns:
        Dictionary containing entities and relationships for visualization
    """
    # Import the databricks logger
    from src.core.logger import LoggerManager

    databricks_logger = LoggerManager.get_instance().databricks_vector_search

    databricks_logger.info(f"[ENTITY] API endpoint called: /databricks/entity-data")
    databricks_logger.info(
        f"[ENTITY] Parameters: index_name={index_name}, workspace_url={workspace_url}, endpoint_name={endpoint_name}, limit={limit}"
    )

    # Extract user token for OBO authentication
    user_token = extract_user_token_from_request(request) if request else None
    databricks_logger.info(
        f"[ENTITY] User token extracted: {'Yes' if user_token else 'No'}"
    )

    # Get the index service
    from src.services.databricks.vector_search.index import DatabricksIndexService

    index_service = DatabricksIndexService()

    # Query the actual entity data from Databricks Vector Search
    result = await index_service.query_entity_data(
        workspace_url=workspace_url,
        endpoint_name=endpoint_name,
        index_name=index_name,
        embedding_dimension=embedding_dimension,
        limit=limit,
        user_token=user_token,
    )

    databricks_logger.info(
        f"[ENTITY] Query result: success={result.get('success')}, entities={len(result.get('entities', []))}, relationships={len(result.get('relationships', []))}"
    )

    # Return the actual data from the index
    return result
