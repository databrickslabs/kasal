"""
API endpoints for documentation embeddings.

This module provides endpoints for managing and searching documentation embeddings.
"""
from fastapi import APIRouter, HTTPException, Query, Depends, Header
from typing import List, Optional, Annotated

from src.core.dependencies import SessionDep
from src.core.unit_of_work import UnitOfWork
from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.schemas.documentation_embedding import (
    DocumentationEmbedding as DocumentationEmbeddingSchema,
    DocumentationEmbeddingCreate,
    DocumentationEmbeddingSearch
)
from src.core.logger import LoggerManager

logger = LoggerManager.get_instance().api

router = APIRouter(
    prefix="/documentation-embeddings",
    tags=["documentation-embeddings"],
    responses={404: {"description": "Not found"}},
)

# Dependency to get DocumentationEmbeddingService using UnitOfWork with injected session
async def get_documentation_embedding_service(session: SessionDep) -> DocumentationEmbeddingService:
    """Get DocumentationEmbeddingService instance with proper session management."""
    async with UnitOfWork(session=session) as uow:
        return DocumentationEmbeddingService(uow)


@router.post("/", response_model=DocumentationEmbeddingSchema)
async def create_documentation_embedding(
    embedding: DocumentationEmbeddingCreate,
    service: Annotated[DocumentationEmbeddingService, Depends(get_documentation_embedding_service)],
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token"),
    x_auth_request_access_token: Optional[str] = Header(None, alias="X-Auth-Request-Access-Token")
):
    """Create a new documentation embedding."""
    try:
        # Extract user token from headers (OAuth2-Proxy takes priority)
        user_token = x_auth_request_access_token or x_forwarded_access_token
        result = await service.create_documentation_embedding(embedding, user_token=user_token)
        return result
    except Exception as e:
        logger.error(f"Error creating documentation embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search", response_model=List[DocumentationEmbeddingSchema])
async def search_documentation_embeddings(
    query_embedding: List[float] = Query(..., description="Query embedding vector"),
    limit: int = Query(5, ge=1, le=20, description="Maximum number of results")
):
    """Search for similar documentation embeddings."""
    try:
        async with UnitOfWork() as uow:
            service = DocumentationEmbeddingService(uow)
            results = await service.search_similar_embeddings(
                query_embedding=query_embedding,
                limit=limit
            )
            return results
    except Exception as e:
        logger.error(f"Error searching documentation embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[DocumentationEmbeddingSchema])
async def get_documentation_embeddings(
    skip: int = Query(0, ge=0, description="Number of items to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of items to return"),
    source: Optional[str] = Query(None, description="Filter by source"),
    title: Optional[str] = Query(None, description="Filter by title (partial match)")
):
    """Get documentation embeddings with optional filtering."""
    try:
        async with UnitOfWork() as uow:
            service = DocumentationEmbeddingService(uow)
            
            if source:
                results = await service.search_by_source(source, skip, limit)
            elif title:
                results = await service.search_by_title(title, skip, limit)
            else:
                results = await service.get_documentation_embeddings(skip, limit)
            
            # Convert to dict and clear embeddings to avoid serialization issues
            # Embeddings are large and not needed in list views
            result_dicts = []
            for result in results:
                result_dict = {
                    "id": result.id,
                    "source": result.source,
                    "title": result.title,
                    "content": result.content,
                    "doc_metadata": result.doc_metadata,
                    "created_at": result.created_at,
                    "updated_at": result.updated_at,
                    "embedding": []  # Clear embedding for list view
                }
                result_dicts.append(result_dict)
            
            return result_dicts
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Error getting documentation embeddings: {e}\n{error_trace}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/recent", response_model=List[DocumentationEmbeddingSchema])
async def get_recent_documentation_embeddings(
    limit: int = Query(10, ge=1, le=50, description="Maximum number of recent items")
):
    """Get the most recently created documentation embeddings."""
    try:
        async with UnitOfWork() as uow:
            service = DocumentationEmbeddingService(uow)
            results = await service.get_recent_embeddings(limit)
            return results
    except Exception as e:
        logger.error(f"Error getting recent documentation embeddings: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{embedding_id}", response_model=DocumentationEmbeddingSchema)
async def get_documentation_embedding(
    embedding_id: int
):
    """Get a specific documentation embedding by ID."""
    try:
        async with UnitOfWork() as uow:
            service = DocumentationEmbeddingService(uow)
            result = await service.get_documentation_embedding(embedding_id)
            
            if not result:
                raise HTTPException(status_code=404, detail="Documentation embedding not found")
            
            return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting documentation embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{embedding_id}")
async def delete_documentation_embedding(
    embedding_id: int
):
    """Delete a documentation embedding by ID."""
    try:
        async with UnitOfWork() as uow:
            service = DocumentationEmbeddingService(uow)
            success = await service.delete_documentation_embedding(embedding_id)
            
            if not success:
                raise HTTPException(status_code=404, detail="Documentation embedding not found")
            
            await uow.commit()
            return {"message": "Documentation embedding deleted successfully"}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting documentation embedding: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/seed-all")
async def seed_all_documentation_embeddings(
    x_forwarded_access_token: Optional[str] = Header(None, alias="X-Forwarded-Access-Token"),
    x_auth_request_access_token: Optional[str] = Header(None, alias="X-Auth-Request-Access-Token")
):
    """Re-seed all documentation embeddings from the docs directory."""
    try:
        # Import the seeding function
        from src.seeds.documentation import seed_documentation_embeddings
        
        logger.info("Starting documentation embeddings re-seeding...")
        
        # Extract user token from headers (OAuth2-Proxy takes priority)
        user_token = x_auth_request_access_token or x_forwarded_access_token
        
        # Run the seeding process with user token
        await seed_documentation_embeddings(user_token=user_token)
        
        logger.info("Documentation embeddings re-seeding completed successfully")
        
        return {
            "success": True,
            "message": "Documentation embeddings re-seeding completed successfully"
        }
    except Exception as e:
        logger.error(f"Error re-seeding documentation embeddings: {e}")
        import traceback
        error_trace = traceback.format_exc()
        logger.error(f"Full error trace: {error_trace}")
        
        return {
            "success": False,
            "message": f"Failed to re-seed documentation embeddings: {str(e)}"
        }