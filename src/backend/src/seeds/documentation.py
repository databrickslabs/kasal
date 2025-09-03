"""
Seed the documentation_embeddings table with CrewAI concepts documentation.

This module downloads and processes documentation from the CrewAI website,
creates embeddings, and stores them in the database for use in providing
context to the LLM during crew generation.
"""
import logging
import requests
import os
from bs4 import BeautifulSoup
from typing import List, Dict, Any, Optional
from datetime import datetime
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.services.memory_backend_service import MemoryBackendService
from src.core.llm_manager import LLMManager
from src.core.unit_of_work import UnitOfWork

# Import OpenAI SDK at module level for mock_create_embedding
from openai import AsyncOpenAI, OpenAI

# Configure logging - use memory logger for documentation embeddings
from src.core.logger import LoggerManager
logger_manager = LoggerManager()
logger = logger_manager.databricks_vector_search

# Documentation URLs
DOCS_URLS = [
    "https://docs.crewai.com/concepts/tasks",
    "https://docs.crewai.com/concepts/agents",
    "https://docs.crewai.com/concepts/crews",
    "https://docs.crewai.com/concepts/tools",
    "https://docs.crewai.com/concepts/processes",
]

# Embedding model configuration
EMBEDDING_MODEL = "databricks-gte-large-en"

async def fetch_url(url: str) -> str:
    """Fetch content from a URL."""
    try:
        logger.info(f"Fetching content from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except Exception as e:
        logger.error(f"Error fetching {url}: {str(e)}")
        return ""

def extract_content(html_content: str) -> str:
    """Extract relevant text content from HTML."""
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract the main content - adjust selectors based on the site structure
        main_content = soup.select('main') or soup.select('.documentation-content') or soup.select('article')
        
        if main_content:
            # Extract text content and clean it up
            content = main_content[0].get_text(separator='\n', strip=True)
            return content
        else:
            # If we can't find main content, extract all text
            logger.warning("Could not find main content, extracting all text")
            return soup.get_text(separator='\n', strip=True)
    except Exception as e:
        logger.error(f"Error extracting content: {str(e)}")
        return ""

async def mock_create_embedding(text: str) -> List[float]:
    """Create a mock embedding when no API key is available.
    
    This generates a deterministic vector based on the hash of the text content
    to ensure consistency for the same input.
    """
    import hashlib
    import random
    
    # Create a deterministic seed from the text hash
    text_hash = hashlib.md5(text.encode()).hexdigest()
    seed = int(text_hash, 16) % (2**32)
    
    # Set the random seed for reproducibility
    random.seed(seed)
    
    # Generate a 1024-dimensional vector (same as Databricks GTE large embeddings)
    mock_embedding = [random.uniform(-0.1, 0.1) for _ in range(1024)]
    
    # Normalize the vector to unit length
    magnitude = sum(x**2 for x in mock_embedding) ** 0.5
    normalized_embedding = [x/magnitude for x in mock_embedding]
    
    logger.info("Generated mock embedding for testing purposes")
    return normalized_embedding

async def create_documentation_chunks(url: str) -> List[Dict[str, Any]]:
    """Create documentation chunks from a URL."""
    from langchain.text_splitter import RecursiveCharacterTextSplitter
    
    html_content = await fetch_url(url)
    if not html_content:
        logger.warning(f"No content retrieved from {url}")
        return []
    
    # Extract text content
    content = extract_content(html_content)
    if not content:
        logger.warning(f"No meaningful content extracted from {url}")
        return []
    
    # Get the page name from the URL for metadata
    page_name = url.split('/')[-1].capitalize()
    title = f"CrewAI {page_name} Documentation"
    
    # Split content into chunks
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
        separators=["\n\n", "\n", ".", " ", ""]
    )
    chunks = text_splitter.split_text(content)
    logger.info(f"Split content into {len(chunks)} chunks for {title}")
    
    # Create result list
    result = []
    for i, chunk in enumerate(chunks):
        chunk_data = {
            "source": url,
            "title": f"{title} - Part {i+1}",
            "content": chunk,
            "chunk_index": i,
            "total_chunks": len(chunks)
        }
        result.append(chunk_data)
    
    return result


async def check_existing_documentation() -> tuple[bool, int]:
    """Check if documentation embeddings already exist.
    
    Returns:
        tuple: (exists: bool, count: int) - Whether records exist and how many
    """
    logger.info("📋 CHECKING FOR EXISTING DOCUMENTATION EMBEDDINGS...")
    
    try:
        from src.services.memory_backend_service import MemoryBackendService
        from src.schemas.memory_backend import MemoryBackendType
        
        async with UnitOfWork() as uow:
            memory_backend_service = MemoryBackendService(uow)
            doc_embedding_service = DocumentationEmbeddingService(uow)
            
            # Check if ANY group has Databricks configured
            # Documentation is global, so we use the latest created Databricks config
            all_backends = await uow.memory_backend_repository.get_all()
            
            # Filter active Databricks backends and sort by created_at descending
            databricks_backends = [
                b for b in all_backends 
                if b.is_active and b.backend_type == MemoryBackendType.DATABRICKS
            ]
            
            if databricks_backends:
                # Sort by created_at descending and take the first (most recent)
                databricks_backends.sort(key=lambda x: x.created_at, reverse=True)
                databricks_backend = databricks_backends[0]
                logger.info(f"Using latest Databricks config from group {databricks_backend.group_id} created at {databricks_backend.created_at}")
                
                # Get index name from config
                db_config = databricks_backend.databricks_config
                if isinstance(db_config, dict):
                    index_name = db_config.get('document_index')
                else:
                    index_name = getattr(db_config, 'document_index', None)
                
                if index_name:
                    # Check if documents already exist in Databricks using the service layer
                    logger.info(f"Checking for existing documents in Databricks index: {index_name}")
                    
                    try:
                        # Use the DatabricksIndexService to check index status (proper architecture pattern)
                        from src.services.databricks_index_service import DatabricksIndexService
                        
                        # Get endpoint name
                        endpoint_name = (db_config.get('document_endpoint_name') or db_config.get('endpoint_name') 
                                       if isinstance(db_config, dict) 
                                       else getattr(db_config, 'document_endpoint_name', None) or getattr(db_config, 'endpoint_name', None))
                        
                        # Get workspace URL
                        workspace_url = (db_config.get('workspace_url') if isinstance(db_config, dict) 
                                       else getattr(db_config, 'workspace_url', None))
                        
                        if not workspace_url:
                            logger.error("No workspace URL configured for Databricks")
                            return False, 0
                        
                        # Create service instance
                        databricks_index_service = DatabricksIndexService(uow)
                        
                        # Check index status using the service layer
                        try:
                            # Use the service's get_index_info method
                            # Pass None for user_token since we're in a seeder context
                            if not endpoint_name:
                                # For direct access indexes, endpoint_name might be in the index data
                                endpoint_name = ""
                            
                            index_info = await databricks_index_service.get_index_info(
                                workspace_url=workspace_url,
                                index_name=index_name,
                                endpoint_name=endpoint_name,
                                user_token=None
                            )
                            
                            # Check if request was successful
                            if not index_info or not index_info.get('success', False):
                                error_msg = index_info.get('message', 'Failed to get index info') if index_info else 'Failed to get index info'
                                logger.error(f"Failed to get index info for {index_name}: {error_msg}")
                                return False, 0
                            
                            # Check if index exists 
                            state = index_info.get('state', 'UNKNOWN')
                            if state == 'NOT_FOUND':
                                logger.info(f"Index {index_name} does not exist yet")
                                return False, 0
                            
                            # Check if index is ready
                            is_ready = index_info.get('ready', False)
                            
                            if not is_ready and state == "PROVISIONING":
                                logger.info(f"Index {index_name} is still provisioning (state: {state})")
                                logger.info("Skipping documentation seeding until index is ready")
                                return False, 0
                            
                            if not is_ready:
                                logger.info(f"Index {index_name} is not ready yet (state: {state})")
                                return False, 0
                            
                        except Exception as e:
                            error_str = str(e)
                            if "does not exist" in error_str or "not found" in error_str:
                                logger.info(f"Index {index_name} does not exist yet")
                                return False, 0
                            elif "not ready" in error_str:
                                logger.info(f"Index {index_name} is not ready yet")
                                return False, 0
                            else:
                                logger.warning(f"Error checking index status: {e}")
                                return False, 0
                        
                        # Check if we have enough documents to skip seeding
                        logger.info(f"Checking for existing documents in Databricks index: {index_name}")
                        
                        # Use the service to check document count (following proper architecture)
                        try:
                            # The index info contains the document count in 'doc_count' field
                            total_docs = index_info.get('doc_count', 0) or index_info.get('indexed_row_count', 0) or index_info.get('row_count', 0)
                            
                            logger.info(f"Index has {total_docs} total documents")
                            
                            if total_docs > 100:
                                logger.info(f"✅ Found {total_docs} documents in index (threshold: 100)")
                                logger.info(f"Skipping seeding - documentation already exists in Databricks")
                                return True, total_docs
                            else:
                                logger.info(f"Only {total_docs} documents found (threshold: 100), will proceed with seeding")
                                return False, 0
                                
                        except Exception as e:
                            logger.warning(f"Failed to check document count: {e}")
                            logger.info("Will proceed with seeding")
                            return False, 0
                            
                    except Exception as e:
                        logger.warning(f"Could not check Databricks index: {e}")
                        logger.info("Proceeding with seeding")
                        return False, 0
                else:
                    logger.warning("No document index configured, will proceed with seeding")
                    return False, 0
            else:
                # Using database backend - check count via service
                logger.info("Database backend is configured for documentation storage")
                
                # Get count of existing embeddings
                embeddings = await doc_embedding_service.get_documentation_embeddings(skip=0, limit=1)
                
                # To get actual count, we need to check if there are any embeddings
                # If there are embeddings, we consider it as already seeded
                exists = len(embeddings) > 0
                count = len(embeddings)  # This is just for the first record check
                
                if exists:
                    logger.info(f"Documentation embeddings already exist in the database")
                else:
                    logger.info("No existing documentation embeddings found in database")
                    
                return exists, count
                    
    except Exception as e:
        logger.error(f"Error checking existing documentation: {e}")
        raise

async def seed_documentation_embeddings(user_token: Optional[str] = None) -> None:
    """Seed documentation embeddings using services only.
    
    Args:
        user_token: Optional user access token for OBO authentication with Databricks
    """
    logger.info("Starting documentation embeddings seeding...")
    
    # Check which backend is configured
    using_databricks = False
    databricks_backend = None
    index_name = None
    
    try:
        from src.services.memory_backend_service import MemoryBackendService
        from src.schemas.memory_backend import MemoryBackendType
        
        async with UnitOfWork() as uow:
            memory_backend_service = MemoryBackendService(uow)
            
            # Get all backends to find the latest Databricks config
            all_backends = await uow.memory_backend_repository.get_all()
            
            # Filter active Databricks backends and sort by created_at descending
            databricks_backends = [
                b for b in all_backends 
                if b.is_active and b.backend_type == MemoryBackendType.DATABRICKS
            ]
            
            if databricks_backends:
                # Sort by created_at descending and take the first (most recent)
                databricks_backends.sort(key=lambda x: x.created_at, reverse=True)
                databricks_backend = databricks_backends[0]
                using_databricks = True
                
                # Get index name for marker file
                db_config = databricks_backend.databricks_config
                if isinstance(db_config, dict):
                    index_name = db_config.get('document_index')
                else:
                    index_name = getattr(db_config, 'document_index', None)
                
                logger.info(f"🚀 Using latest Databricks Vector Search for documentation storage (from group: {databricks_backend.group_id}, created: {databricks_backend.created_at})")
                if index_name:
                    logger.info(f"📍 Target index: {index_name}")
            else:
                logger.info("📊 Using local database for documentation storage")
    except Exception as e:
        logger.warning(f"Could not check backend configuration: {e}")
        logger.info("📊 Defaulting to local database for documentation storage")
    
    # Check if we can create embeddings - fail fast if not configured
    embedding_available = False
    use_mock_embeddings = False
    try:
        embedder_config = {
            'provider': 'databricks',
            'config': {'model': EMBEDDING_MODEL}
        }
        test_embedding = await LLMManager.get_embedding(
            text="test",
            model=EMBEDDING_MODEL,
            embedder_config=embedder_config
        )
        if test_embedding:
            embedding_available = True
            logger.info("✅ Embedding service is available and configured")
    except Exception as e:
        logger.warning(f"⚠️ Embedding service not available: {str(e)}")
        logger.warning("Will use mock embeddings for all documentation.")
        logger.warning("To enable real embeddings, configure Databricks in the frontend settings.")
        use_mock_embeddings = True
    
    # Process each documentation URL
    total_chunks_processed = 0
    
    # Create a UnitOfWork and DocumentationEmbeddingService for saving embeddings
    async with UnitOfWork() as uow:
        doc_embedding_service = DocumentationEmbeddingService(uow)
        logger.info("Created DocumentationEmbeddingService with UnitOfWork")
        
        # If using Databricks, validate index readiness before processing any embeddings
        if using_databricks and databricks_backend:
            logger.info("Pre-validating Databricks index readiness before creating embeddings...")
            
            try:
                # Get the index configuration
                db_config = databricks_backend.databricks_config
                if isinstance(db_config, dict):
                    document_index = db_config.get('document_index')
                    workspace_url = db_config.get('workspace_url')
                    document_endpoint_name = db_config.get('document_endpoint_name')
                    endpoint_name = document_endpoint_name or db_config.get('endpoint_name')
                else:
                    document_index = getattr(db_config, 'document_index', None)
                    workspace_url = getattr(db_config, 'workspace_url', None)
                    document_endpoint_name = getattr(db_config, 'document_endpoint_name', None)
                    endpoint_name = document_endpoint_name or getattr(db_config, 'endpoint_name', None)
                
                if document_index and workspace_url and endpoint_name:
                    # Use DatabricksIndexService to check readiness with retries
                    from src.services.databricks_index_service import DatabricksIndexService
                    index_service = DatabricksIndexService(workspace_url)
                    
                    logger.info(f"Checking if Databricks index {document_index} is ready before embedding creation...")
                    
                    # Wait for index to be ready (longer timeout for seeding process)
                    readiness_result = await index_service.wait_for_index_ready(
                        workspace_url=workspace_url,
                        index_name=document_index,
                        endpoint_name=endpoint_name,
                        max_wait_seconds=120,  # Wait up to 2 minutes for seeding
                        check_interval_seconds=10,  # Check every 10 seconds
                        user_token=None
                    )
                    
                    if not readiness_result.get("ready"):
                        message = readiness_result.get("message", "Index not ready")
                        attempts = readiness_result.get("attempts", 0)
                        elapsed_time = readiness_result.get("elapsed_time", 0)
                        
                        logger.warning(f"❌ Databricks index {document_index} not ready after {attempts} attempts ({elapsed_time:.1f}s): {message}")
                        logger.warning("Skipping documentation seeding - index must be ready before creating embeddings")
                        logger.info("💡 Please wait for the index to be ready and run the seeding again")
                        return  # Exit early without processing
                    
                    logger.info(f"✅ Databricks index {document_index} is ready after {readiness_result.get('attempts', 0)} attempts ({readiness_result.get('elapsed_time', 0):.1f}s)")
                    logger.info("Proceeding with embedding creation and seeding...")
                else:
                    logger.warning("Missing required Databricks configuration for index validation")
                    
            except Exception as e:
                logger.error(f"Error validating Databricks index readiness: {e}")
                logger.warning("Proceeding with seeding, but embeddings may fail if index is not ready")
        
        for url in DOCS_URLS:
            try:
                # Create documentation chunks
                chunks = await create_documentation_chunks(url)
                logger.info(f"Created {len(chunks)} chunks for {url}")
                
                # Create embedding for each chunk and store in database
                for chunk in chunks:
                    try:
                        # Create embedding - use real if available, otherwise mock
                        if use_mock_embeddings or not embedding_available:
                            embedding = await mock_create_embedding(chunk["content"])
                        else:
                            try:
                                embedder_config = {
                                    'provider': 'databricks',
                                    'config': {'model': EMBEDDING_MODEL}
                                }
                                embedding = await LLMManager.get_embedding(
                                    text=chunk["content"],
                                    model=EMBEDDING_MODEL,
                                    embedder_config=embedder_config
                                )
                            except Exception as e:
                                # If embedding fails after initial test passed, use mock for this chunk
                                logger.debug(f"Embedding failed for chunk, using mock: {str(e)}")
                                embedding = await mock_create_embedding(chunk["content"])
                        
                        # Create schema for database record
                        doc_embedding_create = DocumentationEmbeddingCreate(
                            source=chunk["source"],
                            title=chunk["title"],
                            content=chunk["content"],
                            embedding=embedding,
                            doc_metadata={
                                "page_name": chunk["source"].split('/')[-1].capitalize(),
                                "chunk_index": chunk["chunk_index"],
                                "total_chunks": chunk["total_chunks"]
                            }
                        )
                        
                        # Use service to create the record, pass user_token for authentication
                        await doc_embedding_service.create_documentation_embedding(doc_embedding_create, user_token=user_token)
                        total_chunks_processed += 1
                        
                    except Exception as e:
                        logger.error(f"Error processing chunk: {str(e)}")
                        # Continue with other chunks
                
            except Exception as e:
                logger.error(f"Error processing URL {url}: {str(e)}")
                # Continue with other URLs
        
        logger.info(f"Completed seeding documentation embeddings: {total_chunks_processed} chunks processed")
        
        if using_databricks and total_chunks_processed > 0:
            logger.info(f"Successfully seeded {total_chunks_processed} documentation chunks to Databricks")
            logger.info("Future runs will detect these documents and skip re-seeding")

async def seed_async():
    """Seed the documentation_embeddings table asynchronously - DEPRECATED, use seed() instead."""
    logger.info("⚠️ DEPRECATED: seed_async() called, redirecting to seed()")
    # Redirect to the new seed() function which has proper checks
    result = await seed()
    # Convert boolean result to tuple for backward compatibility
    if result:
        # Check if documents exist after seeding
        exists, count = await check_existing_documentation()
        if exists:
            return ("skipped" if count > 100 else "success", count)
    return ("error", 0)

def seed_sync():
    """Seed the documentation_embeddings table synchronously."""
    import asyncio
    
    logger.info("Running documentation embeddings seeder in sync mode...")
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    result = loop.run_until_complete(seed_async())
    return result

async def seed():
    """Main seeding function that will be called by the seeder runner."""
    logger.info("🌱 Starting documentation embeddings seeder...")
    
    # EARLY EXIT: Check if we should skip seeding before doing any heavy work
    try:
        logger.info("Checking if documentation embeddings already exist...")
        exists, count = await check_existing_documentation()
        logger.info(f"Documentation check result: exists={exists}, count={count}")
        
        if exists:
            logger.info(f"⏭️ Documentation embeddings seeding skipped ({count} records already exist)")
            logger.info(f"Documentation seeder completed in < 1 second (skipped - already seeded)")
            return True
    except Exception as e:
        logger.warning(f"Could not check existing documentation: {e}")
        # If we can't check, assume we need to seed
        logger.info("Will proceed with seeding since check failed")
    
    # Only proceed with heavy seeding if necessary
    logger.info("Documentation needs seeding, proceeding...")
    
    try:
        # This is the slow part - only run if actually needed
        await seed_documentation_embeddings()
        logger.info("✅ Documentation embeddings seeded successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Error in documentation seeder: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Don't re-raise in production to avoid crashing the app
        return False

if __name__ == "__main__":
    # This allows running this seeder directly
    import asyncio
    asyncio.run(seed())