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

# Configure logging - use a consistent logger name
logger = logging.getLogger("documentation")

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
    logger.info("Checking for existing documentation embeddings...")
    
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
                    # Check if documents already exist in Databricks directly
                    logger.info(f"Checking for existing documents in Databricks index: {index_name}")
                    
                    try:
                        # Create a temporary DatabricksVectorStorage instance to check stats
                        from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
                        
                        # Get endpoint name
                        endpoint_name = (db_config.get('document_endpoint_name') or db_config.get('endpoint_name') 
                                       if isinstance(db_config, dict) 
                                       else getattr(db_config, 'document_endpoint_name', None) or getattr(db_config, 'endpoint_name', None))
                        
                        # Create storage instance
                        try:
                            # First, check if the index exists and is ready without blocking
                            from databricks.vector_search.client import VectorSearchClient
                            
                            # Create client
                            client_kwargs = {}
                            if isinstance(db_config, dict):
                                client_kwargs['workspace_url'] = db_config.get('workspace_url')
                                if db_config.get('personal_access_token'):
                                    client_kwargs['personal_access_token'] = db_config.get('personal_access_token')
                            else:
                                client_kwargs['workspace_url'] = getattr(db_config, 'workspace_url', None)
                                if getattr(db_config, 'personal_access_token', None):
                                    client_kwargs['personal_access_token'] = getattr(db_config, 'personal_access_token', None)
                            
                            client = VectorSearchClient(**{k: v for k, v in client_kwargs.items() if v is not None})
                            
                            # Check index status without waiting
                            try:
                                index = client.get_index(endpoint_name=endpoint_name, index_name=index_name)
                                index_info = index.describe()
                                
                                # Check if index is ready
                                is_ready = False
                                if isinstance(index_info, dict):
                                    status = index_info.get('status', {})
                                    if isinstance(status, dict):
                                        is_ready = status.get('ready', False)
                                        detailed_state = status.get('detailed_state', '')
                                        if not is_ready and 'PROVISIONING' in detailed_state:
                                            logger.info(f"Index {index_name} is still provisioning (state: {detailed_state})")
                                            logger.info("Skipping documentation seeding until index is ready")
                                            return False, 0
                                    
                                if not is_ready:
                                    logger.info(f"Index {index_name} is not ready yet")
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
                            
                            # Only create DatabricksVectorStorage if index is ready
                            temp_storage = DatabricksVectorStorage(
                                endpoint_name=endpoint_name,
                                index_name=index_name,
                                crew_id="documentation",
                                memory_type="document",
                                embedding_dimension=1024,
                                workspace_url=(db_config.get('workspace_url') if isinstance(db_config, dict) 
                                             else getattr(db_config, 'workspace_url', None)),
                                personal_access_token=(db_config.get('personal_access_token') if isinstance(db_config, dict) 
                                                     else getattr(db_config, 'personal_access_token', None)),
                                service_principal_client_id=(db_config.get('service_principal_client_id') if isinstance(db_config, dict) 
                                                           else getattr(db_config, 'service_principal_client_id', None)),
                                service_principal_client_secret=(db_config.get('service_principal_client_secret') if isinstance(db_config, dict) 
                                                               else getattr(db_config, 'service_principal_client_secret', None))
                            )
                        except Exception as e:
                            logger.warning(f"Could not create Databricks storage: {e}")
                            logger.info("Will skip seeding for now")
                            return False, 0
                        
                        # Check if we have enough documents to skip seeding
                        logger.info(f"Checking for existing documents in Databricks index: {index_name}")
                        
                        # First, let's do a direct similarity search to see what we get
                        try:
                            test_vector = [1.0 / (1024 ** 0.5)] * 1024
                            direct_results = temp_storage.index.similarity_search(
                                query_vector=test_vector,
                                columns=["id"],
                                num_results=10
                            )
                            logger.info(f"Direct similarity search result keys: {list(direct_results.keys()) if direct_results else 'None'}")
                            if direct_results and 'result' in direct_results:
                                row_count = direct_results['result'].get('row_count', 0)
                                logger.info(f"Direct search found row_count: {row_count}")
                                if row_count > 100:
                                    logger.info(f"✅ Found {row_count} documents via direct search (threshold: 100)")
                                    logger.info(f"Skipping seeding - documentation already exists in Databricks")
                                    return True, row_count
                        except Exception as e:
                            error_str = str(e)
                            if "does not exist" in error_str:
                                logger.warning(f"Index does not exist: {e}")
                                logger.info("Index does not exist, cannot seed yet")
                                return False, 0
                            elif "not ready" in error_str:
                                logger.warning(f"Index is not ready: {e}")
                                logger.info("Index is not ready, will skip seeding for now")
                                return False, 0
                            else:
                                logger.warning(f"Direct search failed: {e}")
                        
                        # Fall back to count_documents method
                        doc_count = temp_storage.count_documents()
                        logger.info(f"count_documents returned: {doc_count}")
                        
                        # If we have more than 100 documents, assume seeding was already done
                        if doc_count > 100:
                            logger.info(f"Found {doc_count} existing documents in index (threshold: 100)")
                            logger.info(f"Skipping seeding - documentation already exists in Databricks")
                            return True, doc_count
                        else:
                            logger.info(f"Only {doc_count} documents found (threshold: 100), will proceed with seeding")
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

async def seed_documentation_embeddings() -> None:
    """Seed documentation embeddings using services only."""
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
                        
                        # Use service to create the record
                        await doc_embedding_service.create_documentation_embedding(doc_embedding_create)
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
    """Seed the documentation_embeddings table asynchronously."""
    logger.info("Starting documentation embeddings seeding...")
    
    try:
        # Check if documentation already exists using service
        exists, count = await check_existing_documentation()
        
        if exists:
            logger.info(f"Skipping documentation embeddings seeding: existing records found")
            return ("skipped", count)
        
        # Seed documentation embeddings using service
        await seed_documentation_embeddings()
        
        logger.info("Documentation embeddings seeding completed successfully!")
        return ("success", 0)
    except Exception as e:
        logger.error(f"Error seeding documentation embeddings: {str(e)}")
        logger.error(f"Full error details: {type(e).__name__}: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
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
    print("DEBUG: Documentation seed() function called")  # Direct print to ensure visibility
    logger.info("🌱 Running documentation embeddings seeder...")
    logger.warning("DOCUMENTATION SEEDER STARTING - This message should be visible")  # Use warning to ensure visibility
    
    # Add immediate logging to see what happens next
    logger.warning("ABOUT TO CHECK EXISTING DOCUMENTATION")
    
    # First check if we should skip seeding
    try:
        logger.warning("CALLING check_existing_documentation()")
        exists, count = await check_existing_documentation()
        logger.warning(f"CHECK RESULT: exists={exists}, count={count}")
        
        if exists:
            logger.info(f"⏭️ Documentation embeddings seeding skipped ({count} records already exist)")
            return True
    except Exception as e:
        logger.error(f"Error checking existing documentation: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Continue with seeding if check fails
    
    logger.warning("PROCEEDING TO SEED DOCUMENTATION")
    
    # Proceed with seeding
    try:
        await seed_documentation_embeddings()
        logger.info("✅ Documentation embeddings seeded successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Error in documentation seeder: {e}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        # Re-raise to make the error visible
        raise

if __name__ == "__main__":
    # This allows running this seeder directly
    import asyncio
    asyncio.run(seed())