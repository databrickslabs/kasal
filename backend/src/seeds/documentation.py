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
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.session import async_session_factory
from src.models.documentation_embedding import DocumentationEmbedding
from src.schemas.documentation_embedding import DocumentationEmbeddingCreate
from src.services.documentation_embedding_service import DocumentationEmbeddingService
from src.core.llm_manager import LLMManager

# Import OpenAI SDK at module level for mock_create_embedding
from openai import AsyncOpenAI, OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Documentation URLs
DOCS_URLS = [
    "https://docs.crewai.com/concepts/tasks",
    "https://docs.crewai.com/concepts/agents",
    "https://docs.crewai.com/concepts/crews",
    "https://docs.crewai.com/concepts/tools",
    "https://docs.crewai.com/concepts/processes",
]

# Embedding model configuration
EMBEDDING_MODEL = "text-embedding-ada-002"

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
    
    # Generate a 1536-dimensional vector (same as OpenAI embeddings)
    mock_embedding = [random.uniform(-0.1, 0.1) for _ in range(1536)]
    
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

async def setup_pgvector_extension(session: AsyncSession) -> None:
    """Setup pgvector extension in PostgreSQL if it doesn't exist."""
    try:
        # Check if pgvector extension exists
        query = text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
        result = await session.execute(query)
        extension_exists = result.scalar_one_or_none()
        
        if not extension_exists:
            logger.info("Creating pgvector extension...")
            # Create the extension
            await session.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
            await session.commit()
            logger.info("pgvector extension created successfully!")
        else:
            logger.info("pgvector extension already exists")
    except Exception as e:
        logger.error(f"Error setting up pgvector extension: {str(e)}")
        await session.rollback()
        raise

async def clear_existing_documentation(session: AsyncSession) -> None:
    """Clear existing documentation embeddings."""
    try:
        # Delete all rows from documentation_embeddings table
        query = text("DELETE FROM documentation_embeddings")
        await session.execute(query)
        await session.commit()
        logger.info("Cleared existing documentation embeddings")
    except Exception as e:
        logger.error(f"Error clearing existing documentation: {str(e)}")
        await session.rollback()
        raise

async def check_existing_documentation(session: AsyncSession) -> tuple[bool, int]:
    """Check if documentation embeddings already exist in the database.
    
    Returns:
        tuple: (exists: bool, count: int) - Whether records exist and how many
    """
    try:
        # Count rows in documentation_embeddings table
        count_query = text("SELECT COUNT(*) FROM documentation_embeddings")
        result = await session.execute(count_query)
        count = result.scalar_one_or_none() or 0
        
        exists = count > 0
        
        if exists:
            logger.info(f"Documentation embeddings already exist in the database: {count} records found")
        else:
            logger.info("No existing documentation embeddings found")
            
        return exists, count
    except Exception as e:
        logger.error(f"Error checking existing documentation: {str(e)}")
        return False, 0

async def seed_documentation_embeddings(session: AsyncSession) -> None:
    """Seed documentation embeddings into the database."""
    # Setup pgvector extension
    await setup_pgvector_extension(session)
    
    # Clear any partial documentation before seeding
    await clear_existing_documentation(session)
    
    # Initialize service
    doc_embedding_service = DocumentationEmbeddingService()
    
    # Process each documentation URL
    total_chunks_processed = 0
    
    for url in DOCS_URLS:
        try:
            # Create documentation chunks
            chunks = await create_documentation_chunks(url)
            logger.info(f"Created {len(chunks)} chunks for {url}")
            
            # Create embedding for each chunk and store in database
            for chunk in chunks:
                try:
                    # Create embedding using LLMManager
                    try:
                        embedding = await LLMManager.get_embedding(
                            text=chunk["content"],
                            model=EMBEDDING_MODEL
                        )
                    except Exception as e:
                        logger.warning(f"Error with LLMManager.get_embedding: {str(e)}. Using mock embeddings instead.")
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
                    await doc_embedding_service.create_documentation_embedding(doc_embedding_create, db=session)
                    total_chunks_processed += 1
                    
                except Exception as e:
                    logger.error(f"Error processing chunk: {str(e)}")
                    # Continue with other chunks
            
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}")
            # Continue with other URLs
    
    logger.info(f"Completed seeding documentation embeddings: {total_chunks_processed} chunks processed")

async def seed_async():
    """Seed the documentation_embeddings table asynchronously."""
    logger.info("Starting documentation embeddings seeding...")
    
    try:
        # Create async session
        async with async_session_factory() as session:
            # Check if documentation already exists
            exists, count = await check_existing_documentation(session)
            if exists:
                logger.info(f"Skipping documentation embeddings seeding: {count} existing records found in the database")
                return ("skipped", count)
                
            # Seed documentation embeddings
            await seed_documentation_embeddings(session)
            
        logger.info("Documentation embeddings seeding completed successfully!")
        return ("success", 0)
    except Exception as e:
        logger.error(f"Error seeding documentation embeddings: {str(e)}")
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
    logger.info("🌱 Running documentation embeddings seeder...")
    result, count = await seed_async()
    
    if result == "success":
        logger.info("✅ Documentation embeddings seeded successfully")
        return True
    elif result == "skipped":
        logger.info(f"⏭️ Documentation embeddings seeding skipped ({count} records already exist)")
        return True
    else:
        logger.error("❌ Failed to seed documentation embeddings")
        return False

if __name__ == "__main__":
    # This allows running this seeder directly
    import asyncio
    asyncio.run(seed())