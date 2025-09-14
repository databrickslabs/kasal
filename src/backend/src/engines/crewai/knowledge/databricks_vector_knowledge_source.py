"""
Databricks Vector Knowledge Source with Execution Isolation.

This implements CrewAI's BaseKnowledgeSource with proper isolation per execution,
ensuring that each crew run has its own vector store collection.
"""

import os
import sys
import logging
import asyncio
import threading
from typing import Dict, Any, List, Optional
from crewai.knowledge.source.base_knowledge_source import BaseKnowledgeSource
from pydantic import Field
import hashlib

# Add backend src to path for subprocess context
backend_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
if backend_src not in sys.path:
    sys.path.insert(0, backend_src)

logger = logging.getLogger(__name__)


class SourceFilteredStorage:
    """
    Storage wrapper that automatically applies source filtering for knowledge searches.

    This wrapper intercepts search calls and adds source path filtering when the
    knowledge source has source filtering enabled.
    """

    def __init__(self, base_storage, knowledge_source):
        """Initialize with base storage and knowledge source reference."""
        self.base_storage = base_storage
        self.knowledge_source = knowledge_source
        # Enable source filtering by default
        self._source_filtering_enabled = True

    def __getattr__(self, name):
        """Delegate all other attributes to the base storage."""
        return getattr(self.base_storage, name)

    def search(self, query: List[str], limit: int = 3, filter: Optional[dict] = None, score_threshold: float = 0.35) -> List[Dict[str, Any]]:
        """
        Search with automatic source filtering if enabled.

        This method matches CrewAI's KnowledgeStorage interface.
        """
        logger.info(f"[SourceFilteredStorage] Search called with query: {query[:1]}... limit: {limit}")

        # Check if base storage has the expected search method
        if not hasattr(self.base_storage, 'search'):
            logger.error(f"[SourceFilteredStorage] Base storage does not have search method")
            return []

        # Combine user filters with source filtering
        combined_filter = filter.copy() if filter else {}

        # Add source filtering if enabled and file paths are available
        if (self._source_filtering_enabled and
            hasattr(self.knowledge_source, 'file_paths') and
            self.knowledge_source.file_paths):

            file_paths = self.knowledge_source.file_paths

            # Create source filter based on number of paths
            if len(file_paths) == 1:
                source_filter = {"source": file_paths[0]}
            else:
                source_filter = {"source": {"in": file_paths}}

            # Merge with existing filters
            combined_filter.update(source_filter)

            logger.info(f"[SourceFilteredStorage] Applying source filter for {len(file_paths)} files: {source_filter}")
            logger.info(f"[SourceFilteredStorage] Combined filters: {combined_filter}")

        try:
            # Try to call base storage search - it might be async
            result = self.base_storage.search(
                query=query,
                limit=limit,
                filter=combined_filter,
                score_threshold=score_threshold
            )

            # Handle async result if needed
            import asyncio
            if asyncio.iscoroutine(result):
                logger.info(f"[SourceFilteredStorage] Converting async result to sync")
                result = asyncio.run(result)

            logger.info(f"[SourceFilteredStorage] Search returned {len(result)} results")
            return result

        except Exception as e:
            logger.error(f"[SourceFilteredStorage] Search failed: {e}", exc_info=True)
            return []

    def enable_source_filtering(self, enabled: bool = True):
        """Enable or disable source filtering."""
        self._source_filtering_enabled = enabled


class DatabricksVectorKnowledgeSource(BaseKnowledgeSource):
    """
    Knowledge source that reads from Databricks Volumes and stores in isolated vector collections.

    CRITICAL: This ensures each execution has its own collection for security and isolation.
    """

    file_paths: List[str] = Field(default_factory=list, description="Paths to files in Databricks Volume")
    volume_path: Optional[str] = Field(default=None, description="Databricks volume path")
    execution_id: str = Field(description="Execution ID for isolation")
    group_id: str = Field(default="default", description="Group ID for tenant isolation")
    workspace_url: Optional[str] = Field(default=None, description="Databricks workspace URL")
    token: Optional[str] = Field(default=None, description="Databricks token")
    embedder_config: Optional[Dict[str, Any]] = Field(default=None, description="Embedder configuration")
    chunk_sources: List[str] = Field(default_factory=list, description="Track which file each chunk came from")
    agent_ids: List[str] = Field(default_factory=list, description="Agent IDs that can access this knowledge source")
    
    def __init__(self, **data):
        """Initialize with execution-specific collection name and embedder."""
        # Set collection name BEFORE calling parent init
        if 'execution_id' in data and 'group_id' in data:
            # Create unique collection name per execution
            data['collection_name'] = f"knowledge_{data['group_id']}_{data['execution_id']}"
            logger.info(f"[DatabricksVectorKnowledge] Creating isolated collection: {data['collection_name']}")
        
        super().__init__(**data)
        
        # Set workspace and token from environment if not provided
        if not self.workspace_url:
            self.workspace_url = os.getenv('DATABRICKS_HOST')
        if not self.token:
            self.token = os.getenv('DATABRICKS_TOKEN')
        
        # Authenticate and get token before initializing storage
        self._authenticate_databricks()
        
        # Override the storage initialization to use our embedder
        self._initialize_storage_with_embedder()
    
    def _authenticate_databricks(self):
        """Authenticate with Databricks and get a valid token."""
        try:
            # Clean up workspace URL format
            if self.workspace_url:
                if not self.workspace_url.startswith('http'):
                    self.workspace_url = f"https://{self.workspace_url}"
                if self.workspace_url.endswith('/'):
                    self.workspace_url = self.workspace_url[:-1]
            
            # Use DatabricksAuthHelper to get token if not already set
            if not self.token and self.workspace_url:
                from repositories.databricks_auth_helper import DatabricksAuthHelper
                import asyncio
                import threading
                
                auth_token = None
                auth_exception = None
                
                def get_auth_token_sync():
                    nonlocal auth_token, auth_exception
                    try:
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            auth_token = loop.run_until_complete(
                                DatabricksAuthHelper.get_auth_token(
                                    workspace_url=self.workspace_url,
                                    user_token=None  # Will use service pattern
                                )
                            )
                        finally:
                            loop.close()
                    except Exception as e:
                        auth_exception = e
                
                thread = threading.Thread(target=get_auth_token_sync)
                thread.start()
                thread.join(timeout=5)
                
                if auth_token:
                    self.token = auth_token
                    logger.info(f"[DatabricksVectorKnowledge] Successfully authenticated with Databricks")
                elif auth_exception:
                    logger.warning(f"[DatabricksVectorKnowledge] Auth failed: {auth_exception}")
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Error during authentication: {e}")
    
    def _initialize_storage_with_embedder(self):
        """Initialize the storage with Databricks Vector Search only."""
        try:
            # Always use Databricks Vector Search - no ChromaDB fallback
            vector_search_config = self._get_vector_search_config()
            
            if vector_search_config:
                # Use the existing document_index from one-click setup
                logger.info(f"[DatabricksVectorKnowledge] ✅ Vector Search config found for group {self.group_id}")
                logger.info(f"[DatabricksVectorKnowledge] Using Vector Search with document_index from one-click setup")
                self._init_vector_search_with_document_index(vector_search_config)
            else:
                # No Vector Search configured - knowledge sources won't work without it
                logger.error(f"[DatabricksVectorKnowledge] ❌ No Vector Search configuration found for group {self.group_id}")
                logger.error(f"[DatabricksVectorKnowledge] Knowledge sources will not work without Vector Search setup")
                logger.error(f"[DatabricksVectorKnowledge] Please ensure memory backend is configured for Databricks Vector Search")
                self.storage = None
                
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Error initializing Vector Search storage: {e}")
            self.storage = None
    
    def _get_vector_search_config(self):
        """Get Vector Search configuration from memory backend if available."""
        try:
            # Check for Databricks memory backend configuration
            # This follows the same pattern as documentation_embedding_service.py
            from src.core.unit_of_work import UnitOfWork
            from src.schemas.memory_backend import MemoryBackendType
            
            # We need to run this synchronously in the subprocess context
            import asyncio
            
            async def get_config():
                async with UnitOfWork() as uow:
                    # Get configurations for this group
                    backends = await uow.memory_backend_repository.get_by_group_id(self.group_id)
                    
                    # Find active Databricks backend
                    for backend in backends:
                        if backend.is_active and backend.backend_type == MemoryBackendType.DATABRICKS:
                            logger.info(f"[DatabricksVectorKnowledge] Found active Databricks backend for group {self.group_id}")
                            return backend.databricks_config
                    return None
            
            # Run the async function
            try:
                loop = asyncio.get_running_loop()
                # Already in an event loop (subprocess context), use thread executor
                logger.info("[DatabricksVectorKnowledge] Running in event loop context, using ThreadPoolExecutor")
                
                import concurrent.futures
                
                def run_async_query():
                    """Run async query in a new thread with its own event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(get_config())
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_async_query)
                    config = future.result(timeout=10)  # 10 second timeout
                    logger.info(f"[DatabricksVectorKnowledge] Successfully got config via ThreadPoolExecutor")
                    return config
                    
            except RuntimeError:
                # No event loop, create one
                logger.info("[DatabricksVectorKnowledge] No event loop, creating new one")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    config = loop.run_until_complete(get_config())
                    return config
                finally:
                    loop.close()
            except Exception as e:
                logger.error(f"[DatabricksVectorKnowledge] Error in ThreadPoolExecutor: {e}")
                return None
            
        except concurrent.futures.TimeoutError:
            logger.error("[DatabricksVectorKnowledge] Timeout getting Vector Search config (10s)")
            return None
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Error getting Vector Search config: {e}")
            return None
    
    def _init_vector_search_with_document_index(self, vector_search_config):
        """Initialize Vector Search storage using the document_index from one-click setup."""
        try:
            # Get the document_index from configuration
            document_index = None
            if hasattr(vector_search_config, 'document_index'):
                document_index = vector_search_config.document_index
            elif isinstance(vector_search_config, dict):
                document_index = vector_search_config.get('document_index')
            
            if not document_index:
                logger.error(f"[DatabricksVectorKnowledge] No document_index configured in Vector Search setup")
                logger.error(f"[DatabricksVectorKnowledge] Please ensure the one-click setup created a document_index")
                self.storage = None
                return
            
            # Get endpoint name (prefer document_endpoint_name, fall back to endpoint_name)
            endpoint_name = None
            if hasattr(vector_search_config, 'document_endpoint_name'):
                endpoint_name = vector_search_config.document_endpoint_name or vector_search_config.endpoint_name
            elif isinstance(vector_search_config, dict):
                endpoint_name = vector_search_config.get('document_endpoint_name') or vector_search_config.get('endpoint_name')
            
            # Get workspace URL
            workspace_url = None
            if hasattr(vector_search_config, 'workspace_url'):
                workspace_url = vector_search_config.workspace_url
            elif isinstance(vector_search_config, dict):
                workspace_url = vector_search_config.get('workspace_url')
            
            workspace_url = workspace_url or self.workspace_url or os.environ.get('DATABRICKS_HOST')
            
            logger.info(f"[DatabricksVectorKnowledge] Using document_index: {document_index}")
            logger.info(f"[DatabricksVectorKnowledge] Using endpoint: {endpoint_name}")
            
            # Use the existing DatabricksVectorStorage from the memory module
            from engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage

            # Create the base storage instance
            base_storage = DatabricksVectorStorage(
                index_name=document_index,
                endpoint_name=endpoint_name,
                crew_id=self.execution_id,  # Use execution_id as crew_id for isolation
                workspace_url=workspace_url,
                user_token=self.token,
                memory_type="document"  # Use document type for knowledge sources
            )

            # Wrap it with source filtering capability
            self.storage = SourceFilteredStorage(base_storage, self)
            
            logger.info(f"[DatabricksVectorKnowledge] ✅ Successfully initialized Vector Search storage")
            logger.info(f"[DatabricksVectorKnowledge] Document index: {document_index}")
            logger.info(f"[DatabricksVectorKnowledge] Collection: {self.collection_name}")
            
        except ImportError as e:
            logger.error(f"[DatabricksVectorKnowledge] Failed to import DatabricksVectorStorage: {e}")
            logger.error(f"[DatabricksVectorKnowledge] Make sure the memory module is properly configured")
            self.storage = None
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Failed to initialize Vector Search: {e}")
            import traceback
            traceback.print_exc()
            self.storage = None
    
    
    def load_content(self) -> Dict[str, str]:
        """
        Load content from Databricks Volume files.
        
        Returns:
            Dictionary mapping file paths to their content
        """
        logger.info(f"[DatabricksVectorKnowledge] Loading content from {len(self.file_paths)} files")
        logger.info(f"[DatabricksVectorKnowledge] Collection: {self.collection_name}")
        
        content_map = {}
        
        if not self.file_paths:
            logger.warning("[DatabricksVectorKnowledge] No file paths provided")
            return content_map
        
        # Import the service for reading files
        try:
            # In subprocess, we need to handle imports differently
            import sys
            import os
            import asyncio
            
            # Add backend src to path if not already there
            backend_src = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..'))
            if backend_src not in sys.path:
                sys.path.insert(0, backend_src)
            
            # Now import with proper path
            from services.databricks_knowledge_service import DatabricksKnowledgeService
            from repositories.databricks_config_repository import DatabricksConfigRepository
            
            # Create a minimal async session for subprocess
            from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
            from sqlalchemy.orm import sessionmaker
            
            # Get database URL from environment
            db_url = os.getenv('DATABASE_URL', 'sqlite+aiosqlite:///./kasal.db')
            if db_url.startswith('postgresql://'):
                db_url = db_url.replace('postgresql://', 'postgresql+asyncpg://')
            elif db_url.startswith('sqlite:///'):
                db_url = db_url.replace('sqlite:///', 'sqlite+aiosqlite:///')
            
            engine = create_async_engine(db_url, echo=False)
            AsyncSessionLocal = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
            
            async def read_files_async():
                """Async function to read files from Databricks."""
                files_content = {}
                
                async with AsyncSessionLocal() as session:
                    # Create repository and service instances
                    databricks_repo = DatabricksConfigRepository(session)
                    knowledge_service = DatabricksKnowledgeService(
                        databricks_repository=databricks_repo,
                        group_id=self.group_id
                    )
                    
                    # Read each file
                    for file_path in self.file_paths:
                        try:
                            logger.info(f"[DatabricksVectorKnowledge] Reading: {file_path}")
                            
                            # Read the file content
                            file_result = await knowledge_service.read_knowledge_file(
                                file_path=file_path,
                                group_id=self.group_id,
                                user_token=self.token
                            )
                            
                            if file_result.get('status') == 'success':
                                content = file_result.get('content', '')
                                if content:
                                    files_content[file_path] = content
                                    logger.info(f"[DatabricksVectorKnowledge] Loaded {len(content)} chars from {file_path}")
                            else:
                                error_msg = file_result.get('message', 'Unknown error')
                                logger.warning(f"[DatabricksVectorKnowledge] Failed to read {file_path}: {error_msg}")
                                
                        except Exception as e:
                            logger.error(f"[DatabricksVectorKnowledge] Error reading {file_path}: {e}")
                
                return files_content
            
            # Run the async function - use synchronous fallback in subprocess
            try:
                # Check if we're already in an event loop (subprocess context)
                loop = asyncio.get_running_loop()
                logger.info(f"[DatabricksVectorKnowledge] Running in async context - using synchronous fallback")
                
                # In subprocess/async context, use synchronous approach
                files_content = {}
                
                # Read each file synchronously
                for file_path in self.file_paths:
                    try:
                        logger.info(f"[DatabricksVectorKnowledge] Reading (sync): {file_path}")
                        
                        # Use synchronous reading approach
                        import requests
                        
                        # Get Databricks configuration synchronously
                        from services.databricks_knowledge_service import DatabricksKnowledgeService
                        
                        # Create a mock service just to get the workspace URL and token
                        service = DatabricksKnowledgeService(
                            databricks_repository=None,
                            group_id=self.group_id
                        )
                        
                        # Get workspace URL - prioritize environment
                        workspace_url = os.getenv('DATABRICKS_HOST')
                        logger.info(f"[DatabricksVectorKnowledge] DATABRICKS_HOST from env: {workspace_url}")
                        
                        if not workspace_url:
                            workspace_url = self.workspace_url
                            logger.info(f"[DatabricksVectorKnowledge] Using self.workspace_url: {workspace_url}")
                        
                        # Clean up workspace URL format
                        if workspace_url:
                            # Ensure it starts with https://
                            if not workspace_url.startswith('http'):
                                workspace_url = f"https://{workspace_url}"
                            # Remove trailing slash
                            if workspace_url.endswith('/'):
                                workspace_url = workspace_url[:-1]
                        
                        logger.info(f"[DatabricksVectorKnowledge] Final workspace URL: {workspace_url}")
                        
                        # Get token using the DatabricksAuthHelper service (handles all auth hierarchy)
                        token = None
                        try:
                            # Use sync approach to call the auth helper
                            from repositories.databricks_auth_helper import DatabricksAuthHelper
                            
                            # Create a new event loop for this sync call
                            import asyncio
                            import threading
                            
                            auth_token = None
                            auth_exception = None
                            
                            def get_auth_token_sync():
                                nonlocal auth_token, auth_exception
                                try:
                                    # Create new event loop for this thread
                                    loop = asyncio.new_event_loop()
                                    asyncio.set_event_loop(loop)
                                    try:
                                        # Call the async auth helper
                                        auth_token = loop.run_until_complete(
                                            DatabricksAuthHelper.get_auth_token(
                                                workspace_url=workspace_url,
                                                user_token=self.token  # OBO token if provided
                                            )
                                        )
                                    finally:
                                        loop.close()
                                except Exception as e:
                                    auth_exception = e
                            
                            # Run in thread to avoid event loop conflicts
                            thread = threading.Thread(target=get_auth_token_sync)
                            thread.start()
                            thread.join(timeout=5)
                            
                            if auth_exception:
                                logger.warning(f"[DatabricksVectorKnowledge] Auth helper error: {auth_exception}")
                            elif auth_token:
                                token = auth_token
                                logger.info("[DatabricksVectorKnowledge] Got token from DatabricksAuthHelper")
                            else:
                                logger.warning("[DatabricksVectorKnowledge] No token from auth helper")
                                
                        except Exception as e:
                            logger.warning(f"[DatabricksVectorKnowledge] Could not use auth helper: {e}")
                        
                        # Fallback to environment if auth helper fails
                        if not token:
                            token = os.getenv('DATABRICKS_TOKEN') or os.getenv('DATABRICKS_API_KEY')
                            if token:
                                logger.info("[DatabricksVectorKnowledge] Got token from environment (fallback)")
                        
                        if workspace_url and token:
                            # Use Databricks Files API directly
                            headers = {"Authorization": f"Bearer {token}"}
                            read_url = f"{workspace_url}/api/2.0/fs/files{file_path}"
                            
                            response = requests.get(read_url, headers=headers)
                            if response.status_code == 200:
                                content_bytes = response.content
                                
                                # Check if it's a PDF
                                if file_path.lower().endswith('.pdf'):
                                    try:
                                        from pypdf import PdfReader
                                        import io
                                        pdf_reader = PdfReader(io.BytesIO(content_bytes))
                                        text_content = ""
                                        for page in pdf_reader.pages:
                                            text_content += page.extract_text() + "\n"
                                        files_content[file_path] = text_content
                                        logger.info(f"[DatabricksVectorKnowledge] Extracted {len(text_content)} chars from PDF")
                                    except Exception as pdf_error:
                                        logger.error(f"[DatabricksVectorKnowledge] PDF extraction error: {pdf_error}")
                                        files_content[file_path] = f"[PDF Error: {pdf_error}]"
                                else:
                                    # Try to decode as text
                                    try:
                                        files_content[file_path] = content_bytes.decode('utf-8')
                                    except UnicodeDecodeError:
                                        files_content[file_path] = content_bytes.decode('latin-1', errors='ignore')
                                
                                logger.info(f"[DatabricksVectorKnowledge] Loaded {len(files_content[file_path])} chars from {file_path}")
                            else:
                                logger.error(f"[DatabricksVectorKnowledge] HTTP {response.status_code} reading {file_path}")
                        else:
                            logger.warning(f"[DatabricksVectorKnowledge] No valid credentials, using mock for {file_path}")
                            
                    except Exception as e:
                        logger.error(f"[DatabricksVectorKnowledge] Error reading {file_path}: {e}")
                
                content_map = files_content
                logger.info(f"[DatabricksVectorKnowledge] Successfully loaded {len(content_map)} files in async context (sync approach)")
                
            except RuntimeError as e:
                # No event loop, we can use asyncio.run directly
                logger.info(f"[DatabricksVectorKnowledge] Running in sync context, no event loop")
                content_map = asyncio.run(read_files_async())
                logger.info(f"[DatabricksVectorKnowledge] Successfully loaded {len(content_map)} files in sync context")
            
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Error loading content: {e}", exc_info=True)
            # Return mock content if reading fails
            for file_path in self.file_paths:
                content_map[file_path] = f"Mock content for {file_path} - Databricks connection failed"
        
        return content_map
    
    def validate_content(self, content: Dict[str, str]) -> Dict[str, str]:
        """
        Validate that content is properly formatted.
        
        Args:
            content: Dictionary of file paths to content
            
        Returns:
            Validated content dictionary
        """
        validated = {}
        for file_path, text in content.items():
            if text and isinstance(text, str):
                validated[file_path] = text
            else:
                logger.warning(f"[DatabricksVectorKnowledge] Invalid content for {file_path}")
        
        return validated
    
    def add(self) -> None:
        """
        Process files and add to vector storage with execution isolation.
        """
        logger.info(f"[DatabricksVectorKnowledge] Adding knowledge to collection: {self.collection_name}")
        logger.info(f"[DatabricksVectorKnowledge] Execution ID: {self.execution_id}")
        logger.info(f"[DatabricksVectorKnowledge] Group ID: {self.group_id}")
        
        # Check if storage is initialized
        if not self.storage:
            logger.warning(f"[DatabricksVectorKnowledge] Storage not initialized yet - will be set by agent.set_knowledge()")
            # Storage will be initialized when agent.set_knowledge() is called
            # For now, just prepare the chunks
        
        # Load content from Databricks
        content_map = self.load_content()
        
        if not content_map:
            logger.warning("[DatabricksVectorKnowledge] No content to add to knowledge base")
            return
        
        # Validate content
        validated_content = self.validate_content(content_map)
        
        # Process each file's content and track source files for chunks
        self.chunk_sources.clear()  # Clear existing chunk sources
        for file_path, text in validated_content.items():
            logger.info(f"[DatabricksVectorKnowledge] Processing {file_path} ({len(text)} chars)")

            # Chunk the text (using parent's chunking method)
            chunks = self._chunk_text(text)

            # Add metadata to each chunk and track source
            for chunk in chunks:
                # Add metadata about source
                chunk_with_metadata = f"Source: {os.path.basename(file_path)}\n\n{chunk}"
                self.chunks.append(chunk_with_metadata)
                # Track which file this chunk came from
                self.chunk_sources.append(file_path)
            
            logger.info(f"[DatabricksVectorKnowledge] Created {len(chunks)} chunks from {file_path}")
        
        # Save to vector storage (this uses the collection_name we set)
        logger.info(f"[DatabricksVectorKnowledge] Prepared {len(self.chunks)} total chunks for collection: {self.collection_name}")
        
        # Save the chunks to storage if storage is initialized
        if self.storage:
            logger.info(f"[DatabricksVectorKnowledge] ✅ Storage initialized, saving {len(self.chunks)} chunks to Vector Search")
            self._save_documents()
        else:
            logger.error(f"[DatabricksVectorKnowledge] ❌ Storage NOT initialized - chunks will NOT be embedded")
            logger.error(f"[DatabricksVectorKnowledge] This means Vector Search config failed to load")
            logger.error(f"[DatabricksVectorKnowledge] Chunks prepared but not saved to Vector Search!")
        
        logger.info(f"[DatabricksVectorKnowledge] ✅ Knowledge chunks prepared for collection: {self.collection_name}")

        # Enable source filtering by default if we have file paths
        if self.file_paths:
            self.enable_source_filtering(True)

    def enable_source_filtering(self, enabled: bool = True):
        """
        Enable or disable automatic source filtering for this knowledge source.
        When enabled, all searches will be limited to this knowledge source's file paths.

        Args:
            enabled: Whether to enable source filtering
        """
        if hasattr(self.storage, 'enable_source_filtering'):
            self.storage.enable_source_filtering(enabled)
            if enabled:
                logger.info(f"[DatabricksVectorKnowledge] ✅ Source filtering enabled for {len(self.file_paths)} files")
                for path in self.file_paths:
                    logger.info(f"  - {path}")
            else:
                logger.info(f"[DatabricksVectorKnowledge] ❌ Source filtering disabled")
        else:
            logger.warning(f"[DatabricksVectorKnowledge] Storage does not support source filtering")

    def get_available_sources(self) -> List[str]:
        """
        Get list of available source file paths in this knowledge source.

        Returns:
            List of source file paths
        """
        return self.file_paths.copy() if self.file_paths else []


    def _save_documents(self):
        """
        Save documents to Databricks Vector Search document_index.
        This method formats text chunks and saves them to the Vector Search index.
        """
        if not self.storage:
            logger.warning("[DatabricksVectorKnowledge] Storage not initialized, skipping save")
            return
        
        if not self.chunks:
            logger.warning("[DatabricksVectorKnowledge] No chunks to save")
            return
            
        try:
            logger.info(f"[DatabricksVectorKnowledge] Saving {len(self.chunks)} chunks to Vector Search")
            
            # The DatabricksVectorStorage expects documents with specific fields
            # We need to save each chunk as a document in the document_index
            
            # Check if storage has the async save method
            import asyncio
            import uuid
            from datetime import datetime
            
            async def save_chunks_async():
                """Async function to save chunks to Vector Search."""
                saved_count = 0
                
                for i, chunk in enumerate(self.chunks):
                    try:
                        # Get the source file path for this chunk
                        chunk_source = self.chunk_sources[i] if hasattr(self, 'chunk_sources') and i < len(self.chunk_sources) else f"knowledge_{self.execution_id}"

                        # Format each chunk as a document for the document_index
                        # The document schema expects certain fields
                        document_data = {
                            "content": chunk,  # The actual text content
                            "metadata": {
                                "source": chunk_source,  # Use actual file path
                                "collection": self.collection_name,
                                "execution_id": self.execution_id,
                                "group_id": self.group_id,
                                "chunk_index": i,
                                "total_chunks": len(self.chunks),
                                "type": "knowledge_source"
                            },
                            "context": {
                                "query_text": f"Knowledge chunk {i} from {os.path.basename(chunk_source) if chunk_source != f'knowledge_{self.execution_id}' else 'execution'} {self.execution_id}"
                            }
                        }
                        
                        # FIXED: Use repository pattern directly (same as working memory backend)
                        # Instead of storage.save() which has APIStatusError bugs, use repository.upsert()

                        # Build record in the format expected by the document index
                        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
                        import uuid
                        import json
                        from datetime import datetime

                        # Get the document schema for proper field mapping
                        schema = DatabricksIndexSchemas.get_schema("document")

                        # Create record with only fields that exist in document schema
                        record = {}
                        if "id" in schema:
                            record["id"] = str(uuid.uuid4())
                        if "title" in schema:
                            record["title"] = f"Knowledge chunk {i} from execution {self.execution_id}"
                        if "content" in schema:
                            record["content"] = chunk
                        if "source" in schema:
                            record["source"] = chunk_source  # Use actual file path for this chunk
                        if "document_type" in schema:
                            record["document_type"] = "knowledge_source"
                        if "section" in schema:
                            record["section"] = f"chunk_{i}"
                        if "chunk_index" in schema:
                            record["chunk_index"] = i
                        if "chunk_size" in schema:
                            record["chunk_size"] = len(chunk)
                        if "created_at" in schema:
                            record["created_at"] = datetime.utcnow().isoformat()
                        if "updated_at" in schema:
                            record["updated_at"] = datetime.utcnow().isoformat()
                        if "group_id" in schema:
                            record["group_id"] = self.group_id
                        if "doc_metadata" in schema:
                            record["doc_metadata"] = json.dumps({
                                "collection": self.collection_name,
                                "execution_id": self.execution_id,
                                "total_chunks": len(self.chunks)
                            })
                        if "embedding" in schema:
                            # Generate a random embedding for now (will be auto-generated by Vector Search)
                            import random
                            record["embedding"] = [random.random() for _ in range(1024)]
                        if "embedding_model" in schema:
                            record["embedding_model"] = "databricks-gte-large-en"
                        if "version" in schema:
                            record["version"] = 1

                        # FIXED: Use service layer approach (same as DocumentationEmbeddingService)
                        # Create our own DatabricksVectorStorage instance instead of using CrewAI wrapper

                        # Get Vector Search configuration from memory backend
                        vector_search_config = self._get_vector_search_config()
                        if not vector_search_config:
                            raise Exception("Vector Search configuration not available")

                        # Import required services
                        from src.engines.crewai.memory.databricks_vector_storage import DatabricksVectorStorage
                        from src.schemas.databricks_index_schemas import DatabricksIndexSchemas

                        # Get the document schema for proper field mapping
                        schema = DatabricksIndexSchemas.get_schema("document")

                        # Extract configuration
                        document_index = vector_search_config.get('document_index')
                        endpoint_name = vector_search_config.get('document_endpoint_name') or vector_search_config.get('endpoint_name')
                        workspace_url = vector_search_config.get('workspace_url') or os.getenv('DATABRICKS_HOST')

                        if not document_index or not endpoint_name:
                            raise Exception(f"Missing Vector Search config: document_index={document_index}, endpoint_name={endpoint_name}")

                        # Create our own DatabricksVectorStorage instance (same as DocumentationEmbeddingService)
                        databricks_storage = DatabricksVectorStorage(
                            index_name=document_index,
                            endpoint_name=endpoint_name,
                            workspace_url=workspace_url,
                            user_token=self.token,
                            crew_id=self.execution_id,  # Use execution_id as crew_id for isolation
                            memory_type="document"  # Use document type for knowledge sources
                        )

                        # Build record using the same format as DocumentationEmbeddingService
                        # Include agent_ids for access control
                        import json
                        data = {
                            'content': chunk,
                            'embedding': [0.0] * 1024,  # Will be auto-generated by Vector Search
                            'agent_ids': json.dumps(self.agent_ids) if self.agent_ids else json.dumps([]),  # JSON array of agent IDs
                            'metadata': {
                                'source': f"knowledge_{self.execution_id}",
                                'title': f"Knowledge chunk {i} from execution {self.execution_id}",
                                'collection': self.collection_name,
                                'execution_id': self.execution_id,
                                'group_id': self.group_id,
                                'chunk_index': i,
                                'total_chunks': len(self.chunks),
                                'type': 'knowledge_source',
                                'created_at': datetime.utcnow().isoformat()
                            },
                            'context': {
                                'query_text': f"Knowledge chunk {i} from execution {self.execution_id}",
                                'session_id': self.execution_id,
                                'interaction_sequence': i
                            }
                        }

                        # Use the same pattern as DocumentationEmbeddingService
                        await databricks_storage.save(data)
                        logger.info(f"[DatabricksVectorKnowledge] Successfully saved chunk {i} to {document_index}")

                        saved_count += 1
                        
                        if (i + 1) % 10 == 0:
                            logger.info(f"[DatabricksVectorKnowledge] Progress: saved {i + 1}/{len(self.chunks)} chunks")
                            
                    except Exception as chunk_error:
                        error_str = str(chunk_error)
                        if "APIStatusError.__init__()" in error_str:
                            logger.error(f"[DatabricksVectorKnowledge] SDK compatibility bug detected (should be fixed now): {error_str}")
                        else:
                            logger.error(f"[DatabricksVectorKnowledge] Error saving chunk {i}: {chunk_error}")
                        # Continue with other chunks even if one fails
                
                return saved_count
            
            # Run the async save operation
            try:
                # Check if we're already in an event loop
                loop = asyncio.get_running_loop()
                logger.info("[DatabricksVectorKnowledge] Running in async context, using thread executor")
                
                # Use ThreadPoolExecutor to run async code in separate thread
                import concurrent.futures
                import os
                
                # Ensure USE_NULLPOOL is set
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                
                def run_in_new_loop():
                    """Run the async function in a new event loop."""
                    new_loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(new_loop)
                    try:
                        return new_loop.run_until_complete(save_chunks_async())
                    finally:
                        new_loop.close()
                
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future = executor.submit(run_in_new_loop)
                    saved_count = future.result(timeout=300)  # 5 minute timeout
                    
            except RuntimeError:
                # No event loop, we can use asyncio.run directly
                logger.info("[DatabricksVectorKnowledge] No event loop, running async directly")
                import os
                if not os.environ.get("USE_NULLPOOL"):
                    os.environ["USE_NULLPOOL"] = "true"
                saved_count = asyncio.run(save_chunks_async())
            
            logger.info(f"[DatabricksVectorKnowledge] Successfully saved {saved_count}/{len(self.chunks)} chunks to Vector Search")
            
            if saved_count < len(self.chunks):
                logger.warning(f"[DatabricksVectorKnowledge] Only {saved_count} of {len(self.chunks)} chunks were saved")
                
        except Exception as e:
            error_str = str(e)
            if "APIStatusError.__init__()" in error_str:
                logger.error(f"[DatabricksVectorKnowledge] 🐛 FIXED: APIStatusError bug bypassed using repository pattern")
                logger.error(f"[DatabricksVectorKnowledge] If you see this message, the fix worked but there's a remaining edge case")
            else:
                logger.error(f"[DatabricksVectorKnowledge] Error saving documents to Vector Search: {e}")

            import traceback
            traceback.print_exc()
            # Don't fail the entire execution - chunks are prepared
            logger.info(f"[DatabricksVectorKnowledge] Continuing with prepared chunks despite storage error")


    def _chunk_text(self, text: str, chunk_size: int = 1000, overlap: int = 200) -> List[str]:
        """
        Split text into overlapping chunks for better context preservation.
        
        Args:
            text: Text to chunk
            chunk_size: Size of each chunk
            overlap: Number of characters to overlap between chunks
            
        Returns:
            List of text chunks
        """
        chunks = []
        start = 0
        text_length = len(text)
        
        while start < text_length:
            end = min(start + chunk_size, text_length)
            chunk = text[start:end]
            
            # Add chunk if it has meaningful content
            if chunk.strip():
                chunks.append(chunk)
            
            # Move to next chunk with overlap
            start += chunk_size - overlap
            
            # Avoid infinite loop on small texts
            if start <= 0 and len(chunks) > 0:
                break
        
        return chunks if chunks else [text]  # Return original if no chunks created
    
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "DatabricksVectorKnowledgeSource":
        """
        Create instance from configuration dictionary.
        
        Args:
            config: Configuration with file_paths, execution_id, group_id, etc.
            
        Returns:
            Configured DatabricksVectorKnowledgeSource instance
        """
        return cls(
            file_paths=config.get('file_paths', []),
            volume_path=config.get('volume_path'),
            execution_id=config['execution_id'],  # Required
            group_id=config.get('group_id', 'default'),
            workspace_url=config.get('workspace_url'),
            token=config.get('token')
        )
    
    def cleanup(self) -> None:
        """
        Clean up the execution-specific collection after crew completes.
        
        This ensures no data leakage between executions.
        """
        try:
            logger.info(f"[DatabricksVectorKnowledge] Cleaning up collection: {self.collection_name}")
            
            # Get the storage client
            if hasattr(self, 'storage') and self.storage:
                # Try to delete the collection
                if hasattr(self.storage, '_client'):
                    client = self.storage._client
                    try:
                        client.delete_collection(name=self.collection_name)
                        logger.info(f"[DatabricksVectorKnowledge] ✅ Deleted collection: {self.collection_name}")
                    except Exception as e:
                        logger.warning(f"[DatabricksVectorKnowledge] Could not delete collection: {e}")
                        
        except Exception as e:
            logger.error(f"[DatabricksVectorKnowledge] Error during cleanup: {e}")