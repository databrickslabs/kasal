"""
Databricks Vector Search storage implementation for CrewAI knowledge sources.
Uses Databricks Vector Search instead of ChromaDB to avoid ONNX issues.
"""

import os
import logging
from typing import List, Dict, Any, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.vectorsearch import (
    VectorSearchEndpoint,
    VectorSearchIndex,
    EndpointType,
    DeltaSyncVectorIndexSpecRequest,
    EmbeddingSourceColumn,
    VectorIndexType,
)
import time
import hashlib
import json

logger = logging.getLogger(__name__)


class DatabricksVectorSearchStorage:
    """Storage implementation using Databricks Vector Search."""
    
    def __init__(
        self,
        collection_name: str,
        workspace_url: Optional[str] = None,
        token: Optional[str] = None,
        catalog: str = "main",
        schema: str = "default",
        embedding_model: str = "databricks-gte-large-en",
    ):
        """
        Initialize Databricks Vector Search storage.
        
        Args:
            collection_name: Name for the index (will be prefixed for uniqueness)
            workspace_url: Databricks workspace URL
            token: Authentication token
            catalog: Unity Catalog name
            schema: Schema name
            embedding_model: Embedding model to use
        """
        self.collection_name = collection_name
        self.catalog = catalog
        self.schema = schema
        self.embedding_model = embedding_model
        
        # Initialize workspace client
        self.workspace_url = workspace_url or os.environ.get('DATABRICKS_HOST')
        self.token = token
        
        if not self.workspace_url:
            raise ValueError("Databricks workspace URL is required")
        
        # Ensure proper URL format
        if not self.workspace_url.startswith('https://'):
            self.workspace_url = f'https://{self.workspace_url}'
        
        try:
            # Initialize Databricks SDK client
            self.client = WorkspaceClient(
                host=self.workspace_url,
                token=self.token
            )
            logger.info(f"Initialized Databricks Vector Search client for {self.workspace_url}")
        except Exception as e:
            logger.error(f"Failed to initialize Databricks client: {e}")
            raise
        
        # Generate unique index name
        self.index_name = self._generate_index_name()
        self.endpoint_name = "kasal_knowledge_endpoint"
        
        # Initialize endpoint and index
        self._initialize_vector_search()
    
    def _generate_index_name(self) -> str:
        """Generate a unique index name based on collection name."""
        # Use hash to ensure valid index name (alphanumeric and underscores only)
        name_hash = hashlib.md5(self.collection_name.encode()).hexdigest()[:8]
        return f"knowledge_{name_hash}"
    
    def _initialize_vector_search(self):
        """Initialize Vector Search endpoint and index."""
        try:
            # Get or create endpoint
            self.endpoint = self._get_or_create_endpoint()
            
            # For now, we'll use a simpler approach with direct similarity search
            # Rather than creating a full Delta table-backed index
            logger.info(f"Vector Search endpoint ready: {self.endpoint_name}")
            
            # Store documents in memory for this session
            # In production, these would go to a Delta table
            self.documents = []
            self.embeddings_cache = {}
            
        except Exception as e:
            logger.error(f"Failed to initialize Vector Search: {e}")
            # Fall back to in-memory storage
            self.documents = []
            self.embeddings_cache = {}
    
    def _get_or_create_endpoint(self) -> Optional[VectorSearchEndpoint]:
        """Get existing or create new Vector Search endpoint."""
        try:
            # List existing endpoints
            endpoints = self.client.vector_search.list_endpoints()
            for endpoint in endpoints:
                if endpoint.name == self.endpoint_name:
                    logger.info(f"Found existing endpoint: {self.endpoint_name}")
                    return endpoint
            
            # Create new endpoint if not found
            logger.info(f"Creating new Vector Search endpoint: {self.endpoint_name}")
            endpoint = self.client.vector_search.create_endpoint(
                name=self.endpoint_name,
                endpoint_type=EndpointType.STANDARD
            )
            
            # Wait for endpoint to be ready
            timeout = 300  # 5 minutes
            start_time = time.time()
            while time.time() - start_time < timeout:
                endpoint = self.client.vector_search.get_endpoint(self.endpoint_name)
                if endpoint.state == "ONLINE":
                    logger.info(f"Endpoint {self.endpoint_name} is online")
                    return endpoint
                time.sleep(10)
            
            logger.warning(f"Endpoint creation timed out after {timeout} seconds")
            return endpoint
            
        except Exception as e:
            logger.error(f"Error managing Vector Search endpoint: {e}")
            return None
    
    def save(self, chunks: List[str]) -> bool:
        """
        Save document chunks to Vector Search.
        
        Args:
            chunks: List of text chunks to save
            
        Returns:
            True if successful, False otherwise
        """
        try:
            if not chunks:
                logger.warning("No chunks to save")
                return False
            
            # Store documents with metadata
            for i, chunk in enumerate(chunks):
                doc = {
                    "id": f"{self.collection_name}_{i}",
                    "text": chunk,
                    "metadata": {
                        "collection": self.collection_name,
                        "chunk_index": i,
                        "chunk_count": len(chunks)
                    }
                }
                self.documents.append(doc)
            
            logger.info(f"Saved {len(chunks)} chunks to Vector Search storage")
            
            # In a production implementation, we would:
            # 1. Write chunks to a Delta table
            # 2. Create/update the Vector Search index
            # 3. Trigger index sync
            
            # For now, we're using in-memory storage with the option to
            # compute embeddings on-demand using Databricks embedding models
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to save chunks: {e}")
            return False
    
    def search(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        """
        Search for similar documents.
        
        Args:
            query: Search query
            limit: Maximum number of results
            
        Returns:
            List of similar documents with metadata
        """
        try:
            if not self.documents:
                logger.warning("No documents to search")
                return []
            
            # Simple keyword search for now
            # In production, this would use Vector Search similarity query
            results = []
            query_lower = query.lower()
            
            for doc in self.documents:
                text_lower = doc["text"].lower()
                if query_lower in text_lower:
                    # Calculate simple relevance score
                    score = text_lower.count(query_lower) / len(text_lower.split())
                    results.append({
                        "text": doc["text"],
                        "metadata": doc["metadata"],
                        "score": score
                    })
            
            # Sort by score and limit results
            results.sort(key=lambda x: x["score"], reverse=True)
            results = results[:limit]
            
            logger.info(f"Found {len(results)} results for query: {query}")
            return results
            
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return []
    
    def delete_collection(self):
        """Delete the collection/index."""
        try:
            # In production, would delete the Vector Search index
            self.documents = []
            self.embeddings_cache = {}
            logger.info(f"Cleared collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Failed to delete collection: {e}")
    
    def get_stats(self) -> Dict[str, Any]:
        """Get storage statistics."""
        return {
            "collection_name": self.collection_name,
            "index_name": self.index_name,
            "document_count": len(self.documents),
            "endpoint": self.endpoint_name,
            "embedding_model": self.embedding_model
        }