"""
ChromaDB storage with Databricks embedder for DEFAULT memory backend.

This wrapper provides a simple storage interface that uses ChromaDB with
a custom Databricks embedding function, avoiding the issues with RAGStorage.
"""
import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class ChromaDBDatabricksStorage:
    """
    Simple ChromaDB storage wrapper with Databricks embedding function.
    Compatible with CrewAI memory interfaces.
    """

    def __init__(
        self,
        storage_path: Path,
        collection_name: str,
        embedding_function: Any,
        memory_type: str = "short_term"
    ):
        """
        Initialize ChromaDB storage with Databricks embedder.

        Args:
            storage_path: Path to ChromaDB storage directory
            collection_name: Name of the collection
            embedding_function: Databricks embedding function
            memory_type: Type of memory (short_term, entities, etc.)
        """
        import chromadb

        self.storage_path = storage_path
        self.collection_name = collection_name
        self.embedding_function = embedding_function
        self.type = memory_type
        self.allow_reset = True

        # Create ChromaDB client
        self._client = chromadb.PersistentClient(
            path=str(storage_path),
            settings=chromadb.Settings(anonymized_telemetry=False)
        )

        # Create or get collection with Databricks embedder
        self._collection = self._client.get_or_create_collection(
            name=collection_name,
            embedding_function=embedding_function
        )

        logger.info(f"Initialized ChromaDB storage: {collection_name} at {storage_path}")

    def save(self, value: Any, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Save a value to the collection.

        Args:
            value: Text or data to save
            metadata: Optional metadata dictionary
        """
        try:
            import uuid

            # Convert value to string if needed
            text = str(value) if not isinstance(value, str) else value

            # Generate unique ID
            doc_id = str(uuid.uuid4())

            # Add to collection
            self._collection.add(
                ids=[doc_id],
                documents=[text],
                metadatas=[metadata or {}]
            )
            logger.info(f"✅ SAVED to {self.collection_name}: {text[:200]}... (ID: {doc_id})")

            # Log collection count
            count = self._collection.count()
            logger.info(f"📊 Collection {self.collection_name} now has {count} documents")
        except Exception as e:
            logger.error(f"❌ Error saving to {self.collection_name}: {e}")
            raise

    def search(
        self,
        query: str,
        limit: int = 3,
        filter: Optional[Dict[str, Any]] = None,
        score_threshold: float = 0.35
    ) -> List[Any]:
        """
        Search for similar items in the collection.

        Args:
            query: Search query text
            limit: Maximum number of results
            filter: Optional metadata filter
            score_threshold: Minimum similarity score

        Returns:
            List of matching documents
        """
        try:
            logger.info(f"🔍 SEARCHING {self.collection_name} with query: '{query[:100]}...' (limit={limit})")

            # Check collection count first
            count = self._collection.count()
            logger.info(f"📊 Collection {self.collection_name} has {count} documents before search")

            if count == 0:
                logger.warning(f"⚠️ Collection {self.collection_name} is EMPTY - returning no results")
                return []

            # Query the collection
            results = self._collection.query(
                query_texts=[query],
                n_results=limit,
                where=filter
            )

            logger.info(f"🔍 Raw query results: {results}")

            # Extract documents and format for CrewAI
            if results and results.get('documents'):
                docs = results['documents'][0]  # First query results

                # CrewAI expects list of dicts with 'content' key, not raw strings
                formatted_results = [{'content': doc} for doc in docs]

                logger.info(f"✅ Found {len(docs)} results in {self.collection_name}")
                logger.info(f"✅ Formatted results: {formatted_results}")
                return formatted_results
            else:
                logger.warning(f"⚠️ No documents in query results for {self.collection_name}")
                return []

        except Exception as e:
            logger.error(f"❌ Error searching {self.collection_name}: {e}", exc_info=True)
            return []

    def reset(self) -> None:
        """Reset the collection by deleting and recreating it."""
        try:
            self._client.delete_collection(name=self.collection_name)
            self._collection = self._client.get_or_create_collection(
                name=self.collection_name,
                embedding_function=self.embedding_function
            )
            logger.info(f"Reset collection: {self.collection_name}")
        except Exception as e:
            logger.error(f"Error resetting {self.collection_name}: {e}")
