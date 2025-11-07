"""
Embedding Queue Service for batching documentation embedding operations.

This service queues embedding operations and processes them in batches
to reduce database lock contention in SQLite.
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import insert

from src.core.logger import LoggerManager
from src.models.documentation_embedding import DocumentationEmbedding

logger = LoggerManager.get_instance().system


class EmbeddingQueueService:
    """Service to batch documentation embedding operations."""

    def __init__(self):
        self.queue: List[Dict[str, Any]] = []
        self.lock = asyncio.Lock()
        self.batch_size = 10  # Process 10 embeddings at a time
        self.flush_interval = 5.0  # Flush every 5 seconds
        self._task = None
        self._running = False

    async def start(self):
        """Start the background queue processor."""
        if not self._running:
            self._running = True
            # Create the background task without awaiting it
            self._task = asyncio.create_task(self._process_queue())
            # Add error handling for the background task
            self._task.add_done_callback(self._handle_task_error)
            logger.info("Embedding queue service started")

    def _handle_task_error(self, task):
        """Handle any errors from the background task."""
        try:
            # This will raise any exception that occurred in the task
            task.result()
        except asyncio.CancelledError:
            logger.info("Embedding queue background task was cancelled")
        except Exception as e:
            logger.error(f"Embedding queue background task error: {e}")

    async def stop(self):
        """Stop the background queue processor."""
        self._running = False
        if self._task:
            await self._task
            logger.info("Embedding queue service stopped")

    async def add_embedding(
        self,
        source: str,
        title: str,
        content: str,
        embedding: List[float],
        doc_metadata: Optional[Dict[str, Any]] = None
    ):
        """Add an embedding to the queue for batch processing."""
        async with self.lock:
            self.queue.append({
                "source": source,
                "title": title,
                "content": content,
                "embedding": embedding,
                "doc_metadata": doc_metadata or {},
                "created_at": datetime.utcnow()
            })

            # If queue is full, process immediately
            if len(self.queue) >= self.batch_size:
                await self._flush_queue()

    async def _process_queue(self):
        """Background task to periodically flush the queue."""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self._flush_queue()
            except Exception as e:
                logger.error(f"Error processing embedding queue: {e}")

    async def _flush_queue(self):
        """Flush the queue and batch insert embeddings."""
        async with self.lock:
            if not self.queue:
                return

            batch = self.queue[:self.batch_size]
            self.queue = self.queue[self.batch_size:]

            if batch:
                await self._batch_insert(batch)

    async def _batch_insert(self, batch: List[Dict[str, Any]]):
        """Perform batch insert of embeddings."""
        from src.db.session import async_session_factory

        try:
            async with async_session_factory() as session:
                # Use bulk insert for efficiency
                stmt = insert(DocumentationEmbedding).values(batch)
                await session.execute(stmt)
                await session.commit()
                logger.info(f"Batch inserted {len(batch)} embeddings")
        except Exception as e:
            logger.error(f"Failed to batch insert embeddings: {e}")
            # On failure, try individual inserts with retry
            for item in batch:
                await self._insert_with_retry(item)

    async def _insert_with_retry(self, item: Dict[str, Any], max_retries: int = 3):
        """Insert a single embedding with retry logic."""
        from src.db.session import async_session_factory

        for attempt in range(max_retries):
            try:
                async with async_session_factory() as session:
                    embedding = DocumentationEmbedding(**item)
                    session.add(embedding)
                    await session.commit()
                    logger.debug(f"Inserted embedding after {attempt + 1} attempts")
                    return
            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Retry {attempt + 1}/{max_retries} after {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"Failed to insert embedding after {max_retries} attempts: {e}")


# Global singleton instance
embedding_queue = EmbeddingQueueService()