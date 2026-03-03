"""
CrewAI-compatible wrapper for Lakebase pgvector Storage.

This module bridges the async LakebasePgVectorStorage to CrewAI's
synchronous memory interface using ThreadPoolExecutor + new event loop,
following the same pattern as crewai_databricks_wrapper.py.
"""

import os

# CRITICAL: Set USE_NULLPOOL immediately at import time to prevent asyncpg connection pool issues
if not os.environ.get("USE_NULLPOOL"):
    os.environ["USE_NULLPOOL"] = "true"

import asyncio
import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.memory.lakebase_pgvector_storage import LakebasePgVectorStorage

logger = LoggerManager.get_instance().crew

# Shared executor for running async operations from sync context
_executor = ThreadPoolExecutor(max_workers=4)


def _run_async(coro):
    """Run an async coroutine from synchronous code using a new event loop in a thread."""

    def _run():
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()

    future = _executor.submit(_run)
    return future.result(timeout=30)


class CrewAILakebaseWrapper:
    """
    Wrapper that adapts Lakebase pgvector storage to CrewAI's expected interface.

    CrewAI memory classes (ShortTermMemory, LongTermMemory, EntityMemory) expect
    a storage object with synchronous save(), search(), and reset() methods.
    This wrapper bridges async LakebasePgVectorStorage to that interface.
    """

    def __init__(
        self,
        storage: LakebasePgVectorStorage,
        embedder=None,
        agent_context=None,
        crew=None,
    ):
        self.storage = storage
        self.embedder = embedder
        self.memory_type = storage.memory_type
        self.agent_context = agent_context
        self.crew = crew
        self.trace_context: Optional[dict] = None

    def set_agent_context(self, agent):
        """Set the current agent context for entity memory."""
        self.agent_context = agent

    def _generate_embedding(self, text: str) -> List[float]:
        """Generate embedding vector for the given text."""
        if not self.embedder:
            raise ValueError("No embedder configured for Lakebase memory wrapper")

        try:
            if callable(self.embedder):
                result = self.embedder([text])
                return result[0] if isinstance(result, list) and result else result
            elif hasattr(self.embedder, "embed"):
                return self.embedder.embed(text)
            elif hasattr(self.embedder, "__call__"):
                result = self.embedder([text])
                return result[0] if isinstance(result, list) and result else result
            else:
                raise ValueError(f"Embedder type not supported: {type(self.embedder)}")
        except Exception as e:
            logger.error(f"[LakebaseWrapper] Failed to generate embedding: {e}")
            raise

    def save(self, *args, **kwargs):
        """
        Save a memory record (sync interface for CrewAI).

        Handles different signatures for different memory types:
        - ShortTermMemory: save(value, metadata, **kwargs)
        - LongTermMemory: save(item) where item is LongTermMemoryItem, or save(value, metadata, task_description=...)
        - EntityMemory: save(entity_name, content, metadata)
        """
        try:
            value = None
            metadata = None
            agent = kwargs.get("agent")

            if self.memory_type == "long_term" and len(args) == 1 and hasattr(args[0], '__dict__'):
                # LongTermMemory passes a LongTermMemoryItem object
                item = args[0]
                task_description = getattr(item, 'task', '') or getattr(item, 'task_description', '')
                value = task_description
                metadata = {
                    'agent': getattr(item, 'agent', ''),
                    'expected_output': getattr(item, 'expected_output', ''),
                    'datetime': getattr(item, 'datetime', str(datetime.now(timezone.utc))),
                    'quality': getattr(item, 'quality', None),
                    'task_description': task_description,
                }
                if hasattr(item, 'metadata') and item.metadata:
                    metadata.update(item.metadata)
            elif self.memory_type == "long_term" and "task_description" in kwargs:
                # CrewAI LongTermMemory.save() passes keyword args:
                # storage.save(task_description=..., score=..., metadata=..., datetime=...)
                value = kwargs["task_description"]
                metadata = kwargs.get("metadata", {})
                if isinstance(metadata, dict):
                    if "task_description" not in metadata:
                        metadata["task_description"] = value
                    if kwargs.get("score") is not None:
                        metadata["quality"] = kwargs["score"]
                    if kwargs.get("datetime"):
                        metadata["datetime"] = kwargs["datetime"]
            elif len(args) >= 1:
                value = args[0]
                metadata = args[1] if len(args) > 1 else kwargs.get("metadata")
                if len(args) > 2:
                    agent = args[2]
            else:
                value = kwargs.get("value")
                metadata = kwargs.get("metadata")

            # Extract content from different input formats
            if isinstance(value, str):
                content = value
            elif isinstance(value, dict):
                content = value.get("data", value.get("content", json.dumps(value)))
            else:
                content = str(value) if value else ""

            if not content or not content.strip():
                logger.debug("[LakebaseWrapper] Skipping save of empty content")
                return

            # Get agent name
            agent_name = None
            if agent:
                agent_name = (
                    getattr(agent, "role", str(agent))
                    if not isinstance(agent, str)
                    else agent
                )
            elif self.agent_context:
                agent_name = getattr(self.agent_context, "role", None)

            # Build metadata
            save_metadata = metadata if isinstance(metadata, dict) else {}
            if self.memory_type == "entity" and agent_name:
                save_metadata["agent"] = agent_name

            # Generate embedding
            embedding = self._generate_embedding(content)

            # Generate record ID
            record_id = str(uuid.uuid4())

            # Extract score if present
            score = None
            if isinstance(value, dict):
                score = value.get("score")
            if isinstance(metadata, dict):
                score = metadata.get("score", score)

            # Save via async storage
            _run_async(
                self.storage.save(
                    record_id=record_id,
                    content=content,
                    embedding=embedding,
                    metadata=save_metadata,
                    agent=agent_name,
                    score=score,
                )
            )
            logger.debug(
                f"[LakebaseWrapper] Saved {self.memory_type} memory: "
                f"{content[:80]}..."
            )
        except Exception as e:
            logger.error(f"[LakebaseWrapper] Save failed for {self.memory_type}: {e}")

    def search(self, query, limit=3, score_threshold=0.35):
        """
        Search for similar memory records (sync interface for CrewAI).

        Generates query embedding, then delegates to async storage.
        Returns results formatted for CrewAI consumption.
        """
        try:
            # Extract query text
            if isinstance(query, str):
                query_text = query
            elif isinstance(query, dict):
                query_text = query.get(
                    "data", query.get("content", query.get("query", ""))
                )
            else:
                query_text = str(query)

            if not query_text or not query_text.strip():
                return []

            # Generate query embedding
            query_embedding = self._generate_embedding(query_text)

            # Search via async storage
            results = _run_async(
                self.storage.search(
                    query_embedding=query_embedding,
                    k=limit,
                )
            )

            # Format results for CrewAI
            formatted = []
            for result in results:
                # Filter by score threshold (cosine distance: lower is better)
                distance = result.get("distance")
                if distance is not None and distance > (1.0 - score_threshold):
                    continue

                formatted.append(
                    {
                        "context": result.get("content", ""),
                        "score": (
                            1.0 - distance
                            if distance is not None
                            else result.get("score", 0.0)
                        ),
                        "metadata": result.get("metadata", {}),
                    }
                )

            logger.debug(
                f"[LakebaseWrapper] {self.memory_type} search returned "
                f"{len(formatted)} results (from {len(results)} raw)"
            )
            return formatted

        except Exception as e:
            logger.error(f"[LakebaseWrapper] Search failed for {self.memory_type}: {e}")
            return []

    def load(self, task: str, latest_n: int = 3) -> List[Dict[str, Any]]:
        """
        Load memories for a task (CrewAI LongTermMemory compatibility).

        CrewAI's LongTermMemory calls this method to retrieve past task memories.

        Args:
            task: Task description to search for relevant memories
            latest_n: Number of recent memories to retrieve

        Returns:
            List of memory entries
        """
        try:
            results = self.search(query=task, limit=latest_n)
            formatted = []
            for result in results:
                formatted.append({
                    "content": result.get("context", ""),
                    "metadata": result.get("metadata", {}),
                    "score": result.get("score", 0.0),
                })
            logger.debug(
                f"[LakebaseWrapper] load() returned {len(formatted)} results for task"
            )
            return formatted
        except Exception as e:
            logger.error(f"[LakebaseWrapper] Load failed for {self.memory_type}: {e}")
            return []

    def reset(self):
        """Reset/clear all memory for this crew (sync interface for CrewAI)."""
        try:
            _run_async(self.storage.clear())
            logger.info(f"[LakebaseWrapper] Reset {self.memory_type} memory")
        except Exception as e:
            logger.error(f"[LakebaseWrapper] Reset failed for {self.memory_type}: {e}")
