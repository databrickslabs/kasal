import logging
import queue
from typing import Any, Optional, cast

logger = logging.getLogger(__name__)


class TraceQueue:
    """Singleton holder for the agent trace queue."""

    _instance: Optional["TraceQueue"] = None
    _queue: Optional["queue.Queue[Any]"] = None

    def __new__(cls) -> "TraceQueue":
        if cls._instance is None:
            cls._instance = super(TraceQueue, cls).__new__(cls)
            cls._instance._queue = queue.Queue()
            logger.debug("[TRACE_DEBUG] TraceQueue singleton created with new queue")
        return cls._instance

    def get_queue(self) -> queue.Queue:
        """Get the singleton queue instance."""
        q = cast("queue.Queue[Any]", self._queue)  # __new__ always sets it
        logger.debug(f"[TRACE_DEBUG] get_queue called, queue size: {q.qsize()}")
        return q


# Function to get the singleton queue instance easily
def get_trace_queue() -> queue.Queue:
    logger.debug("[TRACE_DEBUG] get_trace_queue function called")
    return TraceQueue().get_queue()
