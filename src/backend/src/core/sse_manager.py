"""
Server-Sent Events (SSE) Manager for real-time updates.

This module manages SSE connections and provides methods to broadcast
execution updates, traces, and other real-time events to connected clients.
"""

from typing import Dict, Set, Optional, Any, AsyncGenerator
from datetime import datetime
from uuid import UUID
import asyncio
import json

from src.core.logger import LoggerManager


class _SSEEncoder(json.JSONEncoder):
    """JSON encoder that handles UUID and datetime objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, UUID):
            return str(obj)
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

logger = LoggerManager.get_instance().system


class SSEEvent:
    """Represents an SSE event to be sent to clients."""

    def __init__(
        self,
        data: Any,
        event: Optional[str] = None,
        id: Optional[str] = None,
        retry: Optional[int] = None
    ):
        self.data = data
        self.event = event
        self.id = id
        self.retry = retry

    def format(self) -> str:
        """
        Format the event according to SSE specification.

        Returns:
            Formatted SSE event string
        """
        lines = []

        if self.event:
            lines.append(f"event: {self.event}")

        if self.id:
            lines.append(f"id: {self.id}")

        if self.retry:
            lines.append(f"retry: {self.retry}")

        # Convert data to JSON if it's a dict/list
        if isinstance(self.data, (dict, list)):
            data_str = json.dumps(self.data, cls=_SSEEncoder)
        else:
            data_str = str(self.data)

        # SSE requires data to be on separate lines prefixed with "data: "
        for line in data_str.split('\n'):
            lines.append(f"data: {line}")

        # SSE events must end with two newlines
        return '\n'.join(lines) + '\n\n'


class SSEConnectionManager:
    """
    Manages SSE connections and event broadcasting.

    Each job can have multiple listeners. Events are queued per job
    and broadcast to all connected clients.
    """

    def __init__(self):
        # Map job_id to set of event queues
        self.job_queues: Dict[str, Set[asyncio.Queue]] = {}

        # Track connection metadata for monitoring
        self.connection_count = 0

    def create_event_queue(self, job_id: str) -> asyncio.Queue:
        """
        Create a new event queue for a job subscription.

        Args:
            job_id: The job ID to subscribe to

        Returns:
            An asyncio.Queue for receiving events
        """
        if job_id not in self.job_queues:
            self.job_queues[job_id] = set()

        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.job_queues[job_id].add(queue)
        self.connection_count += 1

        logger.info(
            f"SSE connection created for job {job_id}. "
            f"Total connections: {self.connection_count}"
        )

        return queue

    def remove_event_queue(self, job_id: str, queue: asyncio.Queue) -> None:
        """
        Remove an event queue when a client disconnects.

        Args:
            job_id: The job ID
            queue: The queue to remove
        """
        if job_id in self.job_queues:
            self.job_queues[job_id].discard(queue)

            # Clean up empty job subscriptions
            if not self.job_queues[job_id]:
                del self.job_queues[job_id]

        self.connection_count -= 1

        logger.info(
            f"SSE connection closed for job {job_id}. "
            f"Remaining connections: {self.connection_count}"
        )

    async def broadcast_to_job(
        self,
        job_id: str,
        event: SSEEvent
    ) -> int:
        """
        Broadcast an event to all clients subscribed to a job.
        Also broadcasts to all "stream-all" subscribers for cross-browser sync.

        Args:
            job_id: The job ID to broadcast to
            event: The SSE event to send

        Returns:
            Number of clients that received the event
        """
        sent_count = 0

        # Broadcast to job-specific subscribers
        if job_id in self.job_queues:
            queues = list(self.job_queues[job_id])

            for queue in queues:
                try:
                    # Non-blocking put - drop event if queue is full
                    queue.put_nowait(event)
                    sent_count += 1
                except asyncio.QueueFull:
                    logger.warning(
                        f"Event queue full for job {job_id}, dropping event"
                    )
                except Exception as e:
                    logger.error(f"Error broadcasting to queue: {e}")

        # Also broadcast to all "stream-all" subscribers
        # This ensures cross-browser synchronization
        all_stream_keys = [key for key in self.job_queues.keys() if key.startswith('all_groups_')]
        for stream_key in all_stream_keys:
            if stream_key in self.job_queues:
                queues = list(self.job_queues[stream_key])
                for queue in queues:
                    try:
                        queue.put_nowait(event)
                        sent_count += 1
                    except asyncio.QueueFull:
                        logger.warning(
                            f"Event queue full for global stream {stream_key}, dropping event"
                        )
                    except Exception as e:
                        logger.error(f"Error broadcasting to global stream: {e}")

        if sent_count > 0:
            logger.debug(
                f"Broadcasted event to {sent_count} clients for job {job_id} (including global streams)"
            )

        return sent_count

    def get_connection_count(self, job_id: Optional[str] = None) -> int:
        """
        Get the number of active SSE connections.

        Args:
            job_id: Optional job ID to count connections for specific job

        Returns:
            Number of active connections
        """
        if job_id:
            return len(self.job_queues.get(job_id, set()))
        return self.connection_count

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get statistics about current SSE connections.

        Returns:
            Dictionary with connection statistics
        """
        return {
            "total_connections": self.connection_count,
            "active_jobs": list(self.job_queues.keys()),
            "connections_per_job": {
                job_id: len(queues)
                for job_id, queues in self.job_queues.items()
            }
        }


# Global SSE manager instance
sse_manager = SSEConnectionManager()


async def event_stream_generator(
    job_id: str,
    timeout: int = 3600,
    heartbeat_interval: int = 30
) -> AsyncGenerator[str, None]:
    """
    Generator function for SSE event streams.

    Args:
        job_id: The job ID to stream events for
        timeout: Maximum time to keep connection alive (seconds)
        heartbeat_interval: Interval for sending keepalive comments (seconds)

    Yields:
        SSE-formatted event strings
    """
    queue = sse_manager.create_event_queue(job_id)

    try:
        start_time = datetime.now()

        # Send initial connection event
        yield SSEEvent(
            data={"message": f"Connected to job {job_id}"},
            event="connected",
            retry=5000
        ).format()

        last_heartbeat = datetime.now()

        while True:
            # Check timeout
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed > timeout:
                logger.info(f"SSE stream timeout for job {job_id}")
                break

            # Send heartbeat comment to keep connection alive
            if (datetime.now() - last_heartbeat).total_seconds() > heartbeat_interval:
                yield f": heartbeat {datetime.now().isoformat()}\n\n"
                last_heartbeat = datetime.now()

            try:
                # Wait for event with short timeout to allow heartbeat checks
                event = await asyncio.wait_for(queue.get(), timeout=5.0)
                yield event.format()

                # If this is a completion event, close per-job streams only.
                # The global stream (all_groups_*) must stay open to serve
                # subsequent jobs — it should only close on timeout or client
                # disconnect.
                if not job_id.startswith('all_groups_') and isinstance(event.data, dict):
                    status = event.data.get('status')
                    if status in ['completed', 'failed', 'stopped']:
                        logger.info(
                            f"Job {job_id} finished with status {status}, closing SSE stream"
                        )
                        break

            except asyncio.TimeoutError:
                # No event received, continue loop for heartbeat
                continue
            except asyncio.CancelledError:
                logger.info(f"SSE stream cancelled for job {job_id}")
                break

    except Exception as e:
        logger.error(f"Error in SSE stream for job {job_id}: {e}")
    finally:
        # Clean up
        sse_manager.remove_event_queue(job_id, queue)
