"""
Service for managing execution logs and WebSocket connections.

This module provides functionality for broadcasting execution logs to connected clients,
storing logs in the database, and managing WebSocket connections.
"""

import asyncio
import json
from typing import Dict, Set, List, Any, Optional
from datetime import datetime
from queue import Empty

from fastapi import WebSocket
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.logger import LoggerManager
from src.models.execution_logs import ExecutionLog
from src.schemas.execution_logs import LogMessage, ExecutionLogResponse
from src.repositories.execution_logs_repository import execution_logs_repository
from src.services.execution_logs_queue import enqueue_log, get_job_output_queue

# Get logger from the centralized logging system
logger = LoggerManager.get_instance().system

# Singleton instance of the logs writer task
_logs_writer_task: Optional[asyncio.Task] = None

class ExecutionLogsService:
    """
    Service for managing execution logs and WebSocket connections.
    
    This service handles:
    - WebSocket connection management
    - Broadcasting logs to connected clients
    - Storing and retrieving execution logs from the database
    """
    
    def __init__(self):
        """Initialize the execution logs service."""
        self.active_connections: Dict[str, Set[WebSocket]] = {}
        self._lock = asyncio.Lock()
    
    async def connect(self, websocket: WebSocket, execution_id: str):
        """
        Connect a client to an execution's WebSocket stream.
        
        Args:
            websocket: WebSocket connection to register
            execution_id: ID of the execution to connect to
        """
        await websocket.accept()
        async with self._lock:
            if execution_id not in self.active_connections:
                self.active_connections[execution_id] = set()
            self.active_connections[execution_id].add(websocket)
        
        # Send historical logs when client connects
        try:
            historical_logs = await execution_logs_repository.get_by_execution_id_with_managed_session(execution_id)
            
            for log in historical_logs:
                await websocket.send_text(json.dumps({
                    "execution_id": execution_id,
                    "content": log.content,
                    "timestamp": log.timestamp.isoformat(),
                    "type": "historical"
                }))
        except Exception as e:
            logger.error(f"Error sending historical logs: {e}")
        
        logger.debug(f"Client connected to execution {execution_id}. Total connections: {len(self.active_connections[execution_id])}")

    async def disconnect(self, websocket: WebSocket, execution_id: str):
        """
        Disconnect a client from an execution's WebSocket stream.
        
        Args:
            websocket: WebSocket connection to unregister
            execution_id: ID of the execution to disconnect from
        """
        async with self._lock:
            if execution_id in self.active_connections:
                self.active_connections[execution_id].discard(websocket)
                if not self.active_connections[execution_id]:
                    del self.active_connections[execution_id]
        logger.debug(f"Client disconnected from execution {execution_id}")

    async def create_execution_log(self, execution_id: str, content: str, timestamp: datetime = None) -> bool:
        """
        Create a new execution log entry via the repository layer.
        
        Args:
            execution_id: ID of the execution to log for
            content: The log message content
            timestamp: Optional timestamp for the log entry (defaults to current time)
            
        Returns:
            bool: True if log was created successfully, False otherwise
        """
        try:
            logger.debug(f"[create_execution_log] Creating log for execution {execution_id}")
            
            # Use the repository to create the log with a managed session
            await execution_logs_repository.create_with_managed_session(
                execution_id=execution_id,
                content=content,
                timestamp=timestamp or datetime.now()
            )
            
            logger.debug(f"[create_execution_log] Successfully created log for execution {execution_id}")
            return True
        except Exception as e:
            logger.error(f"[create_execution_log] Error creating log for execution {execution_id}: {e}")
            logger.error("[create_execution_log] Exception details:", exc_info=True)
            return False

    async def broadcast_to_execution(self, execution_id: str, message: str):
        """
        Broadcast a log message to all clients connected to an execution.
        
        Args:
            execution_id: ID of the execution to broadcast to
            message: Content message to broadcast
        """
        logger.debug(f"[broadcast_to_execution] Starting broadcast for execution {execution_id}")
        
        # Enqueue the log message to be written to the database asynchronously
        success = enqueue_log(execution_id=execution_id, content=message)
        if not success:
            logger.error(f"[broadcast_to_execution] Failed to enqueue log for execution {execution_id}")

        # Now handle WebSocket connections if any exist
        if execution_id not in self.active_connections:
            logger.debug(f"[broadcast_to_execution] No active connections for execution {execution_id}")
            return

        message_data = {
            "execution_id": execution_id,
            "content": message,
            "timestamp": datetime.utcnow().isoformat(),
            "type": "live"
        }

        logger.debug(f"[broadcast_to_execution] Broadcasting to {len(self.active_connections[execution_id])} connections")
        disconnected = set()
        for connection in self.active_connections[execution_id]:
            try:
                await connection.send_text(json.dumps(message_data))
                logger.debug("[broadcast_to_execution] Successfully sent message to connection")
            except Exception as e:
                logger.error(f"[broadcast_to_execution] Error sending message to client: {e}")
                logger.error("[broadcast_to_execution] Stack trace:", exc_info=True)
                disconnected.add(connection)

        # Clean up disconnected clients
        if disconnected:
            logger.info(f"[broadcast_to_execution] Cleaned up {len(disconnected)} disconnected clients")
            async with self._lock:
                self.active_connections[execution_id] -= disconnected

    async def get_execution_logs(self, execution_id: str, limit: int = 1000, offset: int = 0) -> List[ExecutionLogResponse]:
        """
        Fetch historical execution logs from the database.
        
        Args:
            execution_id: ID of the execution to fetch logs for
            limit: Maximum number of logs to fetch
            offset: Number of logs to skip
            
        Returns:
            List of execution log responses
        """
        logs = await execution_logs_repository.get_by_execution_id_with_managed_session(
            execution_id=execution_id,
            limit=limit,
            offset=offset
        )
        
        return [
            ExecutionLogResponse(
                content=log.content,
                timestamp=log.timestamp.isoformat()
            )
            for log in logs
        ]
    
    async def count_logs(self, execution_id: str) -> int:
        """
        Count logs for a specific execution.
        
        Args:
            execution_id: ID of the execution to count logs for
            
        Returns:
            Number of logs
        """
        return await execution_logs_repository.count_by_execution_id_with_managed_session(execution_id)
    
    async def delete_logs(self, execution_id: str) -> int:
        """
        Delete all logs for a specific execution.
        
        Args:
            execution_id: ID of the execution to delete logs for
            
        Returns:
            Number of deleted logs
        """
        return await execution_logs_repository.delete_by_execution_id_with_managed_session(execution_id)

# Create a singleton instance of the service
execution_logs_service = ExecutionLogsService()

# --- Logs Writer Functions ---

async def logs_writer_loop(shutdown_event: asyncio.Event):
    """
    Background task that reads from the job output queue and writes logs to the database.
    
    Args:
        shutdown_event: Event to signal shutdown
    """
    try:
        logger.info("[logs_writer_loop] Logs writer task started.")
        
        # Get job output queue
        queue = get_job_output_queue()
        logger.debug(f"[logs_writer_loop] Queue retrieved. Initial approximate size: {queue.qsize()}")
        
        batch_count = 0
        total_log_count = 0
        empty_count = 0  # Count consecutive empty queue occurrences
        
        while not shutdown_event.is_set():
            # Create a small batch of logs to process together
            batch = []
            batch_target_size = 10  # Process up to this many at once
            
            try:
                # Try to collect a batch of logs
                for _ in range(batch_target_size):
                    try:
                        # Log queue status periodically
                        if _ == 0:
                            logger.debug(f"[logs_writer_loop] Waiting for logs... Queue size: ~{queue.qsize()}")
                        
                        # Non-blocking get with timeout
                        log_data = queue.get(block=True, timeout=0.1)
                        
                        # Check if this is the shutdown signal (None)
                        if log_data is None:
                            logger.debug("[logs_writer_loop] Received shutdown signal (None) in queue.")
                            continue
                            
                        batch.append(log_data)
                        queue.task_done()
                        empty_count = 0  # Reset empty count when we get an item
                    except Empty:
                        # Queue is empty, break out of the batch collection loop
                        empty_count += 1
                        if empty_count % 100 == 0:  # Log every 100 consecutive empty checks
                            logger.debug(f"[logs_writer_loop] Queue empty for {empty_count} consecutive checks")
                        break
                
                # If we collected any logs, process them
                if batch:
                    batch_count += 1
                    total_log_count += len(batch)
                    
                    # Log batch processing
                    logger.debug(f"[logs_writer_loop] Processing batch #{batch_count} with {len(batch)} logs. Total processed: {total_log_count}")
                    
                    # Process each log in the batch
                    failures = 0
                    for idx, log_data in enumerate(batch):
                        try:
                            job_id = log_data.get("job_id", "unknown")
                            content = log_data.get("content", "")
                            timestamp = log_data.get("timestamp", datetime.now())
                            log_info = f"[{job_id}:{idx+1}/{len(batch)}]"
                            
                            # Create log with execution_logs_service
                            success = await execution_logs_service.create_execution_log(
                                execution_id=job_id,
                                content=content,
                                timestamp=timestamp
                            )
                            
                            if not success:
                                logger.warning(f"[logs_writer_loop] {log_info} ❌ Failed to store log")
                                failures += 1
                                
                        except Exception as e:
                            logger.error(f"[logs_writer_loop] Error processing log: {e}", exc_info=True)
                            failures += 1
                    
                    if failures > 0:
                        logger.warning(f"[logs_writer_loop] Batch #{batch_count} processed with {failures} failures.")
                    else:
                        logger.debug(f"[logs_writer_loop] Batch #{batch_count} processed successfully.")
                
                # If no logs were collected, sleep briefly to avoid CPU spinning
                else:
                    await asyncio.sleep(0.5)
                
            except Exception as e:
                logger.error(f"[logs_writer_loop] Batch processing error: {e}", exc_info=True)
                # Sleep to avoid rapid retry on persistent errors
                await asyncio.sleep(1)
            
        logger.info("[logs_writer_loop] Shutdown event received, exiting logs writer loop.")
    
    except asyncio.CancelledError:
        logger.warning("[logs_writer_loop] Logs writer task cancelled.")
    except Exception as e:
        logger.critical(f"[logs_writer_loop] Unhandled exception in logs writer loop: {e}", exc_info=True)
    finally:
        logger.info("[logs_writer_loop] Logs writer task stopped.")

async def start_logs_writer(shutdown_event: asyncio.Event) -> asyncio.Task:
    """
    Start the logs writer loop if it hasn't been started yet.
    
    Args:
        shutdown_event: Event to signal shutdown
        
    Returns:
        The writer task
    """
    global _logs_writer_task
    
    if _logs_writer_task is None or _logs_writer_task.done():
        logger.info("[start_logs_writer] Starting logs writer task...")
        _logs_writer_task = asyncio.create_task(logs_writer_loop(shutdown_event))
        logger.info("[start_logs_writer] Logs writer task started.")
    else:
        logger.debug("[start_logs_writer] Logs writer task already running.")
        
    return _logs_writer_task

async def stop_logs_writer(timeout: float = 5.0) -> bool:
    """
    Stop the logs writer task.
    
    Args:
        timeout: Maximum time to wait for the task to stop
        
    Returns:
        True if stopped successfully, False otherwise
    """
    global _logs_writer_task
    
    if _logs_writer_task is None or _logs_writer_task.done():
        logger.debug("[stop_logs_writer] Logs writer task not running or already stopped.")
        return True
        
    logger.info("[stop_logs_writer] Stopping logs writer task...")
    try:
        # Add None to logs queue to help unblock queue.get()
        try:
            from queue import Full
            logs_queue = get_job_output_queue()
            logs_queue.put_nowait(None)
        except Full:
            logger.warning("[stop_logs_writer] Logs queue full, writer might take longer to stop.")
            
        # Wait for task to complete
        await asyncio.wait_for(_logs_writer_task, timeout=timeout)
        logger.info("[stop_logs_writer] Logs writer task stopped gracefully.")
        _logs_writer_task = None
        return True
    except asyncio.TimeoutError:
        logger.warning("[stop_logs_writer] Logs writer task did not stop in time, cancelling.")
        _logs_writer_task.cancel()
        try:
            await _logs_writer_task
        except asyncio.CancelledError:
            pass
        _logs_writer_task = None
        return True
    except Exception as e:
        logger.error(f"[stop_logs_writer] Error stopping logs writer task: {e}", exc_info=True)
        return False 