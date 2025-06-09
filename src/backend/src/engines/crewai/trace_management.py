"""
Trace Management for CrewAI engine.

This module provides functionality for managing trace data from CrewAI executions.
"""
import logging
import asyncio
import queue
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

class TraceManager:
    """
    Manages the trace writer for CrewAI engine executions.
    
    This class handles the background tasks that read from the trace queue
    and write to the database.
    """
    
    # Class variables for singleton writer task
    _trace_writer_task: Optional[asyncio.Task] = None
    _logs_writer_task: Optional[asyncio.Task] = None
    _shutdown_event: asyncio.Event = asyncio.Event()
    _writer_started: bool = False
    _lock = asyncio.Lock()  # Lock for starting the writer
    
    @classmethod
    async def _trace_writer_loop(cls):
        """
        Background task that reads from the trace queue and writes to the database.
        """
        from src.services.trace_queue import get_trace_queue
        from src.services.execution_trace_service import ExecutionTraceService
        from src.services.execution_status_service import ExecutionStatusService
        from src.services.execution_history_service import get_execution_history_service
        from queue import Empty  # Import the Empty exception from queue module
        
        try:
            logger.info("[TraceManager._trace_writer_loop] Writer task started.")
            
            # Get trace queue
            queue = get_trace_queue()
            logger.debug(f"[TraceManager._trace_writer_loop] Queue retrieved. Initial approximate size: {queue.qsize()}")
            
            # Get service instance
            execution_history_service = get_execution_history_service()
            
            batch_count = 0
            total_trace_count = 0
            empty_count = 0  # Count consecutive empty queue occurrences
            
            # Keep track of jobs we've confirmed exist
            confirmed_jobs = set()
            
            while not cls._shutdown_event.is_set():
                # Create a small batch of traces to process together
                batch = []
                batch_target_size = 10  # Process up to this many at once
                
                try:
                    # Try to collect a batch of traces
                    for _ in range(batch_target_size):
                        try:
                            # Log queue status periodically
                            if _ == 0:
                                logger.debug(f"[TraceManager._trace_writer_loop] Waiting for traces... Queue size: ~{queue.qsize()}")
                            
                            # Non-blocking get with timeout
                            trace_data = queue.get(block=True, timeout=0.1)
                            
                            # Check if this is the shutdown signal (None)
                            if trace_data is None:
                                logger.debug("[TraceManager._trace_writer_loop] Received shutdown signal (None) in queue.")
                                continue
                                
                            batch.append(trace_data)
                            queue.task_done()
                            empty_count = 0  # Reset empty count when we get an item
                        except Empty:  # Use the imported Empty exception
                            # Queue is empty, break out of the batch collection loop
                            empty_count += 1
                            if empty_count % 100 == 0:  # Log every 100 consecutive empty checks
                                logger.debug(f"[TraceManager._trace_writer_loop] Queue empty for {empty_count} consecutive checks")
                            break
                    
                    # If we collected any traces, process them
                    if batch:
                        batch_count += 1
                        total_trace_count += len(batch)
                        
                        # Log batch processing
                        logger.debug(f"[TraceManager._trace_writer_loop] Processing batch #{batch_count} with {len(batch)} traces. Total processed: {total_trace_count}")
                        
                        # Process each trace in the batch
                        failures = 0
                        for idx, trace_data in enumerate(batch):
                            try:
                                job_id = trace_data.get("job_id", "unknown")
                                event_type = trace_data.get("event_type", "unknown")
                                trace_info = f"[{job_id}:{event_type}:{idx+1}/{len(batch)}]"
                                
                                # Skip processing if this is an "unknown" job_id
                                if job_id == "unknown":
                                    logger.warning(f"[TraceManager._trace_writer_loop] {trace_info} Skipping trace with unknown job_id")
                                    continue
                                
                                # Check if we've already confirmed this job exists
                                job_exists = job_id in confirmed_jobs
                                
                                # If not confirmed, check the database
                                if not job_exists:
                                    # Check if job exists in executionhistory using the service
                                    execution = await execution_history_service.get_execution_by_job_id(job_id)
                                    
                                    if execution:
                                        # Job exists, add to confirmed set
                                        confirmed_jobs.add(job_id)
                                        job_exists = True
                                        logger.debug(f"[TraceManager._trace_writer_loop] {trace_info} Found existing job in database")
                                    else:
                                        # Job doesn't exist, create it
                                        logger.info(f"[TraceManager._trace_writer_loop] {trace_info} Job not found, creating new execution record")
                                        
                                        # Create minimal execution record
                                        job_data = {
                                            "job_id": job_id,
                                            "status": "running",
                                            "trigger_type": "api",
                                            "run_name": f"Auto-created for {event_type}",
                                            "inputs": {"auto_created": True}
                                        }
                                        
                                        # Try to create the job record
                                        success = await ExecutionStatusService.create_execution(job_data)
                                        
                                        if success:
                                            logger.info(f"[TraceManager._trace_writer_loop] {trace_info} Successfully created job record")
                                            confirmed_jobs.add(job_id)
                                            job_exists = True
                                        else:
                                            logger.error(f"[TraceManager._trace_writer_loop] {trace_info} Failed to create job record")
                                
                                # Detailed logging of trace data
                                logger.debug(f"[TraceManager._trace_writer_loop] {trace_info} Processing trace: {str(trace_data)[:200]}...")
                                
                                # Only proceed if job exists
                                if job_exists:
                                    # FILTER: Store important events in execution_trace
                                    # Include agent_execution, tool_usage, crew_started, crew_completed, task_started, task_completed
                                    important_event_types = [
                                        "agent_execution", "tool_usage", "crew_started", 
                                        "crew_completed", "task_started", "task_completed", "llm_call"
                                    ]
                                    
                                    if event_type in important_event_types:
                                        # Prepare trace data in the format expected by ExecutionTraceService
                                        trace_dict = {
                                            "job_id": job_id,
                                            "agent_name": trace_data.get("agent_name", "Unknown Agent"),
                                            "task_name": trace_data.get("task_name", "Unknown Task"),
                                            "event_type": event_type,
                                            "output": trace_data.get("output_content", ""),
                                            "trace_metadata": trace_data.get("extra_data", {})
                                        }
                                        
                                        try:
                                            # Use the ExecutionTraceService to create the trace
                                            await ExecutionTraceService.create_trace(trace_dict)
                                            logger.info(f"[TraceManager._trace_writer_loop] {trace_info} Successfully stored {event_type} trace")
                                        except Exception as e:
                                            logger.error(f"[TraceManager._trace_writer_loop] {trace_info} Failed to store trace: {e}")
                                            failures += 1
                                    else:
                                        # Log that we're skipping this trace type
                                        logger.debug(f"[TraceManager._trace_writer_loop] {trace_info} ⏭️ Skipping non-important event type: {event_type}")
                                else:
                                    logger.warning(f"[TraceManager._trace_writer_loop] {trace_info} Skipping trace due to missing job record")
                                    failures += 1
                                
                            except Exception as e:
                                logger.error(f"[TraceManager._trace_writer_loop] Error processing trace: {e}", exc_info=True)
                                failures += 1
                        
                        if failures > 0:
                            logger.warning(f"[TraceManager._trace_writer_loop] Batch #{batch_count} processed with {failures} failures.")
                        else:
                            logger.debug(f"[TraceManager._trace_writer_loop] Batch #{batch_count} processed successfully.")
                    
                    # If no traces were collected, sleep briefly to avoid CPU spinning
                    else:
                        await asyncio.sleep(0.5)
                    
                except Exception as e:
                    logger.error(f"[TraceManager._trace_writer_loop] Batch processing error: {e}", exc_info=True)
                    # Sleep to avoid rapid retry on persistent errors
                    await asyncio.sleep(1)
                
            logger.info("[TraceManager._trace_writer_loop] Shutdown event received, exiting trace writer loop.")
        
        except asyncio.CancelledError:
            logger.warning("[TraceManager._trace_writer_loop] Writer task cancelled.")
        except Exception as e:
            logger.critical(f"[TraceManager._trace_writer_loop] Unhandled exception in writer loop: {e}", exc_info=True)
        finally:
            logger.info("[TraceManager._trace_writer_loop] Writer task stopped.")

    @classmethod
    async def ensure_writer_started(cls):
        """Starts the writer task if it hasn't been started yet."""
        async with cls._lock:
            if not cls._writer_started:
                if cls._trace_writer_task is None or cls._trace_writer_task.done():
                    logger.info("[TraceManager] Starting trace writer task...")
                    cls._shutdown_event.clear()
                    cls._trace_writer_task = asyncio.create_task(cls._trace_writer_loop())
                    cls._writer_started = True # Mark as started
                    logger.info("[TraceManager] Trace writer task started.")
                else:
                    logger.debug("[TraceManager] Trace writer task already running (found existing task).")
                    cls._writer_started = True # Mark as started even if found existing
                
            # Also start the logs writer task if needed
            if cls._logs_writer_task is None or cls._logs_writer_task.done():
                logger.info("[TraceManager] Starting logs writer task...")
                # Import at function level to avoid circular imports
                from src.services.execution_logs_service import start_logs_writer
                # Start the logs writer using the service and store the task reference
                cls._logs_writer_task = await start_logs_writer(cls._shutdown_event)
                logger.info("[TraceManager] Logs writer task started.")
            else:
                logger.debug("[TraceManager] Logs writer task already running.")
            
    @classmethod
    async def stop_writer(cls):
        """Signals the writer task to stop."""
        async with cls._lock: # Ensure stop logic is sequential
            # Set shutdown event to signal both writer loops to stop
            logger.info("[TraceManager] Setting shutdown event for all writer tasks...")
            cls._shutdown_event.set()
            
            # Add None to trace queue to help unblock queue.get()
            try:
                from queue import Full
                from src.services.trace_queue import get_trace_queue
                queue = get_trace_queue()
                queue.put_nowait(None)
            except Full:
                logger.warning("[TraceManager] Trace queue full, writer might take longer to stop.")
            
            # Stop trace writer task
            if cls._writer_started and cls._trace_writer_task and not cls._trace_writer_task.done():
                logger.info("[TraceManager] Stopping trace writer task...")
                try:
                    await asyncio.wait_for(cls._trace_writer_task, timeout=5.0)
                    logger.info("[TraceManager] Trace writer task stopped.")
                except asyncio.TimeoutError:
                    logger.warning("[TraceManager] Trace writer task did not stop in time, cancelling.")
                    cls._trace_writer_task.cancel()
                except Exception as e:
                    logger.error(f"[TraceManager] Error stopping trace writer task: {e}", exc_info=True)
                finally:
                    cls._trace_writer_task = None
                    cls._writer_started = False # Mark as stopped
            else:
                 logger.debug("[TraceManager] Trace writer task not running or already stopped.")
                 cls._writer_started = False # Ensure marked as stopped
            
            # Use the logs writer service to stop the logs writer
            if cls._logs_writer_task and not cls._logs_writer_task.done():
                logger.info("[TraceManager] Stopping logs writer task...")
                # Import at function level to avoid circular imports
                from src.services.execution_logs_service import stop_logs_writer
                success = await stop_logs_writer(timeout=5.0)
                if success:
                    logger.info("[TraceManager] Logs writer task stopped successfully.")
                else:
                    logger.warning("[TraceManager] Failed to stop logs writer task gracefully.")
                cls._logs_writer_task = None
            else:
                logger.debug("[TraceManager] Logs writer task not running or already stopped.")