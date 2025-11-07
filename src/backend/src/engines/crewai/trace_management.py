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
        from src.services.execution_history_service import ExecutionHistoryService
        from src.db.database_router import get_smart_db_session
        from queue import Empty  # Import the Empty exception from queue module

        try:
            logger.info("[TraceManager._trace_writer_loop] Writer task started.")

            # Get trace queue
            queue = get_trace_queue()
            logger.debug(f"[TraceManager._trace_writer_loop] Queue retrieved. Initial approximate size: {queue.qsize()}")

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
                                logger.debug(f"[TRACE_DEBUG] Checking queue... Queue size: ~{queue.qsize()}")

                            # Non-blocking get with timeout
                            trace_data = queue.get(block=True, timeout=0.1)
                            logger.debug(f"[TRACE_DEBUG] Got trace from queue: type={trace_data.get('event_type') if trace_data else 'None'}")
                            
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
                                    # Check if job exists in executionhistory using the service with managed session
                                    async for session in get_smart_db_session():
                                        execution_history_service = ExecutionHistoryService(session)
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
                                    # Broadcast task status events via WebSocket for real-time updates
                                    if event_type in ["TASK_STARTED", "TASK_COMPLETED", "TASK_FAILED"]:
                                        from src.services.execution_logs_service import execution_logs_service
                                        import json
                                        
                                        task_status_msg = json.dumps({
                                            "type": "task_status_update",
                                            "event_type": event_type,
                                            "task_id": trace_data.get("task_id"),
                                            "task_name": trace_data.get("event_context"),
                                            "timestamp": trace_data.get("created_at", datetime.now().isoformat()) if isinstance(trace_data.get("created_at"), str) else datetime.now().isoformat(),
                                            "output": trace_data.get("output")
                                        })
                                        
                                        # Extract group context if available
                                        group_context = trace_data.get("group_context")
                                        
                                        # Broadcast the task status update
                                        await execution_logs_service.broadcast_to_execution(
                                            job_id,
                                            task_status_msg,
                                            group_context
                                        )
                                        logger.debug(f"[TraceManager._trace_writer_loop] Broadcast task status update for {event_type} - task: {trace_data.get('event_context')}")
                                    
                                    # FILTER: Store important events in execution_trace
                                    # Include agent_execution, tool_usage, crew_started, crew_completed, task_started, task_completed
                                    important_event_types = [
                                        "agent_execution", "tool_usage", "tool_error",
                                        "crew_started", "crew_completed",
                                        "task_started", "task_completed", "task_failed",
                                        "llm_call", "llm_guardrail",
                                        "memory_write", "memory_retrieval",
                                        "memory_write_started", "memory_retrieval_started",
                                        "knowledge_retrieval", "knowledge_retrieval_started",
                                        "agent_reasoning", "agent_reasoning_error"
                                    ]

                                    # Debug-only events that should be suppressed when debug tracing is disabled
                                    debug_only_event_types = {
                                        "memory_write_started", "memory_retrieval_started",
                                        "memory_write", "memory_retrieval",
                                        "knowledge_retrieval_started", "knowledge_retrieval",
                                        "agent_reasoning", "agent_reasoning_error",
                                        "llm_guardrail",
                                    }

                                    if event_type in important_event_types:
                                        # Respect engine debug tracing config for verbose events
                                        try:
                                            if not hasattr(TraceManager, "_debug_tracing_enabled_cache"):
                                                TraceManager._debug_tracing_enabled_cache = None
                                            if TraceManager._debug_tracing_enabled_cache is None:
                                                from src.services.engine_config_service import EngineConfigService
                                                from src.db.session import async_session_factory
                                                async with async_session_factory() as cfg_session:
                                                    cfg_service = EngineConfigService(cfg_session)
                                                    TraceManager._debug_tracing_enabled_cache = await cfg_service.get_crewai_debug_tracing()
                                            if (event_type in debug_only_event_types) and (TraceManager._debug_tracing_enabled_cache is False):
                                                logger.debug(f"[TraceManager._trace_writer_loop] {trace_info} Debug tracing disabled - skipping {event_type}")
                                                continue
                                        except Exception as cfg_err:
                                            logger.debug(f"[TraceManager._trace_writer_loop] Could not read debug tracing flag: {cfg_err}. Defaulting to enabled.")

                                        # Prepare trace data in the format expected by ExecutionTraceService
                                        # Extract output content from the nested structure
                                        output_data = trace_data.get("output", {})
                                        if isinstance(output_data, dict):
                                            output_content = output_data.get("content")
                                            if output_content is None:
                                                # No 'content' field - serialize the entire dict to JSON for visibility
                                                try:
                                                    import json
                                                    output_content = json.dumps(output_data, ensure_ascii=False)
                                                except Exception:
                                                    output_content = str(output_data)
                                        else:
                                            output_content = str(output_data) if output_data else ""

                                        # If no explicit trace_metadata, reuse the structured output_data
                                        trace_metadata = trace_data.get("trace_metadata", trace_data.get("extra_data"))
                                        if trace_metadata is None and isinstance(output_data, dict):
                                            trace_metadata = output_data

                                        trace_dict = {
                                            "job_id": job_id,
                                            "event_source": trace_data.get("event_source", event_type),  # Use event_type as fallback
                                            "event_context": trace_data.get("event_context", ""),
                                            "event_type": event_type,
                                            "output": output_content,
                                            "trace_metadata": trace_metadata or {}
                                        }

                                        # Add group context if available in trace data
                                        if "group_id" in trace_data:
                                            trace_dict["group_id"] = trace_data["group_id"]
                                        if "group_email" in trace_data:
                                            trace_dict["group_email"] = trace_data["group_email"]

                                        try:
                                            # Create ExecutionTraceService with session and use it to create the trace
                                            logger.debug(f"[TRACE_DEBUG] About to write trace to DB: job_id={job_id}, event_type={event_type}")
                                            async for session in get_smart_db_session():
                                                trace_service = ExecutionTraceService(session)
                                                await trace_service.create_trace(trace_dict)
                                            logger.debug(f"[TRACE_DEBUG] Successfully wrote trace to DB")
                                            logger.info(f"[TraceManager._trace_writer_loop] {trace_info} Successfully stored {event_type} trace")
                                        except ValueError as e:
                                            # This is expected when job doesn't exist - not a failure
                                            logger.debug(f"[TraceManager._trace_writer_loop] {trace_info} Trace skipped (job doesn't exist): {e}")
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
        logger.debug("[TRACE_DEBUG] ensure_writer_started called")
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