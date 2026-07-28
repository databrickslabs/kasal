"""
CrewAI Executor Service with Best Practices

This module provides a properly configured executor for running CrewAI crews
following thread management best practices.
"""
import asyncio
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, Future
from typing import Dict, Any, Optional, Callable
from datetime import datetime
import atexit

logger = logging.getLogger(__name__)


class CrewExecutor:
    """
    Manages crew executions with a custom ThreadPoolExecutor following best practices.
    
    Features:
    - Custom thread pool sized for I/O-bound CrewAI operations
    - Descriptive thread naming for debugging
    - Proper lifecycle management with context managers
    - Cooperative cancellation support
    - Monitoring and metrics
    """
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        """Singleton pattern to ensure single executor instance."""
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, max_workers: int = 20):
        """
        Initialize the crew executor.
        
        Args:
            max_workers: Maximum number of worker threads (default 20 for I/O-bound tasks)
        """
        if self._initialized:
            return
        self._initialized = True
        
        # Create custom executor for crew operations
        # Higher thread count for I/O-bound CrewAI operations (API calls, etc.)
        self._executor = ThreadPoolExecutor(
            max_workers=max_workers,
            thread_name_prefix='CrewWorker'
        )
        
        # Track active executions
        self._active_executions: Dict[str, Dict[str, Any]] = {}
        
        # Stop events for cooperative cancellation
        self._stop_events: Dict[str, threading.Event] = {}
        
        # Track running asyncio tasks for cancellation
        self._running_tasks: Dict[str, asyncio.Task] = {}
        
        # Metrics tracking
        self._metrics = {
            'total_executions': 0,
            'active_executions': 0,
            'completed_executions': 0,
            'failed_executions': 0,
            'cancelled_executions': 0,
            'total_duration_seconds': 0.0
        }
        
        # Register cleanup on exit
        atexit.register(self.shutdown)
        
        logger.info(f"CrewExecutor initialized with {max_workers} workers")
    
    async def run_crew(
        self,
        execution_id: str,
        crew: Any,
        inputs: Optional[Dict[str, Any]] = None,
        on_complete: Optional[Callable] = None,
        on_error: Optional[Callable] = None,
        timeout: Optional[float] = None
    ) -> Any:
        """
        Run a crew execution with proper management.
        
        Args:
            execution_id: Unique identifier for the execution
            crew: The CrewAI crew to execute
            inputs: Optional inputs for the crew
            on_complete: Callback for successful completion
            on_error: Callback for errors
            timeout: Optional timeout in seconds
            
        Returns:
            The crew execution result
            
        Raises:
            asyncio.TimeoutError: If execution exceeds timeout
            asyncio.CancelledError: If execution is cancelled
        """
        # Create stop event for cooperative cancellation
        stop_event = threading.Event()
        self._stop_events[execution_id] = stop_event
        
        # Track execution
        start_time = datetime.now()
        self._active_executions[execution_id] = {
            'crew': crew,
            'start_time': start_time,
            'stop_event': stop_event,
            'status': 'RUNNING'
        }
        self._metrics['total_executions'] += 1
        self._metrics['active_executions'] += 1
        
        try:
            # Create wrapper that sets thread name and checks stop event
            def crew_wrapper():
                # Set descriptive thread name
                current_thread = threading.current_thread()
                original_name = current_thread.name
                current_thread.name = f"Crew_{execution_id[:8]}"
                
                try:
                    logger.info(f"Starting crew execution {execution_id} in thread {current_thread.name}")
                    
                    # TODO: Inject stop event checking into crew execution
                    # For now, just run the crew normally
                    if inputs:
                        result = crew.kickoff(inputs=inputs)
                    else:
                        result = crew.kickoff()
                    
                    # Check if we were asked to stop
                    if stop_event.is_set():
                        raise asyncio.CancelledError("Execution was cancelled")
                    
                    return result
                    
                finally:
                    # Restore original thread name
                    current_thread.name = original_name
            
            # Run in executor with optional timeout
            loop = asyncio.get_event_loop()
            
            # Create the task and track it for cancellation
            future = loop.run_in_executor(self._executor, crew_wrapper)
            
            # Store the future directly for cancellation
            # Don't use shield() as it prevents cancellation
            self._running_tasks[execution_id] = future
            
            try:
                if timeout:
                    result = await asyncio.wait_for(future, timeout=timeout)
                else:
                    result = await future
            finally:
                # Clean up task tracking
                if execution_id in self._running_tasks:
                    del self._running_tasks[execution_id]
            
            # Update metrics
            duration = (datetime.now() - start_time).total_seconds()
            self._metrics['completed_executions'] += 1
            self._metrics['total_duration_seconds'] += duration
            self._active_executions[execution_id]['status'] = 'COMPLETED'
            
            # Call success callback
            if on_complete:
                on_complete(result)
            
            logger.info(f"Crew execution {execution_id} completed in {duration:.2f} seconds")
            return result
            
        except asyncio.CancelledError:
            # Handle cancellation
            self._metrics['cancelled_executions'] += 1
            self._active_executions[execution_id]['status'] = 'CANCELLED'
            logger.info(f"Crew execution {execution_id} was cancelled")
            raise
            
        except asyncio.TimeoutError:
            # Handle timeout
            self._metrics['failed_executions'] += 1
            self._active_executions[execution_id]['status'] = 'TIMEOUT'
            stop_event.set()  # Signal thread to stop
            logger.error(f"Crew execution {execution_id} timed out")
            if on_error:
                on_error(asyncio.TimeoutError(f"Execution timed out after {timeout} seconds"))
            raise
            
        except Exception as e:
            # Handle other errors
            self._metrics['failed_executions'] += 1
            self._active_executions[execution_id]['status'] = 'FAILED'
            logger.error(f"Crew execution {execution_id} failed: {e}")
            if on_error:
                on_error(e)
            raise
            
        finally:
            # Cleanup
            self._metrics['active_executions'] -= 1
            if execution_id in self._stop_events:
                del self._stop_events[execution_id]
            # Mark execution as done and clean up old executions
            if execution_id in self._active_executions:
                self._active_executions[execution_id]['end_time'] = datetime.now()
                
                # Clean up old completed executions (keep only last 100)
                # This prevents memory leaks from accumulating execution history
                completed_executions = [
                    exec_id for exec_id, info in self._active_executions.items()
                    if info.get('status') in ['COMPLETED', 'FAILED', 'CANCELLED', 'TIMEOUT', 'STOPPED']
                ]
                
                if len(completed_executions) > 100:
                    # Remove the oldest completed executions
                    oldest = sorted(
                        completed_executions,
                        key=lambda x: self._active_executions[x].get('end_time', datetime.min)
                    )[:len(completed_executions) - 100]
                    
                    for old_exec_id in oldest:
                        del self._active_executions[old_exec_id]
                        logger.debug(f"Cleaned up old execution {old_exec_id} from history")
    
    def request_stop(self, execution_id: str) -> bool:
        """
        Request cooperative stop for an execution.
        
        Args:
            execution_id: The execution to stop
            
        Returns:
            True if stop was requested, False if execution not found or already completed
        """
        # Check if execution exists and is still running
        if execution_id in self._active_executions:
            exec_info = self._active_executions[execution_id]
            status = exec_info.get('status', 'UNKNOWN')
            
            if status == 'RUNNING':
                # Set stop event for cooperative cancellation
                if execution_id in self._stop_events:
                    self._stop_events[execution_id].set()
                    logger.info(f"Set stop event for execution {execution_id}")
                
                # Cancel the asyncio future/task for immediate termination
                if execution_id in self._running_tasks:
                    future = self._running_tasks[execution_id]
                    if not future.done():
                        # Cancel the future - this will raise CancelledError in the running task
                        cancelled = future.cancel()
                        if cancelled:
                            logger.info(f"Successfully cancelled future for execution {execution_id}")
                            # Update status to indicate cancellation in progress
                            self._active_executions[execution_id]['status'] = 'STOPPING'
                        else:
                            logger.warning(f"Failed to cancel future for execution {execution_id} (may be running in thread)")
                            # For futures from run_in_executor, we can't truly cancel the thread
                            # but we can still set the stop event
                        return True
                    else:
                        logger.info(f"Future for execution {execution_id} already done")
                else:
                    logger.warning(f"No running future found for execution {execution_id}")
                
                return True
            else:
                # Execution already completed or failed
                logger.info(f"Execution {execution_id} already has status: {status}")
                return False
        else:
            logger.warning(f"Execution {execution_id} not found in active executions")
            return False
    
    def get_metrics(self) -> Dict[str, Any]:
        """
        Get execution metrics.
        
        Returns:
            Dictionary of metrics
        """
        metrics = self._metrics.copy()
        if metrics['completed_executions'] > 0:
            metrics['average_duration_seconds'] = (
                metrics['total_duration_seconds'] / metrics['completed_executions']
            )
        return metrics
    
    def get_active_executions(self) -> Dict[str, Any]:
        """
        Get information about active executions.
        
        Returns:
            Dictionary of active execution information
        """
        return {
            exec_id: {
                'status': info['status'],
                'start_time': info['start_time'].isoformat(),
                'duration_seconds': (datetime.now() - info['start_time']).total_seconds()
            }
            for exec_id, info in self._active_executions.items()
            if info['status'] == 'RUNNING'
        }
    
    def shutdown(self, wait: bool = True):
        """
        Shutdown the executor gracefully.
        
        Args:
            wait: Whether to wait for pending tasks to complete
        """
        logger.info("Shutting down CrewExecutor")
        
        # Signal all active executions to stop
        for stop_event in self._stop_events.values():
            stop_event.set()
        
        # Shutdown the executor
        self._executor.shutdown(wait=wait)
        
        logger.info(f"CrewExecutor shutdown complete. Final metrics: {self.get_metrics()}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - ensures proper cleanup."""
        self.shutdown(wait=True)
        return False


# Global instance
crew_executor = CrewExecutor()


# Helper function for backwards compatibility
async def run_crew_with_executor(
    execution_id: str,
    crew: Any,
    inputs: Optional[Dict[str, Any]] = None,
    timeout: Optional[float] = None
) -> Any:
    """
    Convenience function to run a crew using the global executor.
    
    Args:
        execution_id: Unique identifier for the execution
        crew: The CrewAI crew to execute
        inputs: Optional inputs for the crew
        timeout: Optional timeout in seconds
        
    Returns:
        The crew execution result
    """
    return await crew_executor.run_crew(
        execution_id=execution_id,
        crew=crew,
        inputs=inputs,
        timeout=timeout
    )