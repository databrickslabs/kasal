"""
Callback manager module for CrewAI flow execution.

This module handles initialization and management of callbacks for CrewAI flows.
"""
import logging
from typing import Dict, List, Optional, Any, Union

from src.core.logger import LoggerManager
from crewai.utilities.events import crewai_event_bus

# Initialize logger
logger = LoggerManager.get_instance().crew

class CallbackManager:
    """
    Helper class for managing callbacks in CrewAI flows.
    """
    
    @staticmethod
    def init_callbacks(job_id=None, config=None):
        """
        Initialize all necessary callbacks for flow execution.
        This ensures all event listeners are properly set up and registered 
        with the CrewAI event bus.
        
        Args:
            job_id: Optional job ID for tracking
            config: Optional configuration dictionary
            
        Returns:
            dict: Dictionary containing initialized callbacks
        """
        logger.info(f"Initializing callbacks for flow with job_id {job_id}")
        
        # Only create callbacks if we have a job_id
        if not job_id:
            logger.warning("No job_id provided, skipping callback initialization")
            return {'handlers': []}
        
        handlers = []
        callbacks_dict = {'handlers': handlers}
        
        try:
            # Create streaming callback for job output
            try:
                from src.engines.crewai.callbacks.streaming_callbacks import JobOutputCallback
                streaming_cb = JobOutputCallback(job_id=job_id, max_retries=3)
                logger.info(f"Created JobOutputCallback for job {job_id}")
                handlers.append(streaming_cb)
                callbacks_dict['streaming'] = streaming_cb
            except Exception as e:
                logger.warning(f"Error creating JobOutputCallback: {e}", exc_info=True)
                streaming_cb = None
            
            # Create event streaming callback to capture CrewAI events
            try:
                from src.engines.crewai.callbacks.streaming_callbacks import EventStreamingCallback
                event_streaming_cb = EventStreamingCallback(job_id=job_id, config=config)
                logger.info(f"Created EventStreamingCallback for job {job_id}")
                handlers.append(event_streaming_cb)
                callbacks_dict['event_streaming'] = event_streaming_cb
            except Exception as e:
                logger.warning(f"Error creating EventStreamingCallback: {e}", exc_info=True)
                event_streaming_cb = None
                
            # Create agent trace event listener for database trace recording
            try:
                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
                agent_trace_cb = AgentTraceEventListener(job_id=job_id)
                logger.info(f"Created AgentTraceEventListener for job {job_id}")
                handlers.append(agent_trace_cb)
                callbacks_dict['agent_trace'] = agent_trace_cb
            except Exception as e:
                logger.warning(f"Error creating AgentTraceEventListener: {e}", exc_info=True)
                agent_trace_cb = None
                
            # Create task completion logger
            try:
                from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionLogger
                task_completion_cb = TaskCompletionLogger(job_id=job_id)
                logger.info(f"Created TaskCompletionLogger for job {job_id}")
                handlers.append(task_completion_cb)
                callbacks_dict['task_completion'] = task_completion_cb
            except Exception as e:
                logger.warning(f"Error creating TaskCompletionLogger: {e}", exc_info=True)
                task_completion_cb = None
            
            # IMPORTANT: Explicitly ensure event listeners are registered
            CallbackManager.ensure_event_listeners_registered([
                handler for handler in handlers if handler is not None
            ])
            
            # Ensure the trace writer is started
            callbacks_dict['start_trace_writer'] = True
            
            logger.info(f"Successfully initialized {len(handlers)} callbacks for job {job_id}")
            return callbacks_dict
        except Exception as e:
            logger.error(f"Error initializing callbacks: {e}", exc_info=True)
            # Return empty callbacks dict if initialization fails
            return {'handlers': []}
    
    @staticmethod
    def ensure_event_listeners_registered(listeners):
        """
        Make sure event listeners are properly registered with CrewAI's event bus.
        
        Args:
            listeners: List of listener instances to register
        """
        if not listeners:
            return
        
        try:
            # Log that we're ensuring registration
            logger.info(f"Ensuring {len(listeners)} event listeners are registered with CrewAI event bus")
            
            # Register the event bus with each listener first to ensure proper initialization
            for i, listener in enumerate(listeners):
                if hasattr(listener, 'event_bus') and listener.event_bus is None:
                    listener.event_bus = crewai_event_bus
                    logger.info(f"Set event_bus for listener {i+1}/{len(listeners)}")
            
            # Next, explicitly register listeners with the event bus
            for i, listener in enumerate(listeners):
                listener_type = type(listener).__name__
                
                # If this listener has a setup_listeners method, call it explicitly
                if hasattr(listener, 'setup_listeners'):
                    try:
                        # Call setup_listeners with the event bus
                        listener.setup_listeners(crewai_event_bus)
                        logger.info(f"Successfully registered {listener_type} listener {i+1}/{len(listeners)}")
                    except Exception as e:
                        # Log any errors but continue
                        logger.warning(f"Error registering {listener_type} listener: {e}", exc_info=True)
                # Alternatively, try to register the listener directly with the event bus
                elif hasattr(crewai_event_bus, 'register'):
                    try:
                        crewai_event_bus.register(listener)
                        logger.info(f"Directly registered {listener_type} listener {i+1}/{len(listeners)} with event bus")
                    except Exception as e:
                        logger.warning(f"Error directly registering {listener_type} listener: {e}", exc_info=True)
                else:
                    logger.warning(f"Listener {listener_type} has no setup_listeners method, might not be properly registered")
                
                # Ensure all event methods are properly connected for this listener
                if hasattr(listener, 'connect_events') and callable(listener.connect_events):
                    try:
                        listener.connect_events()
                        logger.info(f"Connected events for {listener_type} listener")
                    except Exception as e:
                        logger.warning(f"Error connecting events for {listener_type} listener: {e}", exc_info=True)
        except Exception as e:
            # If anything fails, log it but don't crash
            logger.error(f"Error ensuring event listeners are registered: {e}", exc_info=True)
    
    @staticmethod
    def cleanup_callbacks(callbacks):
        """
        Clean up callbacks after flow execution.
        
        Args:
            callbacks: Dictionary of callbacks to clean up
        """
        if not callbacks:
            return
        
        # Clean up the event streaming callback
        event_streaming_cb = callbacks.get('event_streaming')
        if event_streaming_cb:
            try:
                logger.info(f"Cleaning up EventStreamingCallback")
                event_streaming_cb.cleanup()
                logger.info("EventStreamingCallback cleanup completed successfully")
            except Exception as cleanup_error:
                logger.warning(f"Error during EventStreamingCallback cleanup: {cleanup_error}", exc_info=True)
        
        # Get other callbacks for cleanup
        agent_trace = callbacks.get('agent_trace')
        
        # Log completion for trace purposes
        if agent_trace:
            try:
                logger.info("Ensuring traces are processed")
                # The trace data will be processed by the TraceManager in the background
                # No explicit cleanup needed as TraceManager handles the queue
            except Exception as cleanup_error:
                logger.warning(f"Error during trace cleanup: {cleanup_error}", exc_info=True) 