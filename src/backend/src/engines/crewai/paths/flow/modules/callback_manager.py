"""Callback orchestration manager for CrewAI flow execution.

This module provides centralized management of all callbacks used during
CrewAI flow execution, ensuring proper initialization, registration, and
lifecycle management of event listeners.

The CallbackManager acts as the single point of coordination for all
callback-related operations in the flow execution pipeline, managing
various types of callbacks including streaming, tracing, and event logging.

Key Features:
    - Centralized callback initialization and registration
    - Automatic event bus integration
    - Error-resilient callback setup with fallbacks
    - Support for multiple callback types (streaming, tracing, events)
    - Multi-tenant isolation through group context

Callback Types Managed:
    - JobOutputCallback: Streams execution output to database
    - EventStreamingCallback: Captures real-time CrewAI events
    - AgentTraceEventListener: Records execution traces
    - Custom event listeners via BaseEventListener

Example:
    >>> callbacks = CallbackManager.init_callbacks(
    ...     job_id="exec_123",
    ...     config={"enable_streaming": True},
    ...     group_context=user_context
    ... )
    >>> # Use callbacks in flow execution
    >>> flow.execute(callbacks=callbacks['handlers'])
"""
import logging
from typing import Dict, List, Optional, Any, Union

from src.core.logger import LoggerManager
from crewai.events import crewai_event_bus

# Initialize logger
logger = LoggerManager.get_instance().flow

class CallbackManager:
    """Centralized manager for CrewAI flow execution callbacks.
    
    This class provides static methods for initializing, registering, and
    managing all callbacks required during CrewAI flow execution. It ensures
    proper setup of event listeners and their registration with the CrewAI
    event bus system.
    
    The manager handles various callback types including streaming callbacks
    for real-time output, trace callbacks for execution monitoring, and
    event callbacks for capturing CrewAI events.
    
    Methods:
        init_callbacks: Initialize all callbacks for a flow execution
        ensure_event_listeners_registered: Register listeners with event bus
        cleanup_callbacks: Clean up and unregister callbacks
    
    Note:
        All methods are static as the manager doesn't maintain state
        between callback initializations. Each flow execution gets its
        own set of callback instances.
    """
    
    @staticmethod
    def init_callbacks(job_id=None, config=None, group_context=None):
        """Initialize all necessary callbacks for flow execution.
        
        Creates and configures all required callbacks for monitoring and
        streaming flow execution. Ensures proper registration with the
        CrewAI event bus and handles initialization failures gracefully.
        
        Args:
            job_id: Optional unique identifier for the execution.
                Required for callback initialization. If not provided,
                returns empty callback list.
            config: Optional configuration dictionary controlling:
                - enable_streaming: Enable/disable output streaming
                - enable_tracing: Enable/disable execution tracing
                - callback_settings: Additional callback-specific settings
            group_context: Optional multi-tenant context containing:
                - primary_group_id: Group identifier for isolation
                - access_token: User authentication token
                - group_email: Group email for notifications
            
        Returns:
            Dict[str, Any]: Dictionary containing:
                - handlers: List of initialized callback instances
                - streaming: JobOutputCallback instance (if created)
                - event_streaming: EventStreamingCallback instance (if created)
                - agent_trace: AgentTraceEventListener instance (if created)
                - start_trace_writer: Boolean indicating trace writer needed
        
        Note:
            The method is resilient to individual callback failures and will
            continue initializing other callbacks even if one fails.
        
        Example:
            >>> callbacks = CallbackManager.init_callbacks(
            ...     job_id="exec_123",
            ...     config={"enable_streaming": True},
            ...     group_context=GroupContext(primary_group_id="grp_456")
            ... )
            >>> crew.kickoff(callbacks=callbacks['handlers'])
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
                streaming_cb = JobOutputCallback(job_id=job_id, max_retries=3, group_context=group_context)
                logger.info(f"Created JobOutputCallback for job {job_id}")
                handlers.append(streaming_cb)
                callbacks_dict['streaming'] = streaming_cb
            except Exception as e:
                logger.warning(f"Error creating JobOutputCallback: {e}", exc_info=True)
                streaming_cb = None
            
            # Create event streaming callback to capture CrewAI events
            try:
                from src.engines.crewai.callbacks.streaming_callbacks import EventStreamingCallback
                event_streaming_cb = EventStreamingCallback(job_id=job_id, config=config, group_context=group_context)
                logger.info(f"Created EventStreamingCallback for job {job_id}")
                handlers.append(event_streaming_cb)
                callbacks_dict['event_streaming'] = event_streaming_cb
            except Exception as e:
                logger.warning(f"Error creating EventStreamingCallback: {e}", exc_info=True)
                event_streaming_cb = None
                
            # Create agent trace event listener for database trace recording
            try:
                from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
                agent_trace_cb = AgentTraceEventListener(job_id=job_id, group_context=group_context)
                logger.info(f"Created AgentTraceEventListener for job {job_id}")
                handlers.append(agent_trace_cb)
                callbacks_dict['agent_trace'] = agent_trace_cb
            except Exception as e:
                logger.warning(f"Error creating AgentTraceEventListener: {e}", exc_info=True)
                agent_trace_cb = None
                
            # Create task completion logger - DISABLED to prevent duplicates
            # The AgentTraceEventListener already handles task completion events
            # try:
            #     from src.engines.crewai.callbacks.logging_callbacks import TaskCompletionLogger
            #     task_completion_cb = TaskCompletionLogger(job_id=job_id)
            #     logger.info(f"Created TaskCompletionLogger for job {job_id}")
            #     handlers.append(task_completion_cb)
            #     callbacks_dict['task_completion'] = task_completion_cb
            # except Exception as e:
            #     logger.warning(f"Error creating TaskCompletionLogger: {e}", exc_info=True)
            #     task_completion_cb = None
            
            # IMPORTANT: Event listeners inheriting from BaseEventListener are automatically 
            # registered in their __init__ method, so we don't need to register them again.
            # Only register listeners that don't inherit from BaseEventListener
            non_base_listeners = []
            for handler in handlers:
                if handler is not None and not hasattr(handler, 'setup_listeners'):
                    # This handler doesn't inherit from BaseEventListener
                    non_base_listeners.append(handler)
            
            if non_base_listeners:
                CallbackManager.ensure_event_listeners_registered(non_base_listeners)
            
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
                    # Some callbacks like EventStreamingCallback and JobOutputCallback self-register
                    # via decorators or CrewAI's callback system, so no explicit registration is needed
                    logger.debug(f"Listener {listener_type} uses self-registration (decorators or CrewAI callback system)")
                
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