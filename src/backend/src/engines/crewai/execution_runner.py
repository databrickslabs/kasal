"""
Execution Runner for CrewAI engine.

This module provides functionality for running CrewAI crews and handling
the execution lifecycle.
"""
import logging
import asyncio
import traceback
import threading
from typing import Any, Dict, Optional
import os
from src.services.crew_executor import crew_executor
from src.services.process_crew_executor import process_crew_executor


from crewai import Crew, LLM
from src.models.execution_status import ExecutionStatus
from src.core.llm_manager import LLMManager
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

async def run_crew(execution_id: str, crew: Crew, running_jobs: Dict, group_context: GroupContext = None, user_token: str = None, config: Dict[str, Any] = None) -> None:
    """
    Run the crew in a separate task, ensuring final status update
    occurs within its own database session scope.
    
    Args:
        execution_id: Execution ID
        crew: The CrewAI crew to run
        running_jobs: Dictionary tracking running jobs
        group_context: Group context for logging isolation
        user_token: User access token for OAuth authentication
        config: Execution configuration containing inputs
    """
    # Set user context for this execution to enable OAuth authentication in tools
    if user_token or group_context:
        from src.utils.user_context import UserContext
        
        if user_token:
            UserContext.set_user_token(user_token)
            logger.info(f"Set user token for execution {execution_id}")
        
        if group_context:
            UserContext.set_group_context(group_context)
            logger.info(f"Set group context for execution {execution_id}: {group_context.primary_group_id}")
    else:
        logger.warning(f"No user token or group context provided for execution {execution_id}")
    
    # First, ensure status is set to RUNNING
    from src.services.execution_status_service import ExecutionStatusService
    await ExecutionStatusService.update_status(
        job_id=execution_id,
        status=ExecutionStatus.RUNNING.value,
        message="CrewAI execution is running"
    )
    logger.info(f"Set status to RUNNING for execution {execution_id}")
    
    final_status = ExecutionStatus.FAILED.value # Default to FAILED
    final_message = "An unexpected error occurred during crew execution."
    final_result = None
    
    # Set up CrewLogger and execution-scoped callbacks
    from src.engines.crewai.crew_logger import crew_logger
    from src.engines.crewai.callbacks.streaming_callbacks import EventStreamingCallback
    from src.engines.crewai.callbacks.execution_callback import (
        create_execution_callbacks, 
        create_crew_callbacks, 
        log_crew_initialization
    )
    
    # Get the job configuration from the running jobs dictionary
    config = None
    max_retry_limit = 2  # Default retry limit 
    model = None
    
    if execution_id in running_jobs:
        config = running_jobs[execution_id].get("config", {})

        # Get the original_config if it exists
        original_config = config.get("original_config")
        if original_config:
            # We'll use the original configuration from the frontend
            config = original_config
            model = config.get("model")
            
        # Extract max_retry_limit from agent configs if available
        agents = config.get("agents", [])
        if agents:
            # Handle both list and dict formats
            if isinstance(agents, dict):
                agent_configs = agents.values()
            else:
                agent_configs = agents
            
            # Get highest retry limit from all agents
            for agent_config in agent_configs:
                if isinstance(agent_config, dict) and "max_retry_limit" in agent_config:
                    agent_retry_limit = int(agent_config.get("max_retry_limit", 2))
                    max_retry_limit = max(max_retry_limit, agent_retry_limit)
    
    logger.info(f"Using max_retry_limit={max_retry_limit} for execution {execution_id}")
    
    # Initialize logging for this job
    crew_logger.setup_for_job(execution_id, group_context)
    
    # Create execution-scoped callbacks (replaces global event listeners)
    # Pass the crew for enhanced context tracking
    step_callback, task_callback = create_execution_callbacks(
        job_id=execution_id, 
        config=config, 
        group_context=group_context,
        crew=crew
    )
    
    # Set callbacks directly on the crew instance
    try:
        crew.step_callback = step_callback
        crew.task_callback = task_callback
        logger.info(f"Set execution-scoped callbacks on crew for {execution_id}")
    except Exception as callback_error:
        logger.error(f"Failed to set callbacks on crew for {execution_id}: {callback_error}")
        # Continue execution - callbacks are for enhanced logging, not critical functionality
    
    # Create crew lifecycle callbacks
    crew_callbacks = create_crew_callbacks(
        job_id=execution_id,
        config=config,
        group_context=group_context
    )
    
    # Log crew initialization
    log_crew_initialization(execution_id, config, group_context)
    
    # Initialize AgentTraceEventListener for trace processing (without global event listeners)
    from src.engines.crewai.callbacks.logging_callbacks import AgentTraceEventListener
    from src.services.engine_config_service import EngineConfigService
    from src.db.session import get_db

    # Fetch debug tracing flag from engine configuration (default False on errors)
    debug_tracing_enabled = False
    try:
        from src.db.session import async_session_factory
        async with async_session_factory() as session:
            service = EngineConfigService(session)
            debug_tracing_enabled = await service.get_crewai_debug_tracing()
    except Exception as e:
        logger.warning(f"Failed to read CrewAI debug tracing flag; defaulting to False. Error: {e}")

    logger.debug(f"[TRACE_DEBUG] Creating AgentTraceEventListener for execution {execution_id} (debug_tracing={debug_tracing_enabled})")
    trace_listener = AgentTraceEventListener(job_id=execution_id, group_context=group_context, debug_tracing=debug_tracing_enabled)
    logger.debug(f"[TRACE_DEBUG] AgentTraceEventListener created successfully")

    # CRITICAL: Register the event listeners with the CrewAI event bus
    # This was missing and caused events not to be captured
    from crewai.events import crewai_event_bus
    logger.debug(f"[TRACE_DEBUG] About to register event listeners with CrewAI event bus")
    trace_listener.setup_listeners(crewai_event_bus)
    logger.debug(f"[TRACE_DEBUG] Registered AgentTraceEventListener with CrewAI event bus for execution {execution_id}")

    # Debug: Check if event bus has handlers registered
    logger.debug(f"[TRACE_DEBUG] Event bus type: {type(crewai_event_bus)}")
    logger.debug(f"[TRACE_DEBUG] Event bus has handlers: {hasattr(crewai_event_bus, '_handlers')}")
    
    # Note: LLM event routing has been deprecated in CrewAI 0.177+
    # LLM events are now captured through AgentExecutionCompletedEvent in logging_callbacks
    logger.info(f"LLM events will be captured through AgentExecutionCompletedEvent for execution {execution_id}")
    
    # Start the trace writer to process queued traces
    from src.engines.crewai.trace_management import TraceManager
    await TraceManager.ensure_writer_started()
    
    # Initialize event streaming with configuration and group context
    event_streaming = EventStreamingCallback(job_id=execution_id, config=config, group_context=group_context)
    
    # Retry counter
    retry_count = 0
    
    # For capturing the specific error
    last_error = None
    
    # Keep trying until we exceed max retries
    result = None  # Initialize result variable to avoid UnboundLocalError
    while retry_count <= max_retry_limit:
        try:
            # IMPORTANT: Configure LLM for CrewAI before running the crew
            if model:
                try:
                    logger.info(f"Global model configuration detected: {model}")
                    
                    # Add detailed debugging for agent LLM attributes
                    logger.info("Debugging agent LLM configurations:")
                    for idx, agent in enumerate(crew.agents):
                        logger.info(f"Agent {idx} - Role: {agent.role}")
                        logger.info(f"Agent {idx} - Has llm attr: {hasattr(agent, 'llm')}")
                        if hasattr(agent, 'llm'):
                            logger.info(f"Agent {idx} - LLM type: {type(agent.llm)}")
                            logger.info(f"Agent {idx} - LLM value: {agent.llm}")
                        # Check all attributes of the agent
                        agent_attrs = vars(agent)
                        logger.info(f"Agent {idx} - All attributes: {agent_attrs}")
                    
                    # Check for agents with custom LLM configurations
                    # Note: agent_helpers.py should have already configured LLMs properly
                    # This is just a fallback for any agents that might still need configuration
                    agents_needing_llm = []
                    for agent in crew.agents:
                        # An agent needs an LLM if it doesn't have one at all
                        if not hasattr(agent, 'llm') or agent.llm is None:
                            agents_needing_llm.append(agent.role)
                    
                    if agents_needing_llm:
                        logger.info(f"Some agents need LLM configuration: {agents_needing_llm}")
                        
                        # Only configure LLM for agents that need it
                        for agent in crew.agents:
                            if not hasattr(agent, 'llm') or agent.llm is None:
                                # Use LLMManager to configure CrewAI LLM
                                # SECURITY: Pass group_id for multi-tenant isolation
                                group_id = config.get('group_id') if config else None
                                if not group_id:
                                    raise ValueError("group_id is REQUIRED for LLM configuration")
                                crewai_llm = await LLMManager.configure_crewai_llm(model, group_id)
                                agent.llm = crewai_llm
                                logger.info(f"Updated agent {agent.role} with global LLM {model}")
                    else:
                        logger.info(f"All agents already have LLM configurations, no global override needed")
                    
                    logger.info(f"LLM configuration verification completed")
                    
                except Exception as config_error:
                    logger.error(f"Error verifying LLM configurations: {str(config_error)}")
                    raise ValueError(f"Failed to verify LLM configurations: {str(config_error)}")
            
            # Ensure API keys are properly set in environment variables
            # This is crucial for tools and models that use environment variables directly
            try:
                # Import ApiKeysService to ensure API keys are in environment
                from src.services.api_keys_service import ApiKeysService

                # SECURITY: Get group_id for multi-tenant isolation
                group_id = group_context.primary_group_id if group_context else None

                # Explicitly set up API keys for common providers
                # Handle OpenAI API key properly
                try:
                    openai_key = await ApiKeysService.get_provider_api_key("openai", group_id=group_id)
                    if openai_key:
                        # OpenAI key is configured, set it up
                        os.environ["OPENAI_API_KEY"] = openai_key
                        logger.info("OpenAI API key configured and set up")
                    else:
                        # No OpenAI key configured, set dummy key to satisfy CrewAI validation
                        os.environ["OPENAI_API_KEY"] = "sk-dummy-validation-key"
                        logger.info("No OpenAI API key configured, set dummy key for validation")
                except Exception as e:
                    logger.warning(f"Error setting up OpenAI API key: {e}")
                    # Set dummy key to satisfy CrewAI validation
                    os.environ["OPENAI_API_KEY"] = "sk-dummy-validation-key"
                
                # Log API key status (don't log the actual keys)
                logger.info("Verified API keys are set in environment variables")
                for env_var in ["OPENAI_API_KEY", "ANTHROPIC_API_KEY", "GEMINI_API_KEY"]:
                    if os.environ.get(env_var):
                        logger.info(f"{env_var} is properly set")
                    else:
                        logger.warning(f"{env_var} is NOT set in environment")
                
                # Apply Gemini compatibility patches if needed
                gemini_model_detected = False
                for agent in crew.agents:
                    if hasattr(agent, 'llm') and hasattr(agent.llm, 'model') and isinstance(agent.llm.model, str):
                        if "gemini" in agent.llm.model.lower():
                            gemini_model_detected = True
                            break
                
                if gemini_model_detected:
                    logger.info("Gemini model detected, applying compatibility patches")
                    
                    # Set Instructor to be aware of Gemini limitations
                    os.environ["INSTRUCTOR_MODEL_NAME"] = "gemini"
                    
                    # Add monkey patch for CrewAI's Instructor integration if Gemini is being used
                    try:
                        import json
                        from crewai.utilities.internal_instructor import InternalInstructor
                        
                        # Store the original method
                        original_to_pydantic = InternalInstructor.to_pydantic
                        
                        # Define our patched method
                        def gemini_compatible_to_pydantic(self):
                            """Patch to make Instructor work better with Gemini models"""
                            # Remove unsupported schema fields for Gemini models
                            if hasattr(self, '_schema') and isinstance(self._schema, dict):
                                def sanitize_schema(schema):
                                    if not isinstance(schema, dict):
                                        return schema
                                        
                                    # Remove fields known to cause issues with Gemini
                                    for field in ["default", "additionalProperties"]:
                                        if field in schema:
                                            del schema[field]
                                    
                                    # Process nested objects
                                    if "properties" in schema and isinstance(schema["properties"], dict):
                                        for prop, prop_schema in schema["properties"].items():
                                            schema["properties"][prop] = sanitize_schema(prop_schema)
                                    
                                    # Process array items
                                    if "items" in schema and isinstance(schema["items"], dict):
                                        schema["items"] = sanitize_schema(schema["items"])
                                        
                                    return schema
                                
                                # Apply the cleanup
                                self._schema = sanitize_schema(self._schema)
                                logger.info("Sanitized schema for Gemini compatibility")
                            
                            # Call the original method
                            return original_to_pydantic(self)
                        
                        # Apply our patch
                        InternalInstructor.to_pydantic = gemini_compatible_to_pydantic
                        logger.info("Successfully applied Gemini compatibility patch to InternalInstructor")
                        
                    except Exception as patch_error:
                        logger.error(f"Failed to apply Gemini compatibility patch: {str(patch_error)}")
                
            except Exception as api_key_error:
                logger.error(f"Error setting up API keys: {str(api_key_error)}")
                # Don't fail the execution if API key setup fails - let the actual execution
                # fail if the keys are actually required
            
            # Using our context manager to capture stdout/stderr
            with crew_logger.capture_stdout_stderr(execution_id):
                # Log retry attempt if this is a retry
                if retry_count > 0:
                    attempt_msg = f"Retry attempt {retry_count}/{max_retry_limit} for execution {execution_id}"
                    logger.info(attempt_msg)
                    await ExecutionStatusService.update_status(
                        job_id=execution_id,
                        status=ExecutionStatus.RUNNING.value,
                        message=attempt_msg
                    )
                
                # Extract user inputs from config if available
                user_inputs = {}
                if config and 'inputs' in config:
                    # Separate user-provided inputs from system inputs
                    all_inputs = config.get('inputs', {})
                    logger.info(f"All inputs received in execution_runner: {all_inputs}")
                    # System inputs that should not be passed to crew.kickoff
                    system_inputs = {'tools', 'planning_llm', 'reasoning_llm', 'process', 'max_rpm', 'planning', 'reasoning'}
                    # Filter out system inputs to get only user-provided inputs
                    user_inputs = {k: v for k, v in all_inputs.items() if k not in system_inputs}
                    if user_inputs:
                        logger.info(f"Passing user inputs to crew.kickoff: {user_inputs}")
                    else:
                        logger.info("No user inputs found after filtering system inputs")
                
                
                # Call crew start callback
                crew_callbacks['on_start']()
                
                # Use the custom CrewExecutor for better thread management
                # This provides proper thread naming, monitoring, and cancellation support
                try:
                    result = await crew_executor.run_crew(
                        execution_id=execution_id,
                        crew=crew,
                        inputs=user_inputs,
                        on_complete=crew_callbacks.get('on_complete'),
                        on_error=crew_callbacks.get('on_error'),
                        timeout=3600  # 1 hour default timeout for crew executions
                    )
                    
                except Exception as crew_error:
                    # Error callback is already called by crew_executor
                    logger.error(f"Crew execution failed: {crew_error}")
                    raise  # Re-raise to be handled by outer exception handler
            
            # If kickoff successful, prepare for COMPLETED status
            final_status = ExecutionStatus.COMPLETED.value
            final_message = "CrewAI execution completed successfully"
            final_result = result
            logger.info(f"Crew execution completed for {execution_id}. Preparing to update status to COMPLETED.")
            
            # Check for retried tasks due to guardrail failures
            retry_stats = {}
            for task in crew.tasks:
                if hasattr(task, 'retry_count') and task.retry_count > 0:
                    retry_stats[task.description[:50]] = task.retry_count
            
            if retry_stats:
                logger.info(f"Task retry statistics for {execution_id}: {retry_stats}")
                final_message += f" (with {sum(retry_stats.values())} total retries across {len(retry_stats)} tasks)"
            
            # Success - break the retry loop
            break
            
        except asyncio.CancelledError:
            # Execution was cancelled - don't retry, just exit
            final_status = ExecutionStatus.CANCELLED.value
            final_message = "CrewAI execution was cancelled"
            logger.warning(f"Crew execution CANCELLED for {execution_id}. Preparing to update status.")
            break
            
        except Exception as e:
            last_error = e
            
            # Check if this is a rate limit error
            is_rate_limit_error = False
            error_str = str(e).lower()
            
            if "ratelimiterror" in error_str or "rate_limit_error" in error_str or "rate limit" in error_str:
                is_rate_limit_error = True
                logger.warning(f"Rate limit error detected for execution {execution_id}: {str(e)}")
            
            # Check if this is a guardrail validation error
            is_guardrail_error = "guardrail" in error_str.lower() or "validation" in error_str.lower()
            if is_guardrail_error:
                logger.warning(f"Guardrail validation error detected for execution {execution_id}: {str(e)}")
                # Log additional information about the error to help diagnose issues
                logger.warning(f"Error details: {str(e)}")
                logger.warning(f"Error type: {type(e).__name__}")
            
            # If max retries exceeded or non-retryable error
            if retry_count >= max_retry_limit or (not is_rate_limit_error and not is_guardrail_error):
                # Execution failed with non-retryable error or max retries exceeded
                final_status = ExecutionStatus.FAILED.value
                final_message = f"CrewAI execution failed: {str(e)}"
                logger.error(f"Error in CrewAI execution {execution_id}: {str(e)}")
                logger.error(f"Stack trace for failure: {traceback.format_exc()}")
                logger.error(f"Preparing to update status for {execution_id} to FAILED.")
                break
            
            # For retryable errors, we'll retry with a delay
            retry_count += 1
            wait_time = min(2 ** (retry_count - 1), 60)
            logger.info(f"Rate limit encountered. Waiting {wait_time} seconds before retry {retry_count}/{max_retry_limit}...")
            await asyncio.sleep(wait_time)
            
    # If we retried but ultimately failed, make sure we have the right error message
    if retry_count > max_retry_limit and last_error:
        final_status = ExecutionStatus.FAILED.value
        final_message = f"CrewAI execution failed after {retry_count - 1} attempts: {str(last_error)}"
        logger.error(f"Execution {execution_id} failed after maximum retries. Error: {str(last_error)}")
        
    try:
        # Clean up the event streaming
        event_streaming.cleanup()
        
        # Clean up the CrewLogger
        crew_logger.cleanup_for_job(execution_id)
        
        # Note: LLM event routing has been deprecated in CrewAI 0.177+
        logger.debug(f"LLM event cleanup not needed for execution {execution_id} (handled by AgentTraceEventListener)")
        
        # Clean up MCP tools
        try:
            # Stop and cleanup any MCP adapters that were created
            # This prevents process leaks for stdio adapters and cleans up network resources
            from src.engines.crewai.tools.mcp_handler import stop_all_adapters
            await stop_all_adapters()
            logger.info(f"Cleaned up MCP tools for execution {execution_id}")
        except Exception as mcp_cleanup_error:
            logger.error(f"Error cleaning up MCP tools for execution {execution_id}: {str(mcp_cleanup_error)}")
            
        # Clean up the running job entry regardless of outcome
        if execution_id in running_jobs:
            del running_jobs[execution_id]
            logger.info(f"Removed job {execution_id} from running jobs list.")

        # Update final status with retry mechanism
        await update_execution_status_with_retry(
            execution_id, 
            final_status,
            final_message,
            final_result
        )
    except Exception as cleanup_error:
        logger.error(f"Error during cleanup for execution {execution_id}: {str(cleanup_error)}")


async def update_execution_status_with_retry(
    execution_id: str, 
    status: str,
    message: str,
    result: Any = None
) -> bool:
    """
    Update execution status with retry mechanism.
    
    Args:
        execution_id: Execution ID
        status: Status string
        message: Status message
        result: Optional execution result
        
    Returns:
        True if successful, False otherwise
    """
    from src.services.execution_status_service import ExecutionStatusService
    
    max_retries = 3
    retry_count = 0
    update_success = False
    
    while retry_count < max_retries and not update_success:
        try:
            logger.info(f"Attempting final status update for {execution_id} to {status} (attempt {retry_count + 1}/{max_retries}).")
            await ExecutionStatusService.update_status(
                job_id=execution_id, 
                status=status,
                message=message,
                result=result
            )
            logger.info(f"Final status update call for {execution_id} successful.")
            update_success = True
            return True
        except Exception as update_exc:
            retry_count += 1
            logger.error(f"Error updating final status for {execution_id} (attempt {retry_count}/{max_retries}): {update_exc}")
            if retry_count < max_retries:
                # Exponential backoff: 1s, 2s, 4s, etc.
                backoff_time = 2 ** (retry_count - 1)
                logger.info(f"Retrying in {backoff_time} seconds...")
                await asyncio.sleep(backoff_time)
    
    if not update_success:
        logger.error(f"Failed to update execution status for {execution_id} after {max_retries} attempts.")
    
    return update_success


async def run_crew_in_process(
    execution_id: str, 
    config: Dict[str, Any], 
    running_jobs: Dict,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None
) -> None:
    """
    Run a crew in an isolated process that can be truly terminated.
    
    This function uses ProcessCrewExecutor to run the crew in a separate process,
    which allows for true termination (unlike threads which can't be force-stopped).
    
    Args:
        execution_id: Execution ID
        config: Complete execution configuration (must be serializable)
        running_jobs: Dictionary tracking running jobs
        group_context: Group context for logging isolation
        user_token: User access token for OAuth authentication
    """
    # Log immediately to file to ensure we know the function was called
    try:
        import datetime
        with open(f'/tmp/run_crew_in_process_called_{execution_id[:8]}.log', 'w') as f:
            f.write(f"[{datetime.datetime.now()}] run_crew_in_process called\n")
            f.write(f"Execution ID: {execution_id}\n")
            f.write(f"Has config: {config is not None}\n")
            f.write(f"Has running_jobs: {running_jobs is not None}\n")
    except:
        pass  # Ignore file write errors
    
    try:
        logger.info(f"[run_crew_in_process] Starting for execution {execution_id}")
    except Exception as e:
        logger.error(f"[run_crew_in_process] Error logging start: {e}")
        # Write to file directly as fallback
        with open(f'/tmp/run_crew_error_{execution_id[:8]}.log', 'w') as f:
            f.write(f"Error at start of run_crew_in_process: {e}\n")
            import traceback
            f.write(traceback.format_exc())
    
    # First, ensure status is set to RUNNING
    from src.services.execution_status_service import ExecutionStatusService
    await ExecutionStatusService.update_status(
        job_id=execution_id,
        status=ExecutionStatus.RUNNING.value,
        message="CrewAI execution is running in isolated process"
    )
    logger.info(f"[run_crew_in_process] Set status to RUNNING for process execution {execution_id}")
    
    final_status = ExecutionStatus.FAILED.value  # Default to FAILED
    final_message = "An unexpected error occurred during crew execution."
    final_result = None
    
    try:
        # Debug: Write that we got into the function body
        with open(f'/tmp/run_crew_in_process_{execution_id[:8]}.log', 'w') as f:
            import datetime
            f.write(f"[{datetime.datetime.now()}] run_crew_in_process function started\n")
            f.write(f"Execution ID: {execution_id}\n")
            f.write(f"Has config: {config is not None}\n")
    except Exception as debug_error:
        pass  # Ignore debug errors
    
    try:
        # Extract user inputs from config if available
        user_inputs = {}
        if config and 'inputs' in config:
            all_inputs = config.get('inputs', {})
            logger.info(f"All inputs received for process execution: {all_inputs}")
            # System inputs that should not be passed to crew.kickoff
            system_inputs = {'tools', 'planning_llm', 'reasoning_llm', 'process', 'max_rpm', 'planning', 'reasoning'}
            # Filter out system inputs to get only user-provided inputs
            user_inputs = {k: v for k, v in all_inputs.items() if k not in system_inputs}
            if user_inputs:
                logger.info(f"Passing user inputs to process execution: {user_inputs}")
            else:
                logger.info("No user inputs found after filtering system inputs")
        
        # Fetch debug tracing flag before spawning subprocess
        debug_tracing_enabled = False  # Default value
        try:
            # Try to fetch debug tracing configuration
            from src.services.engine_config_service import EngineConfigService
            from src.db.session import async_session_factory

            async with async_session_factory() as session:
                try:
                    service = EngineConfigService(session)
                    debug_tracing_enabled = await service.get_crewai_debug_tracing()
                    logger.info(f"Fetched debug tracing flag for subprocess: {debug_tracing_enabled}")
                except Exception as e:
                    logger.warning(f"Failed to fetch debug tracing flag, using default: {e}")
        except Exception as e:
            logger.warning(f"Could not access database for debug tracing flag: {e}")

        # Use ProcessCrewExecutor for isolated execution
        logger.info(f"[run_crew_in_process] Starting process-based execution for {execution_id}")
        logger.info(f"[run_crew_in_process] Calling process_crew_executor.run_crew_isolated with debug_tracing={debug_tracing_enabled}")

        # CRITICAL: Add user_token to config for OBO authentication in subprocess
        # This ensures tools can authenticate using the user's token
        if user_token:
            config['user_token'] = user_token
            logger.info(f"[run_crew_in_process] Added user_token to crew_config for OBO authentication")
        else:
            logger.info(f"[run_crew_in_process] No user_token - subprocess will use PAT or SPN fallback")

        # CRITICAL: Ensure group_id is in config for PAT authentication fallback
        # Without this, get_auth_context() cannot query ApiKeysService for PAT tokens
        if group_context and hasattr(group_context, 'primary_group_id') and group_context.primary_group_id:
            if 'group_id' not in config or not config['group_id']:
                config['group_id'] = group_context.primary_group_id
                logger.info(f"[run_crew_in_process] Added group_id to crew_config: {group_context.primary_group_id}")

        # Run the crew in an isolated process
        result = await process_crew_executor.run_crew_isolated(
            execution_id=execution_id,
            crew_config=config,
            group_context=group_context,  # MANDATORY for tenant isolation
            inputs=user_inputs,
            timeout=3600,  # 1 hour timeout
            debug_tracing_enabled=debug_tracing_enabled  # Pass the debug tracing flag
        )
        
        logger.info(f"[run_crew_in_process] Process executor returned result for {execution_id}")
        
        # Validate result is a dictionary
        if not isinstance(result, dict):
            logger.error(f"Process executor returned non-dict result: {type(result)} - {result}")
            result = {
                "status": "FAILED",
                "error": f"Invalid result type: {type(result)}"
            }
        
        # Check the result status
        if result.get('status') == 'COMPLETED':
            final_status = ExecutionStatus.COMPLETED.value
            final_message = "CrewAI execution completed successfully"
            final_result = result.get('result')
            logger.info(f"Process execution completed for {execution_id}")
        elif result.get('status') == 'STOPPED':
            final_status = ExecutionStatus.STOPPED.value
            final_message = "CrewAI execution was stopped by user"
            logger.info(f"Process execution stopped for {execution_id}")
        elif result.get('status') == 'TIMEOUT':
            final_status = ExecutionStatus.FAILED.value
            final_message = result.get('error', 'Execution timed out')
            logger.error(f"Process execution timed out for {execution_id}")
        else:
            final_status = ExecutionStatus.FAILED.value
            final_message = result.get('error', 'Process execution failed')
            logger.error(f"Process execution failed for {execution_id}: {final_message}")
            
    except asyncio.CancelledError:
        # Execution was cancelled
        final_status = ExecutionStatus.CANCELLED.value
        final_message = "CrewAI execution was cancelled"
        logger.warning(f"Process execution CANCELLED for {execution_id}")
        # Try to terminate the process
        await process_crew_executor.terminate_execution(execution_id)
        
    except Exception as e:
        final_status = ExecutionStatus.FAILED.value
        final_message = f"CrewAI process execution failed: {str(e)}"
        logger.error(f"Error in process execution {execution_id}: {str(e)}")
        logger.error(f"Stack trace: {traceback.format_exc()}")
        
    finally:
        try:
            # Clean up the running job entry
            if execution_id in running_jobs:
                del running_jobs[execution_id]
                logger.info(f"Removed job {execution_id} from running jobs list.")

            # Log the final status that will be set
            logger.info(f"[run_crew_in_process] About to update final status for {execution_id}:")
            logger.info(f"  - final_status: {final_status}")
            logger.info(f"  - final_message: {final_message}")
            logger.info(f"  - has final_result: {final_result is not None}")

            # Update final status
            update_success = await update_execution_status_with_retry(
                execution_id,
                final_status,
                final_message,
                final_result
            )

            if update_success:
                logger.info(f"[run_crew_in_process] Successfully updated status for {execution_id} to {final_status}")
            else:
                logger.error(f"[run_crew_in_process] Failed to update status for {execution_id} to {final_status}")

        except Exception as cleanup_error:
            logger.error(f"Error during cleanup for process execution {execution_id}: {str(cleanup_error)}")
            logger.error(f"Cleanup error traceback: {traceback.format_exc()}")