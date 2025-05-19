"""
Execution Runner for CrewAI engine.

This module provides functionality for running CrewAI crews and handling
the execution lifecycle.
"""
import logging
import asyncio
import traceback
from typing import Any, Dict
import os


from crewai import Crew, LLM
from src.models.execution_status import ExecutionStatus
from src.core.llm_manager import LLMManager

logger = logging.getLogger(__name__)

async def run_crew(execution_id: str, crew: Crew, running_jobs: Dict) -> None:
    """
    Run the crew in a separate task, ensuring final status update
    occurs within its own database session scope.
    
    Args:
        execution_id: Execution ID
        crew: The CrewAI crew to run
        running_jobs: Dictionary tracking running jobs
    """
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
    
    # Set up CrewLogger and callback handlers
    from src.engines.crewai.crew_logger import crew_logger
    from src.engines.crewai.callbacks.streaming_callbacks import EventStreamingCallback
    
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
        if config.get("agents"):
            # Get highest retry limit from all agents
            for agent_config in config.get("agents", {}).values():
                if isinstance(agent_config, dict) and "max_retry_limit" in agent_config:
                    agent_retry_limit = int(agent_config.get("max_retry_limit", 2))
                    max_retry_limit = max(max_retry_limit, agent_retry_limit)
    
    logger.info(f"Using max_retry_limit={max_retry_limit} for execution {execution_id}")
    
    # Initialize logging for this job
    crew_logger.setup_for_job(execution_id)
    
    # Initialize event streaming with configuration
    event_streaming = EventStreamingCallback(job_id=execution_id, config=config)
    
    # Retry counter
    retry_count = 0
    
    # For capturing the specific error
    last_error = None
    
    # Keep trying until we exceed max retries
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
                                crewai_llm = await LLMManager.configure_crewai_llm(model)
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
                
                # Explicitly set up API keys for common providers
                await ApiKeysService.setup_openai_api_key()
                await ApiKeysService.setup_anthropic_api_key()
                await ApiKeysService.setup_gemini_api_key()  # Ensure Gemini API key is properly set
                
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
                
                # Run the potentially blocking crew.kickoff() in a separate thread
                # to avoid blocking the asyncio event loop
                result = await asyncio.to_thread(crew.kickoff)
            
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
        
        # Clean up MCP tools
        try:
            # Stop and cleanup any MCP adapters that were created
            # This prevents process leaks for stdio adapters and cleans up network resources
            from src.engines.crewai.tools.mcp_handler import stop_all_adapters
            stop_all_adapters()
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