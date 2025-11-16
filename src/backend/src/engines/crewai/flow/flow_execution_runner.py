"""Flow Execution Runner

This module provides functionality for running CrewAI flows and handling
the flow execution lifecycle, similar to execution_runner.py for crews.
"""
import logging
import asyncio
import traceback
from typing import Any, Dict, Optional
import os
from src.services.process_flow_executor import process_flow_executor
from src.models.execution_status import ExecutionStatus
from src.utils.user_context import GroupContext
from src.core.logger import LoggerManager

# Use flow-specific logger
logger = LoggerManager.get_instance().flow


async def update_execution_status_with_retry(
    execution_id: str,
    status: str,
    message: Optional[str] = None,
    max_retries: int = 3,
    retry_delay: float = 0.5
) -> bool:
    """
    Update execution status with retry logic.

    Args:
        execution_id: ID of the execution
        status: New status value
        message: Optional status message
        max_retries: Maximum number of retry attempts
        retry_delay: Delay between retries in seconds

    Returns:
        True if update successful, False otherwise
    """
    from src.services.execution_status_service import ExecutionStatusService

    for attempt in range(max_retries):
        try:
            success = await ExecutionStatusService.update_status(
                job_id=execution_id,
                status=status,
                message=message
            )
            if success:
                return True
            else:
                logger.warning(f"Status update returned False for {execution_id}, attempt {attempt + 1}/{max_retries}")
        except Exception as e:
            logger.error(f"Error updating status for {execution_id}, attempt {attempt + 1}/{max_retries}: {e}")

        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay * (attempt + 1))  # Exponential backoff

    logger.error(f"Failed to update status for {execution_id} after {max_retries} attempts")
    return False


async def run_flow_in_process(
    execution_id: str,
    config: Dict[str, Any],
    running_jobs: Dict,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None
) -> None:
    """
    Run a flow in an isolated process that can be truly terminated.

    This function uses ProcessFlowExecutor to run the flow in a separate process,
    which allows for true termination (unlike threads which can't be force-stopped).

    Args:
        execution_id: Execution ID
        config: Complete execution configuration (must be serializable)
        running_jobs: Dictionary tracking running jobs
        group_context: Group context for logging isolation
        user_token: User access token for OAuth authentication
    """
    # Log immediately to ensure we know the function was called
    logger.info(f"[run_flow_in_process] *** FUNCTION ENTERED *** execution_id: {execution_id}")

    # Write to file as backup logging
    try:
        import os
        log_file = f'/tmp/flow_exec_{execution_id[:8]}.log'
        with open(log_file, 'a') as f:
            f.write(f"[run_flow_in_process] Function called for {execution_id}\n")
    except Exception:
        pass

    # Extract inputs from config
    user_inputs = config.get('inputs', {})

    final_status = None
    final_message = None

    try:
        logger.info(f"[run_flow_in_process] Preparing to run flow {execution_id} in process")

        # CRITICAL: Add user_token to config for OBO authentication in subprocess
        if user_token:
            config['user_token'] = user_token
            logger.info(f"[run_flow_in_process] Added user_token to flow_config for OBO authentication")
        else:
            logger.info(f"[run_flow_in_process] No user_token - subprocess will use PAT or SPN fallback")

        # CRITICAL: Ensure group_id is in config for PAT authentication fallback
        if group_context and hasattr(group_context, 'primary_group_id') and group_context.primary_group_id:
            if 'group_id' not in config or not config['group_id']:
                config['group_id'] = group_context.primary_group_id
                logger.info(f"[run_flow_in_process] Added group_id to flow_config: {group_context.primary_group_id}")

        # Run the flow in an isolated process
        logger.info(f"[run_flow_in_process] Calling process_flow_executor.run_flow_isolated for {execution_id}")
        result = await process_flow_executor.run_flow_isolated(
            execution_id=execution_id,
            flow_config=config,
            group_context=group_context,
            inputs=user_inputs,
            timeout=3600  # 1 hour timeout
        )

        logger.info(f"[run_flow_in_process] Process executor returned result for {execution_id}")

        # Check result status
        if result.get('status') == 'COMPLETED':
            final_status = ExecutionStatus.COMPLETED.value
            final_message = "Flow execution completed successfully"
            logger.info(f"Flow execution COMPLETED for {execution_id}")
        else:
            final_status = ExecutionStatus.FAILED.value
            final_message = result.get('error', 'Process execution failed')
            logger.error(f"Flow execution failed for {execution_id}: {final_message}")

    except asyncio.CancelledError:
        # Execution was cancelled
        final_status = ExecutionStatus.CANCELLED.value
        final_message = "Flow execution was cancelled"
        logger.warning(f"Flow execution CANCELLED for {execution_id}")
        # Try to terminate the process
        await process_flow_executor.terminate_execution(execution_id)

    except Exception as e:
        final_status = ExecutionStatus.FAILED.value
        final_message = f"Flow execution error: {str(e)}"
        logger.error(f"Flow execution EXCEPTION for {execution_id}: {e}", exc_info=True)

        # Write error to file as backup
        try:
            with open(f'/tmp/flow_error_{execution_id[:8]}.log', 'w') as f:
                f.write(f"Exception in run_flow_in_process: {e}\n")
                f.write(traceback.format_exc())
        except Exception:
            pass

    finally:
        # Update execution status
        if final_status:
            logger.info(f"[run_flow_in_process] Updating status to {final_status} for {execution_id}")
            try:
                await update_execution_status_with_retry(
                    execution_id=execution_id,
                    status=final_status,
                    message=final_message
                )
                logger.info(f"[run_flow_in_process] Successfully updated status to {final_status}")
            except Exception as status_error:
                logger.error(f"[run_flow_in_process] Failed to update status: {status_error}")

        # Remove from running jobs
        if execution_id in running_jobs:
            running_jobs.pop(execution_id)
            logger.info(f"[run_flow_in_process] Removed {execution_id} from running_jobs")

        logger.info(f"[run_flow_in_process] *** FUNCTION EXITING *** execution_id: {execution_id}, final_status: {final_status}")


async def run_flow(
    execution_id: str,
    config: Dict[str, Any],
    running_jobs: Dict,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None
) -> None:
    """
    Run a flow execution (wrapper that delegates to process-based execution).

    Args:
        execution_id: Execution ID
        config: Flow configuration
        running_jobs: Dictionary tracking running jobs
        group_context: Group context for isolation
        user_token: User access token
    """
    logger.info(f"[run_flow] Starting flow execution {execution_id}")

    # Always use process-based execution for flows
    await run_flow_in_process(
        execution_id=execution_id,
        config=config,
        running_jobs=running_jobs,
        group_context=group_context,
        user_token=user_token
    )
