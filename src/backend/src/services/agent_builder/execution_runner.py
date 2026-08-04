"""
Execution Runner for CrewAI engine.

This module provides functionality for running CrewAI crews and handling
the execution lifecycle.
"""

import asyncio
import logging
import traceback
from typing import Any, Dict, Optional

from src.models.execution_status import ExecutionStatus
from src.services.agent_builder.process_executor import process_crew_executor
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


async def update_execution_status_with_retry(
    execution_id: str, status: str, message: str, result: Any = None
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
    from src.services.execution.status import ExecutionStatusService

    max_retries = 3
    retry_count = 0
    update_success = False

    while retry_count < max_retries and not update_success:
        try:
            logger.info(
                f"Attempting final status update for {execution_id} to {status} (attempt {retry_count + 1}/{max_retries})."
            )
            # update_status returns False on failure (record not found, DB
            # error swallowed internally) — it must be honored, otherwise the
            # retry loop is dead code and failed writes go unnoticed until the
            # engine's safety net force-completes the run.
            update_success = bool(
                await ExecutionStatusService.update_status(
                    job_id=execution_id, status=status, message=message, result=result
                )
            )
            if update_success:
                logger.info(f"Final status update call for {execution_id} successful.")
                return True
            retry_count += 1
            logger.error(
                f"Final status update for {execution_id} reported failure (attempt {retry_count}/{max_retries})."
            )
        except Exception as update_exc:
            retry_count += 1
            logger.error(
                f"Error updating final status for {execution_id} (attempt {retry_count}/{max_retries}): {update_exc}"
            )
        if not update_success and retry_count < max_retries:
            # Exponential backoff: 1s, 2s, 4s, etc.
            backoff_time = 2 ** (retry_count - 1)
            logger.info(f"Retrying in {backoff_time} seconds...")
            await asyncio.sleep(backoff_time)

    if not update_success:
        logger.error(
            f"Failed to update execution status for {execution_id} after {max_retries} attempts."
        )

    return update_success


async def run_crew_in_process(
    execution_id: str,
    config: Dict[str, Any],
    running_jobs: Dict,
    group_context: Optional[GroupContext] = None,
    user_token: Optional[str] = None,
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

        with open(f"/tmp/run_crew_in_process_called_{execution_id[:8]}.log", "w") as f:
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
        with open(f"/tmp/run_crew_error_{execution_id[:8]}.log", "w") as f:
            f.write(f"Error at start of run_crew_in_process: {e}\n")
            import traceback

            f.write(traceback.format_exc())

    # Ensure status is RUNNING. API-created records are born RUNNING, so this
    # is a no-op there; scheduler-created records start "pending" and need the
    # transition (only_if_changed skips the redundant write in the former case).
    from src.services.execution.status import ExecutionStatusService

    await ExecutionStatusService.update_status(
        job_id=execution_id,
        status=ExecutionStatus.RUNNING.value,
        message="CrewAI execution is running in isolated process",
        only_if_changed=True,
    )
    logger.info(
        f"[run_crew_in_process] Ensured RUNNING status for process execution {execution_id}"
    )

    final_status = ExecutionStatus.FAILED.value  # Default to FAILED
    final_message = "An unexpected error occurred during crew execution."
    final_result = None

    try:
        # Debug: Write that we got into the function body
        with open(f"/tmp/run_crew_in_process_{execution_id[:8]}.log", "w") as f:
            import datetime

            f.write(
                f"[{datetime.datetime.now()}] run_crew_in_process function started\n"
            )
            f.write(f"Execution ID: {execution_id}\n")
            f.write(f"Has config: {config is not None}\n")
    except Exception as debug_error:
        pass  # Ignore debug errors

    try:
        # Extract user inputs from config if available
        user_inputs = {}
        if config and "inputs" in config:
            all_inputs = config.get("inputs", {})
            logger.info(f"All inputs received for process execution: {all_inputs}")
            # System inputs that should not be passed to crew.kickoff.
            # 'planning' / 'planning_llm' are LEGACY-ONLY: Kasal no longer models
            # planning, but saved/scheduled payloads from before its removal can
            # still carry the keys — they must stay filtered so they never leak
            # into kickoff inputs and become template variables.
            system_inputs = {
                "tools",
                "planning_llm",
                "reasoning_llm",
                "reasoning_config",
                "process",
                "max_rpm",
                "planning",
                "reasoning",
            }
            # Filter out system inputs to get only user-provided inputs
            user_inputs = {
                k: v for k, v in all_inputs.items() if k not in system_inputs
            }
            if user_inputs:
                logger.info(f"Passing user inputs to process execution: {user_inputs}")
            else:
                logger.info("No user inputs found after filtering system inputs")

        # SECURITY: Scan user inputs for prompt injection patterns (log-only, non-blocking)
        try:
            from src.services.security.scanner_pipeline import security_scanner

            for _input_key, _input_val in user_inputs.items():
                if isinstance(_input_val, str):
                    security_scanner.scan(
                        _input_val, context=f"user_input:{_input_key}:{execution_id}"
                    )
        except Exception as _pi_err:
            logger.warning("[SECURITY] Prompt injection scan failed: %s", _pi_err)

        # Use ProcessCrewExecutor for isolated execution
        logger.info(
            f"[run_crew_in_process] Starting process-based execution for {execution_id}"
        )

        # CRITICAL: Add user_token to config for OBO authentication in subprocess
        # This ensures tools can authenticate using the user's token
        if user_token:
            config["user_token"] = user_token
            logger.info(
                "[run_crew_in_process] Added user_token to crew_config for OBO authentication"
            )
        else:
            logger.info(
                "[run_crew_in_process] No user_token - subprocess will use PAT or SPN fallback"
            )

        # CRITICAL: Ensure group_id is in config for PAT authentication fallback
        # Without this, get_auth_context() cannot query ApiKeysService for PAT tokens
        if (
            group_context
            and hasattr(group_context, "primary_group_id")
            and group_context.primary_group_id
        ):
            if "group_id" not in config or not config["group_id"]:
                config["group_id"] = group_context.primary_group_id
                logger.info(
                    f"[run_crew_in_process] Added group_id to crew_config: {group_context.primary_group_id}"
                )

        # Run the crew in an isolated process
        result = await process_crew_executor.run_crew_isolated(
            execution_id=execution_id,
            crew_config=config,
            group_context=group_context,  # MANDATORY for tenant isolation
            inputs=user_inputs,
            timeout=3600,  # 1 hour timeout
        )

        logger.info(
            f"[run_crew_in_process] Process executor returned result for {execution_id}"
        )

        # Validate result is a dictionary
        if not isinstance(result, dict):
            logger.error(
                f"Process executor returned non-dict result: {type(result)} - {result}"
            )
            result = {
                "status": "FAILED",
                "error": f"Invalid result type: {type(result)}",
            }

        # Check the result status
        if result.get("status") == "COMPLETED":
            final_status = ExecutionStatus.COMPLETED.value
            final_result = result.get("result")

            # Compose an A2UI surface from the crew's answer using the SAME shared
            # composer the light-agent path and the exported app use — replacing the
            # retired ui_emission prompt-injection so every channel renders through
            # ONE implementation. Gated by the workspace UIConfigurator; returns the
            # result unchanged when no rich surface applies.
            try:
                from src.services.a2ui.runner import (
                    wrap_result_with_surface,
                )

                final_result = await wrap_result_with_surface(
                    final_result,
                    config=config,
                    group_id=(
                        group_context.primary_group_id if group_context else None
                    ),
                    inputs=user_inputs,
                    # This runs in the PARENT, after the subprocess that owned
                    # the OTel bridge exited — so composition can only be filed
                    # under this run by naming it explicitly.
                    execution_id=execution_id,
                    group_context=group_context,
                )
            except Exception as a2ui_err:  # noqa: BLE001 — never break a finished run
                logger.debug(
                    f"[a2ui] crew surface skipped for {execution_id}: {a2ui_err}"
                )

            # Surface MCP warnings in the execution message so they appear in the UI
            warnings = result.get("warnings", [])
            if warnings:
                final_message = (
                    "CrewAI execution completed with warnings: " + "; ".join(warnings)
                )
                logger.warning(
                    f"Process execution completed with MCP warnings for {execution_id}: {warnings}"
                )
            else:
                final_message = "CrewAI execution completed successfully"
                logger.info(f"Process execution completed for {execution_id}")
        elif result.get("status") == "STOPPED":
            final_status = ExecutionStatus.STOPPED.value
            final_message = "CrewAI execution was stopped by user"
            logger.info(f"Process execution stopped for {execution_id}")
        elif result.get("status") == "TIMEOUT":
            final_status = ExecutionStatus.FAILED.value
            final_message = result.get("error", "Execution timed out")
            logger.error(f"Process execution timed out for {execution_id}")
        else:
            final_status = ExecutionStatus.FAILED.value
            final_message = result.get("error", "Process execution failed")
            logger.error(
                f"Process execution failed for {execution_id}: {final_message}"
            )
            # The subprocess ships its full traceback in the result — without
            # logging it here, failures surface as a bare one-line message
            # (e.g. "'Agent' object has no attribute 'i18n'") with no frame
            # information anywhere in the logs.
            subprocess_tb = result.get("traceback")
            if subprocess_tb:
                logger.error(
                    f"Subprocess traceback for {execution_id}:\n{subprocess_tb}"
                )

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
            logger.info(
                f"[run_crew_in_process] About to update final status for {execution_id}:"
            )
            logger.info(f"  - final_status: {final_status}")
            logger.info(f"  - final_message: {final_message}")
            logger.info(f"  - has final_result: {final_result is not None}")

            # Update final status
            update_success = await update_execution_status_with_retry(
                execution_id, final_status, final_message, final_result
            )

            if update_success:
                logger.info(
                    f"[run_crew_in_process] Successfully updated status for {execution_id} to {final_status}"
                )
            else:
                logger.error(
                    f"[run_crew_in_process] Failed to update status for {execution_id} to {final_status}"
                )

            # Distil this crew into a reusable recipe, now that the row it will
            # be mined FROM says COMPLETED. Triggering any earlier — when the
            # subprocess is joined, say — races this write: mining only reads
            # runs whose status is COMPLETED, so it would skip the very run that
            # triggered it and pick it up only when the NEXT run finished, which
            # leaves the newest run permanently unmined. Fire-and-forget.
            if update_success and final_status == ExecutionStatus.COMPLETED.value:
                from src.services.recipes.mining import schedule_mining_after_run

                schedule_mining_after_run(execution_id)

        except BaseException as cleanup_error:
            # Catch BaseException (not just Exception) so CancelledError is also caught.
            # If the asyncio task is cancelled, the DB write would otherwise be skipped
            # silently, leaving the status stuck at RUNNING.
            logger.error(
                f"Error during cleanup for process execution {execution_id}: {str(cleanup_error)}"
            )
            logger.error(f"Cleanup error traceback: {traceback.format_exc()}")
            # For CancelledError: use a thread with its own event loop to write the status
            # (cannot await in a cancelled asyncio task)
            if isinstance(cleanup_error, asyncio.CancelledError):
                try:
                    import concurrent.futures

                    from src.services.execution.status import ExecutionStatusService

                    # _smart, not _with_fresh_engine: executionhistory lives in
                    # Lakebase when it is enabled, and _with_fresh_engine is pinned
                    # to the local engine — so this recovery read would look in the
                    # wrong DB, find no RUNNING row, and silently skip the fix.
                    from src.utils.asyncio_utils import execute_db_operation_smart

                    async def _recovery():
                        async def _op(session):
                            from src.repositories.execution_history_repository import (
                                ExecutionHistoryRepository,
                            )

                            repo = ExecutionHistoryRepository(session)
                            rec = await repo.get_execution_by_job_id(execution_id)
                            if rec and rec.status and rec.status.upper() == "RUNNING":
                                await ExecutionStatusService.update_status(
                                    job_id=execution_id,
                                    status=final_status,
                                    message=final_message + " (task-cancel recovery)",
                                )

                        await execute_db_operation_smart(_op)

                    def _run_recovery():
                        asyncio.run(_recovery())

                    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
                        ex.submit(_run_recovery).result(timeout=30)
                except Exception as rec_err:
                    logger.error(
                        f"[run_crew_in_process] CancelledError recovery also failed: {rec_err}"
                    )
                raise  # re-raise CancelledError so the task is properly marked cancelled
