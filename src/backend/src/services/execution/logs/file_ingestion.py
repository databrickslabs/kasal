"""Persist a process execution's matching file logs through the routed log service."""

from logging import Logger
from pathlib import Path
from typing import Any


async def ingest_execution_log(
    execution_id: str,
    *,
    filename: str,
    default_log_dir: Path,
    group_context: Any,
    logger: Logger,
    executor_name: str,
) -> None:
    import os
    from datetime import datetime

    # Resolve the configured file location
    log_dir = os.environ.get("LOG_DIR")
    if not log_dir:
        log_dir = default_log_dir

    log_path = os.path.join(log_dir, filename)

    if not os.path.exists(log_path):
        logger.warning(f"{filename} file not found at {log_path}")
        return

    # Extract logs for our execution ID
    logs_to_write = []
    exec_id_short = execution_id[:8]  # Use short ID for matching

    # First, add a header log entry to mark the start
    logs_to_write.append(
        {
            "execution_id": execution_id,
            "content": f"[EXECUTION_START] ========== Execution {execution_id} Started ==========",
            "timestamp": datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
            "group_id": (
                getattr(group_context, "primary_group_id", None)
                if group_context
                else None
            ),
            "group_email": (
                getattr(group_context, "group_email", None) if group_context else None
            ),
        }
    )

    # Read the file and extract matching execution logs
    with open(log_path, "r") as f:
        for line in f:
            if exec_id_short in line:
                # This log belongs to our execution
                logs_to_write.append(
                    {
                        "execution_id": execution_id,
                        "content": line.strip(),
                        "timestamp": datetime.utcnow(),  # Use timezone-naive UTC datetime for database consistency
                        "group_id": (
                            getattr(group_context, "primary_group_id", None)
                            if group_context
                            else None
                        ),
                        "group_email": (
                            getattr(group_context, "group_email", None)
                            if group_context
                            else None
                        ),
                    }
                )

    if len(logs_to_write) <= 1:  # Only has the start header
        logger.info(f"No logs found for execution {exec_id_short} in {filename}")
        # Still write the start header
    else:
        logger.info(
            f"Found {len(logs_to_write) - 1} logs for execution {exec_id_short} in {filename}"
        )

    # Route through get_smart_db_session so logs land in
    # Lakebase when enabled (same path as API endpoints).
    from src.db.database_router import get_smart_db_session
    from src.services.execution.logs.writer import ExecutionLogsService

    logger.info(
        f"[{executor_name}] Writing {len(logs_to_write)} logs via smart DB session"
    )
    async for session in get_smart_db_session():
        # Logs are ExecutionService's domain — via its service, not its
        # repository.
        logs_service = ExecutionLogsService(session)
        for log_data in logs_to_write:
            await logs_service.write_log(
                execution_id=log_data["execution_id"],
                content=log_data["content"],
                timestamp=log_data["timestamp"],
                group_id=log_data.get("group_id"),
                group_email=log_data.get("group_email"),
            )
        await session.commit()

    logger.info(
        f"[{executor_name}] Successfully wrote {len(logs_to_write)} logs to execution_logs table"
    )
