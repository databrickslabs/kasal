"""
The background task that drains the execution-logs queue.

Named LogWriterTask for most of its life, which was wrong and cost people time:
it has never written a trace. Traces come off the OTel pipeline
(BatchSpanProcessor -> KasalDBSpanExporter -> services/trace). This owns the
LOGS writer and nothing else.
"""
import asyncio
import logging
import threading
from typing import Optional

logger = logging.getLogger(__name__)


class LogWriterTask:
    """Owns the singleton background task that writes queued execution logs.

    IMPORTANT: the task must be started and stopped in the SAME event loop —
    the subprocess paths start it inside the loop that will later stop it.
    """

    _logs_writer_task: Optional[asyncio.Task] = None
    _shutdown_event: Optional[asyncio.Event] = None
    _writer_started: bool = False
    _lock: Optional[asyncio.Lock] = None
    _writer_loop: Optional[asyncio.AbstractEventLoop] = None
    _thread_lock = threading.Lock()

    @classmethod
    async def ensure_writer_started(cls):
        """Starts the logs writer task if it hasn't been started yet."""
        logger.debug("[LogWriterTask] ensure_writer_started called")

        current_loop = asyncio.get_running_loop()

        if cls._lock is None or cls._writer_loop != current_loop:
            cls._lock = asyncio.Lock()

        async with cls._lock:
            if cls._shutdown_event is None or cls._writer_loop != current_loop:
                cls._shutdown_event = asyncio.Event()

            if cls._writer_loop is not None and cls._writer_loop != current_loop:
                logger.warning(
                    "[LogWriterTask] Loop change detected! Resetting writer state."
                )
                cls._logs_writer_task = None
                cls._writer_started = False

            cls._writer_loop = current_loop

            if cls._logs_writer_task is None or cls._logs_writer_task.done():
                logger.info("[LogWriterTask] Starting logs writer task...")
                cls._shutdown_event.clear()
                from src.services.execution.logs.writer import start_logs_writer
                cls._logs_writer_task = await start_logs_writer(cls._shutdown_event)
                cls._writer_started = True
                logger.info("[LogWriterTask] Logs writer task started.")
            else:
                logger.debug("[LogWriterTask] Logs writer task already running.")
                cls._writer_started = True

    @classmethod
    async def stop_writer(cls):
        """Signals the writer task to stop."""
        try:
            current_loop = asyncio.get_running_loop()
        except RuntimeError:
            current_loop = None

        loop_mismatch = (
            cls._writer_loop is not None
            and current_loop is not None
            and cls._writer_loop != current_loop
        )

        if loop_mismatch:
            logger.warning(
                "[LogWriterTask] LOOP MISMATCH in stop_writer! Forcing cleanup."
            )
            with cls._thread_lock:
                if cls._logs_writer_task and not cls._logs_writer_task.done():
                    try:
                        cls._logs_writer_task.cancel()
                    except Exception:
                        pass
                cls._logs_writer_task = None
                cls._writer_started = False
                cls._writer_loop = None
                cls._shutdown_event = None
                cls._lock = None
            logger.info("[LogWriterTask] State reset due to loop mismatch.")
            return

        if cls._lock is None:
            cls._lock = asyncio.Lock()

        async with cls._lock:
            if cls._shutdown_event is not None:
                logger.info("[LogWriterTask] Setting shutdown event...")
                cls._shutdown_event.set()

            if cls._logs_writer_task and not cls._logs_writer_task.done():
                logger.info("[LogWriterTask] Stopping logs writer task...")
                from src.services.execution.logs.writer import stop_logs_writer
                success = await stop_logs_writer(timeout=5.0)
                if success:
                    logger.info("[LogWriterTask] Logs writer task stopped successfully.")
                else:
                    logger.warning("[LogWriterTask] Failed to stop logs writer gracefully.")
                cls._logs_writer_task = None
            else:
                logger.debug("[LogWriterTask] Logs writer task not running.")

            cls._writer_started = False
            cls._writer_loop = None
