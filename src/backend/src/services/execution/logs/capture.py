"""Install engine and library log redirection into the shared crew logger."""

import logging
import threading
import warnings

# Suppress known deprecation warnings from third-party libraries
warnings.filterwarnings("ignore", category=DeprecationWarning, module="httpx")
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websockets")
warnings.filterwarnings(
    "ignore", message=".*Use 'content=.*' to upload raw bytes/text content.*"
)
warnings.filterwarnings(
    "ignore",
    message=".*Accessing the 'model_fields' attribute on the instance is deprecated.*",
)
warnings.filterwarnings("ignore", message=".*remove second argument of ws_handler.*")

# Import core logger
from src.core.logger import LoggerManager

# Configure logger
logger = logging.getLogger(__name__)


class ExecutionLogCapture:
    """Install process-wide logging handlers once per interpreter."""

    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        """Ensure singleton instance."""
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(ExecutionLogCapture, cls).__new__(cls)
                cls._instance._initialized = False
            return cls._instance

    def __init__(self):
        """Initialize the logger if not already initialized."""
        if not getattr(self, "_initialized", False):
            # Get the crew logger from LoggerManager
            self._crew_logger = LoggerManager.get_instance().crew

            # Set up CrewAI's standard logging redirection
            self._setup_engine_logging()

            # Mark as initialized
            self._initialized = True

    def _setup_engine_logging(self):
        """Set up redirection for the kasal engine's standard logging to our crew logger."""
        try:
            # Get the engine's loggers
            engine_logger = logging.getLogger("kasal_engine")

            # Configure the engine logger to use our formatting
            engine_logger.handlers = []
            engine_logger.propagate = False

            # Create a special handler to redirect to our logger
            crew_logger = self._crew_logger

            class EngineRedirectHandler(logging.Handler):
                def emit(self, record):
                    # Get the log message
                    msg = self.format(record)
                    # Forward to our crew logger with the same level
                    level = record.levelno
                    crew_logger.log(level, f"ENGINE-LOG: {msg}")

            # Add the redirect handler to the engine logger
            formatter = logging.Formatter("%(message)s")
            redirect_handler = EngineRedirectHandler()
            redirect_handler.setFormatter(formatter)
            engine_logger.addHandler(redirect_handler)

            # Set log level to DEBUG to capture all logs
            engine_logger.setLevel(logging.DEBUG)

            # Also capture other related loggers
            for logger_name in [
                "langchain",
                "httpx",
                "openai",
                "src.services.converters",
            ]:
                try:
                    related_logger = logging.getLogger(logger_name)
                    related_logger.handlers = []
                    related_logger.propagate = False
                    handler_copy = EngineRedirectHandler()
                    handler_copy.setFormatter(formatter)
                    related_logger.addHandler(handler_copy)
                    related_logger.setLevel(logging.DEBUG)
                except Exception as related_err:
                    logger.warning(
                        f"Could not set up redirection for {logger_name}: {str(related_err)}"
                    )

            logger.info("Successfully set up engine logging redirection")
        except Exception as e:
            logger.error(f"Error setting up engine logging redirection: {str(e)}")


# Singleton: it patches process-global state.
execution_log_capture = ExecutionLogCapture()
