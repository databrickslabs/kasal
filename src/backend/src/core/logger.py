"""
Core logging functionality for the application.

This module provides a centralized logging system with domain-specific loggers
for different parts of the application, supporting both file and console output.
"""

import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

class LoggerManager:
    """Manages domain-specific loggers with file and console output."""
    
    _instance = None
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LoggerManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not self._initialized:
            self._crew_logger = None
            self._system_logger = None
            self._llm_logger = None
            self._scheduler_logger = None
            self._api_logger = None
            self._access_logger = None
            self._guardrails_logger = None
            self._log_dir = None
            self._initialized = True
    
    @classmethod
    def get_instance(cls, log_dir: str = None):
        """Get or create a LoggerManager instance and initialize it with the given log directory."""
        instance = cls()
        if log_dir:
            instance.initialize(log_dir)
        return instance
    
    def initialize(self, log_dir: str = None):
        """Initialize all domain-specific loggers with both file and console handlers."""
        # Set up log directory - always prefer the environment variable if set
        if log_dir:
            self._log_dir = Path(log_dir)
        else:
            # Check environment variable first
            env_log_dir = os.environ.get("LOG_DIR")
            if env_log_dir:
                self._log_dir = Path(env_log_dir)
            else:
                # Default to backend/logs instead of backend/src/logs
                self._log_dir = Path(__file__).parent.parent.parent / "logs"
        
        # Ensure directory exists
        self._log_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure formatters for different domains
        formatters = {
            'crew': logging.Formatter(
                '[CREW] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'system': logging.Formatter(
                '[SYSTEM] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'llm': logging.Formatter(
                '[LLM] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'scheduler': logging.Formatter(
                '[SCHEDULER] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'api': logging.Formatter(
                '[API] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'access': logging.Formatter(
                '[ACCESS] %(asctime)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            ),
            'guardrails': logging.Formatter(
                '[GUARDRAILS] %(asctime)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
        }
        
        # Set up the uvicorn logger early
        # This helps prevent any stdout logging before our handlers are attached
        uvicorn_logger = logging.getLogger("uvicorn")
        uvicorn_logger.handlers = []
        uvicorn_logger.propagate = True
        
        uvicorn_access_logger = logging.getLogger("uvicorn.access")
        uvicorn_access_logger.handlers = []
        uvicorn_access_logger.propagate = True
        
        # Initialize each logger
        self._crew_logger = self._setup_logger('crew', formatters['crew'])
        self._system_logger = self._setup_logger('system', formatters['system'])
        self._llm_logger = self._setup_logger('llm', formatters['llm'])
        self._scheduler_logger = self._setup_logger('scheduler', formatters['scheduler'])
        self._api_logger = self._setup_logger('api', formatters['api'])
        self._access_logger = self._setup_logger('access', formatters['access'], suppress_stdout=True)
        self._guardrails_logger = self._setup_logger('guardrails', formatters['guardrails'])
        
        # Configure uvicorn access logging after all loggers are initialized
        self._configure_uvicorn_logging()
        
        # Log initialization success
        self._system_logger.info(f"Logging system initialized. Log directory: {self._log_dir}")
    
    def _configure_uvicorn_logging(self):
        """Configure Uvicorn logging to redirect to our loggers."""
        # Set up Uvicorn access logging
        uvicorn_access_logger = logging.getLogger("uvicorn.access")
        uvicorn_access_logger.handlers = []
        uvicorn_access_logger.propagate = False
        
        # Create a special filter to determine where to log
        class APIRequestFilter:
            def __init__(self, api_logger, access_logger):
                self.api_logger = api_logger
                self.access_logger = access_logger
                
            def filter_and_log(self, record):
                try:
                    client_addr = getattr(record, 'client_addr', '-')
                    status_code = getattr(record, 'status_code', '-')
                    request_line = getattr(record, 'request_line', '-')
                    
                    # Skip empty or placeholder requests
                    if request_line == "-":
                        return False
                        
                    msg = f"{client_addr} - \"{request_line}\" {status_code}"
                    
                    # Route API requests to the API log file
                    if '/api/' in request_line:
                        self.api_logger.info(msg)
                    else:
                        self.access_logger.info(msg)
                    
                    # Filter out all messages to prevent them from going to console
                    return False
                except Exception:
                    # In case of any error, let the record pass through (but this won't happen since we've removed default handlers)
                    return False
        
        # Create and attach the filter
        api_request_filter = APIRequestFilter(self._api_logger, self._access_logger)
        
        class UvicornAccessHandler(logging.Handler):
            def __init__(self, filter_func):
                super().__init__()
                self.filter_func = filter_func
                
            def emit(self, record):
                # Process the record with our filter/router
                self.filter_func(record)
        
        # Attach our handler to Uvicorn access logger
        uvicorn_access_logger.addHandler(UvicornAccessHandler(api_request_filter.filter_and_log))
        
        # Also suppress other uvicorn loggers
        for logger_name in ["uvicorn", "uvicorn.error"]:
            logger = logging.getLogger(logger_name)
            logger.handlers = []
            logger.propagate = False
    
    def _setup_logger(self, name: str, formatter: logging.Formatter, suppress_stdout=False) -> logging.Logger:
        """Set up a specific logger with both file and console handlers."""
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.handlers = []  # Clear any existing handlers
        
        # Create file handler
        file_handler = RotatingFileHandler(
            self._log_dir / f"{name}.log",
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # Create console handler for all loggers except scheduler and when not suppressed
        if name != 'scheduler' and not suppress_stdout:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # Special handling for LLM logger
        if name == 'llm':
            litellm_logger = logging.getLogger('LiteLLM')
            litellm_logger.handlers = []
            litellm_logger.propagate = True
            litellm_logger.addHandler(logging.handlers.MemoryHandler(
                capacity=1024*1024,
                target=logger
            ))
            
            llm_config_logger = logging.getLogger('backendcrew.llm_config')
            llm_config_logger.handlers = []
            llm_config_logger.propagate = True
            llm_config_logger.addHandler(logging.handlers.MemoryHandler(
                capacity=1024*1024,
                target=logger
            ))
        
        # Special handling for scheduler logger
        elif name == 'scheduler':
            for scheduler_logger_name in [
                'backendcrew.scheduler',
                'apscheduler.scheduler',
                'apscheduler.executors',
                'apscheduler.jobstores'
            ]:
                sub_logger = logging.getLogger(scheduler_logger_name)
                sub_logger.handlers = []
                sub_logger.propagate = False
                sub_logger.setLevel(logging.INFO)
                sub_logger.addHandler(file_handler)
                # No console handler for scheduler-related loggers
        
        # Special handling for API logger
        elif name == 'api':
            for api_logger_name in [
                'backendcrew.api.runs',
                'backendcrew.api.jobs',
                'backendcrew.api.tools',
                'backendcrew.api.keys',
                'backendcrew.api.uc_tools'
            ]:
                api_logger = logging.getLogger(api_logger_name)
                api_logger.handlers = []
                api_logger.propagate = False
                api_logger.setLevel(logging.INFO)
                api_logger.addHandler(file_handler)
        
        # Special handling for access logger
        elif name == 'access':
            uvicorn_logger = logging.getLogger("uvicorn.access")
            uvicorn_logger.handlers = []
            uvicorn_logger.propagate = False  # Change to False to prevent logging to stdout
            
            class AccessLogHandler(logging.Handler):
                def __init__(self, target_logger, api_logger=None):
                    super().__init__()
                    self.target_logger = target_logger
                    self.api_logger = api_logger

                def emit(self, record):
                    try:
                        client_addr = getattr(record, 'client_addr', '-')
                        status_code = getattr(record, 'status_code', '-')
                        request_line = getattr(record, 'request_line', '-')
                        
                        # Skip empty or placeholder requests
                        if request_line == "-":
                            return
                            
                        msg = f"{client_addr} - \"{request_line}\" {status_code}"
                        
                        # Route API requests to the API log file
                        if self.api_logger and '/api/' in request_line:
                            self.api_logger.info(msg)
                        else:
                            self.target_logger.info(msg)
                    except Exception:
                        self.handleError(record)
            
            # Pass both loggers to handle routing based on the request path
            uvicorn_logger.addHandler(AccessLogHandler(logger, self._api_logger))
        
        return logger
    
    @property
    def crew(self) -> logging.Logger:
        """Get the crew-specific logger."""
        if not self._crew_logger:
            self.initialize()
        return self._crew_logger
    
    @property
    def system(self) -> logging.Logger:
        """Get the system-specific logger."""
        if not self._system_logger:
            self.initialize()
        return self._system_logger
    
    @property
    def llm(self) -> logging.Logger:
        """Get the LLM-specific logger."""
        if not self._llm_logger:
            self.initialize()
        return self._llm_logger
    
    @property
    def scheduler(self) -> logging.Logger:
        """Get the scheduler-specific logger."""
        if not self._scheduler_logger:
            self.initialize()
        return self._scheduler_logger
    
    @property
    def api(self) -> logging.Logger:
        """Get the API-specific logger."""
        if not self._api_logger:
            self.initialize()
        return self._api_logger
    
    @property
    def access(self) -> logging.Logger:
        """Get the access logger."""
        if not self._access_logger:
            self.initialize()
        return self._access_logger
    
    @property
    def guardrails(self) -> logging.Logger:
        """Get the guardrails logger."""
        if not self._guardrails_logger:
            self.initialize()
        return self._guardrails_logger 