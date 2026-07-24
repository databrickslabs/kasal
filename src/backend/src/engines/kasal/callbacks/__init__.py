"""
CrewAI Callbacks Package

This package contains callbacks for use with the CrewAI engine. Callbacks provide
additional functionality that can be attached to task processing pipelines, such as
logging and storage of outputs.
"""

from src.engines.kasal.callbacks.base import KasalCallback
from src.engines.kasal.callbacks.streaming_callbacks import (
    LogCaptureHandler,
    JobOutputCallback
)
from src.engines.kasal.callbacks.logging_callbacks import (
    AgentTraceEventListener,
    TaskCompletionEventListener
)
from src.engines.kasal.callbacks.databricks_volume_callback import (
    DatabricksVolumeCallback
)

__all__ = [
    # Base
    'KasalCallback',

    # Streaming
    'LogCaptureHandler',
    'JobOutputCallback',

    # Logging
    'AgentTraceEventListener',
    'TaskCompletionEventListener',

    # Storage
    'DatabricksVolumeCallback',
]
