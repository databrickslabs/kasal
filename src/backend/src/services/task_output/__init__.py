"""
Task-output sinks.

Where a task's output goes once it is produced. The engine builds these and
calls them; nothing here knows what a crew or a flow is, which is why they
live in services — an exported app or a chat turn can use the same sink.
"""

from src.services.task_output.base import KasalCallback, CallbackFailedError
from src.services.task_output.databricks_volume import DatabricksVolumeCallback

__all__ = [
    'KasalCallback',
    'CallbackFailedError',
    'DatabricksVolumeCallback',
]
