"""
Writing a task's output to a Databricks Volume.

``base`` is a small retrying async callback contract; ``volume_callback`` is its
one implementation. The engine builds it per task (see the ToolFactory/task
adapter) and auto-adds it when the active memory backend is Databricks.
"""

from src.services.databricks.volumes.base import CallbackFailedError, KasalCallback
from src.services.databricks.volumes.volume_callback import DatabricksVolumeCallback

__all__ = ["KasalCallback", "CallbackFailedError", "DatabricksVolumeCallback"]
