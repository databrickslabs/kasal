"""
Kasal engine callbacks.

What lives here is what an execution wires into the engine itself:
the crew's step/task hooks (``execution_callback``) and the volume writer.
Event SUBSCRIPTIONS belong to the OTel bridge (``services/otel_tracing``),
not to this package.
"""

from src.engines.kasal.callbacks.base import KasalCallback
from src.engines.kasal.callbacks.databricks_volume_callback import (
    DatabricksVolumeCallback
)

__all__ = [
    # Base
    'KasalCallback',

    # Storage
    'DatabricksVolumeCallback',
]
