"""
CrewAI Callbacks Package

This package contains callbacks for use with the CrewAI engine. Callbacks provide
additional functionality that can be attached to task processing pipelines, such as
logging, storage, and transformation of outputs.
"""

from src.engines.crewai.callbacks.base import CrewAICallback
from src.engines.crewai.callbacks.streaming_callbacks import (
    LogCaptureHandler,
    JobOutputCallback
)
from src.engines.crewai.callbacks.output_combiner_callbacks import OutputCombinerCallback
from src.engines.crewai.callbacks.logging_callbacks import (
    AgentTraceEventListener,
    TaskCompletionEventListener
)
from src.engines.crewai.callbacks.storage_callbacks import (
    JsonFileStorage,
    DatabaseStorage,
    FileSystemStorage
)
from src.engines.crewai.callbacks.databricks_volume_callback import (
    DatabricksVolumeCallback
)
from src.engines.crewai.callbacks.transformation_callbacks import (
    OutputFormatter,
    DataExtractor,
    OutputEnricher,
    OutputSummarizer
)
from src.engines.crewai.callbacks.validation_callbacks import (
    SchemaValidator,
    ContentValidator,
    TypeValidator
)

__all__ = [
    # Base
    'CrewAICallback',
    
    # Streaming
    'LogCaptureHandler',
    'JobOutputCallback',
    
    # Output Combiner
    'OutputCombinerCallback',
    
    # Logging
    'AgentTraceEventListener',
    'TaskCompletionEventListener',
    
    # Storage
    'JsonFileStorage',
    'DatabaseStorage',
    'FileSystemStorage',
    'DatabricksVolumeCallback',
    
    # Transformation
    'OutputFormatter',
    'DataExtractor',
    'OutputEnricher',
    'OutputSummarizer',
    
    # Validation
    'SchemaValidator',
    'ContentValidator',
    'TypeValidator',
] 