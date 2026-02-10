"""OpenTelemetry tracing integration for Kasal.

Provides OTel-based tracing as the sole trace source for all engine
(CrewAI, future LangGraph, etc.) and service layer components.

Execution spans are captured with proper parent-child hierarchy via
CrewAIInstrumentor and written to the execution_trace DB table by
KasalDBSpanExporter.
"""

from src.services.otel_tracing.otel_config import (
    is_otel_tracing_enabled,
    create_kasal_tracer_provider,
    shutdown_provider,
)
from src.services.otel_tracing.event_bridge import OTelEventBridge
from src.services.otel_tracing.mlflow_setup import (
    configure_mlflow_in_subprocess,
    MlflowSetupResult,
    execute_with_mlflow_trace,
    execute_with_mlflow_trace_async,
    post_execution_mlflow_cleanup,
)
from src.services.otel_tracing.mlflow_exporter import KasalMLflowSpanExporter

__all__ = [
    "is_otel_tracing_enabled",
    "create_kasal_tracer_provider",
    "shutdown_provider",
    "OTelEventBridge",
    "configure_mlflow_in_subprocess",
    "MlflowSetupResult",
    "execute_with_mlflow_trace",
    "execute_with_mlflow_trace_async",
    "post_execution_mlflow_cleanup",
    "KasalMLflowSpanExporter",
]
