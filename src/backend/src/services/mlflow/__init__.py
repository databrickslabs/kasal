"""
MLflow: experiments, traces, and evaluation.

- ``service``           — the MLflow experiment/run API surface the app calls
- ``tracing``           — root traces, trace-id lookup, async flush/cleanup
- ``integration``       — autolog wiring and trace-id write-back for a run
- ``evaluation_runner`` — scoring runs against an MLflow experiment

The OTel side of the seam lives in ``services/otel_tracing`` (``mlflow_setup``,
``mlflow_exporter``, ``mlflow_parent_setup``): those are span EXPORTERS that
happen to target MLflow, so they sit with the rest of the OTel pipeline.
"""

from src.services.mlflow.service import MLflowService

__all__ = ['MLflowService']
