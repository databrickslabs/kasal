"""Select trace storage without changing an experiment's existing UC binding.

These helpers perform blocking MLflow calls; setup callers run them in a worker.
Installation defaults apply when provisioning a destination. The experiment's
saved location remains authoritative on every subsequent start.
"""

from typing import Any


def select_experiment(mlflow: Any, name: str, proposed_location: Any = None) -> Any:
    existing = mlflow.get_experiment_by_name(name)
    saved = getattr(existing, "trace_location", None)
    if isinstance(getattr(saved, "full_table_prefix", None), str):
        proposed_location = saved
    if proposed_location is None:
        return mlflow.set_experiment(name)
    return mlflow.set_experiment(name, trace_location=proposed_location)


def log_trace_storage(log: Any, label: str, experiment: Any) -> None:
    """Report the server's resolved table, never a proposed/default prefix."""
    location = getattr(experiment, "trace_location", None)
    table = getattr(location, "full_otel_spans_table_name", None)
    if not isinstance(table, str) or not table:
        prefix = getattr(location, "full_table_prefix", None)
        table = f"{prefix}_otel_spans" if isinstance(prefix, str) and prefix else None
    if table:
        log.info(
            "[%s] MLflow trace storage: Unity Catalog spans=%s, experiment=%s (ID: %s)",
            label,
            table,
            experiment.name,
            experiment.experiment_id,
        )
    else:
        log.info(
            "[%s] MLflow experiment selected (ID: %s); resolved UC table not returned",
            label,
            experiment.experiment_id,
        )
