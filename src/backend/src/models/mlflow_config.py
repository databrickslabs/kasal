from datetime import datetime, timezone

from sqlalchemy import Boolean, Column, DateTime, Integer, String

from src.db.base import Base


class MLflowConfig(Base):
    """MLflow tracing settings for a workspace, independent of Databricks.

    These three flags used to live on ``DatabricksConfig``, which was coherent
    while MLflow *was* Databricks. It stopped being coherent once tracing could
    also go to a local OSS server — and it was not merely untidy: ``is_enabled``
    read the flag off the Databricks row, so a workspace with no Databricks
    configuration could never switch MLflow on **by construction rather than by
    choice**. The UI showed the same seam from the other side ("Please save
    Databricks settings first to persist MLflow"), which in a dev environment
    with nothing to save is a dead end.

    Memory is the precedent this follows: it can use Databricks Vector Search or
    a local store, and it has its own configuration rather than living inside
    the Databricks one.

    The old columns on ``databricksconfig`` are deliberately LEFT IN PLACE by the
    accompanying migration, which copies rather than moves. That makes the change
    a two-way door: a rollback loses nothing, and the columns can be dropped in a
    later release once this table has proven itself.
    """

    id = Column(Integer, primary_key=True)

    #: Whether crew executions are traced to MLflow at all. Which BACKEND they
    #: are traced to is derived (Databricks when configured, else a local
    #: server) and deliberately not stored — a stored backend choice can only
    #: ever disagree with what is actually available.
    enabled = Column(Boolean, default=False, nullable=False)

    #: Experiment to trace into, or NULL to let the backend name it.
    #:
    #: No column default on purpose. A stored default is indistinguishable from
    #: a name the user typed, so it silently outranks the derived
    #: ``kasal-<teamspace>-traces`` and the per-teamspace naming never applies —
    #: which is exactly what happened with an earlier
    #: ``default="kasal-crew-execution-traces"`` here. NULL means "not chosen",
    #: which is the only value the resolver can safely override.
    #:
    #: Stored without a workspace-path prefix; the Databricks backend adds
    #: ``/Shared/`` and the local one does not (see
    #: services/mlflow/local.local_experiment_name).
    experiment_name = Column(String, nullable=True)

    #: LLM-judge evaluation of finished runs — a separate, more expensive opt-in
    #: than tracing, hence its own flag rather than a mode of ``enabled``.
    evaluation_enabled = Column(Boolean, default=False, nullable=False)

    #: Judge endpoint route, e.g. "databricks:/<endpoint>".
    evaluation_judge_model = Column(String, nullable=True)

    # Multi-tenant fields — one row per group, same isolation rule as every
    # other configuration table.
    group_id = Column(String(100), index=True, nullable=True)
    created_by_email = Column(String(255), index=True, nullable=True)

    created_at = Column(DateTime(timezone=True), default=datetime.now(timezone.utc))
    updated_at = Column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
    )
