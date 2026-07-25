"""
Unit tests for the PromptOptimizationRun model.

The table is what makes an optimization run survive a backend restart, so the
tests pin the columns the service and API depend on — especially
`before_image`, without which an apply is irreversible.
"""

from datetime import datetime

import pytest
from sqlalchemy import JSON, Boolean, DateTime, Float, Integer, String, Text

from src.models.prompt_optimization_run import PromptOptimizationRun, generate_uuid


class TestTableShape:
    def test_table_name(self):
        assert PromptOptimizationRun.__tablename__ == "prompt_optimization_runs"

    def test_primary_key_is_the_run_id_string(self):
        column = PromptOptimizationRun.__table__.c.id
        assert column.primary_key is True
        assert isinstance(column.type, String)

    def test_generate_uuid_produces_distinct_strings(self):
        first, second = generate_uuid(), generate_uuid()
        assert first != second
        assert len(first) == 36

    @pytest.mark.parametrize(
        "name,sql_type",
        [
            ("kind", String),
            ("target_name", String),
            ("crew_id", String),
            ("status", String),
            ("error", Text),
            ("model", String),
            ("judge_model", String),
            ("reflection_model", String),
            ("budget", Integer),
            ("dataset_size", Integer),
            ("executions_used", Integer),
            ("execution_cap", Integer),
            ("candidates_tried", Integer),
            ("human_feedback_count", Integer),
            ("initial_score", Float),
            ("final_score", Float),
            ("baseline_template", Text),
            ("optimized_template", Text),
            ("baseline_fields", JSON),
            ("optimized_fields", JSON),
            ("before_image", JSON),
            ("applied", Boolean),
            ("applied_at", DateTime),
            ("applied_by", String),
            ("created_at", DateTime),
            ("updated_at", DateTime),
        ],
    )
    def test_column_types(self, name, sql_type):
        assert isinstance(PromptOptimizationRun.__table__.c[name].type, sql_type)

    def test_judge_model_is_recorded_separately_from_model(self):
        """Both are stored so a reader can tell whether the run judged itself
        (target == judge is self-preference)."""
        columns = PromptOptimizationRun.__table__.c
        assert "model" in columns and "judge_model" in columns

    def test_before_image_is_json_and_nullable(self):
        """Nullable because a run is not yet applied — and because runs applied
        by an older backend legitimately have none."""
        column = PromptOptimizationRun.__table__.c.before_image
        assert isinstance(column.type, JSON)
        assert column.nullable is True


class TestGroupIsolation:
    def test_group_columns_exist(self):
        columns = PromptOptimizationRun.__table__.c
        for name in ("group_id", "group_email", "created_by_email"):
            assert name in columns

    def test_group_id_is_indexed(self):
        assert PromptOptimizationRun.__table__.c.group_id.index is True

    def test_crew_id_is_indexed(self):
        assert PromptOptimizationRun.__table__.c.crew_id.index is True

    def test_composite_indexes(self):
        names = {index.name for index in PromptOptimizationRun.__table__.indexes}
        assert "idx_prompt_opt_runs_group_created" in names
        assert "idx_prompt_opt_runs_status" in names


class TestDefaults:
    def test_instance_defaults_are_applied_at_flush_time(self):
        """Column defaults are DB-side, so a bare instance has None until it is
        flushed — the service always supplies these explicitly."""
        run = PromptOptimizationRun(id="r1", target_name="detect_intent")
        assert run.kind is None
        columns = PromptOptimizationRun.__table__.c
        assert columns.kind.default.arg == "template"
        assert columns.status.default.arg == "pending"
        assert columns.applied.default.arg is False
        assert columns.dataset_size.default.arg == 0

    def test_updated_at_has_an_onupdate(self):
        """The heartbeat relies on updated_at moving; staleness detection reads
        it to tell a live run from one orphaned by a restart."""
        assert PromptOptimizationRun.__table__.c.updated_at.onupdate is not None

    def test_timestamps_default_to_naive_utc(self):
        """Naive UTC, matching every other model here — the API re-attaches
        tzinfo on read so browsers localize correctly."""
        for name in ("created_at", "updated_at"):
            produced = PromptOptimizationRun.__table__.c[name].default.arg(None)
            assert isinstance(produced, datetime)
            assert produced.tzinfo is None
            assert abs((produced - datetime.utcnow()).total_seconds()) < 5

    def test_repr_names_the_run(self):
        run = PromptOptimizationRun(
            id="abc", kind="crew", target_name="crew:Research", status="completed"
        )
        text = repr(run)
        assert "abc" in text and "crew:Research" in text and "completed" in text
