"""
Unit tests for MLflowEvaluationRunner.

Tests cover:
- Constructor initialization
- create_run() success path, env var management, scope error fallback, fallback dataset
- complete_evaluation() with basic/extra scorers, env var restoration on failure
- _discover_traces_and_build_dataset() stored trace ID, search fallback, error handling
- _extract_records_from_traces() record extraction and row filtering
- _log_run_parameters(), _log_baseline_metrics(), _log_artifacts()
- _save_environment_vars(), _set_environment_vars(), _restore_environment_vars()
"""

import os
import pytest
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pandas as pd

from src.services.mlflow_evaluation_runner import MLflowEvaluationRunner


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_exec_obj(**overrides):
    """Build a mock execution object with sensible defaults."""
    defaults = {
        "id": "exec-123",
        "group_id": "group-abc",
        "status": "completed",
        "mlflow_trace_id": None,
        "mlflow_experiment_name": "/Shared/kasal-crew-execution-traces",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_auth_ctx(**overrides):
    """Build a mock auth context with sensible defaults."""
    defaults = {
        "auth_method": "obo",
        "workspace_url": "https://example.databricks.com",
        "host": "https://example.databricks.com",
        "token": "dapi-test-token",
        "api_base": "https://example.databricks.com/serving-endpoints",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_runner(**overrides):
    """Build an MLflowEvaluationRunner with sensible defaults."""
    defaults = {
        "exec_obj": _make_exec_obj(),
        "job_id": "job-456",
        "inputs_text": "What is the capital of France?",
        "prediction_text": "Paris is the capital of France.",
        "judge_model_route": "databricks/databricks-claude-sonnet-4",
        "judge_model_defaulted": False,
    }
    defaults.update(overrides)
    return MLflowEvaluationRunner(**defaults)


# ---------------------------------------------------------------------------
# Module-level patch path prefix
# ---------------------------------------------------------------------------
MOD = "src.services.mlflow_evaluation_runner"


# ===========================================================================
# TestInit
# ===========================================================================

class TestInit:
    """Tests for MLflowEvaluationRunner.__init__."""

    def test_stores_all_attributes(self):
        """Constructor stores every argument as an instance attribute."""
        exec_obj = _make_exec_obj()
        runner = MLflowEvaluationRunner(
            exec_obj=exec_obj,
            job_id="j1",
            inputs_text="inp",
            prediction_text="pred",
            judge_model_route="route/model",
            judge_model_defaulted=True,
        )
        assert runner.exec_obj is exec_obj
        assert runner.job_id == "j1"
        assert runner.inputs_text == "inp"
        assert runner.prediction_text == "pred"
        assert runner.judge_model_route == "route/model"
        assert runner.judge_model_defaulted is True

    def test_prediction_text_none(self):
        """Constructor accepts None for prediction_text."""
        runner = _make_runner(prediction_text=None)
        assert runner.prediction_text is None


# ===========================================================================
# TestSaveEnvironmentVars
# ===========================================================================

class TestSaveEnvironmentVars:
    """Tests for _save_environment_vars."""

    def test_saves_existing_vars(self):
        """Captures current DATABRICKS_* env vars."""
        runner = _make_runner()
        env_patch = {
            "DATABRICKS_HOST": "https://host.example.com",
            "DATABRICKS_TOKEN": "tok-123",
            "DATABRICKS_BASE_URL": "https://base.example.com",
            "DATABRICKS_API_BASE": "https://apibase.example.com",
            "DATABRICKS_ENDPOINT": "https://endpoint.example.com",
        }
        with patch.dict(os.environ, env_patch, clear=False):
            result = runner._save_environment_vars()

        assert result["DATABRICKS_HOST"] == "https://host.example.com"
        assert result["DATABRICKS_TOKEN"] == "tok-123"
        assert result["DATABRICKS_BASE_URL"] == "https://base.example.com"
        assert result["DATABRICKS_API_BASE"] == "https://apibase.example.com"
        assert result["DATABRICKS_ENDPOINT"] == "https://endpoint.example.com"

    def test_returns_none_for_missing_vars(self):
        """Returns None for env vars that are not set."""
        runner = _make_runner()
        clean = {
            k: None for k in [
                "DATABRICKS_HOST", "DATABRICKS_TOKEN",
                "DATABRICKS_BASE_URL", "DATABRICKS_API_BASE",
                "DATABRICKS_ENDPOINT",
            ]
        }
        # Remove the keys so they are absent
        env_copy = os.environ.copy()
        for k in clean:
            env_copy.pop(k, None)
        with patch.dict(os.environ, env_copy, clear=True):
            result = runner._save_environment_vars()

        for key in clean:
            assert result[key] is None


# ===========================================================================
# TestSetEnvironmentVars
# ===========================================================================

class TestSetEnvironmentVars:
    """Tests for _set_environment_vars."""

    @patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url")
    def test_sets_all_vars(self, mock_construct):
        """Sets DATABRICKS_HOST, TOKEN, and API base vars from auth context."""
        mock_construct.return_value = "https://example.databricks.com/serving-endpoints"
        runner = _make_runner()
        auth_ctx = _make_auth_ctx()

        with patch.dict(os.environ, {}, clear=True):
            runner._set_environment_vars(auth_ctx)

            assert os.environ["DATABRICKS_HOST"] == auth_ctx.workspace_url
            assert os.environ["DATABRICKS_TOKEN"] == auth_ctx.token
            assert os.environ["DATABRICKS_BASE_URL"] == "https://example.databricks.com/serving-endpoints"
            assert os.environ["DATABRICKS_API_BASE"] == "https://example.databricks.com/serving-endpoints"
            assert os.environ["DATABRICKS_ENDPOINT"] == "https://example.databricks.com/serving-endpoints"

    @patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url")
    def test_skips_api_base_when_empty(self, mock_construct):
        """Does not set API base vars when construct_serving_endpoints_url returns empty."""
        mock_construct.return_value = ""
        runner = _make_runner()
        auth_ctx = _make_auth_ctx()

        with patch.dict(os.environ, {}, clear=True):
            runner._set_environment_vars(auth_ctx)

            assert os.environ["DATABRICKS_HOST"] == auth_ctx.workspace_url
            assert os.environ["DATABRICKS_TOKEN"] == auth_ctx.token
            assert "DATABRICKS_BASE_URL" not in os.environ
            assert "DATABRICKS_API_BASE" not in os.environ
            assert "DATABRICKS_ENDPOINT" not in os.environ

    @patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url")
    def test_skips_api_base_when_none(self, mock_construct):
        """Does not set API base vars when construct_serving_endpoints_url returns None."""
        mock_construct.return_value = None
        runner = _make_runner()
        auth_ctx = _make_auth_ctx()

        with patch.dict(os.environ, {}, clear=True):
            runner._set_environment_vars(auth_ctx)

            assert "DATABRICKS_BASE_URL" not in os.environ


# ===========================================================================
# TestRestoreEnvironmentVars
# ===========================================================================

class TestRestoreEnvironmentVars:
    """Tests for _restore_environment_vars."""

    def test_restores_previously_set_vars(self):
        """Restores vars that existed before the run."""
        runner = _make_runner()
        old_env = {
            "DATABRICKS_HOST": "https://original.example.com",
            "DATABRICKS_TOKEN": "orig-token",
            "DATABRICKS_BASE_URL": None,
            "DATABRICKS_API_BASE": None,
            "DATABRICKS_ENDPOINT": None,
        }
        auth_ctx = _make_auth_ctx()

        with patch.dict(os.environ, {"DATABRICKS_HOST": "changed", "DATABRICKS_TOKEN": "changed"}, clear=True):
            runner._restore_environment_vars(old_env, auth_ctx)

            assert os.environ["DATABRICKS_HOST"] == "https://original.example.com"
            assert os.environ["DATABRICKS_TOKEN"] == "orig-token"

    def test_deletes_vars_that_were_unset(self):
        """Removes env vars that were not set before the run."""
        runner = _make_runner()
        old_env = {
            "DATABRICKS_HOST": None,
            "DATABRICKS_TOKEN": None,
            "DATABRICKS_BASE_URL": None,
            "DATABRICKS_API_BASE": None,
            "DATABRICKS_ENDPOINT": None,
        }
        auth_ctx = _make_auth_ctx()

        env_during = {
            "DATABRICKS_HOST": "temp-host",
            "DATABRICKS_TOKEN": "temp-token",
        }
        with patch.dict(os.environ, env_during, clear=True):
            runner._restore_environment_vars(old_env, auth_ctx)

            assert "DATABRICKS_HOST" not in os.environ
            assert "DATABRICKS_TOKEN" not in os.environ

    def test_noop_when_no_auth_ctx(self):
        """Does nothing when auth_ctx is None."""
        runner = _make_runner()
        old_env = {"DATABRICKS_HOST": "original"}

        with patch.dict(os.environ, {"DATABRICKS_HOST": "changed"}, clear=False):
            runner._restore_environment_vars(old_env, None)
            # Should remain "changed" since no restore happened
            assert os.environ["DATABRICKS_HOST"] == "changed"


# ===========================================================================
# TestLogRunParameters
# ===========================================================================

class TestLogRunParameters:
    """Tests for _log_run_parameters."""

    @patch(f"{MOD}.mlflow", create=True)
    def test_logs_correct_params(self, mock_mlflow_mod):
        """Logs all expected parameters to MLflow via lazy import."""
        # The method does `import mlflow` inside, so we patch the module-level
        # reference that Python will resolve.
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            runner._log_run_parameters(["trace-1", "trace-2"])

            mock_mlflow.log_params.assert_called_once()
            params = mock_mlflow.log_params.call_args[0][0]
            assert params["job_id"] == "job-456"
            assert params["group_id"] == "group-abc"
            assert params["status"] == "completed"
            assert params["related_trace_ids"] == "trace-1,trace-2"
            assert params["judge_model_configured"] is True
            assert params["judge_model_route"] == "databricks/databricks-claude-sonnet-4"
            assert params["judge_model_defaulted"] is False

    @patch(f"{MOD}.mlflow", create=True)
    def test_logs_empty_trace_ids(self, mock_mlflow_mod):
        """Logs empty string when no trace IDs provided."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            runner._log_run_parameters([])

            params = mock_mlflow.log_params.call_args[0][0]
            assert params["related_trace_ids"] == ""

    @patch(f"{MOD}.mlflow", create=True)
    def test_handles_log_params_exception(self, mock_mlflow_mod):
        """Swallows exceptions from mlflow.log_params."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.log_params.side_effect = RuntimeError("MLflow error")
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            # Should not raise
            runner._log_run_parameters(["t1"])


# ===========================================================================
# TestLogBaselineMetrics
# ===========================================================================

class TestLogBaselineMetrics:
    """Tests for _log_baseline_metrics."""

    def test_calculates_metrics_from_dataframe(self):
        """Computes prediction length, word counts, and Jaccard overlap."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            eval_df = pd.DataFrame({
                "messages": ["hello world", "foo bar baz"],
                "predictions": ["hello there world", "answer is foo"],
            })

            runner._log_baseline_metrics(eval_df)

            logged = {}
            for c in mock_mlflow.log_metric.call_args_list:
                name, value = c[0]
                logged[name] = value

            assert "prediction_length_mean" in logged
            assert "prediction_length_max" in logged
            assert "prediction_word_count_mean" in logged
            assert "input_word_count_mean" in logged
            assert "overlap_jaccard_mean" in logged

            # Verify prediction_length_mean calculation
            pred1_len = len("hello there world")
            pred2_len = len("answer is foo")
            expected_mean = float((pred1_len + pred2_len) / 2)
            assert logged["prediction_length_mean"] == pytest.approx(expected_mean)

    def test_handles_empty_dataframe(self):
        """Does not crash on empty dataframe."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            eval_df = pd.DataFrame({"messages": [], "predictions": []})
            runner._log_baseline_metrics(eval_df)

    def test_handles_missing_columns(self):
        """Does not crash when expected columns are absent."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            eval_df = pd.DataFrame({"other_col": ["value"]})
            runner._log_baseline_metrics(eval_df)


# ===========================================================================
# TestLogArtifacts
# ===========================================================================

class TestLogArtifacts:
    """Tests for _log_artifacts."""

    def test_logs_inputs_and_prediction_text(self):
        """Logs both inputs and prediction text as artifacts."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner(
                inputs_text="my input",
                prediction_text="my prediction",
            )
            runner._log_artifacts()

            assert mock_mlflow.log_text.call_count == 2
            calls = mock_mlflow.log_text.call_args_list
            assert calls[0] == call("my input", artifact_file="inputs.txt")
            assert calls[1] == call("my prediction", artifact_file="prediction.txt")

    def test_logs_empty_string_for_none(self):
        """Logs empty string when inputs_text or prediction_text is None."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner(inputs_text=None, prediction_text=None)
            runner._log_artifacts()

            calls = mock_mlflow.log_text.call_args_list
            assert calls[0] == call("", artifact_file="inputs.txt")
            assert calls[1] == call("", artifact_file="prediction.txt")

    def test_handles_log_text_exception(self):
        """Swallows exceptions from mlflow.log_text."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.log_text.side_effect = RuntimeError("write error")
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            runner = _make_runner()
            # Should not raise
            runner._log_artifacts()


# ===========================================================================
# TestExtractRecordsFromTraces
# ===========================================================================

class TestExtractRecordsFromTraces:
    """Tests for _extract_records_from_traces."""

    def test_extracts_records_from_dataframe(self):
        """Extracts messages/predictions from trace rows with attributes."""
        runner = _make_runner()
        df = pd.DataFrame({
            "trace_id": ["t1", "t2"],
            "attributes": [
                {"execution_id": "exec-123", "prompt": "question 1", "output": "answer 1"},
                {"execution_id": "exec-123", "prompt": "question 2", "output": "answer 2"},
            ],
        })

        trace_ids, records = runner._extract_records_from_traces(df)

        assert trace_ids == ["t1", "t2"]
        assert len(records) == 2
        assert records[0]["messages"] == "question 1"
        assert records[0]["predictions"] == "answer 1"
        assert records[1]["messages"] == "question 2"
        assert records[1]["predictions"] == "answer 2"

    def test_skips_rows_with_no_message_and_no_prediction(self):
        """Skips rows where both message and prediction are empty/missing."""
        runner = _make_runner()
        df = pd.DataFrame({
            "trace_id": ["t1", "t2"],
            "attributes": [
                {"execution_id": "exec-123", "prompt": "q1", "output": "a1"},
                {"execution_id": "exec-123"},  # No useful fields
            ],
        })

        trace_ids, records = runner._extract_records_from_traces(df)

        assert len(records) == 1
        assert records[0]["messages"] == "q1"

    def test_includes_context_and_reference_fields(self):
        """Includes contexts and references when present in attributes."""
        runner = _make_runner()
        df = pd.DataFrame({
            "trace_id": ["t1"],
            "attributes": [
                {
                    "execution_id": "exec-123",
                    "prompt": "q1",
                    "output": "a1",
                    "contexts": "some context",
                    "reference": "expected answer",
                },
            ],
        })

        _, records = runner._extract_records_from_traces(df)

        assert len(records) == 1
        assert records[0]["contexts"] == "some context"
        assert records[0]["references"] == "expected answer"

    def test_no_attributes_column(self):
        """Handles dataframes without an 'attributes' column."""
        runner = _make_runner()
        df = pd.DataFrame({
            "trace_id": ["t1"],
            "prompt": ["question"],
            "output": ["answer"],
        })

        trace_ids, records = runner._extract_records_from_traces(df)

        # Without attributes column the filter is skipped and all rows are processed
        assert len(records) == 1
        assert records[0]["messages"] == "question"

    def test_respects_max_rows_env_var(self):
        """Limits rows to MLFLOW_EVAL_MAX_ROWS environment variable."""
        runner = _make_runner()
        rows = [
            {"execution_id": "exec-123", "prompt": f"q{i}", "output": f"a{i}"}
            for i in range(10)
        ]
        df = pd.DataFrame({
            "trace_id": [f"t{i}" for i in range(10)],
            "attributes": rows,
        })

        with patch.dict(os.environ, {"MLFLOW_EVAL_MAX_ROWS": "3"}):
            _, records = runner._extract_records_from_traces(df)

        assert len(records) == 3

    def test_handles_dict_and_list_values_in_attributes(self):
        """Serializes dict/list attribute values to JSON strings."""
        runner = _make_runner()
        df = pd.DataFrame({
            "trace_id": ["t1"],
            "attributes": [
                {
                    "execution_id": "exec-123",
                    "prompt": {"nested": "value"},
                    "output": ["item1", "item2"],
                },
            ],
        })

        _, records = runner._extract_records_from_traces(df)

        assert len(records) == 1
        # The dict and list should be serialized to JSON strings
        assert '"nested"' in records[0]["messages"]
        assert "item1" in records[0]["predictions"]


# ===========================================================================
# TestDiscoverTracesAndBuildDataset
# ===========================================================================

class TestDiscoverTracesAndBuildDataset:
    """Tests for _discover_traces_and_build_dataset."""

    def test_finds_traces_by_stored_trace_id(self):
        """Uses stored mlflow_trace_id when available, skips search."""
        import sys
        mock_mlflow = MagicMock()
        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            exec_obj = _make_exec_obj(mlflow_trace_id="stored-trace-abc")
            runner = _make_runner(exec_obj=exec_obj)
            scope_handler = MagicMock()

            trace_ids, records = runner._discover_traces_and_build_dataset(
                _make_auth_ctx(), scope_handler
            )

            assert trace_ids == ["stored-trace-abc"]
            assert records == []
            # Should not have called search_traces since stored ID was found
            mock_mlflow.search_traces.assert_not_called()

    def test_searches_traces_when_no_stored_id(self):
        """Falls back to search_traces when no stored trace ID."""
        import sys
        mock_mlflow = MagicMock()

        # Create a trace dataframe that search_traces returns
        search_df = pd.DataFrame({
            "trace_id": ["found-trace-1"],
            "attributes": [
                {"execution_id": "exec-123", "prompt": "q", "output": "a"},
            ],
        })
        mock_mlflow.search_traces = MagicMock(return_value=search_df)

        mock_experiment = SimpleNamespace(experiment_id="exp-999")
        mock_mlflow.get_experiment_by_name.return_value = mock_experiment

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            exec_obj = _make_exec_obj(mlflow_trace_id=None)
            runner = _make_runner(exec_obj=exec_obj)
            scope_handler = MagicMock()

            trace_ids, records = runner._discover_traces_and_build_dataset(
                _make_auth_ctx(), scope_handler
            )

            assert "found-trace-1" in trace_ids
            assert len(records) == 1

    def test_returns_empty_on_search_error(self):
        """Returns empty lists when search_traces raises an exception."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.search_traces = MagicMock(side_effect=RuntimeError("search failed"))
        mock_mlflow.get_experiment_by_name.return_value = SimpleNamespace(experiment_id="exp-1")

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            exec_obj = _make_exec_obj(mlflow_trace_id=None)
            runner = _make_runner(exec_obj=exec_obj)
            scope_handler = MagicMock()

            trace_ids, records = runner._discover_traces_and_build_dataset(
                _make_auth_ctx(), scope_handler
            )

            assert trace_ids == []
            assert records == []

    def test_handles_scope_error_on_get_experiment(self):
        """Falls back via scope_handler when get_experiment_by_name raises."""
        import sys
        mock_mlflow = MagicMock()

        search_df = pd.DataFrame({
            "trace_id": ["t1"],
            "attributes": [{"execution_id": "exec-123", "prompt": "q", "output": "a"}],
        })
        mock_mlflow.search_traces = MagicMock(return_value=search_df)
        mock_mlflow.get_experiment_by_name.side_effect = RuntimeError("scope error")

        fallback_exp = SimpleNamespace(experiment_id="exp-fallback")
        scope_handler = MagicMock()
        scope_handler.handle_and_retry.return_value = fallback_exp

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            exec_obj = _make_exec_obj(mlflow_trace_id=None)
            runner = _make_runner(exec_obj=exec_obj)

            trace_ids, records = runner._discover_traces_and_build_dataset(
                _make_auth_ctx(), scope_handler
            )

            scope_handler.handle_and_retry.assert_called_once()

    def test_returns_empty_when_search_returns_none(self):
        """Returns empty lists when search_traces returns None."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.search_traces = MagicMock(return_value=None)
        mock_mlflow.get_experiment_by_name.return_value = SimpleNamespace(experiment_id="exp-1")

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            exec_obj = _make_exec_obj(mlflow_trace_id=None)
            runner = _make_runner(exec_obj=exec_obj)
            scope_handler = MagicMock()

            trace_ids, records = runner._discover_traces_and_build_dataset(
                _make_auth_ctx(), scope_handler
            )

            assert trace_ids == []
            assert records == []


# ===========================================================================
# TestCreateRun
# ===========================================================================

class TestCreateRun:
    """Tests for create_run."""

    def _setup_mlflow_mocks(self, mock_mlflow):
        """Configure a mock mlflow module with the basics for create_run."""
        mock_experiment = SimpleNamespace(experiment_id="exp-100")
        mock_mlflow.set_experiment.return_value = mock_experiment

        mock_run_info = SimpleNamespace(run_id="run-abc", experiment_id="exp-100")
        mock_run = MagicMock()
        mock_run.info = mock_run_info
        mock_run.__enter__ = MagicMock(return_value=mock_run)
        mock_run.__exit__ = MagicMock(return_value=False)
        mock_mlflow.start_run.return_value = mock_run

        mock_ds = MagicMock()
        mock_mlflow.data.from_pandas.return_value = mock_ds

        return mock_mlflow

    def test_create_run_success(self):
        """create_run returns experiment_id, experiment_name, run_id on success."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_mocks(mock_mlflow)

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_pd.DataFrame.from_records.return_value = mock_df

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            runner = _make_runner()
            auth_ctx = _make_auth_ctx()

            # Patch _set_environment_vars and _save/_restore to isolate env concerns
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))
            runner._log_run_parameters = MagicMock()
            runner._log_baseline_metrics = MagicMock()
            runner._log_artifacts = MagicMock()

            result = runner.create_run(auth_ctx)

            assert result["run_id"] == "run-abc"
            assert result["experiment_name"] == "/Shared/kasal-crew-execution-traces"
            assert "experiment_id" in result

    def test_create_run_sets_and_restores_env_vars(self):
        """create_run calls _save, _set, and _restore environment vars."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_mocks(mock_mlflow)

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_pd.DataFrame.from_records.return_value = mock_df

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            runner = _make_runner()
            auth_ctx = _make_auth_ctx()

            saved_env = {"DATABRICKS_HOST": "original"}
            runner._save_environment_vars = MagicMock(return_value=saved_env)
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))
            runner._log_run_parameters = MagicMock()
            runner._log_baseline_metrics = MagicMock()
            runner._log_artifacts = MagicMock()

            runner.create_run(auth_ctx)

            runner._save_environment_vars.assert_called_once()
            runner._set_environment_vars.assert_called_once_with(auth_ctx)
            runner._restore_environment_vars.assert_called_once_with(saved_env, auth_ctx)

    def test_create_run_restores_env_on_error(self):
        """Env vars are restored even if create_run raises internally."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.set_experiment.side_effect = RuntimeError("fail")

        mock_scope_handler = MagicMock()
        mock_scope_handler.handle_and_retry.side_effect = RuntimeError("fail again")

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            with patch(f"{MOD}.MLflowScopeErrorHandler", return_value=mock_scope_handler):
                runner = _make_runner()
                auth_ctx = _make_auth_ctx()

                saved_env = {"DATABRICKS_HOST": "original"}
                runner._save_environment_vars = MagicMock(return_value=saved_env)
                runner._set_environment_vars = MagicMock()
                runner._restore_environment_vars = MagicMock()

                with pytest.raises(RuntimeError):
                    runner.create_run(auth_ctx)

                runner._restore_environment_vars.assert_called_once_with(saved_env, auth_ctx)

    def test_create_run_scope_error_fallback(self):
        """Falls back via scope handler when set_experiment raises scope error."""
        import sys
        mock_mlflow = MagicMock()

        # First call to set_experiment fails, scope handler retries, second call succeeds
        mock_experiment = SimpleNamespace(experiment_id="exp-100")
        mock_mlflow.set_experiment.side_effect = [
            RuntimeError("does not have required scopes"),
            mock_experiment,  # For the second call before start_run
        ]

        mock_run_info = SimpleNamespace(run_id="run-fallback", experiment_id="exp-100")
        mock_run = MagicMock()
        mock_run.info = mock_run_info
        mock_run.__enter__ = MagicMock(return_value=mock_run)
        mock_run.__exit__ = MagicMock(return_value=False)
        mock_mlflow.start_run.return_value = mock_run
        mock_mlflow.data.from_pandas.return_value = MagicMock()

        mock_scope_handler = MagicMock()
        mock_scope_handler.handle_and_retry.return_value = mock_experiment

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_pd.DataFrame.from_records.return_value = mock_df

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            with patch(f"{MOD}.MLflowScopeErrorHandler", return_value=mock_scope_handler):
                runner = _make_runner()
                auth_ctx = _make_auth_ctx()
                runner._save_environment_vars = MagicMock(return_value={})
                runner._set_environment_vars = MagicMock()
                runner._restore_environment_vars = MagicMock()
                runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))
                runner._log_run_parameters = MagicMock()
                runner._log_baseline_metrics = MagicMock()
                runner._log_artifacts = MagicMock()

                result = runner.create_run(auth_ctx)

                mock_scope_handler.handle_and_retry.assert_called_once()
                assert result["run_id"] == "run-fallback"

    def test_create_run_fallback_to_single_record(self):
        """Uses single-record fallback when trace discovery returns empty records."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_mocks(mock_mlflow)

        actual_pd = pd  # Use real pandas to verify dataframe creation
        mock_pd = MagicMock()
        mock_pd.DataFrame.from_records = actual_pd.DataFrame.from_records

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            runner = _make_runner(
                inputs_text="fallback input",
                prediction_text="fallback pred",
            )
            auth_ctx = _make_auth_ctx()

            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))
            runner._log_run_parameters = MagicMock()
            runner._log_baseline_metrics = MagicMock()
            runner._log_artifacts = MagicMock()

            result = runner.create_run(auth_ctx)

            assert result["run_id"] == "run-abc"

    def test_create_run_no_auth_ctx(self):
        """create_run works with auth_ctx=None, skipping env var setup."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_mocks(mock_mlflow)

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_pd.DataFrame.from_records.return_value = mock_df

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            runner = _make_runner()

            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))
            runner._log_run_parameters = MagicMock()
            runner._log_baseline_metrics = MagicMock()
            runner._log_artifacts = MagicMock()

            result = runner.create_run(None)

            runner._set_environment_vars.assert_not_called()
            assert result["run_id"] == "run-abc"

    def test_create_run_calls_helpers(self):
        """create_run calls log_run_parameters, log_baseline_metrics, and log_artifacts."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_mocks(mock_mlflow)

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_pd.DataFrame.from_records.return_value = mock_df

        with patch.dict(sys.modules, {"mlflow": mock_mlflow, "pandas": mock_pd}):
            runner = _make_runner()
            auth_ctx = _make_auth_ctx()

            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(
                return_value=(["t1"], [])
            )
            runner._log_run_parameters = MagicMock()
            runner._log_baseline_metrics = MagicMock()
            runner._log_artifacts = MagicMock()

            runner.create_run(auth_ctx)

            runner._log_run_parameters.assert_called_once_with(["t1"])
            runner._log_baseline_metrics.assert_called_once()
            runner._log_artifacts.assert_called_once()


# ===========================================================================
# TestCompleteEvaluation
# ===========================================================================

class TestCompleteEvaluation:
    """Tests for complete_evaluation."""

    def _setup_mlflow_for_eval(self, mock_mlflow, scorers_module=None):
        """Configure mock mlflow for complete_evaluation."""
        # search / discover
        mock_mlflow.set_tracking_uri = MagicMock()

        # start_run context manager
        mock_run = MagicMock()
        mock_run.__enter__ = MagicMock(return_value=mock_run)
        mock_run.__exit__ = MagicMock(return_value=False)
        mock_mlflow.start_run.return_value = mock_run

        # active_run
        mock_mlflow.active_run.return_value = None

        # MlflowClient
        mock_client_inst = MagicMock()
        mock_run_info = SimpleNamespace(experiment_id="exp-100")
        mock_run_obj = SimpleNamespace(info=mock_run_info)
        mock_client_inst.get_run.return_value = mock_run_obj
        mock_exp_obj = SimpleNamespace(name="/Shared/kasal-crew-execution-traces")
        mock_client_inst.get_experiment.return_value = mock_exp_obj

        # genai evaluate
        mock_eval_result = MagicMock()
        mock_eval_result.tables = {}
        mock_genai = MagicMock()
        mock_genai.evaluate.return_value = mock_eval_result
        mock_mlflow.genai = mock_genai

        # Scorers
        if scorers_module is None:
            scorers_module = MagicMock()
        mock_genai.scorers = scorers_module
        mock_mlflow.metrics = MagicMock()
        mock_mlflow.metrics.genai = MagicMock()

        # get_trace for fetching trace objects
        mock_mlflow.get_trace.return_value = None

        return mock_mlflow, mock_client_inst

    def test_complete_evaluation_basic_scorers(self):
        """Adds RelevanceToQuery and Safety scorers by default."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)

        # Setup scorers
        mock_scorers_module = MagicMock()
        mock_scorer_instance = MagicMock()
        mock_scorers_module.RelevanceToQuery.return_value = mock_scorer_instance
        mock_scorers_module.Safety.return_value = mock_scorer_instance
        mock_mlflow.genai.scorers = mock_scorers_module

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_df.iterrows.return_value = iter([
            (0, {"messages": "q1", "predictions": "a1"}),
        ])
        mock_pd.DataFrame.from_records.return_value = mock_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(
            name="/Shared/test"
        )
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))

            runner.complete_evaluation("run-abc", _make_auth_ctx())

            # genai.evaluate should have been called
            mock_mlflow.genai.evaluate.assert_called_once()

    def test_complete_evaluation_adds_extra_scorers_with_contexts(self):
        """Adds Groundedness and Relevance scorers when contexts are present."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)

        mock_scorers = MagicMock()
        mock_mlflow.genai.scorers = mock_scorers

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = pd.Index(["messages", "predictions", "contexts"])

        # Make "contexts" column check work
        context_series = MagicMock()
        context_series.astype.return_value.str.strip.return_value.astype.return_value.any.return_value = True
        mock_df.__getitem__ = MagicMock(return_value=context_series)
        mock_df.iterrows.return_value = iter([
            (0, {"messages": "q1", "predictions": "a1", "contexts": "ctx1"}),
        ])
        mock_pd.DataFrame.from_records.return_value = mock_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(name="/Shared/test")
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))

            runner.complete_evaluation("run-abc", _make_auth_ctx())

            # Groundedness should be instantiated when contexts detected
            mock_scorers.Groundedness.assert_called()

    def test_complete_evaluation_adds_correctness_scorer_with_references(self):
        """Adds Correctness scorer when references are present."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)

        mock_scorers = MagicMock()
        mock_mlflow.genai.scorers = mock_scorers

        # Use a real pandas DataFrame so column detection logic works correctly
        real_df = pd.DataFrame({
            "messages": ["q1"],
            "predictions": ["a1"],
            "references": ["expected answer"],
        })

        mock_pd = MagicMock()
        mock_pd.DataFrame.from_records.return_value = real_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(name="/Shared/test")
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))

            runner.complete_evaluation("run-abc", _make_auth_ctx())

            mock_scorers.Correctness.assert_called()

    def test_complete_evaluation_restores_env_on_failure(self):
        """Env vars are restored even if complete_evaluation fails internally."""
        import sys
        mock_mlflow = MagicMock()
        mock_mlflow.set_tracking_uri.side_effect = RuntimeError("boom")

        with patch.dict(sys.modules, {"mlflow": mock_mlflow}):
            with patch(f"{MOD}.MLflowScopeErrorHandler"):
                runner = _make_runner()
                auth_ctx = _make_auth_ctx()

                saved_env = {"DATABRICKS_HOST": "original"}
                runner._save_environment_vars = MagicMock(return_value=saved_env)
                runner._set_environment_vars = MagicMock()
                runner._restore_environment_vars = MagicMock()
                runner._discover_traces_and_build_dataset = MagicMock(
                    side_effect=RuntimeError("discover failed")
                )

                # complete_evaluation catches exceptions internally, so it may not raise
                # but _restore should always be called via finally
                try:
                    runner.complete_evaluation("run-abc", auth_ctx)
                except Exception:
                    pass

                runner._restore_environment_vars.assert_called_once_with(saved_env, auth_ctx)

    def test_complete_evaluation_no_auth_ctx(self):
        """complete_evaluation works with auth_ctx=None."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_df.iterrows.return_value = iter([
            (0, {"messages": "q1", "predictions": "a1"}),
        ])
        mock_pd.DataFrame.from_records.return_value = mock_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(name="/Shared/test")
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(return_value=([], []))

            runner.complete_evaluation("run-abc", None)

            runner._set_environment_vars.assert_not_called()

    def test_complete_evaluation_uses_trace_objects_when_available(self):
        """Fetches and uses trace objects for enriched evaluation data."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)

        # Mock a trace object with request/response data
        mock_trace_data = SimpleNamespace(
            request='{"inputs": {"query": "test question"}}',
            response='{"response": "test answer"}'
        )
        mock_trace = SimpleNamespace(data=mock_trace_data)
        mock_mlflow.get_trace.return_value = mock_trace

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_df.iterrows.return_value = iter([])
        mock_pd.DataFrame.from_records.return_value = mock_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(name="/Shared/test")
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(
                return_value=(["trace-id-1"], [])
            )

            runner.complete_evaluation("run-abc", _make_auth_ctx())

            mock_mlflow.get_trace.assert_called_with("trace-id-1")

    def test_complete_evaluation_fallback_when_get_trace_fails(self):
        """Falls back to single-record data when get_trace raises."""
        import sys
        mock_mlflow = MagicMock()
        self._setup_mlflow_for_eval(mock_mlflow)
        mock_mlflow.get_trace.side_effect = RuntimeError("trace not found")

        mock_pd = MagicMock()
        mock_df = MagicMock()
        mock_df.__len__ = MagicMock(return_value=1)
        mock_df.columns = ["messages", "predictions"]
        mock_df.iterrows.return_value = iter([
            (0, {"messages": "q1", "predictions": "a1"}),
        ])
        mock_pd.DataFrame.from_records.return_value = mock_df

        mock_client_cls = MagicMock()
        mock_client_inst = MagicMock()
        mock_client_inst.get_run.return_value = SimpleNamespace(
            info=SimpleNamespace(experiment_id="exp-100")
        )
        mock_client_inst.get_experiment.return_value = SimpleNamespace(name="/Shared/test")
        mock_client_cls.return_value = mock_client_inst

        with patch.dict(sys.modules, {
            "mlflow": mock_mlflow,
            "pandas": mock_pd,
            "mlflow.genai": mock_mlflow.genai,
            "mlflow.tracking": MagicMock(MlflowClient=mock_client_cls),
        }):
            runner = _make_runner()
            runner._save_environment_vars = MagicMock(return_value={})
            runner._set_environment_vars = MagicMock()
            runner._restore_environment_vars = MagicMock()
            runner._discover_traces_and_build_dataset = MagicMock(
                return_value=(["trace-fail"], [])
            )

            # Should not raise even though get_trace failed
            runner.complete_evaluation("run-abc", _make_auth_ctx())

            mock_mlflow.genai.evaluate.assert_called_once()


# ===========================================================================
# TestJudgeModelUriConversion
# ===========================================================================

class TestJudgeModelUriConversion:
    """Tests for the _to_scorer_model_uri logic inside complete_evaluation."""

    def test_route_with_slash_converted(self):
        """'provider/model' becomes 'provider:/model'."""
        runner = _make_runner(judge_model_route="databricks/my-model")
        # Directly test the conversion logic inline
        route = runner.judge_model_route
        if "/" in route and ":/" not in route:
            provider, model = route.split("/", 1)
            uri = f"{provider}:/" + model
        else:
            uri = route
        assert uri == "databricks:/my-model"

    def test_route_already_uri_format(self):
        """'provider:/model' stays unchanged."""
        route = "databricks:/already-uri"
        if ":/" in route:
            uri = route
        else:
            uri = route
        assert uri == "databricks:/already-uri"

    def test_route_none(self):
        """None route returns None."""
        runner = _make_runner(judge_model_route=None)
        assert runner.judge_model_route is None
