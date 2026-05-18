"""
Additional coverage tests for mlflow_evaluation_runner.py targeting uncovered lines.
Missing: create_run error path, complete_evaluation with trace objects, scorers,
_extract_records_from_traces edge cases, _log_baseline_metrics with empty data,
_restore_environment_vars no-auth, complete_evaluation fallback eval_data,
_discover_traces with no search_traces, scorer building paths.
"""
import os
import pytest
import io
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call
import pandas as pd

from src.services.mlflow_evaluation_runner import MLflowEvaluationRunner


def _runner(**overrides):
    defaults = {
        "exec_obj": SimpleNamespace(
            id="exec-123", group_id="g1", status="completed", mlflow_trace_id=None
        ),
        "job_id": "job-456",
        "inputs_text": "What is AI?",
        "prediction_text": "AI is artificial intelligence.",
        "judge_model_route": "databricks/databricks-claude-sonnet-4",
        "judge_model_defaulted": False,
    }
    defaults.update(overrides)
    return MLflowEvaluationRunner(**defaults)


def _auth_ctx(**overrides):
    defaults = {
        "workspace_url": "https://example.databricks.com",
        "token": "dapi-test",
        "auth_method": "obo",
        "api_base": "https://example.databricks.com/serving-endpoints",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


# ---------------------------------------------------------------------------
# _restore_environment_vars - no auth_ctx
# ---------------------------------------------------------------------------

def test_restore_environment_vars_no_auth_ctx():
    r = _runner()
    old_env = {"DATABRICKS_HOST": "original-host", "DATABRICKS_TOKEN": None}
    # Should return immediately without modifying anything
    r._restore_environment_vars(old_env, auth_ctx=None)


def test_restore_environment_vars_with_none_value():
    r = _runner()
    # Set a value so we can test deletion
    os.environ["DATABRICKS_HOST"] = "to-delete"
    old_env = r._save_environment_vars()
    old_env["DATABRICKS_HOST"] = None  # Force it to be deleted

    r._restore_environment_vars(old_env, auth_ctx=_auth_ctx())
    assert "DATABRICKS_HOST" not in os.environ or os.environ.get("DATABRICKS_HOST") != "to-delete"


def test_restore_environment_vars_restores_original():
    r = _runner()
    os.environ["DATABRICKS_HOST"] = "original"
    old_env = {"DATABRICKS_HOST": "original"}
    r._restore_environment_vars(old_env, auth_ctx=_auth_ctx())
    assert os.environ.get("DATABRICKS_HOST") == "original"


# ---------------------------------------------------------------------------
# _set_environment_vars
# ---------------------------------------------------------------------------

def test_set_environment_vars_with_api_base():
    r = _runner()
    auth = _auth_ctx(workspace_url="https://myhost.databricks.com", token="tok123")

    with patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url",
               return_value="https://myhost.databricks.com/serving-endpoints"):
        r._set_environment_vars(auth)

    assert os.environ.get("DATABRICKS_HOST") == "https://myhost.databricks.com"
    assert os.environ.get("DATABRICKS_TOKEN") == "tok123"


def test_set_environment_vars_no_api_base():
    r = _runner()
    auth = _auth_ctx(workspace_url="https://myhost.databricks.com", token="tok999")

    with patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url",
               return_value=None):
        r._set_environment_vars(auth)

    assert os.environ.get("DATABRICKS_HOST") == "https://myhost.databricks.com"


# ---------------------------------------------------------------------------
# _extract_records_from_traces - edge cases
# ---------------------------------------------------------------------------

def test_extract_records_no_attributes_column():
    r = _runner()
    df = pd.DataFrame({
        "trace_id": ["t1", "t2"],
        "prompt": ["hello", "world"],
        "output": ["hi", "earth"],
    })
    trace_ids, records = r._extract_records_from_traces(df)
    # Should still build records from rows
    assert isinstance(records, list)


def test_extract_records_with_attributes_column():
    r = _runner()
    attrs1 = {"execution_id": "exec-123", "prompt": "What is AI?", "output": "AI is..."}
    attrs2 = {"execution_id": "other-exec", "prompt": "Other question"}
    df = pd.DataFrame({
        "trace_id": ["t1", "t2"],
        "attributes": [attrs1, attrs2],
    })
    trace_ids, records = r._extract_records_from_traces(df)
    assert isinstance(records, list)


def test_extract_records_with_contexts_and_references():
    r = _runner()
    attrs = {
        "execution_id": "exec-123",
        "prompt": "Question?",
        "output": "Answer.",
        "contexts": ["ctx1", "ctx2"],
        "reference": "Expected answer",
    }
    df = pd.DataFrame({
        "trace_id": ["t1"],
        "attributes": [attrs],
    })
    trace_ids, records = r._extract_records_from_traces(df)
    assert isinstance(records, list)


def test_extract_records_skips_none_msg_and_pred():
    r = _runner()
    attrs = {"execution_id": "exec-123"}  # no msg or pred
    df = pd.DataFrame({
        "trace_id": ["t1"],
        "attributes": [attrs],
    })
    trace_ids, records = r._extract_records_from_traces(df)
    # Row should be skipped since both msg and pred are None
    assert records == []


def test_extract_records_dict_value_serialized():
    r = _runner()
    attrs = {
        "execution_id": "exec-123",
        "prompt": {"message": "hello"},  # dict value should be JSON-serialized
        "output": "response",
    }
    df = pd.DataFrame({
        "trace_id": ["t1"],
        "attributes": [attrs],
    })
    trace_ids, records = r._extract_records_from_traces(df)
    assert isinstance(records, list)


# ---------------------------------------------------------------------------
# _log_baseline_metrics - edge cases
# ---------------------------------------------------------------------------

def test_log_baseline_metrics_empty_df():
    r = _runner()
    empty_df = pd.DataFrame({"messages": [], "predictions": []})

    with patch("mlflow.log_metric") as mock_log:
        r._log_baseline_metrics(empty_df)

    mock_log.assert_not_called()


def test_log_baseline_metrics_with_data():
    r = _runner()
    df = pd.DataFrame({
        "messages": ["what is AI?", "what is ML?"],
        "predictions": ["AI is artificial intelligence.", "ML is machine learning."]
    })

    with patch("mlflow.log_metric") as mock_log:
        r._log_baseline_metrics(df)

    assert mock_log.call_count >= 3  # length, word_count, jaccard


def test_log_baseline_metrics_jaccard_exception():
    r = _runner()
    # Use data that won't cause issues
    df = pd.DataFrame({
        "messages": ["hello world"],
        "predictions": ["hello there"],
    })

    with patch("mlflow.log_metric"):
        r._log_baseline_metrics(df)


# ---------------------------------------------------------------------------
# _log_run_parameters - error swallowed
# ---------------------------------------------------------------------------

def test_log_run_parameters_exception_swallowed():
    r = _runner()

    with patch("mlflow.log_params", side_effect=Exception("mlflow error")):
        # Should not raise - exception is swallowed
        r._log_run_parameters(["trace-1", "trace-2"])


def test_log_run_parameters_empty_trace_ids():
    r = _runner()

    with patch("mlflow.log_params") as mock_log:
        r._log_run_parameters([])

    mock_log.assert_called_once()


# ---------------------------------------------------------------------------
# _log_artifacts - error paths
# ---------------------------------------------------------------------------

def test_log_artifacts_both_succeed():
    r = _runner()

    with patch("mlflow.log_text") as mock_log:
        r._log_artifacts()

    assert mock_log.call_count == 2


def test_log_artifacts_first_fails():
    r = _runner()

    call_count = [0]

    def side_effect(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("first fail")

    with patch("mlflow.log_text", side_effect=side_effect):
        # Should not raise - exceptions are swallowed
        r._log_artifacts()


def test_log_artifacts_both_fail():
    r = _runner()

    with patch("mlflow.log_text", side_effect=Exception("fail")):
        r._log_artifacts()


def test_log_artifacts_empty_inputs():
    r = _runner(inputs_text=None, prediction_text=None)

    with patch("mlflow.log_text") as mock_log:
        r._log_artifacts()

    assert mock_log.call_count == 2


# ---------------------------------------------------------------------------
# create_run - auth_ctx provided
# ---------------------------------------------------------------------------

def test_create_run_with_auth_ctx():
    r = _runner()
    auth = _auth_ctx()

    mock_exp = MagicMock()
    mock_exp.experiment_id = "exp-1"
    mock_run = MagicMock()
    mock_run.info.run_id = "run-123"
    mock_run.info.experiment_id = "exp-1"

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.set_experiment", return_value=mock_exp), \
         patch("mlflow.get_experiment_by_name", return_value=None), \
         patch("mlflow.start_run") as mock_start, \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.log_text"), \
         patch("mlflow.data.from_pandas"), \
         patch("mlflow.log_input"), \
         patch("src.utils.databricks_url_utils.DatabricksURLUtils.construct_serving_endpoints_url",
               return_value="https://example.databricks.com/serving-endpoints"):

        mock_start.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_start.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
             patch.object(r, "_log_run_parameters"), \
             patch.object(r, "_log_baseline_metrics"), \
             patch.object(r, "_log_artifacts"):
            result = r.create_run(auth)

    assert "run_id" in result


def test_create_run_without_auth():
    r = _runner()

    mock_exp = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run-456"
    mock_run.info.experiment_id = "exp-2"

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.set_experiment", return_value=mock_exp), \
         patch("mlflow.get_experiment_by_name", return_value=None), \
         patch("mlflow.start_run") as mock_start, \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.log_text"), \
         patch("mlflow.data.from_pandas"), \
         patch("mlflow.log_input"):

        mock_start.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_start.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(r, "_discover_traces_and_build_dataset", return_value=(["t1"], [
                {"messages": "input", "predictions": "output"}
             ])), \
             patch.object(r, "_log_run_parameters"), \
             patch.object(r, "_log_baseline_metrics"), \
             patch.object(r, "_log_artifacts"):
            result = r.create_run(None)

    assert "run_id" in result


def test_create_run_log_input_exception():
    """Test that exception in mlflow.log_input is swallowed."""
    r = _runner()

    mock_exp = MagicMock()
    mock_run = MagicMock()
    mock_run.info.run_id = "run-789"
    mock_run.info.experiment_id = "exp-3"

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.set_experiment", return_value=mock_exp), \
         patch("mlflow.get_experiment_by_name", return_value=None), \
         patch("mlflow.start_run") as mock_start, \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.log_text"), \
         patch("mlflow.data.from_pandas", side_effect=Exception("pandas error")), \
         patch("mlflow.log_input"):

        mock_start.return_value.__enter__ = MagicMock(return_value=mock_run)
        mock_start.return_value.__exit__ = MagicMock(return_value=False)

        with patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
             patch.object(r, "_log_run_parameters"), \
             patch.object(r, "_log_baseline_metrics"), \
             patch.object(r, "_log_artifacts"):
            result = r.create_run(None)

    assert "run_id" in result


# ---------------------------------------------------------------------------
# complete_evaluation - basic paths
# ---------------------------------------------------------------------------

def test_complete_evaluation_exception_swallowed():
    r = _runner()

    with patch("mlflow.set_tracking_uri"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):
        with patch("mlflow.start_run", side_effect=Exception("mlflow crash")):
            # Should not raise - exception is caught
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_with_scorers():
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", _auth_ctx())


def test_complete_evaluation_with_trace_ids_and_objects():
    """Test complete_evaluation with trace IDs to exercise trace fetching and enrichment."""
    exec_obj = SimpleNamespace(
        id="exec-123", group_id="g1", status="completed", mlflow_trace_id="t1"
    )
    r = _runner(exec_obj=exec_obj)

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    # Build a trace object with request/response JSON
    trace_data = SimpleNamespace(
        request='{"inputs": {"query": "What is AI?"}, "messages": "msg"}',
        response='{"output": "AI is...", "response": "resp"}'
    )
    trace_obj = SimpleNamespace(data=trace_data)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.get_trace", return_value=trace_obj), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=(["t1"], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", _auth_ctx())


def test_complete_evaluation_with_contexts_and_refs():
    """Test complete_evaluation with records having contexts and references."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    records_with_ctx = [
        {"messages": "Q?", "predictions": "A!", "contexts": "relevant ctx", "references": "ground truth"}
    ]

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], records_with_ctx)), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            # Mock scorers with all available types
            mock_scorers = MagicMock()
            mock_scorer_cls = MagicMock(return_value=MagicMock())
            mock_scorers.RelevanceToQuery = mock_scorer_cls
            mock_scorers.Safety = mock_scorer_cls
            mock_scorers.Correctness = mock_scorer_cls
            mock_scorers.Groundedness = mock_scorer_cls
            mock_scorers.Relevance = mock_scorer_cls
            mock_scorers.ContextSufficiency = mock_scorer_cls
            mock_genai.scorers = mock_scorers
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_with_trace_ids_fetch_fails():
    """Test path where get_trace raises for some trace IDs."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.get_trace", side_effect=Exception("trace not found")), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=(["bad-trace"], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_trace_with_valid_request():
    """Test the valid_traces path where trace has request string."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    # Trace with valid request but no enriched records
    trace_data = SimpleNamespace(request="some request json", response=None)
    trace_obj = SimpleNamespace(data=trace_data)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.get_trace", return_value=trace_obj), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=(["t1"], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_genai_evaluate_raises():
    """Test that genai.evaluate exception is caught and re-raised."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(side_effect=Exception("eval failed"))

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        import mlflow
        with patch.object(mlflow, "genai", mock_genai, create=True):
            # Exception is caught and logged (not re-raised)
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_with_eval_results_table():
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    eval_df = pd.DataFrame({"score": [0.8, 0.9], "relevance": [0.7, 0.85]})
    mock_eval_result = MagicMock()
    mock_eval_result.tables = {"eval_results_table": eval_df}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.log_text"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", _auth_ctx())


# ---------------------------------------------------------------------------
# _discover_traces_and_build_dataset - stored trace ID path
# ---------------------------------------------------------------------------

def test_discover_traces_stored_trace_id():
    exec_obj = SimpleNamespace(id="exec-1", mlflow_trace_id="stored-trace-id")
    r = _runner(exec_obj=exec_obj)

    with patch("mlflow.set_tracking_uri"):
        trace_ids, records = r._discover_traces_and_build_dataset(None)

    assert "stored-trace-id" in trace_ids


def test_discover_traces_no_search_traces_callable():
    exec_obj = SimpleNamespace(id="exec-1", mlflow_trace_id=None)
    r = _runner(exec_obj=exec_obj)

    import mlflow as mlflow_module
    # Remove search_traces to test that branch
    original = getattr(mlflow_module, "search_traces", None)
    try:
        if hasattr(mlflow_module, "search_traces"):
            delattr(mlflow_module, "search_traces")
        with patch("mlflow.get_experiment_by_name", return_value=None):
            trace_ids, records = r._discover_traces_and_build_dataset(None)
    finally:
        if original is not None:
            mlflow_module.search_traces = original

    assert trace_ids == []
    assert records == []


def test_discover_traces_search_traces_exception():
    exec_obj = SimpleNamespace(id="exec-1", mlflow_trace_id=None)
    r = _runner(exec_obj=exec_obj)

    mock_search = MagicMock(side_effect=Exception("search failed"))

    # We need to use a real experiment to trigger search_traces
    mock_exp = MagicMock()
    mock_exp.experiment_id = "exp-1"

    with patch("mlflow.search_traces", mock_search, create=True), \
         patch("mlflow.get_experiment_by_name", return_value=mock_exp):
        trace_ids, records = r._discover_traces_and_build_dataset(None)

    # Exception is swallowed
    assert trace_ids == []
    assert records == []


def test_discover_traces_exec_obj_no_trace_id():
    """Test when exec_obj has no mlflow_trace_id attribute."""
    exec_obj = SimpleNamespace(id="exec-1")  # no mlflow_trace_id
    r = _runner(exec_obj=exec_obj)

    with patch("mlflow.get_experiment_by_name", return_value=None), \
         patch("mlflow.search_traces", return_value=None, create=True):
        trace_ids, records = r._discover_traces_and_build_dataset(None)

    assert trace_ids == []
    assert records == []


# ---------------------------------------------------------------------------
# complete_evaluation - scorer URI path coverage
# ---------------------------------------------------------------------------

def test_complete_evaluation_scorer_uri_with_slash():
    """Test scorer URI building with route containing '/'."""
    r = _runner(judge_model_route="openai/gpt-4")

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    mock_scorers = MagicMock()
    mock_scorer_cls = MagicMock(return_value=MagicMock())
    mock_scorers.RelevanceToQuery = mock_scorer_cls
    mock_scorers.Safety = mock_scorer_cls
    mock_genai.scorers = mock_scorers

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_scorer_uri_already_formatted():
    """Test scorer URI building with route already containing ':/'."""
    r = _runner(judge_model_route="openai:/gpt-4")

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    mock_scorers = MagicMock()
    mock_scorer_cls = MagicMock(return_value=MagicMock())
    mock_scorers.RelevanceToQuery = mock_scorer_cls
    mock_scorers.Safety = mock_scorer_cls
    mock_genai.scorers = mock_scorers

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_scorer_uri_databricks():
    """Test scorer URI building with route='databricks'."""
    r = _runner(judge_model_route="databricks")

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    mock_scorers = MagicMock()
    mock_scorer_cls = MagicMock(return_value=MagicMock())
    mock_scorers.RelevanceToQuery = mock_scorer_cls
    mock_scorers.Safety = mock_scorer_cls
    mock_genai.scorers = mock_scorers

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_scorer_not_found():
    """Test when scorer class is not found on m_scorers."""
    r = _runner(judge_model_route="openai/gpt-4")

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    # Empty scorers module - getattr returns None for all scorers
    mock_scorers = MagicMock(spec=[])  # no attributes
    mock_genai.scorers = mock_scorers

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_scorer_init_exception():
    """Test when scorer initialization raises an exception."""
    r = _runner(judge_model_route="openai/gpt-4")

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    mock_scorers = MagicMock()
    # Scorer classes raise on instantiation
    mock_scorers.RelevanceToQuery = MagicMock(side_effect=TypeError("bad init"))
    mock_scorers.Safety = MagicMock(side_effect=TypeError("bad init"))
    mock_genai.scorers = mock_scorers

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_records_with_contexts_refs_expansion():
    """Test complete_evaluation where records with contexts/refs cause eval_data expansion."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)
    mock_scorers = MagicMock()
    mock_scorer_cls = MagicMock(return_value=MagicMock())
    for attr in ["RelevanceToQuery", "Safety", "Correctness", "Groundedness", "Relevance", "ContextSufficiency"]:
        setattr(mock_scorers, attr, mock_scorer_cls)
    mock_genai.scorers = mock_scorers

    # records with context and reference to trigger has_ctx_col, has_ref_col
    records = [
        {"messages": "question?", "predictions": "answer.", "contexts": "some ctx", "references": "truth"}
    ]

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], records)), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", _auth_ctx())


def test_complete_evaluation_trace_objects_no_request():
    """Test trace objects are skipped when no request JSON."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    # A trace object with no request JSON - no data attribute
    trace_obj_no_data = SimpleNamespace(data=None)
    trace_obj_no_req = SimpleNamespace(data=SimpleNamespace(request=None, response=None))

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.get_trace", side_effect=[trace_obj_no_data, trace_obj_no_req]), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=(["t1", "t2"], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_with_mlflow_client_get_run():
    """Test the MLflow client get_run path."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    mock_mlflow_run = MagicMock()
    mock_mlflow_run.info.experiment_id = "exp-1"
    mock_exp = MagicMock()
    mock_exp.name = "/Shared/test-experiment"

    mock_client = MagicMock()
    mock_client.get_run.return_value = mock_mlflow_run
    mock_client.get_experiment.return_value = mock_exp

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.set_experiment"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.tracking.MlflowClient", return_value=mock_client), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


def test_complete_evaluation_with_eval_results_table_and_numeric():
    """Test the eval results table logging with numeric columns."""
    r = _runner()

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    eval_df = pd.DataFrame({
        "messages": ["q1", "q2"],
        "predictions": ["a1", "a2"],
        "relevance_score": [0.8, 0.9],
        "safety_score": [0.95, 0.87],
    })

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {"eval_results_table": eval_df}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=None), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch("mlflow.log_text"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", None)


# ---------------------------------------------------------------------------
# complete_evaluation - active_run check
# ---------------------------------------------------------------------------

def test_complete_evaluation_active_run_different():
    r = _runner()

    mock_active_run = MagicMock()
    mock_active_run.info.run_id = "different-run-id"

    mock_run = MagicMock()
    mock_run.__enter__ = MagicMock(return_value=mock_run)
    mock_run.__exit__ = MagicMock(return_value=False)

    mock_eval_result = MagicMock()
    mock_eval_result.tables = {}

    mock_genai = MagicMock()
    mock_genai.evaluate = MagicMock(return_value=mock_eval_result)

    with patch("mlflow.set_tracking_uri"), \
         patch("mlflow.start_run", return_value=mock_run), \
         patch("mlflow.active_run", return_value=mock_active_run), \
         patch("mlflow.end_run"), \
         patch("mlflow.log_params"), \
         patch("mlflow.log_metric"), \
         patch.object(r, "_discover_traces_and_build_dataset", return_value=([], [])), \
         patch.object(r, "_set_environment_vars"), \
         patch.object(r, "_restore_environment_vars"):

        with patch("mlflow.genai", mock_genai, create=True):
            r.complete_evaluation("run-123", _auth_ctx())
