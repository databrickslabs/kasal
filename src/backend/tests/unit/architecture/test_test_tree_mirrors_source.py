"""A test file should live where the module it tests lives.

`tests/unit/services/memory/test_hooks.py` tests `src/services/memory/hooks.py`.
When that holds, "is this module tested?" has a mechanical answer; when it
drifts, the only way to find out is to grep and read — and that is how coverage
gaps hide.

**Matched on IMPORTS, not on filenames.** A filename matcher gets this wrong in
both directions, and did: an audit of this tree flagged
`tests/unit/models/test_model_config_repository.py` as misfiled because the name
says "repository", when it imports only `src.models.model_config` and was
already in the right place. Conversely a file can be named perfectly and import
something from three layers away. What a test imports is what it tests.

Like the query-construction check next door, this is a RATCHET: the files that
already do not mirror are listed below so the suite stays green, and the test
fails when a NEW one appears or when a listed file is fixed but not removed.
"""

import ast
import pathlib

_TESTS = pathlib.Path(__file__).resolve().parent.parent


def _mirrors_its_directory(path: pathlib.Path) -> bool | None:
    """True/False, or None when the file imports no src module at all.

    None covers the meta-tests (this file, the architecture checks, golden
    fixtures) and the handful of genuinely conceptual suites — they legitimately
    do not mirror a single module, so they are not judged.
    """
    try:
        tree = ast.parse(path.read_text())
    except SyntaxError:
        return None
    modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and (node.module or "").startswith("src.")
    }
    if not modules:
        return None
    expected = path.parent.relative_to(_TESTS).as_posix().replace("/", ".")
    return any(m.replace("src.", "", 1).startswith(expected) for m in modules)


def _offenders() -> set[str]:
    return {
        f.relative_to(_TESTS).as_posix()
        for f in _TESTS.rglob("test_*.py")
        if _mirrors_its_directory(f) is False
    }


#: Files whose directory does not match what they import. Shrink this.
_BASELINE = {
    "api/test_memory_backend_router.py",
    "api/test_sse_router_extended.py",
    "converters/dax/test_context.py",
    "converters/sql/test_formula_parser.py",
    "converters/sql/test_sql_structures.py",
    "converters/sql/test_yaml_to_sql.py",
    "converters/uc_metrics/test_context.py",
    "services/agent_builder/test_execution_runner_callbacks.py",
    "services/chat/test_context_compaction_event.py",
    "services/execution/kernel/test_agent_helpers_comprehensive.py",
    "services/execution/kernel/test_agent_helpers_date_awareness.py",
    "services/execution/kernel/test_agent_helpers_new.py",
    "services/execution/kernel/test_agent_helpers_security.py",
    "services/execution/kernel/test_mcp_session_guard.py",
    "services/execution/kernel/test_output_budget_clamp.py",
    "services/execution/kernel/test_reasoning_effort_with_tools.py",
    "services/execution/kernel/test_structured_output_all_providers.py",
    "services/execution/kernel/test_task_helpers.py",
    "services/execution/kernel/test_task_helpers_engine.py",
    "services/execution/kernel/test_task_helpers_mcp_exclusion.py",
    "services/execution/kernel/test_task_helpers_new.py",
    "services/execution/kernel/test_ui_document.py",
    "services/execution/test_process_log_queue.py",
    "services/guardrails/test_guardrail_events.py",
    "services/guardrails/test_guardrail_trace_integration.py",
    "services/mcp/test_mcp_warning_integration.py",
    "services/memory/test_disabled_memory_backend.py",
    "services/memory/test_memory_optimization.py",
    "services/mlflow/test_mlflow_setup_auth.py",
    "services/mlflow/test_mlflow_setup_failfast.py",
    "services/mlflow/test_mlflow_trace_label.py",
    "test_chat_history_workflow.py",
    "test_execution_workflow.py",
    "test_global_exception_handlers.py",
    "test_main.py",
    "test_main_additional.py",
    "test_main_lifespan.py",
    "test_main_lifespan_coverage.py",
    "test_security_headers_middleware.py",
}


def test_no_new_test_file_is_misfiled():
    new = sorted(_offenders() - _BASELINE)
    assert not new, (
        "these test files do not live where the module they test lives:\n  "
        + "\n  ".join(new)
        + "\n\nMove the file beside its module, or if it genuinely spans several "
          "modules, that is what the baseline is for — add it with a reason."
    )


def test_structure_baseline_has_no_stale_entries():
    """A file fixed but left listed hides the next regression in it."""
    stale = sorted(_BASELINE - _offenders())
    assert not stale, (
        "these now mirror their source — delete them from _BASELINE:\n  "
        + "\n  ".join(stale)
    )
