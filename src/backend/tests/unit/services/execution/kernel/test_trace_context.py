"""Shared execution trace-context attach — the single entry point both the crew
path (crew_preparation) and the flow path (flow_methods) use to tag a crew's
memory + tools with job_id/group attribution."""

from unittest.mock import MagicMock

import src.services.execution.kernel.trace_context as tc_mod
from src.services.execution.kernel.trace_context import (
    attach_execution_trace_context,
    get_current_execution_id,
    resolve_tool_execution_id,
    set_current_execution_id,
)


class TestAttachExecutionTraceContext:
    def test_reuses_passed_service_and_calls_both_in_order(self):
        # Crew path: passes its already-built service so exec_id/group_id come
        # from that service's config (no new service constructed).
        svc = MagicMock()
        calls = []
        svc.attach_memory_trace_context.side_effect = lambda *a, **k: calls.append(
            "memory"
        )
        svc.attach_tools_trace_context.side_effect = lambda *a, **k: calls.append(
            "tools"
        )
        crew = MagicMock()
        crew_kwargs = {"k": "v"}

        attach_execution_trace_context(crew, crew_kwargs, service=svc)

        # memory before tools, both on the SAME passed service
        assert calls == ["memory", "tools"]
        svc.attach_memory_trace_context.assert_called_once_with(crew, None, crew_kwargs)
        svc.attach_tools_trace_context.assert_called_once_with(crew, crew_kwargs)

    def test_builds_minimal_service_from_group_and_job(self):
        # Flow path: no service passed → a minimal one is built from group/job.
        crew = MagicMock()
        crew.agents = []
        crew.tasks = []
        crew._memory = None
        crew._short_term_memory = None
        crew._long_term_memory = None
        crew._entity_memory = None

        # Should not raise and should tag the (empty) crew without error.
        attach_execution_trace_context(crew, {}, group_id="grp", job_id="job-xyz")

    def test_never_raises_on_inner_failure(self):
        # Even a totally broken crew/service must not raise — best-effort.
        attach_execution_trace_context("not-a-crew", {}, group_id="grp", job_id="job-1")

    def test_attach_records_process_execution_id_from_job_id(self):
        # Flow path: attaching with a job_id records it process-wide so a tool
        # whose own trace_context was not attached can still recover it.
        tc_mod._CURRENT_EXECUTION_ID = None
        crew = MagicMock()
        crew.agents = []
        crew.tasks = []
        attach_execution_trace_context(crew, {}, group_id="grp", job_id="job-abc")
        assert get_current_execution_id() == "job-abc"


class TestResolveToolExecutionId:
    def setup_method(self):
        tc_mod._CURRENT_EXECUTION_ID = None

    def teardown_method(self):
        tc_mod._CURRENT_EXECUTION_ID = None

    def test_prefers_tool_trace_context(self):
        set_current_execution_id("proc-global")
        tool = MagicMock()
        tool.trace_context = {"job_id": "from-tc"}
        assert resolve_tool_execution_id(tool) == "from-tc"

    def test_falls_back_to_process_global_when_trace_context_empty(self):
        # This is the OTC handoff bug: ToolFactory rebuilt the instance, so its
        # trace_context is empty — the process-scoped id must still resolve.
        set_current_execution_id("proc-global")
        tool = MagicMock()
        tool.trace_context = None
        assert resolve_tool_execution_id(tool) == "proc-global"

    def test_none_when_neither_available(self):
        tool = MagicMock()
        tool.trace_context = {}
        assert resolve_tool_execution_id(tool) is None

    def test_set_current_execution_id_ignores_falsy(self):
        set_current_execution_id("keep")
        set_current_execution_id(None)
        set_current_execution_id("")
        assert get_current_execution_id() == "keep"
