"""Shared execution trace-context attach — the single entry point both the crew
path (crew_preparation) and the flow path (flow_methods) use to tag a crew's
memory + tools with job_id/group attribution."""
from unittest.mock import MagicMock

from src.engines.crewai.kernel.trace_context import attach_execution_trace_context


class TestAttachExecutionTraceContext:
    def test_reuses_passed_service_and_calls_both_in_order(self):
        # Crew path: passes its already-built service so exec_id/group_id come
        # from that service's config (no new service constructed).
        svc = MagicMock()
        calls = []
        svc.attach_memory_trace_context.side_effect = lambda *a, **k: calls.append("memory")
        svc.attach_tools_trace_context.side_effect = lambda *a, **k: calls.append("tools")
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
