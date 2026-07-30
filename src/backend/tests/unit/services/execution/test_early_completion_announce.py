"""The terminal status is announced when the work finishes, not at process exit.

Both subprocess paths used to leave the final status to the PARENT, whose
finally block cannot run until the subprocess returns. Everything between the
run finishing and the process exiting — MLflow's post-execution flush, the
event-bus flush, the OTel shutdown, and MLflow's "flushing the async trace
logging queue before program exit" — therefore sat between a run being done and
the UI being told: ~10s on a crew, ~5s on a flow. The UI fell back to its 10s
reconciliation poll, so a finished run took a long time to look finished.

These are source-order assertions rather than behavioural ones on purpose: what
broke was WHERE the announcement sat relative to teardown, and a behavioural
test of a 2,500-line subprocess entry point would not pin that.
"""

import inspect
import re

import pytest

from src.services.agent_builder import process_executor as crew_executor
from src.services.flow_builder import process_executor as flow_executor

CREW_SOURCE = inspect.getsource(crew_executor)
FLOW_SOURCE = inspect.getsource(flow_executor)


def _index_of(source: str, pattern: str) -> int:
    match = re.search(pattern, source)
    assert match, f"anchor not found: {pattern}"
    return match.start()


class TestCrewAnnouncesBeforeTeardown:
    def test_it_announces_at_all(self):
        assert "Announced COMPLETED for" in CREW_SOURCE

    @pytest.mark.parametrize(
        "teardown",
        [
            r"await post_execution_mlflow_cleanup\(",
            r"Flushing CrewAI event bus",
            r"shutdown_provider\(\)",
        ],
        ids=["mlflow-cleanup", "event-bus-flush", "otel-shutdown"],
    )
    def test_the_announcement_precedes_teardown(self, teardown):
        announce = _index_of(
            CREW_SOURCE, r'status="COMPLETED",\n\s+message="Crew execution completed"'
        )
        assert announce < _index_of(CREW_SOURCE, teardown), (
            "the completion announcement has moved behind teardown again — "
            "that is the ~10s delay this exists to prevent"
        )

    def test_it_carries_the_result(self):
        """Status alone would leave the UI fetching an empty result.

        The parent later upgrades this plain answer to the A2UI-composed
        surface, which it can only build once the subprocess has exited.
        """
        assert "_early_result" in CREW_SOURCE
        assert "result=_early_result" in CREW_SOURCE

    def test_it_is_fail_open(self):
        """A late announcement must never fail a run that actually succeeded."""
        announce = _index_of(CREW_SOURCE, r"Announced COMPLETED for")
        window = CREW_SOURCE[announce - 2000 : announce + 800]
        assert "Early completion announcement failed" in window


class TestFlowAnnouncesBeforeTeardown:
    def test_it_announces_at_all(self):
        assert "Announced COMPLETED for" in FLOW_SOURCE

    @pytest.mark.parametrize(
        "teardown",
        [r"Flushing CrewAI event bus", r"Final event bus flush"],
        ids=["event-bus-flush", "final-flush"],
    )
    def test_the_announcement_precedes_teardown(self, teardown):
        announce = _index_of(FLOW_SOURCE, r'message="Flow execution completed"')
        assert announce < _index_of(FLOW_SOURCE, teardown)

    def test_it_uses_the_shared_uppercase_status(self):
        """Not FlowExecutionStatus, whose values are lowercase.

        Mixing the two is how a finished run once ended up sitting at a
        lowercase "running" that nothing recognised as terminal.
        """
        announce = _index_of(FLOW_SOURCE, r'message="Flow execution completed"')
        window = FLOW_SOURCE[announce - 600 : announce]
        assert "ExecutionStatus.COMPLETED.value" in window
        assert "FlowExecutionStatus" not in window
