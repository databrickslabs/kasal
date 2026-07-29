"""The exported app still produces traces after CrewAI is gone.

``mlflow.crewai.autolog()`` was the app's only span tracing; with the crew on
Kasal's runtime it hooks nothing, so a turn would trace as an empty
``@mlflow.trace`` span. ``agent_server.mlflow_bridge`` replaces it.

The important test here is ``test_a_real_crew_run_produces_a_nested_trace``: it
runs an actual crew and inspects the trace MLflow recorded, so span nesting is
verified against the runtime's real event stream rather than against hand-built
events that might not match what the runtime emits.
"""

import importlib

import mlflow
import pytest

VENDOR_PKG = "agent_server.kasal_runtime"


def _import(name):
    return importlib.import_module(f"agent_server.{name}")


@pytest.fixture
def bridge(app_bundle):
    """The installed bridge, with MLflow tracing pointed at a temp store."""
    mod = _import("mlflow_bridge")
    mod.install()
    yield mod
    mod.end_turn()


@pytest.fixture
def local_tracking(tmp_path, monkeypatch):
    """Record traces to a temp SQLite store so they can be read back.

    SQLite rather than the ``file://`` store, which MLflow 3.14 put in
    maintenance mode and refuses to read traces from; and async trace logging
    off, or ``get_trace`` races the writer and reports the span data corrupt.

    ``set_tracking_uri``/``set_experiment`` are PROCESS-GLOBAL and monkeypatch
    cannot undo them, so they are restored by hand — otherwise every later test
    in this worker points at a temp database that has since been deleted."""
    monkeypatch.setenv("MLFLOW_ENABLE_ASYNC_TRACE_LOGGING", "false")
    previous_uri = mlflow.get_tracking_uri()
    previous_experiment = mlflow.tracking.fluent._active_experiment_id
    uri = f"sqlite:///{tmp_path / 'mlflow.db'}"
    monkeypatch.setenv("MLFLOW_TRACKING_URI", uri)
    mlflow.set_tracking_uri(uri)
    mlflow.set_experiment("export-bridge-test")
    try:
        yield tmp_path
    finally:
        mlflow.set_tracking_uri(previous_uri)
        mlflow.tracking.fluent._active_experiment_id = previous_experiment


def _spans_of(trace):
    return [(s.name, s.parent_id, s.span_id) for s in trace.data.spans]


def _tree(trace):
    """name -> parent name, for readable assertions."""
    by_id = {s.span_id: s.name for s in trace.data.spans}
    return {s.name: by_id.get(s.parent_id) for s in trace.data.spans}


class TestRealTrace:
    def test_a_real_crew_run_produces_a_nested_trace(
        self, app_bundle, local_tracking, fake_llm, bridge
    ):
        """The whole point of Phase 4: run a crew, get a usable trace.

        Built from the runtime's actual events, so this also proves the bus's
        causality stamping produces the tree the bridge assumes."""
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm

        @mlflow.trace(name="turn")
        def turn():
            return str(agent_mod.build_crew().kickoff())

        assert "Hello from the Kasal runtime." in turn()

        trace = mlflow.get_last_active_trace_id()
        assert trace, "no trace recorded"
        trace = mlflow.get_trace(trace)
        tree = _tree(trace)

        assert "turn" in tree and tree["turn"] is None, f"no root turn span: {tree}"
        crew = next((n for n in tree if n.startswith("crew.")), None)
        assert crew, f"the crew did not trace at all: {list(tree)}"
        assert tree[crew] == "turn", "the crew span is not inside the turn"
        # Named after THIS crew, not the runtime's "crew" default — otherwise
        # every exported app's traces look identical in the UI.
        assert crew == "crew.research-crew", crew

        task = next((n for n in tree if n.startswith("task:")), None)
        assert task, f"no task span: {list(tree)}"
        assert tree[task] == crew, "the task span is not inside the crew"

        agent = next((n for n in tree if n.startswith("agent:")), None)
        assert agent, f"no agent span: {list(tree)}"
        assert tree[agent] == task, "the agent span is not inside the task"

    def test_every_span_is_closed(self, app_bundle, local_tracking, fake_llm, bridge):
        """An unclosed span makes the whole trace unrenderable."""
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm

        @mlflow.trace(name="turn")
        def turn():
            return str(agent_mod.build_crew().kickoff())

        turn()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
        for span in trace.data.spans:
            assert span.end_time_ns is not None, f"{span.name} was never closed"


class TestNestingFromEvents:
    """Direct event-level checks — faster to diagnose when the tree is wrong."""

    def test_a_child_event_nests_under_its_parent(
        self, app_bundle, local_tracking, bridge
    ):
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")
        bus = events.event_bus

        @mlflow.trace(name="turn")
        def turn():
            bus.emit(None, events.CrewKickoffStartedEvent(crew_name="c", inputs=None))
            bus.emit(None, events.TaskStartedEvent(task_name="research"))
            bus.emit(
                None, events.ToolUsageStartedEvent(tool_name="serper", tool_args={})
            )
            bus.emit(
                None,
                events.ToolUsageFinishedEvent(
                    tool_name="serper",
                    tool_args={},
                    started_at=__import__("datetime").datetime.now(),
                    finished_at=__import__("datetime").datetime.now(),
                    output="hits",
                ),
            )
            bus.emit(None, events.TaskCompletedEvent(output="done"))
            bus.emit(
                None, events.CrewKickoffCompletedEvent(crew_name="c", output="final")
            )

        turn()
        tree = _tree(mlflow.get_trace(mlflow.get_last_active_trace_id()))
        assert tree["crew.c"] == "turn"
        assert tree["task: research"] == "crew.c"
        assert tree["tool: serper"] == "task: research"

    def test_a_failed_scope_is_recorded_as_an_error(
        self, app_bundle, local_tracking, bridge
    ):
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")
        bus = events.event_bus

        @mlflow.trace(name="turn")
        def turn():
            bus.emit(None, events.TaskStartedEvent(task_name="research"))
            bus.emit(None, events.TaskFailedEvent(error="the tool exploded"))

        turn()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
        span = next(s for s in trace.data.spans if s.name == "task: research")
        assert "exploded" in str(span.attributes.get("error", ""))

    def test_llm_token_usage_lands_on_the_span(
        self, app_bundle, local_tracking, bridge
    ):
        """Token counts are the main reason anyone opens an LLM span."""
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")
        bus = events.event_bus

        @mlflow.trace(name="turn")
        def turn():
            bus.emit(
                None,
                events.LLMCallStartedEvent(model="databricks-x", messages="hello"),
            )
            bus.emit(
                None,
                events.LLMCallCompletedEvent(
                    model="databricks-x",
                    response="hi",
                    call_type=events.LLMCallType.LLM_CALL,
                    usage={
                        "prompt_tokens": 10,
                        "completion_tokens": 4,
                        "total_tokens": 14,
                    },
                ),
            )

        turn()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
        span = next(s for s in trace.data.spans if s.name.startswith("llm:"))
        assert span.attributes.get("total_tokens") == "14"
        assert span.attributes.get("model") == "databricks-x"


class TestResilience:
    def test_a_cancelled_turn_leaves_no_open_span(
        self, app_bundle, local_tracking, bridge
    ):
        """Stop unwinds through CrewCancelled, which never reaches the runtime's
        completion events — so end_turn() has to close what is left."""
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")

        @mlflow.trace(name="turn")
        def turn():
            events.event_bus.emit(
                None, events.CrewKickoffStartedEvent(crew_name="c", inputs=None)
            )
            events.event_bus.emit(None, events.TaskStartedEvent(task_name="research"))
            assert bridge.end_turn() == 2  # both closed by the cleanup

        turn()
        trace = mlflow.get_trace(mlflow.get_last_active_trace_id())
        for span in trace.data.spans:
            assert span.end_time_ns is not None, f"{span.name} left open"

    def test_a_close_with_no_open_span_is_ignored(
        self, app_bundle, local_tracking, bridge
    ):
        """Events can arrive without their partner (a listener registered
        mid-run). That must not raise into the crew."""
        events = importlib.import_module(f"{VENDOR_PKG}.core.events")

        @mlflow.trace(name="turn")
        def turn():
            events.event_bus.emit(None, events.TaskCompletedEvent(output="orphan"))

        turn()  # no exception is the assertion

    def test_tracing_failure_never_breaks_the_run(
        self, app_bundle, local_tracking, fake_llm, bridge, monkeypatch
    ):
        """A trace is diagnostics; it must not be able to fail a customer's run."""
        monkeypatch.setattr(
            bridge.mlflow,
            "start_span_no_context",
            lambda **kw: (_ for _ in ()).throw(RuntimeError("tracing backend down")),
        )
        agent_mod = _import("agent")
        agent_mod._make_llm = lambda *a, **k: fake_llm
        assert "Hello from the Kasal runtime." in str(agent_mod.build_crew().kickoff())


class TestPayloadBounds:
    def test_long_payloads_are_clipped(self, app_bundle, bridge):
        """UC trace tables are billed storage, and a trace nobody can load is
        not a trace."""
        clipped = bridge._clip("x" * 20000)
        assert len(clipped) < 20000
        assert "more chars" in clipped

    def test_short_payloads_are_untouched(self, app_bundle, bridge):
        assert bridge._clip("hello") == "hello"
        assert bridge._clip(None) is None
        assert bridge._clip(7) == 7


class TestAutologIsGone:
    def test_the_app_no_longer_calls_mlflow_crewai_autolog(self, app_bundle):
        """Checked as code, not prose — the comment above the replacement names
        the call it replaced, and should keep doing so."""
        agent_py = (app_bundle / "agent_server" / "agent.py").read_text("utf-8")
        called = [
            line.strip()
            for line in agent_py.splitlines()
            if "mlflow.crewai.autolog" in line and not line.strip().startswith("#")
        ]
        assert not called, f"autolog is still called: {called}"
        assert "mlflow_bridge.install()" in agent_py

    def test_the_conversation_layer_closes_the_turn(self, app_bundle):
        conv = (app_bundle / "agent_server" / "conversation.py").read_text("utf-8")
        assert "mlflow_bridge.end_turn()" in conv
