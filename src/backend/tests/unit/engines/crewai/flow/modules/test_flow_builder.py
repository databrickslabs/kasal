"""Tests for FlowBuilder – targeting 100 % statement coverage."""

import asyncio
import json
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

MODULE = "src.engines.crewai.flow.modules.flow_builder"


# ---------------------------------------------------------------------------
# Fake CrewAIFlow base (real class so type() works)
# ---------------------------------------------------------------------------
class _FakeFlow:
    """Minimal stand-in for crewai.flow.flow.Flow."""
    def __init__(self, **kwargs):
        self.state = {}


# Decorator stubs imitating @start, @listen, @router, and_, or_
def _fake_start(*a, **kw):
    """@start() → identity decorator."""
    def decorator(fn):
        fn._is_start_method = True
        return fn
    return decorator


def _fake_listen(target):
    """@listen(target) → identity decorator."""
    def decorator(fn):
        fn._listen_to = target
        fn._meth = fn          # needed by name-patching code
        return fn
    return decorator


def _fake_router(target):
    """@router(target) → identity decorator."""
    def decorator(fn):
        fn._router_for = target
        fn._meth = fn
        return fn
    return decorator


def _fake_and(*names):
    return ("AND", names)

def _fake_or(*names):
    return ("OR", names)


# ---------------------------------------------------------------------------
# Common patch context
# ---------------------------------------------------------------------------
def _patches():
    """Return a dict of attribute names → values for patch.multiple(MODULE, ...)."""
    return {
        "CrewAIFlow": _FakeFlow,
        "start": _fake_start,
        "listen": _fake_listen,
        "router": _fake_router,
        "and_": _fake_and,
        "or_": _fake_or,
        "Crew": MagicMock,
        "Task": MagicMock,
        "Process": MagicMock(),
        "BaseModel": MagicMock,
        "FlowConfigManager": MagicMock(),
        "FlowProcessorManager": MagicMock(),
        "FlowStateManager": MagicMock(),
        "FlowMethodFactory": MagicMock(),
        "create_execution_callbacks": MagicMock(return_value=(MagicMock(), MagicMock())),
        "extract_final_answer": MagicMock(return_value="answer"),
        "get_model_context_limits": AsyncMock(return_value=(128000, 16000)),
        "FlowPausedForApprovalException": Exception,
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _make_task(task_id="t1", agent_role="Agent1"):
    t = MagicMock()
    t.agent = MagicMock()
    t.agent.role = agent_role
    t.description = "do stuff"
    t.expected_output = "done"
    t.async_execution = False
    return t


def _make_flow_data(starting_points=None, listeners=None, routers=None,
                    edges=None, flow_config_extra=None, nodes=None):
    fc = {
        "startingPoints": starting_points or [{"taskId": "t1"}],
        "listeners": listeners or [],
        "routers": routers or [],
    }
    if flow_config_extra:
        fc.update(flow_config_extra)
    data = {"flow_config": fc}
    if edges:
        data["edges"] = edges
    if nodes:
        data["nodes"] = nodes
    return data


# ===================================================================
# Tests for build_flow
# ===================================================================
class TestBuildFlowValidation:
    """Validation and early-exit paths in build_flow."""

    @pytest.mark.asyncio
    async def test_no_flow_data_raises(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        with pytest.raises(ValueError, match="No flow data"):
            await FlowBuilder.build_flow(None)

    @pytest.mark.asyncio
    async def test_empty_dict_raises(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        with pytest.raises(ValueError, match="No starting points"):
            await FlowBuilder.build_flow({"flow_config": {"startingPoints": []}})

    @pytest.mark.asyncio
    async def test_empty_flow_config_enters_warning(self):
        """flow_config is {} (empty dict, falsy) enters warning branch."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        fd = {"flow_config": {}}
        with pytest.raises(ValueError, match="No starting points"):
            await FlowBuilder.build_flow(fd)

    @pytest.mark.asyncio
    async def test_empty_flow_config_string_parse_fail(self):
        """flow_config is empty string; isinstance(str)=True but json.loads fails."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        fd = {"flow_config": ""}
        with pytest.raises(ValueError, match="Failed to build flow"):
            await FlowBuilder.build_flow(fd)

    @pytest.mark.asyncio
    async def test_empty_flow_config_string_parse_success(self):
        """Cover json.loads success path with a tricky dict."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        class _TrickyDict(dict):
            _fc_calls = 0
            def get(self, key, *args):
                if key == 'flow_config':
                    self._fc_calls += 1
                    if self._fc_calls == 1:
                        return {}
                    return '{"startingPoints": []}'
                return super().get(key, *args)

        fd = _TrickyDict({"_dummy": True})  # non-empty so truthiness passes
        with pytest.raises(ValueError, match="No starting points"):
            await FlowBuilder.build_flow(fd)


class TestBuildFlowCheckpointEdges:
    """Edges with checkpoint=true enable persistence."""

    @pytest.mark.asyncio
    async def test_checkpoint_edge_enables_persistence(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        fd = _make_flow_data(edges=[{"data": {"checkpoint": True}}])

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None


class TestBuildFlowStartingPointExtraction:
    """Extraction of task IDs from various starting point formats."""

    @pytest.mark.asyncio
    async def test_crew_node_format(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        sp = [{"nodeType": "crewNode", "nodeData": {"allTasks": [{"id": "t1"}]}}]
        fd = _make_flow_data(starting_points=sp)

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None

    @pytest.mark.asyncio
    async def test_agent_node_skipped(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        sp = [
            {"nodeType": "agentNode"},
            {"taskId": "t1"},
        ]
        fd = _make_flow_data(starting_points=sp)

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None

    @pytest.mark.asyncio
    async def test_task_not_in_all_tasks(self):
        """Extracted task ID not in all_tasks logs error."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        # Starting point refs task-999 which is not in all_tasks
        sp = [{"taskId": "task-999"}]
        fd = _make_flow_data(starting_points=sp)

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None

    @pytest.mark.asyncio
    async def test_no_start_methods_warning(self):
        """When dynamic_flow has no starting_point_ methods, logs error."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        # Return empty starting points (no methods, but with a crew that has no tasks)
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None


class TestBuildFlowCheckpointResume:
    """Checkpoint resume paths in build_flow."""

    @pytest.mark.asyncio
    async def test_resume_loads_checkpoint_outputs(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        # Mock repositories
        exec_hist_repo = AsyncMock()
        exec_hist_repo.get_execution_by_job_id = AsyncMock(return_value=MagicMock(job_id="job-123"))
        exec_trace_repo = AsyncMock()
        exec_trace_repo.get_crew_outputs_for_resume = AsyncMock(
            return_value={"Crew1": "output1" * 100}
        )
        repos = {"execution_history": exec_hist_repo, "execution_trace": exec_trace_repo}

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(
                fd, repositories=repos,
                resume_from_execution_id="exec-1",
                resume_from_crew_sequence=1
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_resume_execution_not_found(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        exec_hist_repo = AsyncMock()
        exec_hist_repo.get_execution_by_job_id = AsyncMock(return_value=None)
        exec_trace_repo = AsyncMock()
        repos = {"execution_history": exec_hist_repo, "execution_trace": exec_trace_repo}

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(
                fd, repositories=repos,
                resume_from_execution_id="no-such"
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_resume_missing_repos(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(
                fd, repositories={},
                resume_from_execution_id="exec-1"
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_resume_exception_during_load(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p[f"FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p[f"FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p[f"FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        exec_hist_repo = AsyncMock()
        exec_hist_repo.get_execution_by_job_id = AsyncMock(side_effect=RuntimeError("db err"))
        repos = {"execution_history": exec_hist_repo, "execution_trace": AsyncMock()}

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(
                fd, repositories=repos,
                resume_from_execution_id="exec-1"
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_build_flow_exception_wraps(self):
        """Exceptions during build are wrapped in ValueError."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        p[f"FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(
            side_effect=RuntimeError("boom")
        )

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            with pytest.raises(ValueError, match="Failed to build flow"):
                await FlowBuilder.build_flow(fd)


# ===================================================================
# Tests for _apply_state_operations
# ===================================================================
class TestApplyStateOperations:

    def test_none_operations(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        FlowBuilder._apply_state_operations(MagicMock(), None)

    def test_reads_dict_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        flow = MagicMock()
        flow.state = {"x": 42}
        # dict has 'get' so the dict path should be taken
        FlowBuilder._apply_state_operations(flow, {"reads": ["x"]})

    def test_reads_object_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        class ObjState:
            x = 99
        flow = MagicMock()
        flow.state = ObjState()
        FlowBuilder._apply_state_operations(flow, {"reads": ["x"]})

    def test_writes_expression_dict_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        flow = MagicMock()
        flow.state = {"counter": 5}
        ops = {"writes": [{"variable": "result", "expression": "state['counter'] + 1", "value": None}]}
        FlowBuilder._apply_state_operations(flow, ops)
        assert flow.state["result"] == 6

    def test_writes_expression_object_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        class ObjState:
            counter = 10
        flow = MagicMock()
        flow.state = ObjState()
        ops = {"writes": [{"variable": "result", "expression": "state.counter + 1", "value": None}]}
        FlowBuilder._apply_state_operations(flow, ops)
        assert flow.state.result == 11

    def test_writes_expression_failure(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        flow = MagicMock()
        flow.state = {}
        ops = {"writes": [{"variable": "x", "expression": "undefined_var", "value": None}]}
        # Should not raise – logs error
        FlowBuilder._apply_state_operations(flow, ops)

    def test_writes_value_dict_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder
        flow = MagicMock()
        flow.state = {}
        ops = {"writes": [{"variable": "k", "expression": None, "value": 99}]}
        FlowBuilder._apply_state_operations(flow, ops)
        assert flow.state["k"] == 99

    def test_writes_value_object_state(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        class ObjState:
            pass
        flow = MagicMock()
        flow.state = ObjState()
        ops = {"writes": [{"variable": "k", "expression": None, "value": 99}]}
        FlowBuilder._apply_state_operations(flow, ops)
        assert flow.state.k == 99


# ===================================================================
# Tests for _create_dynamic_flow
# ===================================================================
class TestCreateDynamicFlowInit:
    """Test __init__ creation with state enabled/disabled."""

    @pytest.mark.asyncio
    async def test_state_disabled(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(return_value=_fake_start()(lambda self: "ok"))

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={"state": {"enabled": False}, "persistence": {}},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_state_enabled_with_initial_values(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(return_value=_fake_start()(lambda self: "ok"))

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {"enabled": True, "type": "unstructured", "initialValues": {"x": 1}},
                    "persistence": {},
                },
            )
            assert flow is not None
            assert flow.state.get("x") == 1


class TestCreateDynamicFlowStartMethods:
    """Start method creation paths."""

    @pytest.mark.asyncio
    async def test_skip_crew_with_checkpoint(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_skipped_crew_method = MagicMock(return_value=_fake_start()(lambda self: "skipped"))

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {}, "persistence": {},
                    "startingPoints": [{"taskId": "t1", "crewName": "FrontendCrew"}],
                },
                resume_from_crew_sequence=2,
                checkpoint_outputs={"FrontendCrew": "prev output"},
            )
            assert flow is not None
            factory.create_skipped_crew_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_skip_crew_no_checkpoint_output(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_skipped_crew_method = MagicMock(return_value=_fake_start()(lambda self: "skipped"))

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
                resume_from_crew_sequence=2,
                checkpoint_outputs={"OtherCrew": "data"},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_skip_crew_no_checkpoint_outputs_dict(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_skipped_crew_method = MagicMock(return_value=_fake_start()(lambda self: "skipped"))

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
                resume_from_crew_sequence=2,
                checkpoint_outputs=None,
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_crew_with_tasks(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "result")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
            )
            factory.create_starting_point_crew_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_crew_no_tasks(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        sp = [("starting_point_0", ["t1"], [], "EmptyCrew", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {},
                flow_config={"state": {}, "persistence": {}},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_frontend_crew_name_lookup(self):
        """Starting point uses crewName from frontend config."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "db_name", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {}, "persistence": {},
                    "startingPoints": [{"taskId": "t1", "crewName": "FrontendName"}],
                },
            )
            # Verify factory called with frontend name
            call_kwargs = factory.create_starting_point_crew_method.call_args
            assert call_kwargs[1]["crew_name"] == "FrontendName" or call_kwargs.kwargs.get("crew_name") == "FrontendName"


class TestCreateDynamicFlowListeners:
    """Listener method creation paths."""

    @pytest.mark.asyncio
    async def test_listener_no_tasks_skipped(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        # Listener with no tasks
        listeners = [("listen_1", "crew2", ["t2"], [], "Crew2", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_no_listen_targets_skipped(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        # Listener with tasks but no listen targets
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", [], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_matches_starting_point(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "listened")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            factory.create_listener_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_listener_matches_other_listener(self):
        """Listener chaining – listener B listens to listener A."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        task3 = _make_task("t3")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("listen_1")(lambda self, x: "chained")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [
            ("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {}),
            # listen_2 listens to t2 (from listen_1), but t2 is NOT in starting_points
            ("listen_2", "crew3", ["t3"], [task3], "Crew3", ["t2"], "NONE", {}),
        ]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2, "t3": task3},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_and_condition(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task1b = _make_task("t1b")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "and_result")
        )

        sp = [
            ("starting_point_0", ["t1"], [task1], "Crew1", {}),
            ("starting_point_1", ["t1b"], [task1b], "Crew1b", {}),
        ]
        # Listener with AND condition and multiple matching listen targets
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1", "t1b"], "AND", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {},
                {"t1": task1, "t1b": task1b, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_or_condition(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task1b = _make_task("t1b")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "or_result")
        )

        sp = [
            ("starting_point_0", ["t1"], [task1], "Crew1", {}),
            ("starting_point_1", ["t1b"], [task1b], "Crew1b", {}),
        ]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1", "t1b"], "OR", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {},
                {"t1": task1, "t1b": task1b, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_frontend_crew_name_lookup(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "db_name", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [{"crewId": "crew2", "name": "FrontendListener"}],
                },
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_skip_checkpoint_resume(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_skipped_crew_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "skipped")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
                resume_from_crew_sequence=5,
                checkpoint_outputs={"Crew2": "cached"},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_skip_no_checkpoint_output(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_skipped_crew_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "skipped")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
                resume_from_crew_sequence=5,
                checkpoint_outputs=None,
            )
            assert flow is not None


class TestCreateDynamicFlowHITL:
    """HITL gate creation paths."""

    @pytest.mark.asyncio
    async def test_hitl_edge_gate(self):
        """HITL enabled on an incoming edge creates a gate method."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("hitl_gate")(lambda self, x: "after_gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        edges = [{"id": "e1", "target": "crew2", "data": {"hitl": {"enabled": True, "message": "approve?"}}}]
        nodes = [{"id": "crew2", "data": {"crewId": "crew2"}}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "edges": edges,
                    "nodes": nodes,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_gate_node(self):
        """hitlGateNode type in nodes array creates a gate method."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        nodes = [
            {"id": "gate-1", "type": "hitlGateNode", "data": {"message": "check"}},
        ]
        edges = [{"source": "t1", "target": "gate-1"}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {}, "persistence": {},
                    "nodes": nodes,
                    "edges": edges,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_gate_node_matches_listener_crew(self):
        """hitlGateNode source matches a listener crew's task."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("listen_1")(lambda self, x: "gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        nodes = [
            {"id": "gate-2", "type": "hitlGateNode", "data": {}},
        ]
        # source is a listener task
        edges = [{"source": "t2", "target": "gate-2"}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "nodes": nodes,
                    "edges": edges,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_gate_node_matches_crew_id(self):
        """hitlGateNode source matches a listener by crew_id."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("listen_1")(lambda self, x: "gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        nodes = [
            {"id": "gate-3", "type": "hitlGateNode", "data": {}},
        ]
        # source is the crew_id "crew2"
        edges = [{"source": "crew2", "target": "gate-3"}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "nodes": nodes,
                    "edges": edges,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_gate_node_no_source(self):
        """hitlGateNode with no incoming edge uses default method."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        nodes = [
            {"id": "gate-no-src", "type": "hitlGateNode", "data": {}},
        ]
        # No edge targeting gate-no-src
        edges = []

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {}, "persistence": {},
                    "nodes": nodes,
                    "edges": edges,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_gate_node_source_matches_sp_crew_data(self):
        """hitlGateNode source matches starting_point's crew_data.id."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {"id": "node-xyz"})]

        nodes = [
            {"id": "gate-cd", "type": "hitlGateNode", "data": {}},
        ]
        edges = [{"source": "node-xyz", "target": "gate-cd"}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config={
                    "state": {}, "persistence": {},
                    "nodes": nodes,
                    "edges": edges,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()


class TestCreateDynamicFlowPersistence:
    """Persistence decorator application."""

    @pytest.mark.asyncio
    async def test_persist_decorator_applied(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        mock_persist = MagicMock()
        # persist()(...) should return a class — we make it return a class factory
        mock_persist.return_value = lambda cls: cls

        with patch.multiple(MODULE, **p):
            with patch("crewai.flow.persistence.persist", mock_persist):
                flow = await FlowBuilder._create_dynamic_flow(
                    sp, [], [], {}, {"t1": task1},
                    flow_config={
                        "state": {},
                        "persistence": {"enabled": True, "level": "flow"},
                    },
                    restore_uuid="uuid-123",
                )
                assert flow is not None
                mock_persist.assert_called_once()

    @pytest.mark.asyncio
    async def test_persist_import_error(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            with patch.dict("sys.modules", {"crewai.flow.persistence": None}):
                flow = await FlowBuilder._create_dynamic_flow(
                    sp, [], [], {}, {"t1": task1},
                    flow_config={
                        "state": {},
                        "persistence": {"enabled": True},
                    },
                )
                assert flow is not None

    @pytest.mark.asyncio
    async def test_persist_generic_error(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        mock_persist = MagicMock(side_effect=RuntimeError("persist boom"))

        with patch.multiple(MODULE, **p):
            with patch("crewai.flow.persistence.persist", mock_persist):
                flow = await FlowBuilder._create_dynamic_flow(
                    sp, [], [], {}, {"t1": task1},
                    flow_config={
                        "state": {},
                        "persistence": {"enabled": True},
                    },
                )
                assert flow is not None


# ===================================================================
# Tests for router methods (inline code in _create_dynamic_flow)
# ===================================================================
class TestRouterMethods:
    """Test the router code that builds route_method inside _create_dynamic_flow."""

    def _build_flow_with_router(self, router_config, all_tasks=None, flow_config_extra=None):
        """Helper to build a flow with a router and return the flow instance."""
        import asyncio
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        if all_tasks is None:
            all_tasks = {"t1": task1}
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        routers = [router_config]

        fc = {"state": {}, "persistence": {}}
        if flow_config_extra:
            fc.update(flow_config_extra)

        with patch.multiple(MODULE, **p):
            flow = asyncio.run(
                FlowBuilder._create_dynamic_flow(
                    sp, [], routers, {}, all_tasks,
                    flow_config=fc,
                )
            )
        return flow

    def test_per_route_conditions_match(self):
        """routeConditions with a matching condition returns route name."""
        router_cfg = {
            "name": "test_router",
            "listenTo": None,
            "routes": {"high": [], "low": []},
            "condition": None,
            "routeConditions": {"high": "True", "low": "False"},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        # Find the router method
        method = getattr(flow, "router_test_router_0", None)
        assert method is not None
        result = method()
        assert result == "high"

    def test_per_route_conditions_no_match_default(self):
        """No routeConditions match but 'default' route exists."""
        router_cfg = {
            "name": "r2",
            "listenTo": None,
            "routes": {"a": [], "default": []},
            "condition": None,
            "routeConditions": {"a": "False"},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_r2_0", None)
        result = method()
        assert result == "default"

    def test_per_route_conditions_no_match_no_default(self):
        """No routeConditions match and no 'default' route → None."""
        router_cfg = {
            "name": "r3",
            "listenTo": None,
            "routes": {"a": [], "b": []},
            "condition": None,
            "routeConditions": {"a": "False", "b": "False"},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_r3_0", None)
        result = method()
        assert result is None

    def test_per_route_conditions_eval_error(self):
        """routeCondition with broken expression → skipped."""
        router_cfg = {
            "name": "r4",
            "listenTo": None,
            "routes": {"a": [], "b": []},
            "condition": None,
            "routeConditions": {"a": "undefined_var + 1", "b": "True"},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_r4_0", None)
        result = method()
        assert result == "b"  # a fails, b matches

    def test_per_route_conditions_exception(self):
        """Complete evaluation failure → None."""
        router_cfg = {
            "name": "r5",
            "listenTo": None,
            "routes": {"a": []},
            "condition": None,
            "routeConditions": None,  # This is truthy check – needs special approach
            "conditionField": "success",
        }
        # No routeConditions and no condition_expr → value matching path
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_r5_0", None)
        # No args/kwargs → condition_value stays None → falls through to default
        result = method()
        assert result == "a"  # default to first route

    def test_legacy_condition_true(self):
        """Legacy condition expression evaluates to True."""
        router_cfg = {
            "name": "legacy_t",
            "listenTo": None,
            "routes": {"success": [], "fail": []},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_legacy_t_0", None)
        result = method()
        assert result == "success"

    def test_legacy_condition_false(self):
        """Legacy condition expression evaluates to False."""
        router_cfg = {
            "name": "legacy_f",
            "listenTo": None,
            "routes": {"success": [], "fail": []},
            "condition": "False",
            "routeConditions": {},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_legacy_f_0", None)
        result = method()
        assert result is None

    def test_legacy_condition_error(self):
        """Legacy condition with eval error → None."""
        router_cfg = {
            "name": "legacy_err",
            "listenTo": None,
            "routes": {"a": []},
            "condition": "undefined_var",
            "routeConditions": {},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_legacy_err_0", None)
        result = method()
        assert result is None

    def test_value_matching_kwargs(self):
        """Simple value matching from kwargs."""
        router_cfg = {
            "name": "vm",
            "listenTo": None,
            "routes": {"high": [], "low": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "level",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_vm_0", None)
        result = method(level="high")
        assert result == "high"

    def test_value_matching_state_attr(self):
        """Value matching from self.state attribute."""
        router_cfg = {
            "name": "vma",
            "listenTo": None,
            "routes": {"success": [], "failed": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "outcome",
        }
        flow = self._build_flow_with_router(router_cfg)

        # Set state as object with attribute
        class StateObj:
            outcome = "failed"
        flow.state = StateObj()

        method = getattr(flow, "router_vma_0", None)
        result = method()
        assert result == "failed"

    def test_value_matching_state_dict(self):
        """Value matching from self.state dict."""
        router_cfg = {
            "name": "vmd",
            "listenTo": None,
            "routes": {"high": [], "low": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "level",
        }
        flow = self._build_flow_with_router(router_cfg)
        flow.state = {"level": "low"}

        method = getattr(flow, "router_vmd_0", None)
        result = method()
        assert result == "low"

    def test_value_matching_args_attr(self):
        """Value matching from args[0] attribute."""
        router_cfg = {
            "name": "vaa",
            "listenTo": None,
            "routes": {"pass": [], "fail": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "status",
        }
        flow = self._build_flow_with_router(router_cfg)

        class Result:
            status = "pass"
        method = getattr(flow, "router_vaa_0", None)
        result = method(Result())
        assert result == "pass"

    def test_value_matching_args_dict(self):
        """Value matching from args[0] dict."""
        router_cfg = {
            "name": "vad",
            "listenTo": None,
            "routes": {"alpha": [], "beta": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "choice",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_vad_0", None)
        result = method({"choice": "beta"})
        assert result == "beta"

    def test_value_matching_boolean_true(self):
        """Boolean True → 'success' route."""
        router_cfg = {
            "name": "vbt",
            "listenTo": None,
            "routes": {"success": [], "failed": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_vbt_0", None)
        result = method(success=True)
        assert result == "success"

    def test_value_matching_boolean_false(self):
        """Boolean False → 'failed' route."""
        router_cfg = {
            "name": "vbf",
            "listenTo": None,
            "routes": {"success": [], "failed": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "success",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_vbf_0", None)
        result = method(success=False)
        assert result == "failed"

    def test_value_matching_no_match_default(self):
        """No match → default to first route."""
        router_cfg = {
            "name": "vnm",
            "listenTo": None,
            "routes": {"x": [], "y": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "z",
        }
        flow = self._build_flow_with_router(router_cfg)
        method = getattr(flow, "router_vnm_0", None)
        result = method()  # no args/kwargs
        assert result == "x"  # first route


class TestRouterBuildEvalContext:
    """Test build_eval_context inside the router's route_method."""

    def _build_flow_with_per_route(self, route_conditions, routes=None):
        """Build flow with per-route conditions to exercise build_eval_context."""
        import asyncio
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        if routes is None:
            routes = {k: [] for k in route_conditions}

        router_cfg = {
            "name": "ctx",
            "listenTo": None,
            "routes": routes,
            "condition": None,
            "routeConditions": route_conditions,
            "conditionField": "success",
        }
        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = asyncio.run(
                FlowBuilder._create_dynamic_flow(
                    sp, [], [router_cfg], {}, {"t1": task1},
                    flow_config={"state": {}, "persistence": {}},
                )
            )
        return flow

    def test_eval_context_with_state(self):
        """State is added to eval context."""
        flow = self._build_flow_with_per_route({"yes": "state.get('x') == 1"})
        flow.state = {"x": 1}
        method = getattr(flow, "router_ctx_0")
        result = method()
        assert result == "yes"

    def test_eval_context_with_args_result(self):
        """args[0] is added as 'result' to eval context."""
        flow = self._build_flow_with_per_route({"match": "result == 'hello'"})
        method = getattr(flow, "router_ctx_0")
        result = method("hello")
        assert result == "match"

    def test_eval_context_crew_output_raw_json(self):
        """CrewOutput with .raw JSON object is parsed into context."""
        flow = self._build_flow_with_per_route({"check": "score > 50"})
        crew_output = MagicMock()
        crew_output.raw = '{"score": 75}'
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_crew_output_raw_json_array(self):
        """CrewOutput with .raw JSON array parses first item."""
        flow = self._build_flow_with_per_route({"check": "score > 50"})
        crew_output = MagicMock()
        crew_output.raw = '[{"score": 75}]'
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_crew_output_raw_not_json(self):
        """CrewOutput with .raw that is not JSON → skipped."""
        flow = self._build_flow_with_per_route(
            {"check": "True"},
            routes={"check": [], "fallback": []}
        )
        crew_output = MagicMock()
        crew_output.raw = "just plain text"
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_string_result_json(self):
        """String arg that looks like JSON is parsed."""
        flow = self._build_flow_with_per_route({"check": "value == 42"})
        method = getattr(flow, "router_ctx_0")
        result = method('{"value": 42}')
        assert result == "check"

    def test_eval_context_string_result_not_json(self):
        """String arg that is not JSON → no parse."""
        flow = self._build_flow_with_per_route({"check": "True"})
        method = getattr(flow, "router_ctx_0")
        result = method("plain text")
        assert result == "check"

    def test_eval_context_dict_arg(self):
        """Dict arg is merged into context."""
        flow = self._build_flow_with_per_route({"check": "x == 1"})
        method = getattr(flow, "router_ctx_0")
        result = method({"x": 1})
        assert result == "check"

    def test_eval_context_obj_arg(self):
        """Object arg has __dict__ merged into context."""
        flow = self._build_flow_with_per_route({"check": "x == 1"})

        class Obj:
            def __init__(self):
                self.x = 1
        method = getattr(flow, "router_ctx_0")
        result = method(Obj())
        assert result == "check"

    def test_eval_context_state_json_values(self):
        """JSON string values in state are parsed and merged."""
        flow = self._build_flow_with_per_route({"check": "number == 43"})
        flow.state = {"Random Number": '{"number": 43}'}
        method = getattr(flow, "router_ctx_0")
        # Pass a dummy arg so strip_code_fences/looks_like_json are defined
        result = method("dummy")
        assert result == "check"

    def test_eval_context_state_code_fences(self):
        """JSON wrapped in code fences is parsed."""
        flow = self._build_flow_with_per_route({"check": "val == 7"})
        flow.state = {"data": '```json\n{"val": 7}\n```'}
        method = getattr(flow, "router_ctx_0")
        result = method("dummy")
        assert result == "check"

    def test_eval_context_state_invalid_json(self):
        """Invalid JSON in state is silently ignored."""
        flow = self._build_flow_with_per_route({"check": "True"})
        flow.state = {"bad": '{"incomplete'}
        method = getattr(flow, "router_ctx_0")
        result = method("dummy")
        assert result == "check"

    def test_eval_context_kwargs(self):
        """kwargs are merged into context."""
        flow = self._build_flow_with_per_route({"check": "x == 5"})
        method = getattr(flow, "router_ctx_0")
        result = method(x=5)
        assert result == "check"

    def test_eval_context_safe_helpers(self):
        """Safe int/float/str/len helpers work in conditions."""
        flow = self._build_flow_with_per_route({"check": "int('42') == 42"})
        method = getattr(flow, "router_ctx_0")
        result = method()
        assert result == "check"

    def test_eval_context_auto_convert_string_int(self):
        """String numeric values auto-converted to int."""
        flow = self._build_flow_with_per_route({"check": "score > 50"})
        crew_output = MagicMock()
        crew_output.raw = '{"score": "75"}'
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_auto_convert_string_float(self):
        """String float values auto-converted to float."""
        flow = self._build_flow_with_per_route({"check": "score > 50"})
        crew_output = MagicMock()
        crew_output.raw = '{"score": "75.5"}'
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_code_fences_raw(self):
        """Code fences in raw output stripped before JSON parse."""
        flow = self._build_flow_with_per_route({"check": "x == 1"})
        crew_output = MagicMock()
        crew_output.raw = '```json\n{"x": 1}\n```'
        method = getattr(flow, "router_ctx_0")
        result = method(crew_output)
        assert result == "check"

    def test_eval_context_no_state(self):
        """Flow without state attribute uses empty dict."""
        flow = self._build_flow_with_per_route({"check": "True"})
        if hasattr(flow, 'state'):
            del flow.state
        method = getattr(flow, "router_ctx_0")
        result = method()
        assert result == "check"

    def test_safe_int_invalid(self):
        """safe_int with invalid value returns default."""
        flow = self._build_flow_with_per_route({"check": "int('abc', 0) == 0"})
        method = getattr(flow, "router_ctx_0")
        result = method()
        assert result == "check"

    def test_safe_float_invalid(self):
        """safe_float with invalid value returns default."""
        flow = self._build_flow_with_per_route({"check": "float('abc', 0.0) == 0.0"})
        method = getattr(flow, "router_ctx_0")
        result = method()
        assert result == "check"


# ===================================================================
# Tests for route listener methods
# ===================================================================
class TestRouteListeners:
    """Test route_listener_factory and route_listener_method."""

    @pytest.mark.asyncio
    async def test_route_listener_with_previous_output(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        task2.async_execution = False
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="route_result")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls

        mock_task_cls = MagicMock()
        mock_task_cls.return_value = MagicMock()
        p[f"Task"] = mock_task_cls

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "rt",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2", "crewName": "RouteCrew"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
                callbacks={"job_id": "j1"},
            )

            # Find the route listener method
            route_method = getattr(flow, "route_rt_yes_0", None)
            assert route_method is not None

            # Call it with previous output
            result = await route_method("previous data")
            assert result == "route_result"

    @pytest.mark.asyncio
    async def test_route_listener_no_previous_output(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="result")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "rt2",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
            )
            route_method = getattr(flow, "route_rt2_yes_0", None)
            result = await route_method(None)
            assert result == "result"

    @pytest.mark.asyncio
    async def test_route_listener_truncated_output(self):
        """Previous output longer than context limit is truncated."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())
        # Make extract_final_answer return very long string
        p[f"extract_final_answer"] = MagicMock(return_value="x" * 999999)
        # Small context to force truncation
        p[f"get_model_context_limits"] = AsyncMock(return_value=(1000, 500))

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "trunc",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
                group_context=MagicMock(),
            )
            route_method = getattr(flow, "route_trunc_yes_0", None)
            await route_method("long data")

    @pytest.mark.asyncio
    async def test_route_listener_async_tasks_completion(self):
        """Multiple async tasks trigger auto-created completion task."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2a = _make_task("t2a")
        task2a.async_execution = True
        task2b = _make_task("t2b")
        task2b.async_execution = True
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="combined")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "async_rt",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2a"}, {"id": "t2b"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p), patch("crewai.Task", MagicMock(return_value=MagicMock())):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {},
                {"t1": task1, "t2a": task2a, "t2b": task2b},
                flow_config={"state": {}, "persistence": {}},
                callbacks={"job_id": "j2"},
            )
            route_method = getattr(flow, "route_async_rt_yes_0", None)
            result = await route_method("prev")
            assert result == "combined"

    @pytest.mark.asyncio
    async def test_route_listener_no_job_id(self):
        """No job_id in callbacks → skips callback setup."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "nojobbies",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
                callbacks={},  # No job_id
            )
            route_method = getattr(flow, "route_nojobbies_yes_0", None)
            await route_method(None)

    @pytest.mark.asyncio
    async def test_route_listener_callback_error(self):
        """Callback creation fails → logs warning but continues."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        p[f"create_execution_callbacks"] = MagicMock(side_effect=RuntimeError("cb err"))

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "cberr",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
                callbacks={"job_id": "j3"},
            )
            route_method = getattr(flow, "route_cberr_yes_0", None)
            result = await route_method(None)
            assert result == "ok"

    @pytest.mark.asyncio
    async def test_route_listener_no_crew_name(self):
        """Route with no crewName uses agent role."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2", agent_role="MyAgent")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "nocn",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},  # No crewName
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
            )
            route_method = getattr(flow, "route_nocn_yes_0", None)
            await route_method(None)

    @pytest.mark.asyncio
    async def test_route_listener_no_callbacks(self):
        """callbacks_param is None → skips callback setup."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "nocb",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
                callbacks=None,
            )
            route_method = getattr(flow, "route_nocb_yes_0", None)
            await route_method(None)

    @pytest.mark.asyncio
    async def test_route_listener_empty_route_tasks(self):
        """Route with task IDs not in all_tasks → no listener created."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "empty",
            "listenTo": None,
            "routes": {"yes": [{"id": "nonexistent"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
            )
            # No route listener should exist since no tasks were found
            assert not hasattr(flow, "route_empty_yes_0") or getattr(flow, "route_empty_yes_0", None) is None


class TestEdgeCases:
    """Edge cases and misc coverage."""

    @pytest.mark.asyncio
    async def test_hitl_edge_target_matches_task_id(self):
        """HITL edge target matches a task ID (not crew_id)."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("gate")(lambda self, x: "after")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        # Edge targets a specific task ID
        edges = [{"id": "e2", "target": "t2", "data": {"hitl": {"enabled": True}}}]
        nodes = []

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "edges": edges,
                    "nodes": nodes,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_edge_target_contains_crew_id(self):
        """HITL edge target node ID contains crew_id as substring."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("gate")(lambda self, x: "after")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        # Edge target is "crew-crew2-12345" which contains "crew2"
        edges = [{"id": "e3", "target": "crew-crew2-12345", "data": {"hitl": {"enabled": True}}}]
        nodes = []

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "edges": edges,
                    "nodes": nodes,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_edge_resolved_crew_id(self):
        """HITL edge resolved through node_to_crew_map."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_hitl_gate_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "gate")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("gate")(lambda self, x: "after")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        # Node maps uuid-node to crew2
        nodes = [{"id": "uuid-node", "data": {"crewId": "crew2"}}]
        edges = [{"id": "e4", "target": "uuid-node", "data": {"hitl": {"enabled": True}}}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "edges": edges,
                    "nodes": nodes,
                },
            )
            factory.create_hitl_gate_method.assert_called_once()

    @pytest.mark.asyncio
    async def test_hitl_edge_not_enabled(self):
        """HITL config exists but enabled=False → no gate created."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        edges = [{"id": "e5", "target": "crew2", "data": {"hitl": {"enabled": False}}}]
        nodes = [{"id": "crew2", "data": {"crewId": "crew2"}}]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [],
                    "edges": edges,
                    "nodes": nodes,
                },
            )
            factory.create_hitl_gate_method.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_flow_config(self):
        """flow_config=None uses empty defaults."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [], {}, {"t1": task1},
                flow_config=None,
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_default_method_no_starting_points(self):
        """No starting points at all → uses 'starting_point_0' as default."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )

        listeners = [("listen_1", "crew2", ["t1"], [task1], "Crew2", ["no-match"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                [], listeners, [], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_listener_frontend_crew_name_via_crewName_key(self):
        """Listener frontend config uses 'crewName' instead of 'name'."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_listener_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "db_name", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={
                    "state": {}, "persistence": {},
                    "listeners": [{"crewId": "crew2", "crewName": "FrontendName2"}],
                },
            )
            assert flow is not None

    @pytest.mark.asyncio
    async def test_router_with_listen_to(self):
        """Router with explicit listenTo method name."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "explicit_listen",
            "listenTo": "starting_point_0",
            "routes": {"a": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
            )
            assert hasattr(flow, "router_explicit_listen_0")

    @pytest.mark.asyncio
    async def test_router_no_starting_points(self):
        """Router with no starting points defaults to 'starting_point_0'."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        router_cfg = {
            "name": "no_sp",
            "listenTo": None,
            "routes": {"a": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                [], [], [router_cfg], {}, {},
                flow_config={"state": {}, "persistence": {}},
            )
            assert hasattr(flow, "router_no_sp_0")

    @pytest.mark.asyncio
    async def test_route_listener_crew_name_from_task(self):
        """Route listener gets crew name from route_tasks config."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_instance = MagicMock()
        mock_crew_instance.kickoff_async = AsyncMock(return_value="ok")
        mock_crew_cls.return_value = mock_crew_instance
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "named",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2", "crew_name": "CustomCrew"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
            )
            route_method = getattr(flow, "route_named_yes_0", None)
            await route_method(None)

    @pytest.mark.asyncio
    async def test_per_route_conditions_overall_exception(self):
        """Exception at top level of per-route evaluation → None."""
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        # We need routeConditions to be truthy but cause an exception
        # during the whole evaluation. The issue is that 'build_eval_context'
        # might raise if we mess with things properly.
        # Actually the try/except wraps the whole route evaluation including build_eval_context
        router_cfg = {
            "name": "exc_rt",
            "listenTo": None,
            "routes": {"a": []},
            "condition": None,
            "routeConditions": {"a": "True"},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
            )
            method = getattr(flow, "router_exc_rt_0")

            # Force an exception during build_eval_context
            # by passing an arg whose .raw property raises RuntimeError
            # hasattr() in Python 3 only catches AttributeError, so RuntimeError
            # propagates up to the outer try/except which returns None
            class BadArg:
                @property
                def raw(self):
                    raise RuntimeError("raw boom")

            result = method(BadArg())
            # The outer except (line 1004) catches the RuntimeError and returns None
            assert result is None


class TestRouterMethodNamePatching:
    """Test that _meth __name__/__qualname__ are patched for router and route_listener."""

    @pytest.mark.asyncio
    async def test_router_method_has_correct_name(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "nm",
            "listenTo": None,
            "routes": {"a": []},
            "condition": None,
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1},
                flow_config={"state": {}, "persistence": {}},
            )
            method = getattr(flow, "router_nm_0", None)
            assert method is not None
            assert method.__name__ == "router_nm_0"

    @pytest.mark.asyncio
    async def test_route_listener_method_has_correct_name(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        mock_crew_cls = MagicMock()
        mock_crew_cls.return_value.kickoff_async = AsyncMock(return_value="ok")
        p[f"Crew"] = mock_crew_cls
        p[f"Task"] = MagicMock(return_value=MagicMock())

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        router_cfg = {
            "name": "nmrl",
            "listenTo": None,
            "routes": {"yes": [{"id": "t2"}]},
            "condition": "True",
            "routeConditions": {},
            "conditionField": "success",
        }

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, [], [router_cfg], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}},
            )
            method = getattr(flow, "route_nmrl_yes_0", None)
            assert method is not None
            assert method.__name__ == "route_nmrl_yes_0"


class TestMergeHelpers:
    """Test merge_parsed_json and strip_code_fences inside build_eval_context."""

    def _build(self, route_conditions):
        import asyncio
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p[f"FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        router_cfg = {
            "name": "merge",
            "listenTo": None,
            "routes": {k: [] for k in route_conditions},
            "condition": None,
            "routeConditions": route_conditions,
            "conditionField": "success",
        }
        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = asyncio.run(
                FlowBuilder._create_dynamic_flow(
                    sp, [], [router_cfg], {}, {"t1": task1},
                    flow_config={"state": {}, "persistence": {}},
                )
            )
        return flow

    def test_json_array_non_dict_items(self):
        """JSON array with non-dict items → items stored but not merged."""
        flow = self._build({"check": "len(items) == 3"})
        crew_output = MagicMock()
        crew_output.raw = '[1, 2, 3]'
        method = getattr(flow, "router_merge_0")
        result = method(crew_output)
        assert result == "check"

    def test_state_json_array_first_item(self):
        """State value is JSON array → first item merged."""
        flow = self._build({"check": "name == 'alice'"})
        flow.state = {"data": '[{"name": "alice"}]'}
        method = getattr(flow, "router_merge_0")
        result = method("dummy")  # need arg so strip_code_fences is defined
        assert result == "check"

    def test_crew_output_raw_invalid_json(self):
        """CrewOutput .raw looks like JSON but isn't valid → silently skipped."""
        flow = self._build({"check": "True"})
        crew_output = MagicMock()
        crew_output.raw = '{invalid json}'
        method = getattr(flow, "router_merge_0")
        result = method(crew_output)
        assert result == "check"

    def test_string_arg_invalid_json(self):
        """String arg looks like JSON but invalid → silently skipped."""
        flow = self._build({"check": "True"})
        method = getattr(flow, "router_merge_0")
        result = method('{bad json}')
        assert result == "check"

    def test_no_args_no_state(self):
        """No args and no special state → still works."""
        flow = self._build({"check": "True"})
        method = getattr(flow, "router_merge_0")
        result = method()
        assert result == "check"

    def test_strip_code_fences_no_newline(self):
        """Code fences without newline after first ``` → strips all."""
        flow = self._build({"check": "True"})
        flow.state = {"data": '```{"x": 1}```'}
        method = getattr(flow, "router_merge_0")
        result = method("dummy")  # need arg so strip_code_fences is defined
        assert result == "check"

    def test_string_result_code_fences(self):
        """String result with code fences parsed."""
        flow = self._build({"check": "val == 5"})
        method = getattr(flow, "router_merge_0")
        result = method('```json\n{"val": 5}\n```')
        assert result == "check"

    def test_auto_convert_non_numeric_string(self):
        """String that is not numeric stays as string."""
        flow = self._build({"check": "name == 'hello'"})
        crew_output = MagicMock()
        crew_output.raw = '{"name": "hello"}'
        method = getattr(flow, "router_merge_0")
        result = method(crew_output)
        assert result == "check"


# ===================================================================
# Additional tests to cover remaining lines
# ===================================================================
class TestBuildFlowAgentCollection:
    """Cover lines 154-157: building all_agents from all_tasks."""

    @pytest.mark.asyncio
    async def test_agents_built_from_tasks(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1", agent_role="Researcher")
        p = _patches()
        p["FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})

        def populate_tasks(fc, all_tasks_dict, *args, **kwargs):
            all_tasks_dict["t1"] = task1
            return [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        p["FlowProcessorManager"].process_starting_points = AsyncMock(side_effect=populate_tasks)
        p["FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p["FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None


class TestBuildFlowTaskValidation:
    """Cover line 210: task ID found in all_tasks."""

    @pytest.mark.asyncio
    async def test_task_found_in_all_tasks(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p["FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})

        def populate_tasks(fc, all_tasks_dict, *args, **kwargs):
            all_tasks_dict["t1"] = task1
            return [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        p["FlowProcessorManager"].process_starting_points = AsyncMock(side_effect=populate_tasks)
        p["FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p["FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        fd = _make_flow_data(starting_points=[{"taskId": "t1"}])

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(fd)
            assert flow is not None


class TestBuildFlowMissingRepos:
    """Cover line 241: missing repos for checkpoint loading."""

    @pytest.mark.asyncio
    async def test_missing_execution_trace_repo(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        task1 = _make_task("t1")
        p = _patches()
        p["FlowConfigManager"].collect_agent_mcp_requirements = AsyncMock(return_value={})
        p["FlowProcessorManager"].process_starting_points = AsyncMock(
            return_value=[("starting_point_0", ["t1"], [task1], "Crew1", {})]
        )
        p["FlowProcessorManager"].process_listeners = AsyncMock(return_value=[])
        p["FlowProcessorManager"].process_routers = AsyncMock(return_value=[])

        repos = {"execution_history": AsyncMock(), "execution_trace": None}

        fd = _make_flow_data()

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder.build_flow(
                fd, repositories=repos,
                resume_from_execution_id="exec-1"
            )
            assert flow is not None


class TestListenerSkipCheckpointMissing:
    """Cover line 694: listener skip with truthy checkpoint_outputs but wrong crew name."""

    @pytest.mark.asyncio
    async def test_listener_skip_checkpoint_output_not_found(self):
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        task2 = _make_task("t2")
        factory = p["FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )
        factory.create_skipped_crew_method = MagicMock(
            return_value=_fake_listen("starting_point_0")(lambda self, x: "skipped")
        )

        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]
        listeners = [("listen_1", "crew2", ["t2"], [task2], "Crew2", ["t1"], "NONE", {})]

        with patch.multiple(MODULE, **p):
            flow = await FlowBuilder._create_dynamic_flow(
                sp, listeners, [], {}, {"t1": task1, "t2": task2},
                flow_config={"state": {}, "persistence": {}, "listeners": []},
                resume_from_crew_sequence=5,
                checkpoint_outputs={"OtherCrew": "data"},
            )
            assert flow is not None


class TestStateJsonParseException:
    """Cover lines 966-968: state JSON looks like JSON but fails to parse."""

    def _build_flow(self, route_conditions):
        import asyncio
        from src.engines.crewai.flow.modules.flow_builder import FlowBuilder

        p = _patches()
        task1 = _make_task("t1")
        factory = p["FlowMethodFactory"]
        factory.create_starting_point_crew_method = MagicMock(
            return_value=_fake_start()(lambda self: "ok")
        )

        router_cfg = {
            "name": "sjpe",
            "listenTo": None,
            "routes": {k: [] for k in route_conditions},
            "condition": None,
            "routeConditions": route_conditions,
            "conditionField": "success",
        }
        sp = [("starting_point_0", ["t1"], [task1], "Crew1", {})]

        with patch.multiple(MODULE, **p):
            flow = asyncio.run(
                FlowBuilder._create_dynamic_flow(
                    sp, [], [router_cfg], {}, {"t1": task1},
                    flow_config={"state": {}, "persistence": {}},
                )
            )
        return flow

    def test_state_looks_like_json_but_fails_parse(self):
        """State value starts with { and ends with } but is not valid JSON."""
        flow = self._build_flow({"check": "True"})
        flow.state = {"data": "{not valid json}"}
        method = getattr(flow, "router_sjpe_0")
        result = method("dummy")
        assert result == "check"
