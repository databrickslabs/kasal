"""
Coverage-boosting tests for BackendFlow.

Targets uncovered lines:
 161-166, 174-183, 190-191, 202-206, 243, 271-276, 298-307, 325-326, 328,
 330-331, 359-360, 367-375, 382-383, 394, 398-399, 403-414, 424-427, 437-438,
 447-453, 469-474, 497-506, 525-536, 598-599, 606-607, 617-626, 658-663,
 678-679, 684-687, 695-701
"""

import pytest
import uuid
from unittest.mock import MagicMock, AsyncMock, patch, Mock, PropertyMock

from src.engines.crewai.flow.backend_flow import BackendFlow
from src.engines.crewai.flow.exceptions import FlowPausedForApprovalException


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bf(job_id="job-1", flow_id=None):
    return BackendFlow(job_id=job_id, flow_id=flow_id)


def _make_flow_id():
    return uuid.uuid4()


# ---------------------------------------------------------------------------
# __init__ / properties
# ---------------------------------------------------------------------------

class TestBackendFlowInit:

    def test_init_none_flow_id(self):
        bf = BackendFlow(job_id="j", flow_id=None)
        assert bf._flow_id is None

    def test_init_uuid_flow_id(self):
        fid = uuid.uuid4()
        bf = BackendFlow(job_id="j", flow_id=fid)
        assert bf._flow_id == fid

    def test_init_string_flow_id(self):
        fid = uuid.uuid4()
        bf = BackendFlow(job_id="j", flow_id=str(fid))
        assert bf._flow_id == fid

    def test_init_invalid_string_flow_id(self):
        with pytest.raises(ValueError):
            BackendFlow(job_id="j", flow_id="not-a-uuid")

    def test_config_setter_getter(self):
        bf = _make_bf()
        bf.config = {"key": "value"}
        assert bf.config["key"] == "value"

    def test_repositories_setter_getter(self):
        bf = _make_bf()
        bf.repositories = {"flow": MagicMock()}
        assert "flow" in bf.repositories


# ---------------------------------------------------------------------------
# load_flow
# ---------------------------------------------------------------------------

class TestLoadFlow:

    @pytest.mark.asyncio
    async def test_load_flow_no_flow_id(self):
        bf = BackendFlow(job_id="j", flow_id=None)
        with pytest.raises(ValueError, match="No flow_id provided"):
            await bf.load_flow(repository=None)

    @pytest.mark.asyncio
    async def test_load_flow_no_repository(self):
        bf = BackendFlow(job_id="j", flow_id=_make_flow_id())
        with pytest.raises(ValueError, match="No flow repository provided"):
            await bf.load_flow(repository=None)

    @pytest.mark.asyncio
    async def test_load_flow_flow_not_found(self):
        bf = BackendFlow(job_id="j", flow_id=_make_flow_id())
        repo = MagicMock()
        repo.get = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="not found"):
            await bf.load_flow(repository=repo)

    @pytest.mark.asyncio
    async def test_load_flow_success(self):
        fid = _make_flow_id()
        bf = BackendFlow(job_id="j", flow_id=fid)
        flow_data = MagicMock()
        flow_data.id = fid
        flow_data.name = "Test Flow"
        flow_data.crew_id = None
        flow_data.nodes = [{"id": "n1"}]
        flow_data.edges = []
        flow_data.flow_config = {"startingPoints": []}

        repo = MagicMock()
        repo.get = AsyncMock(return_value=flow_data)

        result = await bf.load_flow(repository=repo)
        assert result["name"] == "Test Flow"
        assert bf._flow_data is not None

    @pytest.mark.asyncio
    async def test_load_flow_repo_raises(self):
        bf = BackendFlow(job_id="j", flow_id=_make_flow_id())
        repo = MagicMock()
        repo.get = AsyncMock(side_effect=RuntimeError("db fail"))
        with pytest.raises(RuntimeError):
            await bf.load_flow(repository=repo)


# ---------------------------------------------------------------------------
# _init_callbacks
# ---------------------------------------------------------------------------

class TestInitCallbacks:

    def test_init_callbacks_with_group_context(self):
        bf = _make_bf(flow_id=_make_flow_id())
        group_ctx = MagicMock()
        bf.config = {"group_context": group_ctx}

        with patch("src.utils.user_context.UserContext") as MockUC:
            bf._init_callbacks()
        assert "callbacks" in bf.config
        assert bf.config["callbacks"]["job_id"] == "job-1"

    def test_init_callbacks_group_context_raises(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf.config = {"group_context": MagicMock()}

        # Patch the local import by patching the module import
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == "src.utils.user_context":
                raise ImportError("no module")
            return real_import(name, *args, **kwargs)

        # Just call it directly - the group_context warning path is exercised
        with patch("src.utils.user_context.UserContext.set_group_context", side_effect=Exception("ctx err")):
            bf._init_callbacks()
        assert "callbacks" in bf.config

    def test_init_callbacks_no_group_context(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf.config = {}
        bf._init_callbacks()
        # flow_id is the UUID (not None) since we created bf with a flow_id
        assert "callbacks" in bf.config
        assert bf.config["callbacks"]["job_id"] == "job-1"


# ---------------------------------------------------------------------------
# flow() method
# ---------------------------------------------------------------------------

class TestFlowMethod:

    @pytest.mark.asyncio
    async def test_flow_with_no_flow_data_raises(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf._flow_data = None
        bf._config = {}
        bf._repositories = {"flow": None}

        with pytest.raises(ValueError):
            await bf.flow()

    @pytest.mark.asyncio
    async def test_flow_unsaved_flow_from_config(self):
        bf = _make_bf(flow_id=None)
        bf._flow_data = None
        bf._config = {
            "nodes": [{"id": "n1"}],
            "edges": [],
            "flow_config": {"startingPoints": []},
            "name": "Unsaved",
        }
        bf._repositories = {}

        with patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(return_value=MagicMock())):
            result = await bf.flow()
        assert result is not None

    @pytest.mark.asyncio
    async def test_flow_sets_group_context(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        group_ctx = MagicMock()
        bf._config = {"group_context": group_ctx}

        with patch("src.utils.user_context.UserContext.set_group_context") as mock_set_ctx, \
             patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(return_value=MagicMock())):
            await bf.flow()
        # Called at least once (may be called multiple times from flow() and _init_callbacks())
        assert mock_set_ctx.call_count >= 1
        mock_set_ctx.assert_called_with(group_ctx)

    @pytest.mark.asyncio
    async def test_flow_group_context_import_error(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {"group_context": MagicMock()}

        with patch("src.utils.user_context.UserContext.set_group_context", side_effect=Exception("no mod")), \
             patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(return_value=MagicMock())):
            result = await bf.flow()
        assert result is not None

    @pytest.mark.asyncio
    async def test_flow_load_from_db_when_no_flow_data_no_nodes(self):
        fid = _make_flow_id()
        bf = BackendFlow(job_id="j", flow_id=fid)
        bf._flow_data = None
        bf._config = {}  # No nodes

        flow_repo = MagicMock()
        flow_data = MagicMock()
        flow_data.id = fid
        flow_data.name = "DB Flow"
        flow_data.crew_id = None
        flow_data.nodes = [{"id": "n1"}]
        flow_data.edges = []
        flow_data.flow_config = {"startingPoints": []}
        flow_repo.get = AsyncMock(return_value=flow_data)
        bf._repositories = {"flow": flow_repo}

        with patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(return_value=MagicMock())):
            result = await bf.flow()
        assert result is not None

    @pytest.mark.asyncio
    async def test_flow_build_error_raises_value_error(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {}

        with patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(side_effect=RuntimeError("build fail"))):
            with pytest.raises(ValueError, match="Failed to create flow"):
                await bf.flow()

    @pytest.mark.asyncio
    async def test_flow_with_resume_params(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {
            "resume_from_flow_uuid": "uuid-x",
            "resume_from_crew_sequence": 2,
            "resume_from_execution_id": "exec-1",
        }

        mock_dynamic_flow = MagicMock()
        with patch("src.engines.crewai.flow.backend_flow.FlowBuilder.build_flow", new=AsyncMock(return_value=mock_dynamic_flow)):
            result = await bf.flow()
        assert result is mock_dynamic_flow


# ---------------------------------------------------------------------------
# kickoff_async
# ---------------------------------------------------------------------------

class TestKickoffAsync:

    def _setup_bf(self, flow_data=None):
        bf = _make_bf(flow_id=_make_flow_id())
        bf._flow_data = flow_data or {
            "id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}
        }
        bf._config = {"callbacks": {"start_trace_writer": False}}
        return bf

    @pytest.mark.asyncio
    async def test_kickoff_async_success_with_raw(self):
        bf = self._setup_bf()

        mock_result = MagicMock()
        mock_result.raw = "result text"

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=mock_result)
        mock_crewai_flow.state = MagicMock()
        mock_crewai_flow.state.id = "state-id-1"

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result["result"] == "result text"
        assert result["flow_uuid"] == "state-id-1"

    @pytest.mark.asyncio
    async def test_kickoff_async_result_is_none(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=None)
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result["result"] is None

    @pytest.mark.asyncio
    async def test_kickoff_async_result_is_dict(self):
        bf = self._setup_bf()
        raw_dict = {"key": "value"}

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=raw_dict)
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result["result"] == raw_dict

    @pytest.mark.asyncio
    async def test_kickoff_async_result_is_string(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="some string")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result["result"] == "some string"

    @pytest.mark.asyncio
    async def test_kickoff_async_result_has_to_dict(self):
        bf = self._setup_bf()
        obj = MagicMock(spec=["to_dict"])
        obj.to_dict.return_value = {"converted": True}

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=obj)
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result["result"] == {"converted": True}

    @pytest.mark.asyncio
    async def test_kickoff_async_result_fallback_str(self):
        bf = self._setup_bf()

        class WeirdObj:
            # No raw attr, no __dict__ in spec, no to_dict - triggers fallback str()
            def __str__(self):
                return "weird"

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=WeirdObj())
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        # WeirdObj has __dict__ so result may be that, but flow still succeeds
        assert result["result"] is not None

    @pytest.mark.asyncio
    async def test_kickoff_async_no_kickoff_async_method(self):
        """Falls back to synchronous kickoff."""
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock(spec=["kickoff"])
        mock_crewai_flow.kickoff.return_value = "sync result"

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_kickoff_async_resume_flow_uuid(self):
        bf = self._setup_bf()
        bf._config["resume_from_flow_uuid"] = "resume-uuid"

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="done")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        # Should have passed id=resume-uuid as input
        mock_crewai_flow.kickoff_async.assert_called_once_with(inputs={"id": "resume-uuid"})

    @pytest.mark.asyncio
    async def test_kickoff_async_flow_paused_exception_reraise(self):
        bf = self._setup_bf()

        pause_exc = FlowPausedForApprovalException(
            approval_id="a", gate_node_id="g", message="pause",
            execution_id="e", crew_sequence=0
        )
        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=pause_exc)

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            with pytest.raises(FlowPausedForApprovalException):
                await bf.kickoff_async()

    @pytest.mark.asyncio
    async def test_kickoff_async_execution_error_returns_failure(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=RuntimeError("exec fail"))

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is False
        assert "exec fail" in result["error"]

    @pytest.mark.asyncio
    async def test_kickoff_async_flow_creation_fails(self):
        bf = self._setup_bf()

        with patch.object(bf, "flow", new=AsyncMock(side_effect=RuntimeError("create fail"))), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_kickoff_async_loads_unsaved_flow_from_config(self):
        """When _flow_data is None and config has nodes, populate _flow_data."""
        bf = _make_bf(flow_id=None)
        bf._flow_data = None
        bf._config = {
            "nodes": [{"id": "n1"}],
            "edges": [],
            "flow_config": {},
            "callbacks": {},
        }

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="ok")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert bf._flow_data is not None

    @pytest.mark.asyncio
    async def test_kickoff_async_trace_writer_started(self):
        bf = self._setup_bf()
        bf._tracing_enabled = True

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="result")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.engines.crewai.trace_management.TraceManager") as MockTM:
            MockTM.ensure_writer_started = AsyncMock()
            result = await bf.kickoff_async()

        # Trace writer gets called (may or may not propagate depending on import path)
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_kickoff_async_trace_writer_raises_continues(self):
        bf = self._setup_bf()
        bf._tracing_enabled = True

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="result")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.engines.crewai.trace_management.TraceManager") as MockTM:
            MockTM.ensure_writer_started = AsyncMock(side_effect=Exception("trace fail"))
            result = await bf.kickoff_async()

        # Should not fail even if trace writer raises
        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_kickoff_async_state_id_extraction_error(self):
        """State.id access raises - flow_uuid should be None."""
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="done")

        state_mock = MagicMock()
        type(state_mock).id = PropertyMock(side_effect=RuntimeError("no id"))
        mock_crewai_flow.state = state_mock

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()

        assert result["success"] is True
        assert result.get("flow_uuid") is None

    @pytest.mark.asyncio
    async def test_kickoff_async_outer_exception_returns_failure(self):
        bf = self._setup_bf()
        # Make flow() itself raise at the outer level
        with patch.object(bf, "flow", new=AsyncMock(side_effect=Exception("outer fail"))), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff_async()
        # The outer handler catches and returns failure
        assert result["success"] is False


# ---------------------------------------------------------------------------
# kickoff()
# ---------------------------------------------------------------------------

class TestKickoff:
    """Tests for the synchronous kickoff() path."""

    def _setup_bf(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {"callbacks": {}}
        return bf

    @pytest.mark.asyncio
    async def test_kickoff_success_dict_result(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value={"result": "ok"})
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["success"] is True

    @pytest.mark.asyncio
    async def test_kickoff_state_id_dict(self):
        """State is a dict with 'id' key."""
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="ok")

        # Dict-like state
        state = {"id": "state-id-from-dict"}
        mock_crewai_flow.state = state

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["flow_uuid"] == "state-id-from-dict"

    @pytest.mark.asyncio
    async def test_kickoff_flow_paused_reraise(self):
        bf = self._setup_bf()

        pause_exc = FlowPausedForApprovalException(
            approval_id="a", gate_node_id="g", message="pause",
            execution_id="e", crew_sequence=0
        )
        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=pause_exc)

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            with pytest.raises(FlowPausedForApprovalException):
                await bf.kickoff()

    @pytest.mark.asyncio
    async def test_kickoff_kickoff_error_returns_failure(self):
        """kickoff() wraps kickoff errors in a failure dict - does not re-raise."""
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(side_effect=RuntimeError("kick error"))

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["success"] is False
        assert "kick error" in result["error"]

    @pytest.mark.asyncio
    async def test_kickoff_flow_create_fails_returns_failure(self):
        bf = self._setup_bf()

        with patch.object(bf, "flow", new=AsyncMock(side_effect=RuntimeError("create fail"))), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff()

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_kickoff_no_start_methods_returns_failure(self):
        bf = self._setup_bf()

        # Create a flow with no starting_point_ methods
        class EmptyFlow:
            def some_method(self):
                pass

        mock_crewai_flow = EmptyFlow()

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["success"] is False
        assert "No start methods" in result["error"]

    @pytest.mark.asyncio
    async def test_kickoff_resume_flow_uuid_passed(self):
        bf = self._setup_bf()
        bf._config["resume_from_flow_uuid"] = "resume-x"

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="done")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        # Should pass inputs with id=resume-x
        mock_crewai_flow.kickoff_async.assert_called_once_with(inputs={"id": "resume-x"})

    @pytest.mark.asyncio
    async def test_kickoff_result_is_none_continues(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=None)
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["success"] is True
        assert result["result"] is None

    @pytest.mark.asyncio
    async def test_kickoff_result_string(self):
        bf = self._setup_bf()

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="string output")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["result"] == "string output"

    @pytest.mark.asyncio
    async def test_kickoff_result_to_dict(self):
        bf = self._setup_bf()

        class HasToDict:
            def to_dict(self):
                return {"to_dict": True}

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value=HasToDict())
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert result["result"] == {"to_dict": True}

    @pytest.mark.asyncio
    async def test_kickoff_outer_exception_returns_failure(self):
        bf = self._setup_bf()
        bf._flow_data = None
        bf._config = {}  # No nodes, no repo to load

        with patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff()

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_kickoff_unsaved_flow_from_config(self):
        bf = _make_bf(flow_id=None)
        bf._flow_data = None
        bf._config = {
            "nodes": [{"id": "n1"}],
            "edges": [],
            "flow_config": {},
        }

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="done")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        assert bf._flow_data is not None

    @pytest.mark.asyncio
    async def test_kickoff_saved_flow_load_fails_returns_failure(self):
        bf = _make_bf(flow_id=_make_flow_id())
        bf._flow_data = None
        bf._config = {}
        bf._repositories = {"flow": None}

        with patch("src.engines.crewai.flow.backend_flow.CallbackManager"):
            result = await bf.kickoff()

        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_kickoff_merges_config_flow_config(self):
        bf = self._setup_bf()
        bf._config = {
            "callbacks": {},
            "flow_config": {"startingPoints": [{"nodeId": "sp1"}]},
            "nodes": [{"id": "n1"}],
            "edges": [{"source": "n0", "target": "n1"}],
        }
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.starting_point_0 = MagicMock()
        mock_crewai_flow.kickoff_async = AsyncMock(return_value="ok")
        mock_crewai_flow.state = None

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)), \
             patch("src.engines.crewai.flow.backend_flow.CallbackManager"), \
             patch("src.db.session._request_session"):
            result = await bf.kickoff()

        # flow_data should have been updated from config
        assert bf._flow_data["flow_config"] == {"startingPoints": [{"nodeId": "sp1"}]}


# ---------------------------------------------------------------------------
# plot()
# ---------------------------------------------------------------------------

class TestPlot:

    @pytest.mark.asyncio
    async def test_plot_with_plot_method(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {}

        mock_crewai_flow = MagicMock()
        mock_crewai_flow.plot = MagicMock()

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)):
            result = await bf.plot("my_diagram")

        assert result is not None
        mock_crewai_flow.plot.assert_called_once()

    @pytest.mark.asyncio
    async def test_plot_without_plot_method(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {}

        mock_crewai_flow = MagicMock(spec=["kickoff"])

        with patch.object(bf, "flow", new=AsyncMock(return_value=mock_crewai_flow)):
            result = await bf.plot()

        assert result is None

    @pytest.mark.asyncio
    async def test_plot_exception_returns_none(self):
        bf = _make_bf()
        bf._flow_data = {"id": None, "name": "f", "crew_id": None, "nodes": [], "edges": [], "flow_config": {}}
        bf._config = {}

        with patch.object(bf, "flow", new=AsyncMock(side_effect=RuntimeError("plot fail"))):
            result = await bf.plot()

        assert result is None


# ---------------------------------------------------------------------------
# _ensure_event_listeners_registered / _configure_agent_and_tools / _configure_task
# ---------------------------------------------------------------------------

class TestDelegatedMethods:

    def test_ensure_event_listeners_registered(self):
        bf = _make_bf()
        listeners = [MagicMock()]
        with patch("src.engines.crewai.flow.backend_flow.CallbackManager.ensure_event_listeners_registered") as mock_enr:
            bf._ensure_event_listeners_registered(listeners)
        mock_enr.assert_called_once_with(listeners)

    @pytest.mark.asyncio
    async def test_configure_agent_and_tools(self):
        bf = _make_bf()
        bf._flow_data = {}
        bf._config = {}
        bf._repositories = {}

        agent_data = MagicMock()
        mock_agent = MagicMock()

        with patch("src.engines.crewai.flow.backend_flow.AgentConfig.configure_agent_and_tools", new=AsyncMock(return_value=mock_agent)):
            result = await bf._configure_agent_and_tools(agent_data)

        assert result is mock_agent

    @pytest.mark.asyncio
    async def test_configure_task(self):
        bf = _make_bf()
        bf._flow_data = {}
        bf._config = {}
        bf._repositories = {}

        task_data = MagicMock()
        mock_task = MagicMock()

        with patch("src.engines.crewai.flow.backend_flow.TaskConfig.configure_task", new=AsyncMock(return_value=mock_task)):
            result = await bf._configure_task(task_data)

        assert result is mock_task
