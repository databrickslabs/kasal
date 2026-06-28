"""
Coverage-boosting tests for flow_methods.py and flow_processors.py.

flow_methods.py targets:
 63, 127-128, 222, 282-283, 315-316, 328-336, 349, 352-353, 370-371,
 392-393, 428-429, 433-435, 442-445, 593-594, 611-612, 623-632, 638-639,
 662-677, 683-691, 695-708, 722-723, 744-747, 783-786, 788-803, 818-842,
 844-846, 862-863, 867-869, 876-879, 894-897, 963, 970-973, 1021-1028,
 1169-1344

flow_processors.py targets:
 17, 20, 91, 99-100, 134, 146-149, 174-179, 191-198, 234-235, 237-238, 278,
 473, 484-509, 513-515, 518-519, 530-537, 573-574, 576-577, 605, 616,
 635-639, 701-702, 724, 754, 766-769, 794-796, 799-800, 811-819, 855-856,
 858-859, 886
"""

import pytest
import uuid
import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, AsyncMock, patch, Mock, call


# ---------------------------------------------------------------------------
# flow_methods.py
# ---------------------------------------------------------------------------

from src.engines.crewai.paths.flow.modules.flow_methods import (
    extract_final_answer,
    get_model_context_limits,
    FlowMethodFactory,
)


class TestExtractFinalAnswer:

    def test_empty_results(self):
        assert extract_final_answer([]) == ""
        assert extract_final_answer(None) == ""

    def test_string_result_with_final_answer(self):
        result = extract_final_answer(["The answer is 42. Final Answer: 42"])
        assert result == "42"

    def test_string_result_without_final_answer(self):
        result = extract_final_answer(["Just a plain answer"])
        assert result == "Just a plain answer"

    def test_dict_result_with_content(self):
        result = extract_final_answer([{"content": "My answer Final Answer: win"}])
        assert "win" in result

    def test_raw_attribute_result(self):
        obj = MagicMock()
        obj.raw = "Raw output Final Answer: the raw one"
        result = extract_final_answer([obj])
        assert "the raw one" in result

    def test_list_of_dicts_result(self):
        items = [{"content": "result 1"}, {"content": "result 2"}]
        result = extract_final_answer([items])
        assert "result 1" in result or "result 2" in result

    def test_list_of_mixed_items(self):
        items = [{"content": "item Final Answer: answer1"}, "plain string"]
        result = extract_final_answer([items])
        assert result  # Should not be empty

    def test_fallback_to_str(self):
        class WeirdObj:
            def __str__(self):
                return "weird object"
        result = extract_final_answer([WeirdObj()])
        assert result == "weird object"

    def test_string_final_answer_no_colon(self):
        """'Final Answer' without colon (edge case)."""
        result = extract_final_answer(["Some text Final Answer the thing"])
        assert result  # some result returned

    def test_list_content_without_final_answer(self):
        items = [{"content": "just content"}, {"content": "more content"}]
        result = extract_final_answer([items])
        assert "just content" in result or "more content" in result


class TestGetModelContextLimits:

    @pytest.mark.asyncio
    async def test_no_llm_returns_defaults(self):
        agent = MagicMock(spec=[])  # no llm attr
        ctx, max_out = await get_model_context_limits(agent, None)
        assert ctx == 128000
        assert max_out == 16000

    @pytest.mark.asyncio
    async def test_llm_string(self):
        agent = MagicMock()
        agent.llm = "gpt-4o"
        # no group_id → should return defaults
        ctx, max_out = await get_model_context_limits(agent, None)
        assert ctx == 128000

    @pytest.mark.asyncio
    async def test_llm_with_model_attr(self):
        agent = MagicMock()
        agent.llm = MagicMock()
        agent.llm.model = "claude-3"
        ctx, max_out = await get_model_context_limits(agent, None)
        assert ctx == 128000  # default because no group_id

    @pytest.mark.asyncio
    async def test_llm_unknown_type_returns_defaults(self):
        agent = MagicMock()
        agent.llm = 12345  # not str, no .model attr
        ctx, max_out = await get_model_context_limits(agent, None)
        assert ctx == 128000

    @pytest.mark.asyncio
    async def test_with_group_id_model_found(self):
        agent = MagicMock()
        agent.llm = "my-model"

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        mock_config = MagicMock()
        mock_config.context_window = 65536
        mock_config.max_output_tokens = 8192

        mock_session_ctx = MagicMock()
        mock_session = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("src.db.session.request_scoped_session", return_value=mock_session_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockMCS:
            mcs_instance = MagicMock()
            mcs_instance.find_by_key = AsyncMock(return_value=mock_config)
            MockMCS.return_value = mcs_instance

            ctx, max_out = await get_model_context_limits(agent, group_ctx)

        assert ctx == 65536
        assert max_out == 8192

    @pytest.mark.asyncio
    async def test_with_group_id_model_not_found(self):
        agent = MagicMock()
        agent.llm = "unknown-model"

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        mock_session_ctx = MagicMock()
        mock_session = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("src.db.session.request_scoped_session", return_value=mock_session_ctx), \
             patch("src.services.model_config_service.ModelConfigService") as MockMCS:
            mcs_instance = MagicMock()
            mcs_instance.find_by_key = AsyncMock(return_value=None)
            MockMCS.return_value = mcs_instance

            ctx, max_out = await get_model_context_limits(agent, group_ctx)

        assert ctx == 128000
        assert max_out == 16000

    @pytest.mark.asyncio
    async def test_group_ids_list(self):
        agent = MagicMock()
        agent.llm = "model"

        group_ctx = MagicMock(spec=["group_ids"])
        group_ctx.group_ids = ["g1", "g2"]

        ctx, max_out = await get_model_context_limits(agent, group_ctx)
        # No primary_group_id, group_ids available but no session, defaults returned
        assert ctx == 128000

    @pytest.mark.asyncio
    async def test_exception_returns_defaults(self):
        agent = MagicMock()
        agent.llm = "model"

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        fail_ctx = MagicMock()
        fail_ctx.__aenter__ = AsyncMock(side_effect=Exception("session err"))
        fail_ctx.__aexit__ = AsyncMock(return_value=False)

        with patch("src.db.session.request_scoped_session", return_value=fail_ctx):
            ctx, max_out = await get_model_context_limits(agent, group_ctx)

        assert ctx == 128000
        assert max_out == 16000


class TestFlowMethodFactoryCreateStartingPoint:
    """Tests for FlowMethodFactory.create_starting_point_crew_method."""

    def _make_task(self, role="Agent"):
        task = MagicMock()
        task.description = "Do task"
        task.agent = MagicMock()
        task.agent.role = role
        task.agent.tools = []
        task.agent.llm = None
        task.agent._kasal_memory_disabled = False
        task.expected_output = "done"
        task.context = []
        return task

    def test_creates_callable(self):
        tasks = [self._make_task()]
        create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_0",
            task_list=tasks,
            crew_name="Test Crew",
            callbacks={"job_id": "job-1"},
            group_context=None,
            create_execution_callbacks=create_callbacks,
        )

        assert callable(method)
        assert method.__name__ == "starting_point_0"

    def test_method_name_set_on_wrapped(self):
        tasks = [self._make_task()]
        create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_starting_point_crew_method(
            method_name="starting_point_1",
            task_list=tasks,
            crew_name="Crew",
            callbacks={},
            group_context=None,
            create_execution_callbacks=create_callbacks,
        )

        assert method.__name__ == "starting_point_1"
        assert method._meth.__name__ == "starting_point_1"


class TestFlowMethodFactoryCreateListenerMethod:
    """Tests for FlowMethodFactory.create_listener_method."""

    def _make_task(self, role="Listener Agent"):
        task = MagicMock()
        task.description = "Listen task"
        task.agent = MagicMock()
        task.agent.role = role
        task.agent.tools = []
        task.agent.llm = None
        task.agent._kasal_memory_disabled = False
        task.expected_output = "result"
        return task

    def test_creates_callable_with_method_condition(self):
        tasks = [self._make_task()]
        create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_0",
            listener_tasks=tasks,
            method_condition="starting_point_0",
            condition_type="NONE",
            callbacks={"job_id": "job-1"},
            group_context=None,
            create_execution_callbacks=create_callbacks,
        )

        assert callable(method)
        assert method.__name__ == "listener_0"

    def test_method_name_set_on_wrapped(self):
        tasks = [self._make_task()]
        create_callbacks = MagicMock(return_value=(MagicMock(), MagicMock()))

        method = FlowMethodFactory.create_listener_method(
            method_name="listener_1",
            listener_tasks=tasks,
            method_condition="starting_point_0",
            condition_type="AND",
            callbacks={},
            group_context=None,
            create_execution_callbacks=create_callbacks,
        )

        assert method.__name__ == "listener_1"
        assert method._meth.__name__ == "listener_1"


class TestFlowMethodFactoryCreateSkippedCrew:
    """Tests for FlowMethodFactory.create_skipped_crew_method."""

    def test_starting_point_stub_created(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="Skip Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output="previous result"
        )
        assert callable(method)
        assert method.__name__ == "starting_point_0"

    def test_listener_stub_created(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="listener_0",
            crew_name="Skip Listener",
            crew_sequence=1,
            is_starting_point=False,
            method_condition="starting_point_0",
        )
        assert callable(method)
        assert method.__name__ == "listener_0"

    @pytest.mark.asyncio
    async def test_starting_point_stub_returns_checkpoint_output(self):
        checkpoint = "checkpoint data"
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=checkpoint
        )

        mock_self = MagicMock()
        mock_self.state = {}
        result = await method._meth(mock_self)
        assert result == checkpoint

    @pytest.mark.asyncio
    async def test_starting_point_stub_no_checkpoint_uses_placeholder(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self.state = {}
        mock_self._method_outputs = {}
        result = await method._meth(mock_self)
        # Falls back to placeholder dict
        assert isinstance(result, dict) or result is not None

    @pytest.mark.asyncio
    async def test_starting_point_stub_reads_from_state_dict(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="My Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self.state = {"starting_point_0": "state data"}
        mock_self._method_outputs = {}
        result = await method._meth(mock_self)
        assert result == "state data"

    @pytest.mark.asyncio
    async def test_starting_point_stub_reads_from_state_crew_name(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="My Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self.state = {"My Crew": "crew state data"}
        mock_self._method_outputs = {}
        result = await method._meth(mock_self)
        assert result == "crew state data"

    @pytest.mark.asyncio
    async def test_listener_stub_returns_checkpoint_output(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="listener_1",
            crew_name="Listener",
            crew_sequence=1,
            is_starting_point=False,
            method_condition="starting_point_0",
            checkpoint_output="listener checkpoint"
        )

        mock_self = MagicMock()
        mock_self.state = {}
        result = await method._meth(mock_self, previous_output=None)
        assert result == "listener checkpoint"

    @pytest.mark.asyncio
    async def test_listener_stub_falls_back_to_previous_output(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="listener_1",
            crew_name="Listener",
            crew_sequence=1,
            is_starting_point=False,
            method_condition="starting_point_0",
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self.state = {}
        mock_self._method_outputs = {}
        result = await method._meth(mock_self, previous_output="fallback output")
        assert result == "fallback output"

    @pytest.mark.asyncio
    async def test_listener_stub_no_output_uses_placeholder(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="listener_1",
            crew_name="Listener",
            crew_sequence=1,
            is_starting_point=False,
            method_condition="starting_point_0",
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self.state = {}
        mock_self._method_outputs = {}
        result = await method._meth(mock_self, previous_output=None)
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_starting_point_reads_from_method_outputs(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="Crew",
            crew_sequence=0,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self._method_outputs = {"starting_point_0": "from_persist"}
        mock_self.state = {}
        result = await method._meth(mock_self)
        assert result == "from_persist"

    @pytest.mark.asyncio
    async def test_starting_point_reads_crew_sequence_key_from_state(self):
        method = FlowMethodFactory.create_skipped_crew_method(
            method_name="starting_point_0",
            crew_name="Crew",
            crew_sequence=3,
            is_starting_point=True,
            checkpoint_output=None
        )

        mock_self = MagicMock()
        mock_self._method_outputs = {}
        mock_self.state = {"crew_3_output": "seq data"}
        result = await method._meth(mock_self)
        assert result == "seq data"


class TestFlowMethodFactoryCreateHitlGate:
    """Tests for FlowMethodFactory.create_hitl_gate_method."""

    def test_creates_callable(self):
        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-node-1",
            gate_config={"message": "Please review"},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1"},
            group_context=None,
        )
        assert callable(method)

    @pytest.mark.asyncio
    async def test_hitl_gate_no_job_id_raises(self):
        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={},  # No job_id
            group_context=None,
        )

        mock_self = MagicMock()
        mock_self.state = {}

        with pytest.raises(ValueError, match="job_id"):
            await method._meth(mock_self, previous_output=None)

    @pytest.mark.asyncio
    async def test_hitl_gate_no_group_id_raises(self):
        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1"},
            group_context=None,  # No group_context
        )

        mock_self = MagicMock()
        mock_self.state = {}

        with pytest.raises(ValueError, match="group_id"):
            await method._meth(mock_self, previous_output=None)

    def _make_session_ctx(self):
        mock_session_ctx = MagicMock()
        mock_session = AsyncMock()
        mock_session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_ctx.__aexit__ = AsyncMock(return_value=False)
        return mock_session_ctx

    @pytest.mark.asyncio
    async def test_hitl_gate_approved_returns_previous_output(self):
        """When gate already has an APPROVED approval, passes through to next step."""
        from src.models.hitl_approval import HITLApprovalStatus

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1", "flow_id": "flow-1"},
            group_context=group_ctx,
        )

        mock_self = MagicMock()
        mock_self.state = {}

        approved = MagicMock()
        approved.gate_node_id = "gate-1"
        approved.status = HITLApprovalStatus.APPROVED
        approved.id = "approved-1"
        approved.responded_by = "user@example.com"
        approved.responded_at = "2024-01-01"

        session_ctx = self._make_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.repositories.hitl_repository.HITLApprovalRepository") as MockRepo, \
             patch("src.repositories.execution_history_repository.ExecutionHistoryRepository") as MockExecRepo:

            repo_instance = MagicMock()
            repo_instance.get_all_for_execution = AsyncMock(return_value=[approved])
            MockRepo.return_value = repo_instance

            exec_repo_inst = MagicMock()
            exec_repo_inst.get_execution_by_job_id = AsyncMock(return_value=None)
            MockExecRepo.return_value = exec_repo_inst

            result = await method._meth(mock_self, previous_output="prev data")

        assert result == "prev data"

    @pytest.mark.asyncio
    async def test_hitl_gate_approved_with_edited_config(self):
        """When approved with edited_config, returns edited config JSON."""
        from src.models.hitl_approval import HITLApprovalStatus

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1", "flow_id": "flow-1"},
            group_context=group_ctx,
        )

        mock_self = MagicMock()
        mock_self.state = {}

        approved = MagicMock()
        approved.gate_node_id = "gate-1"
        approved.status = HITLApprovalStatus.APPROVED
        approved.id = "approved-1"

        execution = MagicMock()
        execution.checkpoint_data = {"edited_config": {"key": "edited_value"}}

        session_ctx = self._make_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.repositories.hitl_repository.HITLApprovalRepository") as MockRepo, \
             patch("src.repositories.execution_history_repository.ExecutionHistoryRepository") as MockExecRepo:

            repo_instance = MagicMock()
            repo_instance.get_all_for_execution = AsyncMock(return_value=[approved])
            MockRepo.return_value = repo_instance

            exec_repo_inst = MagicMock()
            exec_repo_inst.get_execution_by_job_id = AsyncMock(return_value=execution)
            MockExecRepo.return_value = exec_repo_inst

            result = await method._meth(mock_self, previous_output="prev data")

        import json
        assert result == json.dumps({"key": "edited_value"})

    @pytest.mark.asyncio
    async def test_hitl_gate_creates_approval_and_raises(self):
        """When no approved approval, creates one and raises FlowPausedForApprovalException."""
        from src.engines.crewai.paths.flow.exceptions import FlowPausedForApprovalException

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={"message": "Approve me"},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1", "flow_id": "flow-1"},
            group_context=group_ctx,
        )

        mock_self = MagicMock()
        mock_self.state = SimpleNamespace(id="state-uuid-1")

        approval = MagicMock()
        approval.id = "new-approval-1"
        approval.expires_at = "2099-01-01"

        session_ctx = self._make_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.repositories.hitl_repository.HITLApprovalRepository") as MockRepo, \
             patch("src.services.hitl_service.HITLService") as MockHITLSvc, \
             patch("src.services.hitl_webhook_service.HITLWebhookService") as MockWebhook:

            repo_instance = MagicMock()
            repo_instance.get_all_for_execution = AsyncMock(return_value=[])
            MockRepo.return_value = repo_instance

            hitl_svc_instance = MagicMock()
            hitl_svc_instance.create_approval_request = AsyncMock(return_value=approval)
            MockHITLSvc.return_value = hitl_svc_instance

            webhook_instance = MagicMock()
            webhook_instance.send_gate_reached_notification = AsyncMock()
            MockWebhook.return_value = webhook_instance

            with pytest.raises(FlowPausedForApprovalException) as exc_info:
                await method._meth(mock_self, previous_output="crew output")

        assert exc_info.value.gate_node_id == "gate-1"
        assert exc_info.value.approval_id == "new-approval-1"

    @pytest.mark.asyncio
    async def test_hitl_gate_fallback_flow_uuid_generated(self):
        """When state has no id, a flow_uuid should be generated."""
        from src.engines.crewai.paths.flow.exceptions import FlowPausedForApprovalException

        group_ctx = MagicMock()
        group_ctx.primary_group_id = "g1"

        method = FlowMethodFactory.create_hitl_gate_method(
            method_name="hitl_gate_0",
            gate_node_id="gate-1",
            gate_config={},
            previous_method_name="starting_point_0",
            crew_sequence=0,
            callbacks={"job_id": "job-1", "flow_id": None},
            group_context=group_ctx,
        )

        mock_self = MagicMock()
        mock_self.state = {}  # Dict state with no 'id'

        approval = MagicMock()
        approval.id = "appr-2"
        approval.expires_at = "2099-01-01"

        session_ctx = self._make_session_ctx()

        with patch("src.db.session.request_scoped_session", return_value=session_ctx), \
             patch("src.repositories.hitl_repository.HITLApprovalRepository") as MockRepo, \
             patch("src.services.hitl_service.HITLService") as MockHITLSvc, \
             patch("src.services.hitl_webhook_service.HITLWebhookService") as MockWebhook:

            repo_instance = MagicMock()
            repo_instance.get_all_for_execution = AsyncMock(return_value=[])
            MockRepo.return_value = repo_instance

            hitl_svc_instance = MagicMock()
            hitl_svc_instance.create_approval_request = AsyncMock(return_value=approval)
            MockHITLSvc.return_value = hitl_svc_instance

            webhook_instance = MagicMock()
            webhook_instance.send_gate_reached_notification = AsyncMock()
            MockWebhook.return_value = webhook_instance

            with pytest.raises(FlowPausedForApprovalException) as exc_info:
                await method._meth(mock_self, previous_output=None)

        # flow_uuid should have been generated (not None)
        assert exc_info.value.flow_uuid is not None


# ---------------------------------------------------------------------------
# flow_processors.py
# ---------------------------------------------------------------------------

from src.engines.crewai.paths.flow.modules.flow_processors import FlowProcessorManager, _to_uuid


class TestToUuid:

    def test_uuid_passthrough(self):
        fid = uuid.uuid4()
        assert _to_uuid(fid) == fid

    def test_string_conversion(self):
        fid = uuid.uuid4()
        result = _to_uuid(str(fid))
        assert result == fid

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError):
            _to_uuid(12345)


class TestProcessStartingPoints:

    @pytest.mark.asyncio
    async def test_empty_flow_config_returns_empty(self):
        result = await FlowProcessorManager.process_starting_points(
            flow_config={},
            all_tasks={},
            repositories={}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_no_task_repo_returns_empty(self):
        flow_config = {"startingPoints": [{"crewId": str(uuid.uuid4()), "taskId": "t1"}]}
        result = await FlowProcessorManager.process_starting_points(
            flow_config=flow_config,
            all_tasks={},
            repositories={}  # No task repo
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_crew_not_found_continues(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        flow_config = {"startingPoints": [{"crewId": crew_id, "taskId": task_id}]}

        task_repo = MagicMock()
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_starting_points(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_task_not_found_in_db_no_nodes_skipped(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        flow_config = {"startingPoints": [{"crewId": crew_id, "taskId": task_id}]}

        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=None)

        crew_data = MagicMock()
        crew_data.name = "Test Crew"
        crew_data.nodes = []
        crew_data.edges = []
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew_data)

        result = await FlowProcessorManager.process_starting_points(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_agent_id_not_resolved_skipped(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        flow_config = {"startingPoints": [{"crewId": crew_id, "taskId": task_id}]}

        task_data = SimpleNamespace(
            id=task_id, name="Task", description="desc", expected_output="output",
            agent_id=None,  # No agent_id
            tools=[], tool_configs={}, async_execution=False, config={}, memory=False,
            markdown=False, guardrail=None
        )
        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task_data)

        crew_data = MagicMock()
        crew_data.name = "Test Crew"
        crew_data.nodes = []
        crew_data.edges = []
        crew_data.tool_configs = {}
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew_data)

        result = await FlowProcessorManager.process_starting_points(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_starting_point(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        # startingPoints uses taskId (singular), not taskIds
        flow_config = {"startingPoints": [{"crewId": crew_id, "taskId": task_id}]}

        task_data = SimpleNamespace(
            id=task_id, name="Task", description="desc", expected_output="output",
            agent_id=agent_id, tools=[], tool_configs={}, async_execution=False,
            config={}, memory=False, markdown=False, guardrail=None
        )
        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task_data)

        agent_data = MagicMock()
        agent_data.id = agent_id
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        crew_data = MagicMock()
        crew_data.name = "Test Crew"
        crew_data.nodes = []
        crew_data.edges = []
        crew_data.tool_configs = {}
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew_data)

        mock_agent_obj = MagicMock()
        mock_task_obj = MagicMock()
        mock_task_obj.async_execution = False

        with patch("src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent_obj)), \
             patch("src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig.configure_task",
                   new=AsyncMock(return_value=mock_task_obj)):
            result = await FlowProcessorManager.process_starting_points(
                flow_config=flow_config,
                all_tasks={},
                repositories={"task": task_repo, "crew": crew_repo, "agent": agent_repo}
            )

        assert len(result) == 1
        assert result[0][0] == "starting_point_0"


class TestProcessListeners:

    @pytest.mark.asyncio
    async def test_empty_listeners_returns_empty(self):
        result = await FlowProcessorManager.process_listeners(
            flow_config={},
            all_tasks={},
            repositories={}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_no_task_repo_returns_empty(self):
        flow_config = {"listeners": [{"crewId": str(uuid.uuid4()), "taskIds": ["t1"], "listenTo": ["sp0"]}]}
        result = await FlowProcessorManager.process_listeners(
            flow_config=flow_config,
            all_tasks={},
            repositories={}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_crew_not_found_continues(self):
        crew_id = str(uuid.uuid4())
        flow_config = {"listeners": [
            {"crewId": crew_id, "taskIds": ["t1"], "listenTo": ["sp0"], "conditionType": "NONE"}
        ]}

        task_repo = MagicMock()
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_listeners(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_listener(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {"listeners": [
            {
                "crewId": crew_id,
                "tasks": [{"id": task_id}],
                "listenToTaskIds": ["starting_point_0"],
                "conditionType": "NONE",
                "crewName": "Listener Crew"
            }
        ]}

        task_data = SimpleNamespace(
            id=task_id, name="Listener Task", description="desc", expected_output="output",
            agent_id=agent_id, tools=[], tool_configs={}, async_execution=False,
            config={}, memory=False, markdown=False, guardrail=None
        )
        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task_data)

        agent_data = MagicMock()
        agent_data.id = agent_id
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        crew_data = MagicMock()
        crew_data.name = "Listener Crew"
        crew_data.nodes = []
        crew_data.edges = []
        crew_data.tool_configs = {}
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew_data)

        mock_agent_obj = MagicMock()
        mock_task_obj = MagicMock()
        mock_task_obj.async_execution = False

        with patch("src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent_obj)), \
             patch("src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig.configure_task",
                   new=AsyncMock(return_value=mock_task_obj)):
            result = await FlowProcessorManager.process_listeners(
                flow_config=flow_config,
                all_tasks={},
                repositories={"task": task_repo, "crew": crew_repo, "agent": agent_repo}
            )

        assert len(result) == 1
        assert result[0][0] == "listener_0"


class TestProcessRouters:

    @pytest.mark.asyncio
    async def test_empty_routers_returns_empty(self):
        result = await FlowProcessorManager.process_routers(
            flow_config={},
            all_tasks={},
            repositories={}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_no_task_repo_returns_empty(self):
        flow_config = {"routers": [{"listenTo": "sp0", "routes": {}}]}
        result = await FlowProcessorManager.process_routers(
            flow_config=flow_config,
            all_tasks={},
            repositories={}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_router_missing_listen_to(self):
        flow_config = {"routers": [{"routes": {}}]}  # No listenTo
        task_repo = MagicMock()
        crew_repo = MagicMock()
        result = await FlowProcessorManager.process_routers(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_router_empty_route_task_configs(self):
        flow_config = {
            "routers": [{
                "listenTo": "sp0",
                "routes": {"route_a": []},  # Empty list
                "routeConditions": {"route_a": "condition_a"}
            }]
        }
        task_repo = MagicMock()
        crew_repo = MagicMock()
        result = await FlowProcessorManager.process_routers(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_router_crew_not_found(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        flow_config = {
            "routers": [{
                "listenTo": "sp0",
                "routes": {"route_a": [{"id": task_id, "crewId": crew_id}]},
                "routeConditions": {}
            }]
        }
        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=None)
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=None)

        result = await FlowProcessorManager.process_routers(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_router_route_missing_crew_id(self):
        task_id = str(uuid.uuid4())
        flow_config = {
            "routers": [{
                "listenTo": "sp0",
                "routes": {"route_a": [{"id": task_id}]},  # No crewId
                "routeConditions": {}
            }]
        }
        task_repo = MagicMock()
        crew_repo = MagicMock()

        result = await FlowProcessorManager.process_routers(
            flow_config=flow_config,
            all_tasks={},
            repositories={"task": task_repo, "crew": crew_repo}
        )
        assert result == []

    @pytest.mark.asyncio
    async def test_successful_router(self):
        crew_id = str(uuid.uuid4())
        task_id = str(uuid.uuid4())
        agent_id = str(uuid.uuid4())

        flow_config = {
            "routers": [{
                "listenTo": "starting_point_0",
                "routes": {
                    "route_a": [{"id": task_id, "crewId": crew_id}]
                },
                "routeConditions": {"route_a": "result == 'A'"}
            }]
        }

        task_data = SimpleNamespace(
            id=task_id, name="Router Task", description="desc", expected_output="output",
            agent_id=agent_id, tools=[], tool_configs={}, async_execution=False,
            config={}, memory=False, markdown=False, guardrail=None
        )
        task_repo = MagicMock()
        task_repo.get = AsyncMock(return_value=task_data)

        agent_data = MagicMock()
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(return_value=agent_data)

        crew_data = MagicMock()
        crew_data.name = "Router Crew"
        crew_data.nodes = []
        crew_data.edges = []
        crew_data.tool_configs = {}
        crew_repo = MagicMock()
        crew_repo.get = AsyncMock(return_value=crew_data)

        mock_agent_obj = MagicMock()
        mock_task_obj = MagicMock()

        with patch("src.engines.crewai.paths.flow.modules.agent_adapter.AgentConfig.configure_agent_and_tools",
                   new=AsyncMock(return_value=mock_agent_obj)), \
             patch("src.engines.crewai.paths.flow.modules.task_adapter.TaskConfig.configure_task",
                   new=AsyncMock(return_value=mock_task_obj)):
            result = await FlowProcessorManager.process_routers(
                flow_config=flow_config,
                all_tasks={},
                repositories={"task": task_repo, "crew": crew_repo, "agent": agent_repo}
            )

        assert len(result) == 1
        assert result[0][0] == "router_0"
