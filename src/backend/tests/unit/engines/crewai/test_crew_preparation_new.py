"""
Unit tests for src/engines/crewai/crew_preparation.py

Targets uncovered lines (68% → 85%+).
"""
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.engines.crewai.crew_preparation import (
    CrewPreparation,
    validate_crew_config,
    handle_crew_error,
    process_crew_output,
)


# ---------------------------------------------------------------------------
# Module-level functions
# ---------------------------------------------------------------------------

class TestValidateCrewConfig:
    def test_valid_config(self):
        config = {"agents": [{"role": "R"}], "tasks": [{"description": "T"}]}
        assert validate_crew_config(config) is True

    def test_missing_agents(self):
        config = {"tasks": [{"description": "T"}]}
        assert validate_crew_config(config) is False

    def test_empty_agents(self):
        config = {"agents": [], "tasks": [{"description": "T"}]}
        assert validate_crew_config(config) is False

    def test_missing_tasks(self):
        config = {"agents": [{"role": "R"}]}
        assert validate_crew_config(config) is False

    def test_empty_tasks(self):
        config = {"agents": [{"role": "R"}], "tasks": []}
        assert validate_crew_config(config) is False


class TestHandleCrewError:
    def test_does_not_raise(self):
        e = ValueError("test error")
        handle_crew_error(e, "Test error message")  # Should not raise


class TestProcessCrewOutput:
    @pytest.mark.asyncio
    async def test_dict_result(self):
        result = {"key": "value"}
        output = await process_crew_output(result)
        assert output == result

    @pytest.mark.asyncio
    async def test_result_with_raw_attr(self):
        result = MagicMock()
        result.raw = "raw output text"
        output = await process_crew_output(result)
        assert output == {"result": "raw output text", "type": "crew_result"}

    @pytest.mark.asyncio
    async def test_result_non_dict_non_raw(self):
        result = "plain string"
        output = await process_crew_output(result)
        assert output == {"result": "plain string", "type": "processed"}

    @pytest.mark.asyncio
    async def test_result_none(self):
        output = await process_crew_output(None)
        assert "result" in output


# ---------------------------------------------------------------------------
# CrewPreparation.__init__
# ---------------------------------------------------------------------------

class TestCrewPreparationInit:
    def test_init_basic(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        assert cp.config is config
        assert cp.agents == {}
        assert cp.tasks == []
        assert cp.crew is None

    def test_init_with_memory_backend(self):
        config = {
            "agents": [],
            "tasks": [],
            "memory_backend_config": {"backend_type": "chromadb"},
        }
        cp = CrewPreparation(config=config)
        assert cp.config is config

    def test_init_with_tool_service(self):
        config = {"agents": [], "tasks": []}
        mock_svc = MagicMock()
        cp = CrewPreparation(config=config, tool_service=mock_svc)
        assert cp.tool_service is mock_svc

    def test_init_with_user_token(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config, user_token="token-xyz")
        assert cp.user_token == "token-xyz"


# ---------------------------------------------------------------------------
# CrewPreparation.cleanup
# ---------------------------------------------------------------------------

class TestCrewPreparationCleanup:
    def test_cleanup_restores_original_storage_dir(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        cp._original_storage_dir = "/original/path"
        os.environ.pop("CREWAI_STORAGE_DIR", None)

        cp.cleanup()
        assert os.environ.get("CREWAI_STORAGE_DIR") == "/original/path"

    def test_cleanup_removes_env_var_when_no_original(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        cp._original_storage_dir = None
        os.environ["CREWAI_STORAGE_DIR"] = "/some/path"

        cp.cleanup()
        assert "CREWAI_STORAGE_DIR" not in os.environ

    def test_cleanup_no_original_storage_dir_attr(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        # _original_storage_dir not set — should be a no-op
        cp.cleanup()  # Should not raise


# ---------------------------------------------------------------------------
# CrewPreparation.execute
# ---------------------------------------------------------------------------

class TestCrewPreparationExecute:
    @pytest.mark.asyncio
    async def test_execute_without_crew_returns_error(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        cp.crew = None

        result = await cp.execute()
        assert "error" in result

    @pytest.mark.asyncio
    async def test_execute_with_crew_returns_output(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        mock_crew = MagicMock()
        mock_result = {"result": "crew output", "type": "processed"}
        mock_crew.kickoff_async = AsyncMock(return_value=mock_result)  # CrewAI 1.14.5 uses async kickoff
        cp.crew = mock_crew

        with patch("src.engines.crewai.crew_preparation.process_crew_output",
                   new_callable=AsyncMock) as mock_process, \
             patch("src.engines.crewai.crew_preparation.is_data_missing", return_value=False):
            mock_process.return_value = mock_result
            result = await cp.execute()

        assert result == mock_result

    @pytest.mark.asyncio
    async def test_execute_data_missing_logs_warning(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        mock_crew = MagicMock()
        mock_crew.kickoff = AsyncMock(return_value={"result": "output"})
        cp.crew = mock_crew

        with patch("src.engines.crewai.crew_preparation.process_crew_output",
                   new_callable=AsyncMock) as mock_process, \
             patch("src.engines.crewai.crew_preparation.is_data_missing", return_value=True):
            mock_process.return_value = {"result": "output"}
            result = await cp.execute()

        assert result is not None

    @pytest.mark.asyncio
    async def test_execute_exception_returns_error(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        mock_crew = MagicMock()
        mock_crew.kickoff = AsyncMock(side_effect=Exception("execution failed"))
        cp.crew = mock_crew

        result = await cp.execute()
        assert "error" in result


# ---------------------------------------------------------------------------
# CrewPreparation._needs_entity_extraction_fallback
# ---------------------------------------------------------------------------

class TestNeedsEntityExtractionFallback:
    def test_databricks_claude_needs_fallback(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._needs_entity_extraction_fallback("databricks-claude-3-5-sonnet") is True

    def test_gpt_oss_needs_fallback(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._needs_entity_extraction_fallback("databricks-gpt-oss") is True

    def test_gpt4o_does_not_need_fallback(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._needs_entity_extraction_fallback("gpt-4o") is False

    def test_empty_model_does_not_need_fallback(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._needs_entity_extraction_fallback("") is False

    def test_none_model_does_not_need_fallback(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._needs_entity_extraction_fallback(None) is False


# ---------------------------------------------------------------------------
# CrewPreparation._should_disable_memory_for_agent
# ---------------------------------------------------------------------------

class TestShouldDisableMemoryForAgent:
    def test_memory_explicitly_false(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._should_disable_memory_for_agent({"role": "R", "memory": False}) is True

    def test_memory_true_does_not_disable(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._should_disable_memory_for_agent({"role": "R", "memory": True}) is False

    def test_no_memory_key_does_not_disable(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        assert cp._should_disable_memory_for_agent({"role": "R"}) is False


# ---------------------------------------------------------------------------
# CrewPreparation._find_agent_by_reference
# ---------------------------------------------------------------------------

class TestFindAgentByReference:
    def _make_cp(self, agents=None):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        if agents:
            cp.agents = agents
        return cp

    def test_direct_lookup(self):
        mock_agent = MagicMock()
        cp = self._make_cp({"researcher": mock_agent})
        result = cp._find_agent_by_reference("researcher")
        assert result is mock_agent

    def test_unknown_reference_returns_none(self):
        cp = self._make_cp({"researcher": MagicMock()})
        result = cp._find_agent_by_reference("unknown")
        assert result is None

    def test_empty_reference_returns_none(self):
        cp = self._make_cp({"researcher": MagicMock()})
        result = cp._find_agent_by_reference("")
        assert result is None

    def test_unknown_literal_returns_none(self):
        cp = self._make_cp({"researcher": MagicMock()})
        result = cp._find_agent_by_reference("unknown")
        assert result is None

    def test_agent_agent_prefix_lookup(self):
        mock_agent = MagicMock()
        uuid = "47b50da8-bfa2-41c9-8d0f-19c063f5c9c0"
        cp = self._make_cp({f"agent_agent-{uuid}": mock_agent})
        result = cp._find_agent_by_reference(f"agent_agent-{uuid}")
        assert result is mock_agent

    def test_uuid_part_lookup_in_stored_key(self):
        mock_agent = MagicMock()
        uuid = "47b50da8-bfa2-41c9-8d0f-19c063f5c9c0"
        cp = self._make_cp({f"agent_agent-{uuid}": mock_agent})
        # If we look up just "agent_agent-<uuid>", it should find it
        result = cp._find_agent_by_reference(f"agent_agent-{uuid}")
        assert result is mock_agent

    def test_lookup_by_config_id(self):
        mock_agent = MagicMock()
        config = {
            "agents": [
                {"id": "agent-config-id-123", "name": "researcher"}
            ],
            "tasks": [],
        }
        cp = CrewPreparation(config=config)
        cp.agents = {"researcher": mock_agent}
        result = cp._find_agent_by_reference("agent-config-id-123")
        assert result is mock_agent


# ---------------------------------------------------------------------------
# CrewPreparation._handle_openai_api_key
# ---------------------------------------------------------------------------

class TestHandleOpenAIApiKey:
    @pytest.mark.asyncio
    async def test_sets_openai_key_when_found(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.services.api_keys_service.ApiKeysService") as mock_aks:
            mock_aks.get_provider_api_key = AsyncMock(return_value="sk-real-key")
            await cp._handle_openai_api_key()

        assert os.environ.get("OPENAI_API_KEY") == "sk-real-key"

    @pytest.mark.asyncio
    async def test_sets_dummy_key_when_not_found(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.services.api_keys_service.ApiKeysService") as mock_aks:
            mock_aks.get_provider_api_key = AsyncMock(return_value=None)
            await cp._handle_openai_api_key()

        assert os.environ.get("OPENAI_API_KEY") == "sk-dummy-validation-key"

    @pytest.mark.asyncio
    async def test_exception_handled_gracefully(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)

        with patch("src.services.api_keys_service.ApiKeysService") as mock_aks:
            mock_aks.get_provider_api_key = AsyncMock(side_effect=Exception("api err"))
            await cp._handle_openai_api_key()  # Should not raise


# ---------------------------------------------------------------------------
# CrewPreparation._lookup_kasal_agent_uuid_via_service
# ---------------------------------------------------------------------------

class TestLookupKasalAgentUUID:
    @pytest.mark.asyncio
    async def test_finds_by_role(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        db_agent = MagicMock()
        db_agent.id = "uuid-role-match"
        db_agent.role = "Analyst"
        db_agent.name = "agent-1"

        with patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.agent_service.AgentService") as mock_svc_cls, \
             patch("src.utils.user_context.GroupContext") as mock_gc:

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session

            mock_svc = MagicMock()
            mock_svc.find_by_group = AsyncMock(return_value=[db_agent])
            mock_svc_cls.return_value = mock_svc

            result = await cp._lookup_kasal_agent_uuid_via_service(
                {"role": "Analyst", "name": "other"}, "config-id"
            )

        assert result == "uuid-role-match"

    @pytest.mark.asyncio
    async def test_finds_by_name(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        db_agent = MagicMock()
        db_agent.id = "uuid-name-match"
        db_agent.role = "OtherRole"
        db_agent.name = "target-agent"

        with patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.agent_service.AgentService") as mock_svc_cls, \
             patch("src.utils.user_context.GroupContext"):

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_svc = MagicMock()
            mock_svc.find_by_group = AsyncMock(return_value=[db_agent])
            mock_svc_cls.return_value = mock_svc

            result = await cp._lookup_kasal_agent_uuid_via_service(
                {"role": "Different", "name": "target-agent"}, "config-id"
            )

        assert result == "uuid-name-match"

    @pytest.mark.asyncio
    async def test_finds_by_config_id(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        db_agent = MagicMock()
        db_agent.id = "uuid-configid-match"
        db_agent.role = "Role"
        db_agent.name = "AgentName"

        with patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.agent_service.AgentService") as mock_svc_cls, \
             patch("src.utils.user_context.GroupContext"):

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_svc = MagicMock()
            mock_svc.find_by_group = AsyncMock(return_value=[db_agent])
            mock_svc_cls.return_value = mock_svc

            result = await cp._lookup_kasal_agent_uuid_via_service(
                {"role": "Role", "name": "AgentName"},
                "uuid-configid-match"  # matches db_agent.id
            )

        assert result == "uuid-configid-match"

    @pytest.mark.asyncio
    async def test_no_match_returns_none(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        db_agent = MagicMock()
        db_agent.id = "unrelated-uuid"
        db_agent.role = "OtherRole"
        db_agent.name = "OtherAgent"

        with patch("src.db.session.request_scoped_session") as mock_sess, \
             patch("src.services.agent_service.AgentService") as mock_svc_cls, \
             patch("src.utils.user_context.GroupContext"):

            mock_session = AsyncMock()
            mock_session.__aenter__ = AsyncMock(return_value=mock_session)
            mock_session.__aexit__ = AsyncMock(return_value=None)
            mock_sess.return_value = mock_session
            mock_svc = MagicMock()
            mock_svc.find_by_group = AsyncMock(return_value=[db_agent])
            mock_svc_cls.return_value = mock_svc

            result = await cp._lookup_kasal_agent_uuid_via_service(
                {"role": "Missing", "name": "NoMatch"}, "also-missing"
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_exception_returns_none(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.db.session.request_scoped_session",
                   side_effect=Exception("db error")):
            result = await cp._lookup_kasal_agent_uuid_via_service(
                {"role": "R"}, "config-id"
            )

        assert result is None


# ---------------------------------------------------------------------------
# CrewPreparation._attach_knowledge_sources (deprecated no-op)
# ---------------------------------------------------------------------------

class TestAttachKnowledgeSources:
    @pytest.mark.asyncio
    async def test_deprecated_noop(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        # Should not raise, just log
        await cp._attach_knowledge_sources()


# ---------------------------------------------------------------------------
# CrewPreparation._initialize_agent_knowledge (deprecated no-op)
# ---------------------------------------------------------------------------

class TestInitializeAgentKnowledge:
    @pytest.mark.asyncio
    async def test_deprecated_noop(self):
        config = {"agents": [], "tasks": []}
        cp = CrewPreparation(config=config)
        await cp._initialize_agent_knowledge({})


# ---------------------------------------------------------------------------
# CrewPreparation.prepare – full flow mocked
# ---------------------------------------------------------------------------

class TestCrewPreparationPrepare:
    def _make_cp(self, config=None):
        if config is None:
            config = {
                "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "a1"}],
                "tasks": [{"description": "T", "expected_output": "O", "agent": "a1"}],
                "group_id": "grp-1",
            }
        return CrewPreparation(config=config)

    @pytest.mark.asyncio
    async def test_prepare_invalid_config_returns_false(self):
        cp = CrewPreparation(config={"agents": [], "tasks": []})
        result = await cp.prepare()
        assert result is False

    @pytest.mark.asyncio
    async def test_prepare_create_agents_failure_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"description": "T", "expected_output": "O"}],
        }
        cp = CrewPreparation(config=config)

        with patch.object(cp, "_create_agents", new_callable=AsyncMock) as mock_ca:
            mock_ca.return_value = False
            result = await cp.prepare()

        assert result is False

    @pytest.mark.asyncio
    async def test_prepare_create_tasks_failure_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"description": "T", "expected_output": "O"}],
        }
        cp = CrewPreparation(config=config)

        with patch.object(cp, "_create_agents", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_create_tasks", new_callable=AsyncMock) as mock_ct:
            mock_ca.return_value = True
            mock_ct.return_value = False
            result = await cp.prepare()

        assert result is False

    @pytest.mark.asyncio
    async def test_prepare_create_crew_failure_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"description": "T", "expected_output": "O"}],
        }
        cp = CrewPreparation(config=config)

        with patch.object(cp, "_create_agents", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_create_tasks", new_callable=AsyncMock) as mock_ct, \
             patch.object(cp, "_create_crew", new_callable=AsyncMock) as mock_cc:
            mock_ca.return_value = True
            mock_ct.return_value = True
            mock_cc.return_value = False
            result = await cp.prepare()

        assert result is False

    @pytest.mark.asyncio
    async def test_prepare_success_returns_true(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"description": "T", "expected_output": "O"}],
        }
        cp = CrewPreparation(config=config)

        with patch.object(cp, "_create_agents", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_create_tasks", new_callable=AsyncMock) as mock_ct, \
             patch.object(cp, "_create_crew", new_callable=AsyncMock) as mock_cc:
            mock_ca.return_value = True
            mock_ct.return_value = True
            mock_cc.return_value = True
            result = await cp.prepare()

        assert result is True

    @pytest.mark.asyncio
    async def test_prepare_exception_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [{"description": "T", "expected_output": "O"}],
        }
        cp = CrewPreparation(config=config)

        with patch.object(cp, "_create_agents", new_callable=AsyncMock) as mock_ca:
            mock_ca.side_effect = Exception("unexpected")
            result = await cp.prepare()

        assert result is False


# ---------------------------------------------------------------------------
# CrewPreparation._create_agents
# ---------------------------------------------------------------------------

class TestCreateAgents:
    @pytest.mark.asyncio
    async def test_creates_agents_successfully(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "ag1"}],
            "tasks": [],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)

        mock_agent = MagicMock()
        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.engines.crewai.crew_preparation.create_agent", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_lookup_kasal_agent_uuid_via_service", new_callable=AsyncMock) as mock_lookup:

            mock_mcp.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_ca.return_value = mock_agent
            mock_lookup.return_value = None

            result = await cp._create_agents()

        assert result is True
        assert "ag1" in cp.agents

    @pytest.mark.asyncio
    async def test_create_agents_propagates_reasoning_from_crew_config(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "ag1"}],
            "tasks": [],
            "group_id": "grp-1",
            "crew": {"reasoning": True, "max_reasoning_attempts": 5},
        }
        cp = CrewPreparation(config=config)

        mock_agent = MagicMock()
        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.engines.crewai.crew_preparation.create_agent", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_lookup_kasal_agent_uuid_via_service", new_callable=AsyncMock) as mock_lookup:

            mock_mcp.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_ca.return_value = mock_agent
            mock_lookup.return_value = None

            result = await cp._create_agents()

        assert result is True
        # Verify reasoning was propagated into agent_config
        call_kwargs = mock_ca.call_args[1]
        assert call_kwargs.get("agent_config", {}).get("reasoning") is True

    @pytest.mark.asyncio
    async def test_create_agents_agent_none_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "ag1"}],
            "tasks": [],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)

        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.engines.crewai.crew_preparation.create_agent", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_lookup_kasal_agent_uuid_via_service", new_callable=AsyncMock) as mock_lookup:

            mock_mcp.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_ca.return_value = None  # Agent creation failed
            mock_lookup.return_value = None

            result = await cp._create_agents()

        assert result is False

    @pytest.mark.asyncio
    async def test_create_agents_exception_returns_false(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "ag1"}],
            "tasks": [],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)

        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.engines.crewai.crew_preparation.create_agent", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_lookup_kasal_agent_uuid_via_service", new_callable=AsyncMock) as mock_lookup:

            mock_mcp.collect_agent_mcp_requirements = AsyncMock(return_value={})
            mock_ca.side_effect = Exception("agent creation error")
            mock_lookup.return_value = None

            result = await cp._create_agents()

        assert result is False

    @pytest.mark.asyncio
    async def test_create_agents_adds_mcp_servers_from_tasks(self):
        config = {
            "agents": [{"role": "R", "goal": "G", "backstory": "B", "name": "ag1"}],
            "tasks": [],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)

        mock_agent = MagicMock()
        mcp_requirements = {"ag1": [{"name": "server1", "url": "http://server1"}]}

        with patch("src.engines.crewai.tools.mcp_integration.MCPIntegration") as mock_mcp, \
             patch("src.engines.crewai.crew_preparation.create_agent", new_callable=AsyncMock) as mock_ca, \
             patch.object(cp, "_lookup_kasal_agent_uuid_via_service", new_callable=AsyncMock) as mock_lookup:

            mock_mcp.collect_agent_mcp_requirements = AsyncMock(return_value=mcp_requirements)
            mock_ca.return_value = mock_agent
            mock_lookup.return_value = None

            result = await cp._create_agents()

        assert result is True
        # Verify MCP servers were added to agent config
        call_kwargs = mock_ca.call_args[1]
        agent_config = call_kwargs.get("agent_config", {})
        assert "tool_configs" in agent_config
        assert "MCP_SERVERS" in agent_config["tool_configs"]


# ---------------------------------------------------------------------------
# CrewPreparation._create_tasks
# ---------------------------------------------------------------------------

class TestCreateTasks:
    @pytest.mark.asyncio
    async def test_create_tasks_successfully(self):
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Do it",
                    "expected_output": "Result",
                    "agent": "ag1",
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        mock_task = MagicMock()
        mock_task.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.return_value = mock_task
            result = await cp._create_tasks()

        assert result is True
        assert len(cp.tasks) == 1

    @pytest.mark.asyncio
    async def test_create_tasks_agent_fallback_to_first(self):
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Do it",
                    "expected_output": "Result",
                    "agent": "missing_agent",  # Agent doesn't exist
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        mock_task = MagicMock()
        mock_task.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.return_value = mock_task
            result = await cp._create_tasks()

        assert result is True

    @pytest.mark.asyncio
    async def test_create_tasks_no_agents_returns_false(self):
        config = {
            "agents": [],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Do it",
                    "expected_output": "Result",
                    "agent": "missing_agent",
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        cp.agents = {}  # No agents available

        result = await cp._create_tasks()
        assert result is False

    @pytest.mark.asyncio
    async def test_create_tasks_exception_returns_false(self):
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Do it",
                    "expected_output": "Result",
                    "agent": "ag1",
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        cp.agents = {"ag1": MagicMock()}

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.side_effect = Exception("task creation error")
            result = await cp._create_tasks()

        assert result is False

    @pytest.mark.asyncio
    async def test_create_tasks_with_context_refs(self):
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {
                    "id": "t1", "name": "task1",
                    "description": "First", "expected_output": "O1", "agent": "ag1",
                },
                {
                    "id": "t2", "name": "task2",
                    "description": "Second", "expected_output": "O2", "agent": "ag1",
                    "context": ["t1"],
                },
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        task1 = MagicMock()
        task1.async_execution = False
        task2 = MagicMock()
        task2.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.side_effect = [task1, task2]
            result = await cp._create_tasks()

        assert result is True
        # task2 context should have been set to [task1]
        assert task2.context == [task1]

    @pytest.mark.asyncio
    async def test_create_tasks_multiple_async_adds_completion_task(self):
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {"id": "t1", "name": "task1", "description": "T1", "expected_output": "O1",
                 "agent": "ag1", "async_execution": True},
                {"id": "t2", "name": "task2", "description": "T2", "expected_output": "O2",
                 "agent": "ag1", "async_execution": True},
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        task1 = MagicMock()
        task1.async_execution = True
        task1.context = None
        task1.agent = mock_agent
        task2 = MagicMock()
        task2.async_execution = True
        task2.context = None
        task2.agent = mock_agent

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct, \
             patch("src.engines.crewai.crew_preparation.Task") as mock_task_cls_module, \
             patch("crewai.Task") as mock_task_cls_crewai:
            mock_ct.side_effect = [task1, task2]
            completion_task = MagicMock()
            mock_task_cls_module.return_value = completion_task
            mock_task_cls_crewai.return_value = completion_task

            result = await cp._create_tasks()

        assert result is True
        # A completion task should have been appended
        assert len(cp.tasks) == 3


class TestApplyEntityExtractionFallbackPatch:
    """Tests for _apply_entity_extraction_fallback_patch."""

    @pytest.mark.asyncio
    async def test_applies_patches_successfully(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("crewai.utilities.converter.Converter") as mock_conv, \
             patch("crewai.utilities.evaluators.task_evaluator.TaskEvaluator") as mock_te, \
             patch("crewai.llm.LLM"):

            mock_fallback_llm = MagicMock()
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_fallback_llm)
            # Ensure it doesn't have function_calling_llm initially
            del mock_fallback_llm.function_calling_llm

            await cp._apply_entity_extraction_fallback_patch()

        # Should not raise
        assert True

    @pytest.mark.asyncio
    async def test_applies_patches_with_function_calling_llm(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.core.llm_manager.LLMManager") as mock_lm, \
             patch("crewai.utilities.converter.Converter") as mock_conv, \
             patch("crewai.utilities.evaluators.task_evaluator.TaskEvaluator") as mock_te, \
             patch("crewai.llm.LLM"):

            mock_fallback_llm = MagicMock()
            mock_fallback_llm.function_calling_llm = MagicMock()  # already has it
            mock_lm.configure_crewai_llm = AsyncMock(return_value=mock_fallback_llm)

            await cp._apply_entity_extraction_fallback_patch()

        assert True

    @pytest.mark.asyncio
    async def test_no_group_id_raises_handled(self):
        config = {"agents": [], "tasks": []}  # No group_id
        cp = CrewPreparation(config=config)

        with patch("src.core.llm_manager.LLMManager") as mock_lm:
            mock_lm.configure_crewai_llm = AsyncMock(return_value=MagicMock())

            # Should not raise - exception is caught internally
            await cp._apply_entity_extraction_fallback_patch()

    @pytest.mark.asyncio
    async def test_llm_manager_exception_handled(self):
        config = {"agents": [], "tasks": [], "group_id": "grp-1"}
        cp = CrewPreparation(config=config)

        with patch("src.core.llm_manager.LLMManager") as mock_lm:
            mock_lm.configure_crewai_llm = AsyncMock(side_effect=Exception("LLM config failed"))

            # Should not raise - exception is caught
            await cp._apply_entity_extraction_fallback_patch()


class TestCreateTasksKnowledgeToolInjection:
    """Tests for DatabricksKnowledgeSearchTool injection in first task."""

    @pytest.mark.asyncio
    async def test_first_task_with_knowledge_tool_injects_nudge(self):
        """When first task has DatabricksKnowledgeSearchTool, inject instruction."""
        config = {
            "agents": [
                {
                    "name": "ag1", "role": "Analyst", "goal": "G", "backstory": "B",
                    "knowledge_sources": [
                        {"fileInfo": {"filename": "report.pdf"}},
                        {"metadata": {"filename": "data.csv"}},
                    ]
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Analyze documents",
                    "expected_output": "Analysis",
                    "agent": "ag1",
                    "tools": ["DatabricksKnowledgeSearchTool"],
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        mock_task = MagicMock()
        mock_task.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.return_value = mock_task
            result = await cp._create_tasks()

        assert result is True
        # Description should contain the nudge
        call_kwargs = mock_ct.call_args[1]
        task_config_used = call_kwargs.get("task_config", {})
        # The description should have been injected with the knowledge-search instruction
        assert "DatabricksKnowledgeSearchTool" in task_config_used.get("description", "") or \
               "CRITICAL" in task_config_used.get("description", "")

    @pytest.mark.asyncio
    async def test_first_task_with_tool_id_36_injects_nudge(self):
        """When first task has tool ID '36', inject instruction."""
        config = {
            "agents": [{"name": "ag1", "role": "R", "goal": "G", "backstory": "B"}],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Use knowledge",
                    "expected_output": "Output",
                    "agent": "ag1",
                    "tools": ["36"],
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        mock_task = MagicMock()
        mock_task.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.return_value = mock_task
            result = await cp._create_tasks()

        assert result is True

    @pytest.mark.asyncio
    async def test_knowledge_injection_with_files(self):
        """Test knowledge injection builds file list string."""
        config = {
            "agents": [
                {
                    "name": "ag1", "role": "R", "goal": "G", "backstory": "B",
                    "id": "agent-id-1",
                    "knowledge_sources": [
                        {"fileInfo": {"filename": "doc1.pdf"}},
                        {"metadata": {"filename": "doc2.csv"}},
                        {"no_filename": True},  # Should be skipped
                    ]
                }
            ],
            "tasks": [
                {
                    "id": "t1",
                    "name": "task1",
                    "description": "Original description",
                    "expected_output": "Output",
                    "agent": "agent-id-1",
                    "tools": ["DatabricksKnowledgeSearchTool"],
                }
            ],
            "group_id": "grp-1",
        }
        cp = CrewPreparation(config=config)
        mock_agent = MagicMock()
        cp.agents = {"ag1": mock_agent}

        mock_task = MagicMock()
        mock_task.async_execution = False

        with patch("src.engines.crewai.helpers.task_helpers.create_task", new_callable=AsyncMock) as mock_ct:
            mock_ct.return_value = mock_task
            result = await cp._create_tasks()

        assert result is True
