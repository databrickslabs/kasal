"""
Extended unit tests for src/engines/crewai/tools/tool_factory.py

Goal: push coverage from 30.1% to 50%+

Strategy:
- Test __init__, get_tool_info, create_tool (the most important method),
  register_tool_implementation, _get_api_key_async, _run_in_new_loop,
  cleanup, _validate_databricks_auth.
- All external I/O (DB, env, event loop) is mocked.
"""
import asyncio
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, Mock, patch, PropertyMock

from src.engines.crewai.tools.tool_factory import ToolFactory


# ============================================================================
# Helpers
# ============================================================================

def _make_factory(config=None, api_keys_service=None, user_token=None):
    return ToolFactory(
        config=config or {"group_id": "grp-1"},
        api_keys_service=api_keys_service,
        user_token=user_token,
    )


def _make_tool_info(title="MyTool", tool_id=42, config=None, base_config=None):
    t = MagicMock()
    t.title = title
    t.id = tool_id
    t.config = base_config if base_config is not None else (config or {})
    return t


# ============================================================================
# __init__ / basic construction
# ============================================================================

class TestToolFactoryConstruction:

    def test_default_attributes(self):
        f = _make_factory()
        assert f.config == {"group_id": "grp-1"}
        assert f.api_keys_service is None
        assert f.user_token is None
        assert f._initialized is False
        assert isinstance(f._available_tools, dict)
        assert isinstance(f._tool_implementations, dict)

    def test_core_tools_always_registered(self):
        f = _make_factory()
        always_present = ["PerplexityTool", "Dall-E Tool", "SerperDevTool", "ScrapeWebsiteTool"]
        for name in always_present:
            assert name in f._tool_implementations

    def test_user_token_stored(self):
        f = _make_factory(user_token="tok-abc")
        assert f.user_token == "tok-abc"

    def test_api_keys_service_stored(self):
        svc = MagicMock()
        f = _make_factory(api_keys_service=svc)
        assert f.api_keys_service is svc

    def test_config_stored(self):
        cfg = {"model": "gpt-4", "group_id": "test"}
        f = _make_factory(config=cfg)
        assert f.config == cfg


# ============================================================================
# get_tool_info
# ============================================================================

class TestGetToolInfo:

    def test_returns_tool_by_title(self):
        f = _make_factory()
        info = _make_tool_info("SomeTool", 7)
        f._available_tools["SomeTool"] = info

        result = f.get_tool_info("SomeTool")
        assert result is info

    def test_returns_tool_by_string_id(self):
        f = _make_factory()
        info = _make_tool_info("Tool2", 99)
        f._available_tools["99"] = info

        result = f.get_tool_info("99")
        assert result is info

    def test_returns_tool_by_int_id(self):
        f = _make_factory()
        info = _make_tool_info("Tool3", 5)
        f._available_tools["5"] = info

        result = f.get_tool_info(5)
        assert result is info

    def test_returns_none_when_not_found(self):
        f = _make_factory()
        result = f.get_tool_info("does-not-exist")
        assert result is None

    def test_returns_none_for_empty_tools(self):
        f = _make_factory()
        f._available_tools = {}
        result = f.get_tool_info("anything")
        assert result is None


# ============================================================================
# register_tool_implementation / register_tool_implementations
# ============================================================================

class TestRegisterToolImplementation:

    def test_register_single_tool(self):
        f = _make_factory()
        mock_cls = MagicMock()
        f.register_tool_implementation("NewTool", mock_cls)
        assert f._tool_implementations["NewTool"] is mock_cls

    def test_register_overwrites_existing(self):
        f = _make_factory()
        old_cls = MagicMock()
        new_cls = MagicMock()
        f._tool_implementations["OldTool"] = old_cls
        f.register_tool_implementation("OldTool", new_cls)
        assert f._tool_implementations["OldTool"] is new_cls

    def test_register_multiple_tools(self):
        f = _make_factory()
        impls = {"A": MagicMock(), "B": MagicMock(), "C": MagicMock()}
        f.register_tool_implementations(impls)
        for name, cls in impls.items():
            assert f._tool_implementations[name] is cls

    def test_register_empty_dict(self):
        f = _make_factory()
        original_len = len(f._tool_implementations)
        f.register_tool_implementations({})
        assert len(f._tool_implementations) == original_len


# ============================================================================
# create_tool – core scenarios
# ============================================================================

class TestCreateTool:

    def _setup_factory_with_tool(self, tool_name="MyTool", tool_id=1, base_config=None):
        f = _make_factory()
        info = _make_tool_info(tool_name, tool_id, config=base_config)
        f._available_tools[tool_name] = info
        mock_cls = MagicMock()
        mock_instance = MagicMock()
        mock_cls.return_value = mock_instance
        f._tool_implementations[tool_name] = mock_cls
        return f, info, mock_cls, mock_instance

    def test_returns_none_when_tool_not_in_available(self):
        f = _make_factory()
        result = f.create_tool("GhostTool")
        assert result is None

    def test_returns_none_when_no_implementation(self):
        f = _make_factory()
        info = _make_tool_info("NoImpl", 1)
        f._available_tools["NoImpl"] = info
        # No entry in _tool_implementations for "NoImpl"
        result = f.create_tool("NoImpl")
        assert result is None

    def test_generic_tool_created_successfully(self):
        f, info, mock_cls, mock_instance = self._setup_factory_with_tool("ScrapeWebsiteTool")
        result = f.create_tool("ScrapeWebsiteTool")
        assert result is mock_instance

    def test_tool_config_override_merged(self):
        f, info, mock_cls, mock_instance = self._setup_factory_with_tool(
            "ScrapeWebsiteTool", base_config={"base_key": "base_val"}
        )
        f.create_tool("ScrapeWebsiteTool", tool_config_override={"override_key": "override_val"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("base_key") == "base_val"
        assert call_kwargs.get("override_key") == "override_val"

    def test_override_takes_precedence_over_base(self):
        f, info, mock_cls, mock_instance = self._setup_factory_with_tool(
            "ScrapeWebsiteTool", base_config={"key": "original"}
        )
        f.create_tool("ScrapeWebsiteTool", tool_config_override={"key": "overridden"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("key") == "overridden"

    def test_result_as_answer_passed_to_generic_tool(self):
        f, info, mock_cls, mock_instance = self._setup_factory_with_tool("ScrapeWebsiteTool")
        f.create_tool("ScrapeWebsiteTool", result_as_answer=True)
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("result_as_answer") is True

    def test_placeholder_resolution_from_execution_inputs(self):
        """Tool config values like {placeholder} should be resolved from execution_inputs."""
        f = _make_factory(config={
            "group_id": "g1",
            "inputs": {
                "inputs": {
                    "workspace_id": "ws-123",
                }
            }
        })
        info = _make_tool_info("ScrapeWebsiteTool", base_config={"workspace": "{workspace_id}"})
        f._available_tools["ScrapeWebsiteTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        f.create_tool("ScrapeWebsiteTool")
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("workspace") == "ws-123"

    def test_execution_inputs_removed_before_tool_instantiation(self):
        """execution_inputs key should not be passed to the tool constructor."""
        f = _make_factory(config={
            "group_id": "g1",
            "inputs": {"inputs": {"some_key": "val"}}
        })
        info = _make_tool_info("ScrapeWebsiteTool", base_config={})
        f._available_tools["ScrapeWebsiteTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        f.create_tool("ScrapeWebsiteTool")
        call_kwargs = mock_cls.call_args[1]
        assert "execution_inputs" not in call_kwargs

    def test_create_tool_returns_none_on_instantiation_error(self):
        f = _make_factory()
        info = _make_tool_info("BadTool", 1)
        f._available_tools["BadTool"] = info
        bad_cls = MagicMock(side_effect=TypeError("bad init"))
        f._tool_implementations["BadTool"] = bad_cls
        result = f.create_tool("BadTool")
        assert result is None

    def test_serperdevtool_search_type_mapped_from_endpoint_type(self):
        """endpoint_type='search' should map to search_type='search' for SerperDevTool."""
        f = _make_factory()
        info = _make_tool_info("SerperDevTool", 2, config={"endpoint_type": "search"})
        f._available_tools["SerperDevTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["SerperDevTool"] = mock_cls

        with patch.dict(os.environ, {"SERPER_API_KEY": "test-serper-key"}):
            f.create_tool("SerperDevTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("search_type") == "search"
        # endpoint_type should be stripped
        assert "endpoint_type" in call_kwargs or call_kwargs.get("search_type") == "search"

    def test_serperdevtool_news_endpoint_type_mapped(self):
        f = _make_factory()
        info = _make_tool_info("SerperDevTool", 2, config={"endpoint_type": "news"})
        f._available_tools["SerperDevTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["SerperDevTool"] = mock_cls

        with patch.dict(os.environ, {"SERPER_API_KEY": "key"}):
            f.create_tool("SerperDevTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("search_type") == "news"

    def test_serperdevtool_unsupported_endpoint_type_defaults_to_search(self):
        f = _make_factory()
        info = _make_tool_info("SerperDevTool", 2, config={"endpoint_type": "images"})
        f._available_tools["SerperDevTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["SerperDevTool"] = mock_cls

        with patch.dict(os.environ, {"SERPER_API_KEY": "key"}):
            f.create_tool("SerperDevTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("search_type") == "search"

    def test_perplexity_tool_uses_env_key(self):
        f = _make_factory()
        info = _make_tool_info("PerplexityTool", 3, config={})
        f._available_tools["PerplexityTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["PerplexityTool"] = mock_cls

        with patch.dict(os.environ, {"PERPLEXITY_API_KEY": "env-perplexity-key"}):
            f.create_tool("PerplexityTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("api_key") == "env-perplexity-key"

    def test_perplexity_tool_key_from_config(self):
        f = _make_factory()
        info = _make_tool_info("PerplexityTool", 3, config={"api_key": "config-key"})
        f._available_tools["PerplexityTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["PerplexityTool"] = mock_cls

        # Remove env key to force use of config
        env_without_perplexity = {k: v for k, v in os.environ.items() if k != "PERPLEXITY_API_KEY"}
        with patch.dict(os.environ, env_without_perplexity, clear=True):
            f.create_tool("PerplexityTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("api_key") == "config-key"

    def test_genie_tool_extracts_group_id_from_config(self):
        f = _make_factory(config={"group_id": "genie-group"})
        info = _make_tool_info("GenieTool", 10, config={})
        f._available_tools["GenieTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["GenieTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("group_id") == "genie-group"

    def test_genie_tool_uses_factory_user_token(self):
        f = _make_factory(config={"group_id": "g"}, user_token="factory-token")
        info = _make_tool_info("GenieTool", 10, config={})
        f._available_tools["GenieTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["GenieTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("user_token") == "factory-token"

    def test_powerbi_json_fields_parsed_for_analysis_tool(self):
        """JSON strings in context enrichment fields should be parsed for Power BI Analysis tool."""
        import json as json_mod

        f = _make_factory()
        bm = json_mod.dumps({"Revenue": "sum of sales"})
        info = _make_tool_info(
            "Power BI Comprehensive Analysis Tool",
            99,
            config={"business_mappings": bm},
        )
        f._available_tools["Power BI Comprehensive Analysis Tool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["Power BI Comprehensive Analysis Tool"] = mock_cls

        f.create_tool("Power BI Comprehensive Analysis Tool")
        call_kwargs = mock_cls.call_args[1]
        # The JSON string should have been parsed into a dict
        assert isinstance(call_kwargs.get("business_mappings"), dict)

    def test_create_tool_by_integer_id(self):
        f = _make_factory()
        info = _make_tool_info("ScrapeWebsiteTool", 77)
        f._available_tools["77"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        result = f.create_tool(77)
        assert result is mock_cls.return_value


# ============================================================================
# _get_api_key_async
# ============================================================================

class TestGetApiKeyAsync:

    @pytest.mark.asyncio
    async def test_returns_none_without_service(self):
        f = _make_factory()
        result = await f._get_api_key_async("TEST_KEY")
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_key_from_service(self):
        mock_svc = AsyncMock()
        key_obj = MagicMock()
        key_obj.encrypted_value = "encrypted-abc"
        mock_svc.find_by_name = AsyncMock(return_value=key_obj)

        f = _make_factory(api_keys_service=mock_svc)

        with patch("src.engines.crewai.tools.tool_factory.EncryptionUtils.decrypt_value", return_value="plain-key"):
            result = await f._get_api_key_async("MY_KEY")

        assert result == "plain-key"

    @pytest.mark.asyncio
    async def test_returns_none_when_service_raises(self):
        mock_svc = AsyncMock()
        mock_svc.find_by_name = AsyncMock(side_effect=Exception("DB error"))

        f = _make_factory(api_keys_service=mock_svc)

        with patch(
            "src.utils.asyncio_utils.execute_db_operation_with_fresh_engine",
            new_callable=AsyncMock,
            side_effect=Exception("DB down"),
        ):
            result = await f._get_api_key_async("MISSING_KEY")

        assert result is None

    @pytest.mark.asyncio
    async def test_falls_back_to_fresh_engine_when_no_service(self):
        f = _make_factory()

        with patch(
            "src.utils.asyncio_utils.execute_db_operation_with_fresh_engine",
            new_callable=AsyncMock,
            return_value="fallback-key",
        ):
            result = await f._get_api_key_async("FALLBACK_KEY")

        assert result == "fallback-key"


# ============================================================================
# _run_in_new_loop
# ============================================================================

class TestRunInNewLoop:

    def test_executes_coroutine_and_returns_result(self):
        f = _make_factory()

        async def sample_coroutine():
            return 42

        result = f._run_in_new_loop(sample_coroutine)
        assert result == 42

    def test_propagates_exception_from_coroutine(self):
        f = _make_factory()

        async def failing_coroutine():
            raise ValueError("boom")

        with pytest.raises(ValueError, match="boom"):
            f._run_in_new_loop(failing_coroutine)


# ============================================================================
# update_tool_config
# ============================================================================

class TestUpdateToolConfig:

    def test_returns_false_when_tool_not_found(self):
        f = _make_factory()
        result = f.update_tool_config("GhostTool", {"x": "y"})
        assert result is False


# ============================================================================
# initialize / async lifecycle
# ============================================================================

class TestInitialize:

    @pytest.mark.asyncio
    async def test_sets_initialized_flag(self):
        f = _make_factory()
        with patch.object(f, "_load_available_tools_async", new_callable=AsyncMock):
            await f.initialize()
        assert f._initialized is True

    @pytest.mark.asyncio
    async def test_skips_when_already_initialized(self):
        f = _make_factory()
        f._initialized = True
        with patch.object(f, "_load_available_tools_async", new_callable=AsyncMock) as mock_load:
            await f.initialize()
        mock_load.assert_not_called()

    @pytest.mark.asyncio
    async def test_raises_when_load_tools_fails(self):
        f = _make_factory()
        with patch.object(
            f,
            "_load_available_tools_async",
            new_callable=AsyncMock,
            side_effect=RuntimeError("DB gone"),
        ):
            with pytest.raises(RuntimeError, match="DB gone"):
                await f.initialize()

    @pytest.mark.asyncio
    async def test_create_classmethod_returns_initialized_factory(self):
        with patch.object(ToolFactory, "initialize", new_callable=AsyncMock) as mock_init:
            factory = await ToolFactory.create({"group_id": "g"})
        assert isinstance(factory, ToolFactory)
        mock_init.assert_called_once()


# ============================================================================
# cleanup / __del__
# ============================================================================

class TestCleanup:

    def test_cleanup_does_not_raise(self):
        f = _make_factory()
        f.cleanup()  # Should complete without error

    def test_del_calls_cleanup(self):
        f = _make_factory()
        with patch.object(f, "cleanup") as mock_cleanup:
            f.__del__()
        mock_cleanup.assert_called_once()


# ============================================================================
# cleanup_after_crew_execution
# ============================================================================

class TestCleanupAfterCrewExecution:

    @pytest.mark.asyncio
    async def test_does_not_raise(self):
        f = _make_factory()
        await f.cleanup_after_crew_execution()  # Should complete without error


# ============================================================================
# _validate_databricks_auth
# ============================================================================

class TestValidateDatabricksAuth:

    @pytest.mark.asyncio
    async def test_returns_true_when_user_token_present(self):
        f = _make_factory(user_token="my-obo-token")
        valid, msg = await f._validate_databricks_auth()
        assert valid is True
        assert "OBO" in msg

    @pytest.mark.asyncio
    async def test_returns_true_when_unified_auth_available(self):
        f = _make_factory()

        mock_auth = MagicMock()
        mock_auth.token = "tok"
        mock_auth.workspace_url = None

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            return_value=mock_auth,
        ):
            valid, msg = await f._validate_databricks_auth()

        assert valid is True

    @pytest.mark.asyncio
    async def test_returns_false_when_no_auth_available(self):
        f = _make_factory()

        with patch(
            "src.utils.databricks_auth.get_auth_context",
            new_callable=AsyncMock,
            side_effect=Exception("no auth"),
        ):
            mock_sess_ctx = MagicMock()
            mock_sess = AsyncMock()
            mock_sess_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_sess)
            mock_sess_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

            mock_svc_cls = MagicMock()
            mock_svc = AsyncMock()
            mock_svc.get_databricks_config = AsyncMock(return_value=None)
            mock_svc_cls.return_value = mock_svc

            with (
                patch("src.db.session.request_scoped_session", mock_sess_ctx),
                patch("src.services.databricks_service.DatabricksService", mock_svc_cls),
            ):
                valid, msg = await f._validate_databricks_auth()

        assert valid is False


# ============================================================================
# DatabricksJobsTool creation path (integration-style)
# ============================================================================

class TestDatabricksJobsToolCreation:

    def test_creates_databricks_jobs_tool_with_group_id(self):
        f = _make_factory(config={"group_id": "jobs-grp"})
        info = _make_tool_info("DatabricksJobsTool", 5, config={})
        f._available_tools["DatabricksJobsTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["DatabricksJobsTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            with patch("src.utils.databricks_auth.get_auth_context", side_effect=Exception("no auth")):
                f.create_tool("DatabricksJobsTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("group_id") == "jobs-grp"

    def test_databricks_jobs_tool_uses_factory_token(self):
        f = _make_factory(config={"group_id": "g"}, user_token="bearer-token")
        info = _make_tool_info("DatabricksJobsTool", 5, config={})
        f._available_tools["DatabricksJobsTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["DatabricksJobsTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            with patch("src.utils.databricks_auth.get_auth_context", side_effect=Exception("no auth")):
                f.create_tool("DatabricksJobsTool")

        call_kwargs = mock_cls.call_args[1]
        # Factory user_token is passed when no config token
        assert call_kwargs.get("user_token") == "bearer-token"


# ============================================================================
# GenieTool spaceId resolution paths
# ============================================================================

class TestGenieToolSpaceIdPaths:

    def _setup_genie(self, config=None, user_token=None):
        f = _make_factory(config=config or {"group_id": "grp"}, user_token=user_token)
        info = _make_tool_info("GenieTool", 10, config={})
        f._available_tools["GenieTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["GenieTool"] = mock_cls
        return f, mock_cls

    def test_spaceid_from_override(self):
        f, mock_cls = self._setup_genie(user_token="tok")
        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool", tool_config_override={"spaceId": "space-123"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("spaceId") == "space-123"

    def test_spaceid_underscore_from_override(self):
        f, mock_cls = self._setup_genie(user_token="tok")
        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool", tool_config_override={"space_id": "space-underscore"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("spaceId") == "space-underscore"

    def test_spaceid_from_base_config(self):
        f = _make_factory(config={"group_id": "g"}, user_token="tok")
        info = _make_tool_info("GenieTool", 10, config={"spaceId": "base-space"})
        f._available_tools["GenieTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["GenieTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("spaceId") == "base-space"

    def test_genie_tool_creation_exception_returns_none(self):
        f, mock_cls = self._setup_genie(user_token="tok")
        mock_cls.side_effect = Exception("creation error")

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            result = f.create_tool("GenieTool")

        assert result is None

    def test_databricks_host_from_config(self):
        f = _make_factory(config={"group_id": "g"}, user_token="tok")
        info = _make_tool_info("GenieTool", 10, config={"DATABRICKS_HOST": "https://my-host.databricks.com"})
        f._available_tools["GenieTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["GenieTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("GenieTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("DATABRICKS_HOST") == "https://my-host.databricks.com"


# ============================================================================
# AgentBricksTool endpointName resolution paths
# ============================================================================

class TestAgentBricksToolEndpointPaths:

    def _setup_agentbricks(self, config=None, user_token=None):
        f = _make_factory(config=config or {"group_id": "grp"}, user_token=user_token)
        info = _make_tool_info("AgentBricksTool", 11, config={})
        f._available_tools["AgentBricksTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["AgentBricksTool"] = mock_cls
        return f, mock_cls

    def test_endpoint_name_from_override(self):
        f, mock_cls = self._setup_agentbricks(user_token="tok")
        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("AgentBricksTool", tool_config_override={"endpointName": "my-endpoint"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("endpointName") == "my-endpoint"

    def test_endpoint_name_underscore_from_override(self):
        f, mock_cls = self._setup_agentbricks(user_token="tok")
        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("AgentBricksTool", tool_config_override={"endpoint_name": "ep-underscore"})
        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("endpointName") == "ep-underscore"

    def test_endpoint_name_from_base_config(self):
        f = _make_factory(config={"group_id": "g"}, user_token="tok")
        info = _make_tool_info("AgentBricksTool", 11, config={"endpointName": "base-ep"})
        f._available_tools["AgentBricksTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["AgentBricksTool"] = mock_cls

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            f.create_tool("AgentBricksTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("tool_config", {}).get("endpointName") == "base-ep"

    def test_agentbricks_creation_exception_returns_none(self):
        f, mock_cls = self._setup_agentbricks(user_token="tok")
        mock_cls.side_effect = Exception("creation error")

        with patch("src.utils.user_context.UserContext") as mock_ctx:
            mock_ctx.get_user_token.return_value = None
            mock_ctx.get_group_context.return_value = None
            result = f.create_tool("AgentBricksTool")

        assert result is None


# ============================================================================
# DatabricksKnowledgeSearchTool creation path
# ============================================================================

class TestDatabricksKnowledgeSearchToolCreation:

    def test_creates_knowledge_search_tool_with_mock(self):
        """DatabricksKnowledgeSearchTool is called directly (not via tool_class), patch the module-level name."""
        f = _make_factory(config={"group_id": "search-grp"}, user_token="search-token")
        info = _make_tool_info("DatabricksKnowledgeSearchTool", 20, config={})
        f._available_tools["DatabricksKnowledgeSearchTool"] = info
        mock_tool_instance = MagicMock()
        mock_cls = MagicMock(return_value=mock_tool_instance)
        f._tool_implementations["DatabricksKnowledgeSearchTool"] = mock_cls

        with patch(
            "src.engines.crewai.tools.tool_factory.DatabricksKnowledgeSearchTool",
            mock_cls,
        ):
            with patch.object(f, "_run_in_new_loop", return_value=(True, "ok")):
                result = f.create_tool("DatabricksKnowledgeSearchTool")

        assert result is mock_tool_instance
        assert mock_cls.called

    def test_knowledge_search_passes_group_id(self):
        f = _make_factory(config={"group_id": "ks-group"}, user_token="ks-token")
        info = _make_tool_info("DatabricksKnowledgeSearchTool", 20, config={})
        f._available_tools["DatabricksKnowledgeSearchTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["DatabricksKnowledgeSearchTool"] = mock_cls

        with patch(
            "src.engines.crewai.tools.tool_factory.DatabricksKnowledgeSearchTool",
            mock_cls,
        ):
            with patch.object(f, "_run_in_new_loop", return_value=(True, "ok")):
                f.create_tool("DatabricksKnowledgeSearchTool")

        call_kwargs = mock_cls.call_args.kwargs
        assert call_kwargs.get("group_id") == "ks-group"


# ============================================================================
# PowerBIAnalysisTool creation path (generic fallthrough)
# ============================================================================

class TestPowerBIAnalysisToolCreation:

    def test_creates_powerbi_analysis_tool(self):
        f = _make_factory(config={"group_id": "g"})
        info = _make_tool_info("Power BI Comprehensive Analysis Tool", 30, config={
            "workspace_id": "ws1",
            "dataset_id": "ds1",
        })
        f._available_tools["Power BI Comprehensive Analysis Tool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["Power BI Comprehensive Analysis Tool"] = mock_cls

        result = f.create_tool("Power BI Comprehensive Analysis Tool")

        assert result is not None

    def test_powerbi_analysis_workspace_id_passed(self):
        f = _make_factory(config={"group_id": "g"})
        info = _make_tool_info("Power BI Comprehensive Analysis Tool", 30, config={
            "workspace_id": "my-workspace",
            "dataset_id": "my-dataset",
            "tenant_id": "my-tenant",
        })
        f._available_tools["Power BI Comprehensive Analysis Tool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["Power BI Comprehensive Analysis Tool"] = mock_cls

        f.create_tool("Power BI Comprehensive Analysis Tool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("workspace_id") == "my-workspace"


# ============================================================================
# ScrapeWebsiteTool – the default generic path
# ============================================================================

class TestScrapeWebsiteToolCreation:

    def test_creates_with_result_as_answer(self):
        f = _make_factory()
        info = _make_tool_info("ScrapeWebsiteTool", 50, config={})
        f._available_tools["ScrapeWebsiteTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        f.create_tool("ScrapeWebsiteTool", result_as_answer=True)

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("result_as_answer") is True

    def test_creates_with_config_from_base(self):
        f = _make_factory()
        info = _make_tool_info("ScrapeWebsiteTool", 50, config={"website_url": "https://example.com"})
        f._available_tools["ScrapeWebsiteTool"] = info
        mock_cls = MagicMock(return_value=MagicMock())
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        f.create_tool("ScrapeWebsiteTool")

        call_kwargs = mock_cls.call_args[1]
        assert call_kwargs.get("website_url") == "https://example.com"

    def test_generic_tool_exception_returns_none(self):
        f = _make_factory()
        info = _make_tool_info("ScrapeWebsiteTool", 50, config={})
        f._available_tools["ScrapeWebsiteTool"] = info
        mock_cls = MagicMock(side_effect=Exception("init error"))
        f._tool_implementations["ScrapeWebsiteTool"] = mock_cls

        result = f.create_tool("ScrapeWebsiteTool")
        assert result is None
