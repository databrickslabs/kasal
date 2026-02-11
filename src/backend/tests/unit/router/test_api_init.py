"""
Unit tests for the API module initialization.

Verifies that the api_router is properly constructed, all expected
sub-routers are included, and exports are consistent.
"""
import pytest
from fastapi import APIRouter


class TestAPIModuleImport:
    """Verify the API module can be imported and has the expected structure."""

    def test_api_router_is_api_router_instance(self):
        """api_router must be an APIRouter instance."""
        from src.api import api_router

        assert isinstance(api_router, APIRouter)

    def test_api_router_has_routes(self):
        """api_router must have at least one route registered."""
        from src.api import api_router

        assert len(api_router.routes) > 0


class TestSubRouterImports:
    """Verify that individual sub-routers can be imported from src.api."""

    @pytest.mark.parametrize(
        "router_name",
        [
            "agents_router",
            "crews_router",
            "crews_export_router",
            "databricks_router",
            "databricks_knowledge_router",
            "flows_router",
            "healthcheck_router",
            "logs_router",
            "models_router",
            "databricks_secrets_router",
            "api_keys_router",
            "tasks_router",
            "templates_router",
            "schemas_router",
            "tools_router",
            "scheduler_router",
            "agent_generation_router",
            "connections_router",
            "crew_generation_router",
            "task_generation_router",
            "template_generation_router",
            "executions_router",
            "execution_history_router",
            "execution_trace_router",
            "flow_execution_router",
            "mcp_router",
            "dispatcher_router",
            "engine_config_router",
            "users_router",
            "runs_router",
            "group_router",
            "chat_history_router",
            "memory_backend_router",
            "documentation_embeddings_router",
            "database_management_router",
            "genie_router",
            "agentbricks_router",
            "mlflow_router",
            "hitl_router",
            "sse_router",
            "powerbi_router",
            "group_tools_router",
        ],
    )
    def test_sub_router_importable_and_is_api_router(self, router_name):
        """Each sub-router must be importable and be an APIRouter instance."""
        import src.api as api_module

        assert hasattr(api_module, router_name), f"{router_name} not found in src.api"
        router_obj = getattr(api_module, router_name)
        assert isinstance(router_obj, APIRouter), (
            f"{router_name} is {type(router_obj)}, expected APIRouter"
        )


class TestDunderAll:
    """Verify __all__ is consistent with actual module exports."""

    def test_dunder_all_exists(self):
        """__all__ must be defined in src.api."""
        from src.api import __all__ as api_all

        assert isinstance(api_all, list)
        assert len(api_all) > 0

    def test_dunder_all_contains_api_router(self):
        """__all__ must include 'api_router'."""
        from src.api import __all__ as api_all

        assert "api_router" in api_all

    @pytest.mark.parametrize(
        "expected_export",
        [
            "api_router",
            "agents_router",
            "crews_router",
            "databricks_router",
            "flows_router",
            "healthcheck_router",
            "models_router",
            "tools_router",
            "agent_generation_router",
            "crew_generation_router",
            "task_generation_router",
            "template_generation_router",
            "executions_router",
            "execution_history_router",
            "execution_trace_router",
            "mcp_router",
            "dispatcher_router",
            "engine_config_router",
            "users_router",
            "group_router",
            "database_management_router",
            "genie_router",
            "agentbricks_router",
            "mlflow_router",
            "hitl_router",
            "sse_router",
        ],
    )
    def test_expected_export_in_dunder_all(self, expected_export):
        """Key exports must be present in __all__."""
        from src.api import __all__ as api_all

        assert expected_export in api_all

    def test_all_dunder_all_entries_are_importable(self):
        """Every name listed in __all__ must actually exist on the module."""
        import src.api as api_module
        from src.api import __all__ as api_all

        for name in api_all:
            assert hasattr(api_module, name), (
                f"'{name}' listed in __all__ but not found on src.api"
            )


class TestRouterPrefixes:
    """Verify selected routers have the expected prefix configuration."""

    def test_agent_generation_router_prefix(self):
        from src.api.agent_generation_router import router

        assert router.prefix == "/agent-generation"

    def test_crew_generation_router_prefix(self):
        from src.api.crew_generation_router import router

        assert router.prefix == "/crew"

    def test_task_generation_router_prefix(self):
        from src.api.task_generation_router import router

        assert router.prefix == "/task-generation"

    def test_template_generation_router_prefix(self):
        from src.api.template_generation_router import router

        assert router.prefix == "/template-generation"


class TestRouterTags:
    """Verify selected routers have meaningful tags configured."""

    def test_agent_generation_router_tags(self):
        from src.api.agent_generation_router import router

        assert "Agent Generation" in router.tags

    def test_crew_generation_router_tags(self):
        from src.api.crew_generation_router import router

        assert "crew" in router.tags

    def test_task_generation_router_tags(self):
        from src.api.task_generation_router import router

        assert "task generation" in router.tags

    def test_template_generation_router_tags(self):
        from src.api.template_generation_router import router

        assert "template generation" in router.tags
