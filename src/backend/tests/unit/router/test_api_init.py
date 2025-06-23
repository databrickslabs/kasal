"""
Unit tests for API module initialization.

Tests the functionality of the API router initialization and
router inclusion logic.
"""
import pytest
from unittest.mock import patch, MagicMock
from fastapi import APIRouter


class TestAPIInit:
    """Test cases for API module initialization."""
    
    def test_api_router_creation(self):
        """Test that the main API router is created correctly."""
        from src.api import api_router
        
        assert isinstance(api_router, APIRouter)
    
    def test_all_routers_imported(self):
        """Test that all expected routers are imported."""
        from src.api import (
            agents_router,
            crews_router,
            databricks_router,
            flows_router,
            healthcheck_router,
            logs_router,
            models_router,
            databricks_secrets_router,
            api_keys_router,
            tasks_router,
            templates_router,
            schemas_router,
            tools_router,
            upload_router,
            task_tracking_router,
            scheduler_router,
            agent_generation_router,
            connections_router,
            crew_generation_router,
            task_generation_router,
            template_generation_router,
            executions_router,
            execution_history_router,
            execution_trace_router,
            flow_execution_router,
            mcp_router,
            dispatcher_router,
            engine_config_router,
            databricks_role_router,
            auth_router,
            users_router,
            roles_router,
            privileges_router,
            user_roles_router,
            identity_providers_router,
            group_router,
            chat_history_router
        )
        
        # Test that all routers are APIRouter instances
        routers = [
            agents_router, crews_router, databricks_router,
            flows_router, healthcheck_router, logs_router, models_router,
            databricks_secrets_router, api_keys_router, tasks_router, templates_router,
            schemas_router, tools_router, upload_router,
            task_tracking_router, scheduler_router,
            agent_generation_router, connections_router, crew_generation_router,
            task_generation_router, template_generation_router, executions_router,
            execution_history_router, execution_trace_router, flow_execution_router,
            mcp_router, dispatcher_router, engine_config_router, databricks_role_router,
            auth_router, users_router, roles_router, privileges_router,
            user_roles_router, identity_providers_router, group_router,
            chat_history_router
        ]
        
        for router in routers:
            assert isinstance(router, APIRouter)
    
    def test_execution_logs_router_imports(self):
        """Test that execution logs routers are imported correctly."""
        from src.api import runs_router, execution_logs_router
        
        assert isinstance(runs_router, APIRouter)
        assert isinstance(execution_logs_router, APIRouter)
    
    def test_api_router_includes_all_routers(self):
        """Test that the main API router includes all sub-routers."""
        from src.api import api_router
        
        # Check that the router has the expected number of routes
        # This is a basic check - in a real scenario, you might want to check specific routes
        assert len(api_router.routes) > 0
    
    def test_router_prefixes_and_tags(self):
        """Test that routers have appropriate prefixes and tags."""
        from src.api import (
            agents_router, crews_router, users_router, roles_router,
            group_router
        )
        
        # Test agents router
        assert agents_router.prefix == "/agents"
        assert "agents" in agents_router.tags
        
        # Test crews router  
        assert crews_router.prefix == "/crews"
        assert "crews" in crews_router.tags
        
        # Test users router
        assert users_router.prefix == "/users"
        assert "users" in users_router.tags
        
        # Test roles router
        assert roles_router.prefix == "/roles"
        assert "roles" in roles_router.tags
        
        # Test group router
        assert group_router.prefix == "/groups"
        assert "groups" in group_router.tags
    
    def test_dunder_all_contains_expected_exports(self):
        """Test that __all__ contains all expected router exports."""
        from src.api import __all__ as api_all
        
        expected_exports = [
            "api_router",
            "agents_router",
            "crews_router",
            "databricks_router",
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
            "upload_router",
            "task_tracking_router",
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
            "auth_router",
            "users_router",
            "runs_router",
            "roles_router",
            "privileges_router",
            "user_roles_router",
            "identity_providers_router",
            "group_router",
            "databricks_role_router",
            "chat_history_router"
        ]
        
        for export in expected_exports:
            assert export in api_all
    
    def test_router_dependency_injection_setup(self):
        """Test that routers are set up with proper dependency injection."""
        # This is a more complex test that would verify the dependency injection setup
        # For now, we'll just test that the routers can be imported without errors
        try:
            from src.api import api_router
            from src.api.agents_router import router as agents_router
            from src.api.users_router import router as users_router
            from src.api.roles_router import router as roles_router
            
            # Verify that these are APIRouter instances
            assert isinstance(api_router, APIRouter)
            assert isinstance(agents_router, APIRouter)
            assert isinstance(users_router, APIRouter)
            assert isinstance(roles_router, APIRouter)
            
        except ImportError as e:
            pytest.fail(f"Failed to import routers: {e}")
    
    def test_user_management_routers_inclusion(self):
        """Test that user management routers are properly included."""
        from src.api import api_router
        
        # Get all included routers
        included_routers = [route.path for route in api_router.routes if hasattr(route, 'path')]
        
        # Check that user management paths are included
        # Note: This is a simplified check - actual route testing would be more complex
        assert len(included_routers) > 0
    
    def test_new_rbac_routers_inclusion(self):
        """Test that new RBAC routers are properly included."""
        from src.api import (
            roles_router,
            privileges_router,
            user_roles_router,
            group_router,
            databricks_role_router
        )
        
        # Verify these are all APIRouter instances
        rbac_routers = [
            roles_router,
            privileges_router,
            user_roles_router,
            group_router,
            databricks_role_router
        ]
        
        for router in rbac_routers:
            assert isinstance(router, APIRouter)
    
    def test_api_module_structure(self):
        """Test the overall structure of the API module."""
        import src.api as api_module
        
        # Test that the module has the expected attributes
        expected_attributes = [
            'api_router',
            'agents_router',
            'users_router',
            'roles_router',
            'group_router'
        ]
        
        for attr in expected_attributes:
            assert hasattr(api_module, attr), f"Missing attribute: {attr}"
    
    def test_router_error_handling_setup(self):
        """Test that routers have proper error handling setup."""
        from src.api.agents_router import router as agents_router
        from src.api.users_router import router as users_router
        
        # Check that routers have error responses configured
        assert agents_router.responses is not None
        assert 404 in agents_router.responses
        assert users_router.responses is not None
        # Users router has 401 for unauthorized instead of 404
        assert 401 in users_router.responses