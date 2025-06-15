"""
Unit tests for all_models module.

Tests the functionality of the all_models module including
model imports and SQLAlchemy metadata registration.
"""
import pytest
from unittest.mock import patch
import importlib
import sys

from src.db.all_models import __all__ as all_models_exports


class TestAllModels:
    """Test cases for all_models module."""
    
    def test_base_import(self):
        """Test that Base is imported correctly."""
        from src.db.all_models import Base
        from src.db.base import Base as BaseOriginal
        
        assert Base is BaseOriginal
    
    def test_core_models_import(self):
        """Test that core models are imported correctly."""
        from src.db.all_models import (
            Agent, Task, ExecutionHistory, TaskStatus, ErrorTrace,
            Tool, LLMLog, ModelConfig, DatabricksConfig, InitializationStatus
        )
        
        # Verify these are the actual model classes
        assert hasattr(Agent, '__tablename__')
        assert hasattr(Task, '__tablename__')
        assert hasattr(ExecutionHistory, '__tablename__')
        assert hasattr(Tool, '__tablename__')
        assert hasattr(LLMLog, '__tablename__')
    
    def test_workflow_models_import(self):
        """Test that workflow-related models are imported correctly."""
        from src.db.all_models import (
            PromptTemplate, ExecutionTrace, Crew, Plan, Flow,
            FlowExecution, FlowNodeExecution, Schedule
        )
        
        # Verify these are the actual model classes
        assert hasattr(PromptTemplate, '__tablename__')
        assert hasattr(ExecutionTrace, '__tablename__')
        assert hasattr(Crew, '__tablename__')
        assert hasattr(Plan, '__tablename__')
        assert hasattr(Flow, '__tablename__')
        assert hasattr(Schedule, '__tablename__')
    
    def test_api_models_import(self):
        """Test that API-related models are imported correctly."""
        from src.db.all_models import (
            ApiKey, Schema, ExecutionLog, MCPServer, MCPSettings
        )
        
        # Verify these are the actual model classes
        assert hasattr(ApiKey, '__tablename__')
        assert hasattr(Schema, '__tablename__')
        assert hasattr(ExecutionLog, '__tablename__')
        assert hasattr(MCPServer, '__tablename__')
        assert hasattr(MCPSettings, '__tablename__')
    
    def test_group_models_import(self):
        """Test that group-related models are imported correctly."""
        from src.db.all_models import Group, GroupUser
        
        # Verify these are the actual model classes
        assert hasattr(Group, '__tablename__')
        assert hasattr(GroupUser, '__tablename__')
    
    def test_user_rbac_models_import(self):
        """Test that user and RBAC models are imported correctly."""
        from src.db.all_models import (
            User, UserProfile, RefreshToken, ExternalIdentity,
            Role, Privilege, RolePrivilege, UserRole, IdentityProvider
        )
        
        # Verify these are the actual model classes
        assert hasattr(User, '__tablename__')
        assert hasattr(UserProfile, '__tablename__')
        assert hasattr(RefreshToken, '__tablename__')
        assert hasattr(ExternalIdentity, '__tablename__')
        assert hasattr(Role, '__tablename__')
        assert hasattr(Privilege, '__tablename__')
        assert hasattr(RolePrivilege, '__tablename__')
        assert hasattr(UserRole, '__tablename__')
        assert hasattr(IdentityProvider, '__tablename__')
    
    def test_all_exports_completeness(self):
        """Test that __all__ contains all expected model exports."""
        expected_models = [
            "Base",
            "Agent", "Task", "ExecutionHistory", "TaskStatus", "ErrorTrace",
            "Tool", "LLMLog", "ModelConfig", "DatabricksConfig", "InitializationStatus",
            "PromptTemplate", "ExecutionTrace", "Crew", "Plan", "Flow",
            "FlowExecution", "FlowNodeExecution", "Schedule", "ApiKey", "Schema",
            "ExecutionLog", "MCPServer", "MCPSettings",
            "Group", "GroupUser",
            "User", "UserProfile", "RefreshToken", "ExternalIdentity",
            "Role", "Privilege", "RolePrivilege", "UserRole", "IdentityProvider"
        ]
        
        for model in expected_models:
            assert model in all_models_exports, f"Missing {model} in __all__"
    
    def test_all_exported_models_importable(self):
        """Test that all models in __all__ can be imported."""
        from src.db import all_models
        
        for model_name in all_models_exports:
            assert hasattr(all_models, model_name), f"Cannot import {model_name}"
            model_class = getattr(all_models, model_name)
            assert model_class is not None
    
    def test_sqlalchemy_metadata_registration(self):
        """Test that models are registered with SQLAlchemy metadata."""
        from src.db.all_models import Base
        
        # All models should be registered in the Base metadata
        table_names = list(Base.metadata.tables.keys())
        
        # Should have multiple tables registered
        assert len(table_names) > 0
        
        # Check for some key tables
        expected_tables = [
            'agents', 'tasks', 'executionhistory', 'tools', 'llmlog',
            'users', 'roles', 'privileges', 'groups'
        ]
        
        for table in expected_tables:
            assert table in table_names, f"Table {table} not registered in metadata"
    
    def test_model_relationships(self):
        """Test that models have expected relationships."""
        from src.db.all_models import User, Role, Group, UserRole
        
        # Test that relationship attributes exist
        assert hasattr(User, 'user_roles')
        assert hasattr(Role, 'user_roles')
        assert hasattr(Group, 'group_users')
    
    def test_imports_dont_fail(self):
        """Test that importing the module doesn't raise any errors."""
        # This test ensures that all import statements in the module work
        try:
            import src.db.all_models
            importlib.reload(src.db.all_models)
        except ImportError as e:
            pytest.fail(f"Failed to import all_models module: {e}")
    
    def test_no_circular_imports(self):
        """Test that there are no circular import issues."""
        # Remove the module from cache if it exists
        if 'src.db.all_models' in sys.modules:
            del sys.modules['src.db.all_models']
        
        # Import should work without circular dependency issues
        try:
            from src.db import all_models
            # Try to access a few models to ensure full import
            assert all_models.Base is not None
            assert all_models.User is not None
            assert all_models.Agent is not None
        except Exception as e:
            pytest.fail(f"Circular import or other import error: {e}")
    
    def test_model_base_inheritance(self):
        """Test that all models inherit from Base."""
        from src.db.all_models import (
            Base, Agent, User, Role, Group, Task, Tool
        )
        
        models_to_test = [Agent, User, Role, Group, Task, Tool]
        
        for model in models_to_test:
            assert issubclass(model, Base), f"{model.__name__} should inherit from Base"
    
    def test_documentation_import_comments(self):
        """Test that the module has proper documentation."""
        import src.db.all_models as all_models_module
        
        # Module should have a docstring
        assert all_models_module.__doc__ is not None
        assert "Collection of all database models" in all_models_module.__doc__
    
    def test_import_organization(self):
        """Test that imports are well organized and categorized."""
        import inspect
        import src.db.all_models
        
        # Get the source code to check import organization
        source = inspect.getsource(src.db.all_models)
        
        # Should have comments organizing the imports
        assert "# Import base" in source or "Import base" in source
        assert "# Import all models" in source or "Import all models" in source
        assert "# User and RBAC models" in source
        assert "# Multi-group models" in source
    
    def test_future_extensibility(self):
        """Test that the module is set up for future extensions."""
        import inspect
        import src.db.all_models
        
        # Get the source code
        source = inspect.getsource(src.db.all_models)
        
        # Should have a comment about adding additional models
        assert "# Add additional models" in source or "additional models" in source.lower()
    
    def test_model_count_reasonable(self):
        """Test that we have a reasonable number of models registered."""
        # Should have a substantial number of models (at least 15)
        assert len(all_models_exports) >= 15
        
        # But not an unreasonable number (less than 100)
        assert len(all_models_exports) < 100
    
    def test_enum_imports(self):
        """Test that enum classes are imported correctly."""
        from src.db.all_models import TaskStatus, ErrorTrace
        
        # These should be available and should be enum-like classes
        assert TaskStatus is not None
        assert ErrorTrace is not None