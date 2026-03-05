"""Unit tests for MCPServer model — column defaults, constraints, and init logic."""

import pytest
from datetime import datetime
from sqlalchemy import UniqueConstraint

from src.models.mcp_server import MCPServer


class TestMCPServerModelDefaults:
    """Verify that column-level defaults are declared correctly on the model."""

    def test_tablename(self):
        """__tablename__ should be 'mcp_servers'."""
        assert MCPServer.__tablename__ == "mcp_servers"

    # --- Column default declarations (inspected via __table__) ---

    def test_auth_type_default_is_api_key(self):
        """auth_type column default should be 'api_key'."""
        col = MCPServer.__table__.columns["auth_type"]
        assert col.default.arg == "api_key"

    def test_server_type_default_is_sse(self):
        """server_type column default should be 'sse'."""
        col = MCPServer.__table__.columns["server_type"]
        assert col.default.arg == "sse"

    def test_enabled_default_is_false(self):
        """enabled column default should be False."""
        col = MCPServer.__table__.columns["enabled"]
        assert col.default.arg is False

    def test_global_enabled_default_is_false(self):
        """global_enabled column default should be False."""
        col = MCPServer.__table__.columns["global_enabled"]
        assert col.default.arg is False

    def test_timeout_seconds_default_is_30(self):
        """timeout_seconds column default should be 30."""
        col = MCPServer.__table__.columns["timeout_seconds"]
        assert col.default.arg == 30

    def test_max_retries_default_is_3(self):
        """max_retries column default should be 3."""
        col = MCPServer.__table__.columns["max_retries"]
        assert col.default.arg == 3

    def test_model_mapping_enabled_default_is_false(self):
        """model_mapping_enabled column default should be False."""
        col = MCPServer.__table__.columns["model_mapping_enabled"]
        assert col.default.arg is False

    def test_rate_limit_default_is_60(self):
        """rate_limit column default should be 60."""
        col = MCPServer.__table__.columns["rate_limit"]
        assert col.default.arg == 60

    def test_additional_config_default_is_dict_callable(self):
        """additional_config column default should be a callable that produces {}."""
        col = MCPServer.__table__.columns["additional_config"]
        # SQLAlchemy wraps the callable; verify it is callable and is the dict builtin
        assert col.default is not None
        assert callable(col.default.arg)

    def test_created_at_default_is_callable(self):
        """created_at should have a callable default (datetime.utcnow)."""
        col = MCPServer.__table__.columns["created_at"]
        assert col.default is not None
        assert callable(col.default.arg)

    def test_updated_at_default_is_callable(self):
        """updated_at should have a callable default (datetime.utcnow)."""
        col = MCPServer.__table__.columns["updated_at"]
        assert col.default is not None
        assert callable(col.default.arg)

    def test_updated_at_has_onupdate(self):
        """updated_at should have an onupdate clause."""
        col = MCPServer.__table__.columns["updated_at"]
        assert col.onupdate is not None


class TestMCPServerModelConstraints:
    """Verify table-level constraints declared on the model."""

    def test_unique_constraint_name_group_id_exists(self):
        """A UniqueConstraint on (name, group_id) must be present."""
        table_args = MCPServer.__table_args__
        unique_constraints = [
            arg for arg in table_args if isinstance(arg, UniqueConstraint)
        ]
        assert len(unique_constraints) >= 1

        # Check the specific constraint
        uq = unique_constraints[0]
        column_names = {col.name for col in uq.columns}
        assert column_names == {"name", "group_id"}
        assert uq.name == "uq_mcpserver_name_group"

    def test_primary_key_is_id(self):
        """The primary key should be the 'id' column."""
        col = MCPServer.__table__.columns["id"]
        assert col.primary_key is True

    def test_name_is_not_nullable(self):
        """name column must be NOT NULL."""
        col = MCPServer.__table__.columns["name"]
        assert col.nullable is False

    def test_server_url_is_not_nullable(self):
        """server_url column must be NOT NULL."""
        col = MCPServer.__table__.columns["server_url"]
        assert col.nullable is False

    def test_encrypted_api_key_is_nullable(self):
        """encrypted_api_key column must allow NULL."""
        col = MCPServer.__table__.columns["encrypted_api_key"]
        assert col.nullable is True

    def test_group_id_is_nullable(self):
        """group_id column must allow NULL."""
        col = MCPServer.__table__.columns["group_id"]
        assert col.nullable is True


class TestMCPServerInit:
    """Test the custom __init__ behaviour."""

    def test_instantiation_with_required_fields(self):
        """MCPServer can be instantiated with just name and server_url."""
        server = MCPServer(name="test-server", server_url="https://example.com/mcp")
        assert server.name == "test-server"
        assert server.server_url == "https://example.com/mcp"

    def test_additional_config_defaults_to_empty_dict_when_none(self):
        """When additional_config is explicitly None, __init__ should coerce to {}."""
        server = MCPServer(
            name="test",
            server_url="https://example.com",
            additional_config=None,
        )
        assert server.additional_config == {}

    def test_additional_config_not_provided_defaults_to_empty_dict(self):
        """When additional_config is omitted, __init__ should coerce to {}."""
        server = MCPServer(name="test", server_url="https://example.com")
        assert server.additional_config == {}

    def test_additional_config_with_explicit_value_is_preserved(self):
        """When additional_config is passed with data, that data is kept."""
        cfg = {"headers": {"Authorization": "Bearer tok"}}
        server = MCPServer(
            name="test",
            server_url="https://example.com",
            additional_config=cfg,
        )
        assert server.additional_config == cfg

    def test_auth_type_can_be_set_explicitly(self):
        """auth_type can be overridden at init time."""
        server = MCPServer(
            name="spn-server",
            server_url="https://example.com",
            auth_type="databricks_spn",
        )
        assert server.auth_type == "databricks_spn"

    def test_all_fields_can_be_set_at_init(self):
        """All model columns can be populated through __init__."""
        server = MCPServer(
            name="full-server",
            server_url="https://example.com/mcp",
            encrypted_api_key="enc_key_value",
            server_type="streamable",
            auth_type="api_key",
            enabled=True,
            global_enabled=True,
            group_id="workspace-123",
            timeout_seconds=60,
            max_retries=5,
            model_mapping_enabled=True,
            rate_limit=120,
            additional_config={"custom": True},
        )
        assert server.name == "full-server"
        assert server.server_url == "https://example.com/mcp"
        assert server.encrypted_api_key == "enc_key_value"
        assert server.server_type == "streamable"
        assert server.auth_type == "api_key"
        assert server.enabled is True
        assert server.global_enabled is True
        assert server.group_id == "workspace-123"
        assert server.timeout_seconds == 60
        assert server.max_retries == 5
        assert server.model_mapping_enabled is True
        assert server.rate_limit == 120
        assert server.additional_config == {"custom": True}

    def test_fields_can_be_mutated_after_init(self):
        """Model instance fields should be mutable."""
        server = MCPServer(name="mutable", server_url="https://example.com")
        server.enabled = True
        server.timeout_seconds = 90
        server.additional_config = {"updated": True}

        assert server.enabled is True
        assert server.timeout_seconds == 90
        assert server.additional_config == {"updated": True}
