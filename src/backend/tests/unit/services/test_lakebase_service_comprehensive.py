import asyncio

import pytest
from unittest.mock import Mock, MagicMock, patch, AsyncMock
from typing import Optional, Dict, Any

# Test LakebaseService - based on actual code inspection

from src.services.lakebase_service import LakebaseService


class TestLakebaseServiceInit:
    """Test LakebaseService initialization"""

    def test_lakebase_service_init_with_session(self):
        """Test LakebaseService __init__ with session"""
        mock_session = Mock()
        user_token = "test-token"
        user_email = "test@example.com"
        
        with patch('src.services.lakebase_service.LakebaseConnectionService') as mock_conn_service, \
             patch('src.services.lakebase_service.LakebaseSchemaService') as mock_schema_service, \
             patch('src.services.lakebase_service.LakebasePermissionService') as mock_perm_service:
            
            service = LakebaseService(mock_session, user_token, user_email)
            
            assert service.session == mock_session
            assert service.user_token == user_token
            assert service.user_email == user_email
            assert service.config_repository is not None
            assert service.migration_service is None
            mock_conn_service.assert_called_once_with(user_token, user_email)
            mock_schema_service.assert_called_once()
            mock_perm_service.assert_called_once()

    def test_lakebase_service_init_without_session(self):
        """Test LakebaseService __init__ without session"""
        user_token = "test-token"
        user_email = "test@example.com"
        
        with patch('src.services.lakebase_service.LakebaseConnectionService') as mock_conn_service, \
             patch('src.services.lakebase_service.LakebaseSchemaService') as mock_schema_service, \
             patch('src.services.lakebase_service.LakebasePermissionService') as mock_perm_service:
            
            service = LakebaseService(None, user_token, user_email)
            
            assert service.session is None
            assert service.user_token == user_token
            assert service.user_email == user_email
            assert service.config_repository is None
            assert service.migration_service is None
            mock_conn_service.assert_called_once_with(user_token, user_email)
            mock_schema_service.assert_called_once()
            mock_perm_service.assert_called_once()

    def test_lakebase_service_init_defaults(self):
        """Test LakebaseService __init__ with default parameters"""
        with patch('src.services.lakebase_service.LakebaseConnectionService') as mock_conn_service, \
             patch('src.services.lakebase_service.LakebaseSchemaService') as mock_schema_service, \
             patch('src.services.lakebase_service.LakebasePermissionService') as mock_perm_service:
            
            service = LakebaseService()
            
            assert service.session is None
            assert service.user_token is None
            assert service.user_email is None
            assert service.config_repository is None
            assert service.migration_service is None
            mock_conn_service.assert_called_once_with(None, None)
            mock_schema_service.assert_called_once()
            mock_perm_service.assert_called_once()


class TestLakebaseServiceWorkspaceClient:
    """Test LakebaseService workspace client methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = LakebaseService(self.mock_session)

    @pytest.mark.asyncio
    async def test_get_workspace_client(self):
        """Test get_workspace_client delegates to connection service"""
        mock_client = Mock()
        self.service.connection_service.get_workspace_client = AsyncMock(return_value=mock_client)
        
        result = await self.service.get_workspace_client()
        
        assert result == mock_client
        self.service.connection_service.get_workspace_client.assert_called_once()


class TestLakebaseServiceGetConfig:
    """Test LakebaseService get_config method"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = LakebaseService(self.mock_session)

    @pytest.mark.asyncio
    async def test_get_config_with_existing_config(self):
        """Test get_config with existing configuration"""
        mock_config = Mock()
        mock_config.value = {
            "enabled": True,
            "instance_name": "test-instance",
            "capacity": "CU_2",
            "retention_days": 30,
            "node_count": 2,
            "instance_status": "RUNNING",
            "endpoint": "test-endpoint",
            "created_at": "2023-01-01",
            "database_type": "lakebase"
        }
        self.service.config_repository.get_by_key = AsyncMock(return_value=mock_config)
        
        result = await self.service.get_config()
        
        expected = {
            "enabled": True,
            "instance_name": "test-instance",
            "capacity": "CU_2",
            "retention_days": 30,
            "node_count": 2,
            "instance_status": "RUNNING",
            "endpoint": "test-endpoint",
            "created_at": "2023-01-01",
            "database_type": "lakebase"
        }
        assert result == expected
        self.service.config_repository.get_by_key.assert_called_once_with("lakebase")

    @pytest.mark.asyncio
    async def test_get_config_with_partial_config(self):
        """Test get_config with partial configuration (missing some fields)"""
        mock_config = Mock()
        mock_config.value = {
            "enabled": True,
            "instance_name": "test-instance"
            # Missing other fields
        }
        self.service.config_repository.get_by_key = AsyncMock(return_value=mock_config)
        
        result = await self.service.get_config()
        
        expected = {
            "enabled": True,
            "instance_name": "test-instance",
            "capacity": "CU_1",  # Default value
            "retention_days": 14,  # Default value
            "node_count": 1,  # Default value
            "instance_status": "NOT_CREATED",  # Default value
            "endpoint": None,  # Missing field
            "created_at": None,  # Missing field
            "database_type": "lakebase"  # Default value
        }
        assert result == expected

    @pytest.mark.asyncio
    async def test_get_config_no_existing_config(self):
        """Test get_config with no existing configuration"""
        self.service.config_repository.get_by_key = AsyncMock(return_value=None)
        
        result = await self.service.get_config()
        
        expected = {
            "enabled": False,
            "instance_name": "kasal-lakebase",
            "capacity": "CU_1",
            "retention_days": 14,
            "node_count": 1,
            "instance_status": "NOT_CREATED",
            "database_type": "lakebase"
        }
        assert result == expected
        self.service.config_repository.get_by_key.assert_called_once_with("lakebase")

    @pytest.mark.asyncio
    async def test_get_config_exception_handling(self):
        """Test get_config handles exceptions"""
        self.service.config_repository.get_by_key = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception, match="Database error"):
            await self.service.get_config()


class TestLakebaseServiceAttributes:
    """Test LakebaseService attribute access and properties"""

    def test_service_has_required_attributes_with_session(self):
        """Test that service has all required attributes when initialized with session"""
        mock_session = Mock()
        
        with patch('src.services.lakebase_service.LakebaseConnectionService'), \
             patch('src.services.lakebase_service.LakebaseSchemaService'), \
             patch('src.services.lakebase_service.LakebasePermissionService'):
            
            service = LakebaseService(mock_session, "token", "email@test.com")
            
            # Check all required attributes exist
            assert hasattr(service, 'session')
            assert hasattr(service, 'user_token')
            assert hasattr(service, 'user_email')
            assert hasattr(service, 'config_repository')
            assert hasattr(service, 'connection_service')
            assert hasattr(service, 'schema_service')
            assert hasattr(service, 'permission_service')
            assert hasattr(service, 'migration_service')
            
            # Check attribute values
            assert service.session == mock_session
            assert service.user_token == "token"
            assert service.user_email == "email@test.com"
            assert service.config_repository is not None
            assert service.migration_service is None

    def test_service_has_required_attributes_without_session(self):
        """Test that service has all required attributes when initialized without session"""
        with patch('src.services.lakebase_service.LakebaseConnectionService'), \
             patch('src.services.lakebase_service.LakebaseSchemaService'), \
             patch('src.services.lakebase_service.LakebasePermissionService'):
            
            service = LakebaseService(None, "token", "email@test.com")
            
            # Check all required attributes exist
            assert hasattr(service, 'session')
            assert hasattr(service, 'user_token')
            assert hasattr(service, 'user_email')
            assert hasattr(service, 'config_repository')
            assert hasattr(service, 'connection_service')
            assert hasattr(service, 'schema_service')
            assert hasattr(service, 'permission_service')
            assert hasattr(service, 'migration_service')
            
            # Check attribute values
            assert service.session is None
            assert service.user_token == "token"
            assert service.user_email == "email@test.com"
            assert service.config_repository is None
            assert service.migration_service is None


class TestLakebaseServiceConstants:
    """Test LakebaseService constants and module-level attributes"""

    def test_lakebase_available_constant(self):
        """Test LAKEBASE_AVAILABLE constant is defined"""
        from src.services.lakebase_service import LAKEBASE_AVAILABLE
        assert isinstance(LAKEBASE_AVAILABLE, bool)

    def test_logger_manager_initialization(self):
        """Test logger_manager is properly initialized"""
        from src.services.lakebase_service import logger_manager
        assert logger_manager is not None


class TestLakebaseServiceBasicMethods:
    """Test basic LakebaseService utility methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = Mock()
        self.service = LakebaseService(self.mock_session)

    def test_service_initialization_creates_specialized_services(self):
        """Test that initialization creates all specialized services"""
        assert self.service.connection_service is not None
        assert self.service.schema_service is not None
        assert self.service.permission_service is not None
        assert self.service.migration_service is None  # Created when needed

    def test_service_stores_user_credentials(self):
        """Test that service properly stores user credentials"""
        service = LakebaseService(self.mock_session, "test-token", "test@example.com")
        
        assert service.user_token == "test-token"
        assert service.user_email == "test@example.com"

    def test_service_handles_none_credentials(self):
        """Test that service handles None credentials gracefully"""
        service = LakebaseService(self.mock_session, None, None)
        
        assert service.user_token is None
        assert service.user_email is None


class TestLakebaseServiceUtilityMethods:
    """Test LakebaseService utility methods"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mock_session = AsyncMock()
        self.user_token = "test-token"
        self.user_email = "test@example.com"

        with patch('src.services.lakebase_service.LakebaseConnectionService'), \
             patch('src.services.lakebase_service.LakebaseSchemaService'), \
             patch('src.services.lakebase_service.LakebasePermissionService'):
            self.service = LakebaseService(self.mock_session, self.user_token, self.user_email)

    def test_service_attributes(self):
        """Test service has expected attributes"""
        assert self.service.session == self.mock_session
        assert self.service.user_token == self.user_token
        assert self.service.user_email == self.user_email
        assert hasattr(self.service, 'connection_service')
        assert hasattr(self.service, 'schema_service')
        assert hasattr(self.service, 'permission_service')

    def test_service_with_none_session(self):
        """Test service initialization with None session"""
        with patch('src.services.lakebase_service.LakebaseConnectionService'), \
             patch('src.services.lakebase_service.LakebaseSchemaService'), \
             patch('src.services.lakebase_service.LakebasePermissionService'):
            service = LakebaseService(None, self.user_token, self.user_email)

            assert service.session is None
            assert service.config_repository is None
            assert service.migration_service is None


# ===========================================================================
# _validate_identifier tests (across lakebase services)
# ===========================================================================

from src.services.lakebase_schema_service import (
    _validate_identifier as schema_validate_identifier,
    _quote_pg_role as schema_quote_pg_role,
)
from src.services.lakebase_service import (
    _validate_identifier as service_validate_identifier,
)
from src.services.lakebase_migration_service import (
    _validate_identifier as migration_validate_identifier,
)
from src.services.lakebase_permission_service import (
    _quote_pg_role as permission_quote_pg_role,
)

VALIDATE_IDENTIFIER_FNS = [
    pytest.param(schema_validate_identifier, id="schema_service"),
    pytest.param(service_validate_identifier, id="lakebase_service"),
    pytest.param(migration_validate_identifier, id="migration_service"),
]

QUOTE_PG_ROLE_FNS = [
    pytest.param(permission_quote_pg_role, id="permission_service"),
    pytest.param(schema_quote_pg_role, id="schema_service"),
]


class TestLakebaseValidateIdentifier:

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    @pytest.mark.parametrize(
        "name",
        ["users", "execution_logs", "_private", "Table123", "a", "_", "ALLCAPS"],
    )
    def test_accepts_valid(self, fn, name):
        assert fn(name) == name

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    def test_rejects_empty(self, fn):
        with pytest.raises(ValueError):
            fn("")

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    def test_rejects_none(self, fn):
        with pytest.raises((ValueError, TypeError, AttributeError)):
            fn(None)

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    def test_rejects_leading_digit(self, fn):
        with pytest.raises(ValueError):
            fn("123table")

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    @pytest.mark.parametrize(
        "payload",
        [
            "users; DROP TABLE users; --",
            "users UNION SELECT * FROM passwords",
            "schema.table",
            "my-table",
            "my table",
            "table'name",
            "table()",
            "table\nname",
            "user@domain",
            "users;",
            "users--",
        ],
    )
    def test_rejects_injection_payloads(self, fn, payload):
        with pytest.raises(ValueError):
            fn(payload)

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    def test_error_message_includes_kind(self, fn):
        with pytest.raises(ValueError, match="table name"):
            fn("bad;input", "table name")

    @pytest.mark.parametrize("fn", VALIDATE_IDENTIFIER_FNS)
    def test_error_message_default_kind(self, fn):
        with pytest.raises(ValueError, match="identifier"):
            fn("bad;input")


class TestLakebaseQuotePgRole:

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_simple_email(self, fn):
        result = fn("user@example.com")
        assert result == '"user@example.com"'

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_email_with_dots(self, fn):
        result = fn("john.doe@company.co.uk")
        assert result == '"john.doe@company.co.uk"'

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_email_with_plus(self, fn):
        result = fn("user+tag@example.com")
        assert result == '"user+tag@example.com"'

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_email_with_underscore(self, fn):
        result = fn("user_name@example.com")
        assert result == '"user_name@example.com"'

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_empty(self, fn):
        with pytest.raises(ValueError):
            fn("")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_none(self, fn):
        with pytest.raises((ValueError, TypeError, AttributeError)):
            fn(None)

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_plain_string(self, fn):
        with pytest.raises(ValueError):
            fn("not-an-email")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_missing_local_part(self, fn):
        with pytest.raises(ValueError):
            fn("@domain.com")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_missing_domain(self, fn):
        with pytest.raises(ValueError):
            fn("user@")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_sql_injection(self, fn):
        with pytest.raises(ValueError):
            fn('user"; DROP TABLE users; --@example.com')

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_semicolon_in_local(self, fn):
        with pytest.raises(ValueError):
            fn("user;drop@example.com")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_newline(self, fn):
        with pytest.raises(ValueError):
            fn("user\n@example.com")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_rejects_double_quote(self, fn):
        with pytest.raises(ValueError):
            fn('user"test@example.com')

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_error_mentions_role(self, fn):
        with pytest.raises(ValueError, match="PostgreSQL role"):
            fn("not-valid")

    @pytest.mark.parametrize("fn", QUOTE_PG_ROLE_FNS)
    def test_output_wrapped_in_double_quotes(self, fn):
        result = fn("test@example.com")
        assert result.startswith('"') and result.endswith('"')


class TestLakebaseCrossImplementationConsistency:

    @pytest.mark.parametrize("name", ["users", "_private", "Table123"])
    def test_validate_consistent(self, name):
        results = [fn(name) for fn in [
            schema_validate_identifier,
            service_validate_identifier,
            migration_validate_identifier,
        ]]
        assert all(r == name for r in results)

    @pytest.mark.parametrize("bad", ["123x", "my-table", "a.b", ""])
    def test_validate_rejects_consistently(self, bad):
        for fn in [schema_validate_identifier, service_validate_identifier, migration_validate_identifier]:
            with pytest.raises(ValueError):
                fn(bad)

    @pytest.mark.parametrize("email", ["user@example.com", "a+b@c.co.uk"])
    def test_quote_pg_role_consistent(self, email):
        assert permission_quote_pg_role(email) == schema_quote_pg_role(email)


# ---------------------------------------------------------------------------
# LakebaseService.test_connection — scope error detection
# ---------------------------------------------------------------------------
class TestLakebaseServiceTestConnection:
    """Tests for test_connection error handling."""

    @pytest.mark.asyncio
    async def test_scope_error_returns_missing_database_resource(self):
        """test_connection should return MISSING_DATABASE_RESOURCE for postgres scope errors."""
        from src.services.lakebase_service import LakebaseService

        svc = LakebaseService.__new__(LakebaseService)
        svc.connection_service = MagicMock()

        mock_ws = MagicMock()
        mock_ws.database.get_database_instance.side_effect = Exception(
            "Provided OAuth token does not have required scopes: postgres"
        )

        with patch.object(svc, "get_workspace_client", new_callable=AsyncMock, return_value=mock_ws), \
             patch.dict("os.environ", {"DATABRICKS_CLIENT_ID": "test-spn-id"}):
            result = await svc.test_connection("my-instance")

        assert result["success"] is False
        assert result["error_code"] == "MISSING_DATABASE_RESOURCE"
        assert result["client_id"] == "test-spn-id"
        assert "required scopes: postgres" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_scope_error_without_client_id(self):
        """test_connection should return empty client_id when env var not set."""
        from src.services.lakebase_service import LakebaseService

        svc = LakebaseService.__new__(LakebaseService)
        svc.connection_service = MagicMock()

        mock_ws = MagicMock()
        mock_ws.database.get_database_instance.side_effect = Exception(
            "Provided OAuth token does not have required scopes: postgres"
        )

        with patch.object(svc, "get_workspace_client", new_callable=AsyncMock, return_value=mock_ws), \
             patch.dict("os.environ", {}, clear=True):
            result = await svc.test_connection("my-instance")

        assert result["success"] is False
        assert result["error_code"] == "MISSING_DATABASE_RESOURCE"
        assert result["client_id"] == ""

    @pytest.mark.asyncio
    async def test_generic_error_has_no_error_code(self):
        """test_connection should NOT set error_code for non-scope errors."""
        from src.services.lakebase_service import LakebaseService

        svc = LakebaseService.__new__(LakebaseService)
        svc.connection_service = MagicMock()

        mock_ws = MagicMock()
        mock_ws.database.get_database_instance.side_effect = Exception("Connection refused")

        with patch.object(svc, "get_workspace_client", new_callable=AsyncMock, return_value=mock_ws):
            result = await svc.test_connection("my-instance")

        assert result["success"] is False
        assert "error_code" not in result
        assert result["error"] == "Connection refused"


class TestMigrateExistingDataStreamSequenceReset:
    """Tests for the sequence reset step inside migrate_existing_data_stream."""

    def _build_service(self, mock_lb_engine):
        """Create a LakebaseService with mocked sub-services."""
        svc = LakebaseService.__new__(LakebaseService)
        conn_svc = MagicMock()
        # Async methods
        conn_svc.generate_credentials = AsyncMock()
        conn_svc.get_username = AsyncMock(return_value="user@example.com")
        # Sync methods stay as MagicMock
        conn_svc.create_lakebase_engine_sync = MagicMock(return_value=mock_lb_engine)
        svc.connection_service = conn_svc
        svc.schema_service = MagicMock()
        svc.permission_service = MagicMock()
        svc.config_repository = AsyncMock()
        svc.session = MagicMock()
        svc.user_token = "tok"
        svc.user_email = "user@example.com"
        svc.migration_service = MagicMock()
        return svc

    async def _collect_events(self, async_gen):
        """Consume all events from an async generator."""
        events = []
        async for event in async_gen:
            events.append(event)
        return events

    def _make_lb_engine(self):
        """Build a mock Lakebase engine with connect/begin context managers."""
        mock_lb_engine = MagicMock()
        mock_verify_conn = MagicMock()
        mock_verify_result = MagicMock()
        mock_verify_result.scalar.return_value = "user@example.com"
        mock_verify_conn.execute.return_value = mock_verify_result
        mock_verify_conn.__enter__ = MagicMock(return_value=mock_verify_conn)
        mock_verify_conn.__exit__ = MagicMock(return_value=False)
        mock_lb_engine.connect.return_value = mock_verify_conn
        mock_lb_begin = MagicMock()
        mock_lb_begin.__enter__ = MagicMock(return_value=mock_lb_begin)
        mock_lb_begin.__exit__ = MagicMock(return_value=False)
        mock_lb_engine.begin.return_value = mock_lb_begin
        return mock_lb_engine

    def _make_mig_service(self, reset_return=None, reset_side_effect=None):
        """Build a mock migration service for single-table migration."""
        mock_mig = MagicMock()
        mock_mig.get_table_list_sync.return_value = ["users"]
        mock_mig.get_sorted_tables.return_value = ["users"]
        mock_mig.get_migration_waves.return_value = [["users"]]
        mock_mig.migrate_table_data_sync.return_value = (5, None)
        if reset_side_effect:
            mock_mig.reset_sequences_sync.side_effect = reset_side_effect
        elif reset_return is not None:
            mock_mig.reset_sequences_sync.return_value = reset_return
        else:
            mock_mig.reset_sequences_sync.return_value = [("users_id_seq", True, None)]
        return mock_mig

    def _setup_service_and_mocks(self, MockMigSvc, mock_create_engine, mock_settings, mock_mig):
        """Wire up service, credential mock, engine, and migration service."""
        mock_lb_engine = self._make_lb_engine()
        svc = self._build_service(mock_lb_engine)

        mock_cred = MagicMock()
        mock_cred.token = "test-token"
        svc.connection_service.generate_credentials = AsyncMock(return_value=mock_cred)

        mock_settings.DATABASE_URI = "sqlite:///test.db"
        mock_create_engine.return_value = MagicMock()
        MockMigSvc.return_value = mock_mig

        svc.schema_service.create_schema_sync = MagicMock()
        svc.schema_service.create_tables_sync_stream = MagicMock(
            return_value=iter([{"type": "success", "message": "ok", "table": "users", "success": True}])
        )
        return svc, mock_lb_engine

    @pytest.mark.asyncio
    @patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True)
    @patch("src.services.lakebase_service.settings")
    @patch("src.services.lakebase_service.create_engine")
    @patch("src.services.lakebase_service.LakebaseMigrationService")
    async def test_sequence_reset_called_after_migration(
        self, MockMigSvc, mock_create_engine, mock_settings
    ):
        """After successful migration, reset_sequences_sync should be called."""
        mock_mig = self._make_mig_service(reset_return=[("users_id_seq", True, None)])
        svc, mock_lb_engine = self._setup_service_and_mocks(
            MockMigSvc, mock_create_engine, mock_settings, mock_mig
        )

        events = await self._collect_events(
            svc.migrate_existing_data_stream("inst", "https://example.com", migrate_data=True)
        )
        messages = [e.get("message", "") for e in events]

        mock_mig.reset_sequences_sync.assert_called_once_with(mock_lb_engine, ["users"])
        assert any("Reset 1 database sequence" in m for m in messages)

    @pytest.mark.asyncio
    @patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True)
    @patch("src.services.lakebase_service.settings")
    @patch("src.services.lakebase_service.create_engine")
    @patch("src.services.lakebase_service.LakebaseMigrationService")
    async def test_sequence_reset_error_yields_warning(
        self, MockMigSvc, mock_create_engine, mock_settings
    ):
        """If reset_sequences_sync raises, a warning event should be yielded."""
        mock_mig = self._make_mig_service(reset_side_effect=Exception("connection lost"))
        svc, _ = self._setup_service_and_mocks(
            MockMigSvc, mock_create_engine, mock_settings, mock_mig
        )

        events = await self._collect_events(
            svc.migrate_existing_data_stream("inst", "https://example.com", migrate_data=True)
        )
        messages = [e.get("message", "") for e in events]

        assert any("Sequence reset encountered an error" in m for m in messages)

    @pytest.mark.asyncio
    @patch("src.services.lakebase_service.LAKEBASE_AVAILABLE", True)
    @patch("src.services.lakebase_service.settings")
    @patch("src.services.lakebase_service.create_engine")
    @patch("src.services.lakebase_service.LakebaseMigrationService")
    async def test_sequence_reset_reports_failed_sequences(
        self, MockMigSvc, mock_create_engine, mock_settings
    ):
        """Failed individual sequences should be reported as warnings."""
        mock_mig = self._make_mig_service(reset_return=[
            ("users_id_seq", True, None),
            ("bad_id_seq", False, "table not found"),
        ])
        svc, _ = self._setup_service_and_mocks(
            MockMigSvc, mock_create_engine, mock_settings, mock_mig
        )

        events = await self._collect_events(
            svc.migrate_existing_data_stream("inst", "https://example.com", migrate_data=True)
        )
        messages = [e.get("message", "") for e in events]

        assert any("Reset 1 database sequence" in m for m in messages)
        assert any("Could not reset sequence bad_id_seq" in m for m in messages)
