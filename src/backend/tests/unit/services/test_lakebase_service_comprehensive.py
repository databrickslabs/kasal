import pytest
from unittest.mock import Mock, patch, AsyncMock
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
