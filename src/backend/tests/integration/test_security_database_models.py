"""
Security tests for database management and model configuration endpoints.

Tests authorization requirements for critical operations:
- Database export/import/list (requires system admin)
- Model toggle (requires workspace admin)

These are integration tests that verify the actual authorization flow.
"""

import pytest
import pytest_asyncio
from fastapi import status
from httpx import AsyncClient, ASGITransport
from src.main import app


@pytest_asyncio.fixture
async def async_client():
    """Create async HTTP client for testing."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


class TestDatabaseManagementSecurity:
    """Test security for database management endpoints.

    These tests verify that non-system-admin users (including workspace admins)
    cannot export, import, or list database backups.
    """

    @pytest.mark.asyncio
    async def test_export_database_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that non-system-admin users cannot export the database."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/database-management/export",
            json={},  # Empty body to use defaults
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrators" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_import_database_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that non-system-admin users cannot import database backups."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/database-management/import",
            json={
                "catalog": "users",
                "schema": "default",
                "volume_name": "kasal_backups",
                "backup_filename": "backup_20240101_120000.db"
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrators" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_list_backups_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that non-system-admin users cannot list database backups."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/database-management/list-backups",
            json={},  # Empty body to use defaults
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrators" in response.json()["detail"].lower()


class TestModelConfigurationSecurity:
    """Test security for model configuration endpoints.

    These tests verify that non-admin users cannot toggle, create,
    update, or delete model configurations.
    """

    @pytest.mark.asyncio
    async def test_toggle_model_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot toggle model configurations."""
        # Regular user should get 403
        response = await async_client.patch(
            "/api/v1/models/test-model/toggle",
            json={"enabled": False},
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_create_model_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot create model configurations."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/models",
            json={
                "key": "new-model",
                "name": "New Model",
                "provider": "test",
                "enabled": True
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_update_model_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot update model configurations."""
        # Regular user should get 403
        response = await async_client.put(
            "/api/v1/models/test-model",
            json={"key": "test-model", "name": "Test Model"},  # Minimal required fields
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_delete_model_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot delete model configurations."""
        # Regular user should get 403
        response = await async_client.delete(
            "/api/v1/models/test-model",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_enable_all_models_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot enable all models."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/models/enable-all",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_disable_all_models_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot disable all models."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/models/disable-all",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_regular_user_can_read_models(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users can read model configurations (GET operations)."""
        # GET operations should work for regular users
        response = await async_client.get(
            "/api/v1/models",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should succeed - users can read models
        assert response.status_code == status.HTTP_200_OK


class TestDatabricksConfigurationSecurity:
    """Test security for Databricks configuration endpoints.

    These tests verify that non-admin users cannot view or manage
    Databricks configuration settings.
    """

    @pytest.mark.asyncio
    async def test_get_databricks_config_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot view Databricks configuration."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/databricks/config",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_set_databricks_config_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot set Databricks configuration."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/databricks/config",
            json={
                "workspace_url": "https://example.databricks.com",
                "warehouse_id": "test-warehouse",
                "catalog": "test_catalog",
                "schema": "test_schema",
                "enabled": False  # Disabled config doesn't require all fields
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_check_personal_token_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot check personal token requirements."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/databricks/status/personal-token-required",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_check_connection_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot check Databricks connection status."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/databricks/connection",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_get_environment_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot view Databricks environment information."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/databricks/environment",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()


class TestMemoryBackendSecurity:
    """Test security for memory backend endpoints.

    These tests verify that non-admin users cannot manage memory backend
    configurations and Databricks Vector Search resources.
    """

    @pytest.mark.asyncio
    async def test_one_click_setup_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot perform one-click Databricks setup."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/memory-backend/databricks/one-click-setup",
            json={
                "workspace_url": "https://example.databricks.com",
                "catalog": "test",
                "schema": "test",
                "embedding_dimension": 1024
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_create_index_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot create Databricks indexes."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/memory-backend/databricks/create-index",
            json={
                "config": {
                    "endpoint_name": "test-endpoint",
                    "embedding_dimension": 1024
                },
                "index_type": "short_term",
                "catalog": "test",
                "schema": "test",
                "table_name": "test_table"
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_delete_index_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot delete Databricks indexes."""
        # Regular user should get 403
        response = await async_client.request(
            "DELETE",
            "/api/v1/memory-backend/databricks/index",
            json={
                "workspace_url": "https://example.databricks.com",
                "index_name": "test.test.index",
                "endpoint_name": "test-endpoint"
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_delete_endpoint_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot delete Databricks endpoints."""
        # Regular user should get 403
        response = await async_client.request(
            "DELETE",
            "/api/v1/memory-backend/databricks/endpoint",
            json={
                "workspace_url": "https://example.databricks.com",
                "endpoint_name": "test-endpoint"
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_empty_index_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot empty Databricks indexes."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/memory-backend/databricks/empty-index",
            json={
                "workspace_url": "https://example.databricks.com",
                "index_name": "test.test.index",
                "endpoint_name": "test-endpoint",
                "index_type": "short_term",
                "embedding_dimension": 1024
            },
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_switch_to_disabled_requires_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot switch memory backend to disabled mode."""
        # Regular user should get 403
        response = await async_client.post(
            "/api/v1/memory-backend/configs/switch-to-disabled",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "admin" in response.json()["detail"].lower()


class TestGroupUserEnumerationSecurity:
    """Test security for group user enumeration endpoints.

    These tests verify that users cannot enumerate users in workspaces
    they don't belong to or don't have admin rights in.
    """

    @pytest.mark.asyncio
    async def test_list_users_requires_membership(
        self,
        async_client: AsyncClient
    ):
        """Test that users cannot list users in groups they don't belong to."""
        # User trying to access a non-existent or other workspace should get 403 or 404
        response = await async_client.get(
            "/api/v1/groups/other_workspace_id/users",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-members or 404 if group doesn't exist
        # The security check happens before the group lookup in the fixed code
        assert response.status_code in [status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
        if response.status_code == status.HTTP_403_FORBIDDEN:
            assert "member" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_list_users_requires_admin_role(
        self,
        async_client: AsyncClient
    ):
        """Test that the endpoint requires both membership and admin role."""
        # This test verifies the security is in place by trying to access
        # a workspace the user is not a member of or doesn't have admin rights in
        response = await async_client.get(
            "/api/v1/groups/test_workspace_restricted/users",
            headers={
                "X-Forwarded-Email": "member_not_admin@example.com"
            }
        )

        # Should be forbidden (403) or 404 (group doesn't exist)
        # The important thing is that unauthorized access is blocked
        assert response.status_code in [status.HTTP_403_FORBIDDEN, status.HTTP_404_NOT_FOUND]
        if response.status_code == status.HTTP_403_FORBIDDEN:
            detail_lower = response.json()["detail"].lower()
            assert "admin" in detail_lower or "member" in detail_lower


class TestEngineConfigurationSecurity:
    """Test security for engine configuration endpoints.

    These tests verify that non-system-admin users cannot access or modify
    engine configuration settings.
    """

    @pytest.mark.asyncio
    async def test_get_flow_enabled_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot view flow enabled status."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/engine-config/crewai/flow-enabled",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrator" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_set_flow_enabled_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot modify flow enabled status."""
        # Regular user should get 403
        response = await async_client.patch(
            "/api/v1/engine-config/crewai/flow-enabled",
            json={"flow_enabled": True},
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrator" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_get_debug_tracing_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot view debug tracing status."""
        # Regular user should get 403
        response = await async_client.get(
            "/api/v1/engine-config/crewai/debug-tracing",
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrator" in response.json()["detail"].lower()

    @pytest.mark.asyncio
    async def test_set_debug_tracing_requires_system_admin(
        self,
        async_client: AsyncClient
    ):
        """Test that regular users cannot modify debug tracing status."""
        # Regular user should get 403
        response = await async_client.patch(
            "/api/v1/engine-config/crewai/debug-tracing",
            json={"debug_tracing": True},
            headers={
                "X-Forwarded-Email": "regular_user@example.com"
            }
        )

        # Should be forbidden (403) for non-system-admin
        assert response.status_code == status.HTTP_403_FORBIDDEN
        assert "system administrator" in response.json()["detail"].lower()
