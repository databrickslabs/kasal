"""
Unit tests for crews export router.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi import HTTPException
import io
import zipfile

from src.api.crews_export_router import (
    router,
    get_export_service,
    get_deployment_service,
    export_crew,
    download_export,
    deploy_crew,
    get_deployment_status,
    delete_deployment,
)
from src.schemas.crew_export import (
    CrewExportRequest,
    CrewExportResponse,
    ExportFormat,
    ExportOptions,
    DeploymentRequest,
    DeploymentResponse,
    DeploymentTarget,
    ModelServingConfig,
    DeploymentStatus,
)


class TestGetExportService:
    """Tests for get_export_service dependency."""

    @pytest.mark.asyncio
    async def test_returns_export_service(self):
        """Test that get_export_service returns a CrewExportService instance."""
        mock_session = AsyncMock()

        with patch('src.api.crews_export_router.CrewExportService') as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            result = await get_export_service(mock_session)

            mock_service_class.assert_called_once_with(session=mock_session)
            assert result == mock_service


class TestGetDeploymentService:
    """Tests for get_deployment_service dependency."""

    @pytest.mark.asyncio
    async def test_returns_deployment_service(self):
        """Test that get_deployment_service returns a CrewDeploymentService instance."""
        mock_session = AsyncMock()

        with patch('src.api.crews_export_router.CrewDeploymentService') as mock_service_class:
            mock_service = MagicMock()
            mock_service_class.return_value = mock_service

            result = await get_deployment_service(mock_session)

            mock_service_class.assert_called_once_with(session=mock_session)
            assert result == mock_service


class TestExportCrew:
    """Tests for export_crew endpoint."""

    @pytest.fixture
    def mock_service(self):
        """Create a mock export service."""
        service = AsyncMock()
        return service

    @pytest.fixture
    def valid_group_context(self):
        """Create a valid group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        context.group_ids = ["group-1"]
        return context

    @pytest.fixture
    def operator_group_context(self):
        """Create an operator (non-admin/editor) group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        context.group_ids = ["group-1"]
        context.role = "operator"
        return context

    @pytest.mark.asyncio
    async def test_export_crew_success(self, mock_service, valid_group_context):
        """Test successful crew export."""
        crew_id = "test-crew-123"
        request = CrewExportRequest(
            export_format=ExportFormat.DATABRICKS_NOTEBOOK,
            options=ExportOptions()
        )

        mock_service.export_crew.return_value = {
            'crew_id': crew_id,
            'crew_name': 'Test Crew',
            'export_format': 'databricks_notebook',
            'notebook': {},
            'notebook_content': '{}',
            'metadata': {},
            'generated_at': '2025-01-01 00:00:00 UTC',
        }

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            result = await export_crew(
                crew_id=crew_id,
                request=request,
                service=mock_service,
                group_context=valid_group_context
            )

        assert result.crew_id == crew_id
        assert result.crew_name == 'Test Crew'
        assert f"/api/crews/{crew_id}/export/download" in result.download_url

    @pytest.mark.asyncio
    async def test_export_crew_forbidden_for_non_editors(self, mock_service, valid_group_context):
        """Test that non-editors cannot export crews."""
        crew_id = "test-crew-123"
        request = CrewExportRequest(
            export_format=ExportFormat.DATABRICKS_NOTEBOOK,
            options=ExportOptions()
        )

        with patch('src.api.crews_export_router.check_role_in_context', return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                await export_crew(
                    crew_id=crew_id,
                    request=request,
                    service=mock_service,
                    group_context=valid_group_context
                )

        assert exc_info.value.status_code == 403
        assert "editors and admins" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_export_crew_invalid_group_context(self, mock_service):
        """Test export with invalid group context."""
        crew_id = "test-crew-123"
        request = CrewExportRequest(
            export_format=ExportFormat.DATABRICKS_NOTEBOOK,
            options=ExportOptions()
        )

        invalid_context = MagicMock()
        invalid_context.is_valid.return_value = False

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                await export_crew(
                    crew_id=crew_id,
                    request=request,
                    service=mock_service,
                    group_context=invalid_context
                )

        assert exc_info.value.status_code == 400
        assert "valid group context" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_export_crew_not_found(self, mock_service, valid_group_context):
        """Test export when crew is not found."""
        crew_id = "non-existent-crew"
        request = CrewExportRequest(
            export_format=ExportFormat.DATABRICKS_NOTEBOOK,
            options=ExportOptions()
        )

        mock_service.export_crew.side_effect = ValueError("Crew not found")

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            with pytest.raises(HTTPException) as exc_info:
                await export_crew(
                    crew_id=crew_id,
                    request=request,
                    service=mock_service,
                    group_context=valid_group_context
                )

        assert exc_info.value.status_code == 404


class TestDownloadExport:
    """Tests for download_export endpoint."""

    @pytest.fixture
    def mock_service(self):
        """Create a mock export service."""
        return AsyncMock()

    @pytest.fixture
    def valid_group_context(self):
        """Create a valid group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_download_python_project(self, mock_service, valid_group_context):
        """Test downloading a Python project export."""
        crew_id = "test-crew-123"

        mock_service.export_crew.return_value = {
            'crew_id': crew_id,
            'crew_name': 'Test Crew',
            'export_format': 'python_project',
            'files': [
                {'path': 'README.md', 'content': '# Test'},
                {'path': 'requirements.txt', 'content': 'crewai>=0.80.0'},
            ],
            'metadata': {},
            'generated_at': '2025-01-01 00:00:00 UTC',
        }

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            result = await download_export(
                crew_id=crew_id,
                service=mock_service,
                group_context=valid_group_context,
                format=ExportFormat.PYTHON_PROJECT,
                include_custom_tools=True,
                include_comments=True,
                include_tracing=True,
                include_evaluation=True,
                include_deployment=True,
                model_override=None
            )

        # Check that it returns a streaming response
        assert result.media_type == 'application/zip'

    @pytest.mark.asyncio
    async def test_download_databricks_notebook(self, mock_service, valid_group_context):
        """Test downloading a Databricks notebook export."""
        crew_id = "test-crew-123"

        mock_service.export_crew.return_value = {
            'crew_id': crew_id,
            'crew_name': 'Test Crew',
            'export_format': 'databricks_notebook',
            'notebook': {},
            'notebook_content': '{"cells": []}',
            'metadata': {},
            'generated_at': '2025-01-01 00:00:00 UTC',
        }

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            result = await download_export(
                crew_id=crew_id,
                service=mock_service,
                group_context=valid_group_context,
                format=ExportFormat.DATABRICKS_NOTEBOOK,
                include_custom_tools=True,
                include_comments=True,
                include_tracing=True,
                include_evaluation=True,
                include_deployment=True,
                model_override=None
            )

        # Check that it returns an ipynb response
        assert result.media_type == 'application/x-ipynb+json'


class TestDeployCrew:
    """Tests for deploy_crew endpoint."""

    @pytest.fixture
    def mock_service(self):
        """Create a mock deployment service."""
        return AsyncMock()

    @pytest.fixture
    def valid_group_context(self):
        """Create a valid group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_deploy_crew_success(self, mock_service, valid_group_context):
        """Test successful crew deployment."""
        crew_id = "test-crew-123"
        request = DeploymentRequest(
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            config=ModelServingConfig(model_name="test-model")
        )

        mock_service.deploy_to_model_serving.return_value = DeploymentResponse(
            crew_id=crew_id,
            crew_name="Test Crew",
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            model_name="test-model",
            endpoint_name="test-model",
            endpoint_status=DeploymentStatus.PENDING,
        )

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            result = await deploy_crew(
                crew_id=crew_id,
                request=request,
                service=mock_service,
                group_context=valid_group_context
            )

        assert result.crew_id == crew_id
        assert result.model_name == "test-model"

    @pytest.mark.asyncio
    async def test_deploy_crew_admin_only(self, mock_service, valid_group_context):
        """Test that only admins can deploy crews."""
        crew_id = "test-crew-123"
        request = DeploymentRequest(
            deployment_target=DeploymentTarget.DATABRICKS_MODEL_SERVING,
            config=ModelServingConfig(model_name="test-model")
        )

        with patch('src.api.crews_export_router.check_role_in_context', return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                await deploy_crew(
                    crew_id=crew_id,
                    request=request,
                    service=mock_service,
                    group_context=valid_group_context
                )

        assert exc_info.value.status_code == 403
        assert "admins" in exc_info.value.detail


class TestGetDeploymentStatus:
    """Tests for get_deployment_status endpoint."""

    @pytest.fixture
    def valid_group_context(self):
        """Create a valid group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_get_deployment_status_success(self, valid_group_context):
        """Test successful status retrieval."""
        crew_id = "test-crew-123"
        endpoint_name = "test-endpoint"

        mock_endpoint = MagicMock()
        mock_endpoint.state = MagicMock()
        mock_endpoint.state.ready = MagicMock(value="READY")
        mock_endpoint.state.config_update = None
        mock_endpoint.pending_config = None
        mock_endpoint.creator = "test-user"
        mock_endpoint.creation_timestamp = 1234567890
        mock_endpoint.last_updated_timestamp = 1234567890
        mock_endpoint.config = MagicMock()

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            # Patch the import inside the function
            with patch('databricks.sdk.WorkspaceClient') as mock_ws:
                mock_ws_instance = MagicMock()
                mock_ws.return_value = mock_ws_instance
                mock_ws_instance.serving_endpoints.get.return_value = mock_endpoint

                result = await get_deployment_status(
                    crew_id=crew_id,
                    group_context=valid_group_context,
                    endpoint_name=endpoint_name
                )

        assert result["endpoint_name"] == endpoint_name
        assert result["state"] == "READY"


class TestDeleteDeployment:
    """Tests for delete_deployment endpoint."""

    @pytest.fixture
    def valid_group_context(self):
        """Create a valid group context."""
        context = MagicMock()
        context.is_valid.return_value = True
        return context

    @pytest.mark.asyncio
    async def test_delete_deployment_success(self, valid_group_context):
        """Test successful deployment deletion."""
        crew_id = "test-crew-123"
        endpoint_name = "test-endpoint"

        with patch('src.api.crews_export_router.check_role_in_context', return_value=True):
            # Patch the import inside the function
            with patch('databricks.sdk.WorkspaceClient') as mock_ws:
                mock_ws_instance = MagicMock()
                mock_ws.return_value = mock_ws_instance

                result = await delete_deployment(
                    crew_id=crew_id,
                    endpoint_name=endpoint_name,
                    group_context=valid_group_context
                )

                # Assert inside the context manager where mock is active
                mock_ws_instance.serving_endpoints.delete.assert_called_once_with(endpoint_name)

        assert result["endpoint_name"] == endpoint_name
        assert "deleted successfully" in result["message"]

    @pytest.mark.asyncio
    async def test_delete_deployment_admin_only(self, valid_group_context):
        """Test that only admins can delete deployments."""
        crew_id = "test-crew-123"
        endpoint_name = "test-endpoint"

        with patch('src.api.crews_export_router.check_role_in_context', return_value=False):
            with pytest.raises(HTTPException) as exc_info:
                await delete_deployment(
                    crew_id=crew_id,
                    endpoint_name=endpoint_name,
                    group_context=valid_group_context
                )

        assert exc_info.value.status_code == 403
        assert "admins" in exc_info.value.detail
