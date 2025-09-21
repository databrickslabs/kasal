"""
Helper module for testing permission-based access control in router tests.

This module provides reusable fixtures and test helpers for validating
that endpoints properly enforce role-based permissions.
"""

import pytest
from src.utils.user_context import GroupContext
from fastapi.testclient import TestClient


def create_group_context_with_role(role: str) -> GroupContext:
    """Create a GroupContext with a specific role."""
    return GroupContext(
        group_ids=["group-123"],
        group_email=f"{role}@example.com",
        email_domain="example.com",
        user_id=f"user-{role}-123",
        user_role=role
    )


def get_forbidden_response_test(app, mock_group_context, endpoint, method, expected_message, json_data=None):
    """
    Test that a specific role is forbidden from accessing an endpoint.

    Args:
        app: The FastAPI app instance
        mock_group_context: The group context with the role to test
        endpoint: The endpoint URL
        method: The HTTP method (post, put, patch, delete)
        expected_message: The expected error message
        json_data: Optional JSON data for the request
    """
    from src.core.dependencies import get_group_context

    # Override group context
    async def override_get_group_context():
        return mock_group_context

    app.dependency_overrides[get_group_context] = override_get_group_context
    client = TestClient(app)

    # Make the request
    method_func = getattr(client, method.lower())
    if json_data:
        response = method_func(endpoint, json=json_data)
    else:
        response = method_func(endpoint)

    # Assert forbidden
    assert response.status_code == 403
    assert expected_message in response.json()["detail"]

    return response


# Common test fixtures for different roles
@pytest.fixture
def mock_group_context_admin():
    """Create a mock group context with admin role."""
    return create_group_context_with_role("admin")


@pytest.fixture
def mock_group_context_editor():
    """Create a mock group context with editor role."""
    return create_group_context_with_role("editor")


@pytest.fixture
def mock_group_context_operator():
    """Create a mock group context with operator role."""
    return create_group_context_with_role("operator")


# Permission test generators for common patterns
class PermissionTestGenerator:
    """Generate permission tests for common CRUD operations."""

    @staticmethod
    def test_admin_only_create(app, forbidden_context, endpoint, create_data, error_message="Only admins can create"):
        """Test that only admins can create resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "post", error_message,
            json_data=create_data.model_dump() if hasattr(create_data, 'model_dump') else create_data
        )

    @staticmethod
    def test_admin_only_update(app, forbidden_context, endpoint, update_data, error_message="Only admins can update"):
        """Test that only admins can update resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "put", error_message,
            json_data=update_data.model_dump() if hasattr(update_data, 'model_dump') else update_data
        )

    @staticmethod
    def test_admin_only_delete(app, forbidden_context, endpoint, error_message="Only admins can delete"):
        """Test that only admins can delete resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "delete", error_message
        )

    @staticmethod
    def test_admin_editor_only_create(app, forbidden_context, endpoint, create_data, error_message="Only admins and editors can create"):
        """Test that only admins and editors can create resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "post", error_message,
            json_data=create_data.model_dump() if hasattr(create_data, 'model_dump') else create_data
        )

    @staticmethod
    def test_admin_editor_only_update(app, forbidden_context, endpoint, update_data, error_message="Only admins and editors can update"):
        """Test that only admins and editors can update resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "put", error_message,
            json_data=update_data.model_dump() if hasattr(update_data, 'model_dump') else update_data
        )

    @staticmethod
    def test_admin_editor_only_delete(app, forbidden_context, endpoint, error_message="Only admins and editors can delete"):
        """Test that only admins and editors can delete resources."""
        return get_forbidden_response_test(
            app, forbidden_context, endpoint, "delete", error_message
        )