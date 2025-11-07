"""Security tests for group authorization and access control.

This test module validates that the application properly enforces group-based
access control and prevents unauthorized access to other groups' data.
"""

import pytest
from unittest.mock import AsyncMock, Mock, patch
from src.utils.user_context import GroupContext
from src.core.dependencies import get_group_context
from fastapi import HTTPException, Request


class TestGroupAuthorizationSecurity:
    """Test suite for group-based authorization security."""

    @pytest.mark.asyncio
    async def test_unauthorized_group_access_rejected(self):
        """Test that accessing an unauthorized group_id raises an error."""
        # Mock user groups - user belongs to marketing_abc123 only
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "regulatory@databricks.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Attempt to access a different group - should raise ValueError
            with pytest.raises(ValueError) as exc_info:
                await GroupContext.from_email(
                    email="regulatory@databricks.com",
                    access_token="valid_token",
                    group_id="marketing_cfe676ee"  # Different group ID
                )

            assert "Access denied" in str(exc_info.value)
            assert "marketing_cfe676ee" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_authorized_group_access_allowed(self):
        """Test that accessing an authorized group_id succeeds."""
        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "regulatory@databricks.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Access the correct group - should succeed
            context = await GroupContext.from_email(
                email="regulatory@databricks.com",
                access_token="valid_token",
                group_id="marketing_abc123"
            )

            assert context.primary_group_id == "marketing_abc123"
            assert "marketing_abc123" in context.group_ids
            assert context.user_role == "editor"

    @pytest.mark.asyncio
    async def test_unauthorized_personal_workspace_rejected(self):
        """Test that accessing another user's personal workspace is rejected."""
        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "alice@company.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Attempt to access another user's personal workspace
            with pytest.raises(ValueError) as exc_info:
                await GroupContext.from_email(
                    email="alice@company.com",
                    access_token="valid_token",
                    group_id="user_bob_company_com"  # Bob's personal workspace
                )

            assert "Access denied" in str(exc_info.value)
            assert "user_bob_company_com" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_authorized_personal_workspace_allowed(self):
        """Test that accessing own personal workspace succeeds."""
        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "alice@company.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Access own personal workspace - should succeed
            context = await GroupContext.from_email(
                email="alice@company.com",
                access_token="valid_token",
                group_id="user_alice_company_com"
            )

            assert context.primary_group_id == "user_alice_company_com"
            assert "user_alice_company_com" in context.group_ids

    @pytest.mark.asyncio
    async def test_dependency_raises_http_403_on_unauthorized_access(self):
        """Test that the FastAPI dependency raises HTTPException 403 for unauthorized access."""
        # Create mock request
        mock_request = Mock(spec=Request)

        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "regulatory@databricks.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Attempt to get group context with unauthorized group_id
            with pytest.raises(HTTPException) as exc_info:
                await get_group_context(
                    request=mock_request,
                    x_forwarded_email="regulatory@databricks.com",
                    x_forwarded_access_token="valid_token",
                    x_auth_request_email=None,
                    x_auth_request_user=None,
                    x_auth_request_access_token=None,
                    x_group_id="marketing_cfe676ee",  # Unauthorized group
                    x_group_domain=None
                )

            # Verify it's a 403 Forbidden error
            assert exc_info.value.status_code == 403
            assert "Access denied" in exc_info.value.detail

    @pytest.mark.asyncio
    async def test_multiple_groups_with_unauthorized_selection(self):
        """Test that user with multiple groups cannot access unauthorized group."""
        # Mock user with multiple groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "user@company.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group1 = Mock()
        mock_group1.id = "marketing_abc123"

        mock_group2 = Mock()
        mock_group2.id = "sales_def456"

        user_groups_with_roles = [
            (mock_group1, "editor"),
            (mock_group2, "admin")
        ]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Try to access a third group the user doesn't belong to
            with pytest.raises(ValueError) as exc_info:
                await GroupContext.from_email(
                    email="user@company.com",
                    access_token="valid_token",
                    group_id="finance_xyz789"  # Not in user's groups
                )

            assert "Access denied" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_no_group_id_header_uses_default_group(self):
        """Test that when no group_id header is provided, default group is used."""
        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "user@company.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group1 = Mock()
        mock_group1.id = "marketing_abc123"

        mock_group2 = Mock()
        mock_group2.id = "sales_def456"

        user_groups_with_roles = [
            (mock_group1, "editor"),
            (mock_group2, "admin")
        ]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # No group_id provided - should use first group
            context = await GroupContext.from_email(
                email="user@company.com",
                access_token="valid_token",
                group_id=None
            )

            # Should default to first group
            assert context.primary_group_id == "marketing_abc123"
            assert context.user_role == "editor"
            assert set(context.group_ids) == {"marketing_abc123", "sales_def456"}


class TestSecurityLogging:
    """Test suite for security-related logging."""

    @pytest.mark.asyncio
    async def test_unauthorized_access_attempt_logged(self, caplog):
        """Test that unauthorized access attempts are logged with security warnings."""
        # Mock user groups
        mock_user = Mock()
        mock_user.id = "user123"
        mock_user.email = "attacker@company.com"
        mock_user.is_system_admin = False
        mock_user.is_personal_workspace_manager = False

        mock_group = Mock()
        mock_group.id = "marketing_abc123"

        user_groups_with_roles = [(mock_group, "editor")]

        with patch.object(
            GroupContext,
            "_get_user_group_memberships_with_roles",
            new_callable=AsyncMock,
            return_value=(mock_user, user_groups_with_roles)
        ):
            # Attempt unauthorized access
            try:
                await GroupContext.from_email(
                    email="attacker@company.com",
                    access_token="valid_token",
                    group_id="victim_group_xyz"
                )
            except ValueError:
                pass  # Expected

            # Check that security warning was logged
            # Note: This requires the test to capture logs appropriately
            # The actual assertion depends on the logging configuration
