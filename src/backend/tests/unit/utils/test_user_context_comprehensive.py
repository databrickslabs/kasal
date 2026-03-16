import pytest
from unittest.mock import Mock, patch, AsyncMock
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

# Test user_context utilities - based on actual code inspection

from src.utils.user_context import GroupContext, UserContext


class TestGroupContextBasic:
    """Test GroupContext basic functionality"""

    def test_group_context_creation(self):
        """Test GroupContext creation with basic parameters"""
        group_ids = ["group1", "group2"]
        email_domain = "example.com"
        access_token = "test-token"
        user_id = "user123"
        
        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain,
            access_token=access_token,
            user_id=user_id
        )
        
        assert context.group_ids == group_ids
        assert context.email_domain == email_domain
        assert context.access_token == access_token
        assert context.user_id == user_id

    def test_group_context_creation_minimal(self):
        """Test GroupContext creation with minimal parameters"""
        group_ids = ["group1"]
        email_domain = "example.com"
        
        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain
        )
        
        assert context.group_ids == group_ids
        assert context.email_domain == email_domain
        assert context.access_token is None
        assert context.user_id is None

    def test_group_context_primary_group_id_with_groups(self):
        """Test primary_group_id property with groups"""
        group_ids = ["group1", "group2", "group3"]
        email_domain = "example.com"
        
        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain
        )
        
        assert context.primary_group_id == "group1"

    def test_group_context_primary_group_id_empty_groups(self):
        """Test primary_group_id property with empty groups"""
        group_ids = []
        email_domain = "example.com"
        
        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain
        )
        
        assert context.primary_group_id is None

    def test_group_context_primary_group_id_none_groups(self):
        """Test primary_group_id property with None groups"""
        group_ids = None
        email_domain = "example.com"
        
        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain
        )
        
        assert context.primary_group_id is None


class TestGroupContextToDict:
    """Test GroupContext to_dict method"""

    def test_to_dict_complete(self):
        """Test to_dict with all fields populated"""
        group_ids = ["group1", "group2"]
        email_domain = "example.com"
        access_token = "test-token"
        user_id = "user123"

        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain,
            access_token=access_token,
            user_id=user_id
        )

        result = context.to_dict()

        # Check that result contains expected fields
        assert result['group_ids'] == group_ids
        assert result['email_domain'] == email_domain
        assert result['access_token'] == access_token
        assert result['user_id'] == user_id
        assert 'group_email' in result
        assert 'user_role' in result
        assert 'highest_role' in result
        assert 'primary_group_id' in result
        assert 'group_id' in result
        assert 'is_system_admin' in result
        assert 'is_personal_workspace_manager' in result

    def test_to_dict_minimal(self):
        """Test to_dict with minimal fields"""
        group_ids = ["group1"]
        email_domain = "example.com"

        context = GroupContext(
            group_ids=group_ids,
            email_domain=email_domain
        )

        result = context.to_dict()

        # Check that result contains expected fields with correct values
        assert result['group_ids'] == group_ids
        assert result['email_domain'] == email_domain
        assert result['access_token'] is None
        assert result['user_id'] is None
        assert result['group_email'] is None
        assert result['user_role'] is None
        assert result['highest_role'] is None

    def test_to_dict_returns_dict(self):
        """Test to_dict returns a dictionary"""
        context = GroupContext(
            group_ids=["group1"],
            email_domain="example.com"
        )

        result = context.to_dict()

        assert isinstance(result, dict)
        assert len(result) == 11  # Should have 11 keys based on actual implementation


class TestGroupContextStaticMethods:
    """Test GroupContext static methods"""

    def test_generate_group_id(self):
        """Test generate_group_id static method"""
        email_domain = "example.com"

        result = GroupContext.generate_group_id(email_domain)

        assert isinstance(result, str)
        assert len(result) > 0
        # Based on actual implementation: replaces . with _ and converts to lowercase
        assert result == "example_com"

    def test_generate_group_id_different_domains(self):
        """Test generate_group_id returns different results for different domains"""
        domain1 = "example.com"
        domain2 = "test.org"
        
        result1 = GroupContext.generate_group_id(domain1)
        result2 = GroupContext.generate_group_id(domain2)
        
        assert result1 != result2

    def test_generate_group_id_consistent(self):
        """Test generate_group_id returns consistent results for same domain"""
        email_domain = "example.com"
        
        result1 = GroupContext.generate_group_id(email_domain)
        result2 = GroupContext.generate_group_id(email_domain)
        
        assert result1 == result2

    def test_generate_individual_group_id(self):
        """Test generate_individual_group_id static method"""
        email = "user@example.com"

        result = GroupContext.generate_individual_group_id(email)

        assert isinstance(result, str)
        assert len(result) > 0
        # Based on actual implementation: user_<sanitized_email>
        assert result == "user_user_example_com"

    def test_generate_individual_group_id_different_emails(self):
        """Test generate_individual_group_id returns different results for different emails"""
        email1 = "user1@example.com"
        email2 = "user2@example.com"
        
        result1 = GroupContext.generate_individual_group_id(email1)
        result2 = GroupContext.generate_individual_group_id(email2)
        
        assert result1 != result2

    def test_generate_individual_group_id_consistent(self):
        """Test generate_individual_group_id returns consistent results for same email"""
        email = "user@example.com"
        
        result1 = GroupContext.generate_individual_group_id(email)
        result2 = GroupContext.generate_individual_group_id(email)
        
        assert result1 == result2


class TestGroupContextIsValid:
    """Test GroupContext is_valid method"""

    def test_is_valid_with_valid_context(self):
        """Test is_valid with valid context"""
        context = GroupContext(
            group_ids=["group1"],
            email_domain="example.com"
        )
        
        assert context.is_valid() is True

    def test_is_valid_with_multiple_groups(self):
        """Test is_valid with multiple groups"""
        context = GroupContext(
            group_ids=["group1", "group2"],
            email_domain="example.com"
        )
        
        assert context.is_valid() is True

    def test_is_valid_with_empty_groups(self):
        """Test is_valid with empty groups"""
        context = GroupContext(
            group_ids=[],
            email_domain="example.com"
        )
        
        assert context.is_valid() is False

    def test_is_valid_with_none_groups(self):
        """Test is_valid with None groups"""
        context = GroupContext(
            group_ids=None,
            email_domain="example.com"
        )
        
        assert context.is_valid() is False

    def test_is_valid_with_none_email_domain(self):
        """Test is_valid with None email_domain"""
        context = GroupContext(
            group_ids=["group1"],
            email_domain=None
        )
        
        assert context.is_valid() is False

    def test_is_valid_with_empty_email_domain(self):
        """Test is_valid with empty email_domain"""
        context = GroupContext(
            group_ids=["group1"],
            email_domain=""
        )
        
        assert context.is_valid() is False


class TestUserContextStaticMethods:
    """Test UserContext static methods"""

    def test_set_user_token(self):
        """Test set_user_token static method"""
        token = "test-token-123"
        
        # Should not raise an exception
        UserContext.set_user_token(token)
        
        # Verify it was set by getting it back
        result = UserContext.get_user_token()
        assert result == token

    def test_get_user_token_after_set(self):
        """Test get_user_token after setting token"""
        token = "test-token-456"
        
        UserContext.set_user_token(token)
        result = UserContext.get_user_token()
        
        assert result == token

    def test_get_user_token_default(self):
        """Test get_user_token returns None by default"""
        # Clear context first
        UserContext.clear_context()
        
        result = UserContext.get_user_token()
        
        assert result is None

    def test_set_user_context(self):
        """Test set_user_context static method"""
        context = {"user_id": "123", "email": "test@example.com"}
        
        # Should not raise an exception
        UserContext.set_user_context(context)
        
        # Verify it was set by getting it back
        result = UserContext.get_user_context()
        assert result == context

    def test_get_user_context_after_set(self):
        """Test get_user_context after setting context"""
        context = {"user_id": "456", "role": "admin"}
        
        UserContext.set_user_context(context)
        result = UserContext.get_user_context()
        
        assert result == context

    def test_get_user_context_default(self):
        """Test get_user_context returns None by default"""
        # Clear context first
        UserContext.clear_context()
        
        result = UserContext.get_user_context()
        
        assert result is None

    def test_set_group_context(self):
        """Test set_group_context static method"""
        group_context = GroupContext(
            group_ids=["group1"],
            email_domain="example.com"
        )
        
        # Should not raise an exception
        UserContext.set_group_context(group_context)
        
        # Verify it was set by getting it back
        result = UserContext.get_group_context()
        assert result == group_context

    def test_get_group_context_after_set(self):
        """Test get_group_context after setting context"""
        group_context = GroupContext(
            group_ids=["group2"],
            email_domain="test.org"
        )
        
        UserContext.set_group_context(group_context)
        result = UserContext.get_group_context()
        
        assert result == group_context

    def test_get_group_context_default(self):
        """Test get_group_context returns None by default"""
        # Clear context first
        UserContext.clear_context()
        
        result = UserContext.get_group_context()
        
        assert result is None

    def test_clear_context(self):
        """Test clear_context clears all context"""
        # Set some context first
        UserContext.set_user_token("test-token")
        UserContext.set_user_context({"user_id": "123"})
        group_context = GroupContext(group_ids=["group1"], email_domain="example.com")
        UserContext.set_group_context(group_context)
        
        # Clear context
        UserContext.clear_context()
        
        # Verify all context is cleared
        assert UserContext.get_user_token() is None
        assert UserContext.get_user_context() is None
        assert UserContext.get_group_context() is None


class TestUserContextConstants:
    """Test UserContext constants and module-level attributes"""

    def test_context_variables_exist(self):
        """Test that context variables are properly defined"""
        from src.utils.user_context import _user_access_token, _user_context, _group_context
        
        assert _user_access_token is not None
        assert _user_context is not None
        assert _group_context is not None

    def test_logger_initialization(self):
        """Test logger is properly initialized"""
        from src.utils.user_context import logger

        assert logger is not None
        assert hasattr(logger, 'info')
        assert hasattr(logger, 'error')
        assert hasattr(logger, 'warning')


# ---------------------------------------------------------------------------
# _get_user_group_memberships / _get_user_group_memberships_with_roles
# — uses execute_db_operation_smart (Lakebase-aware smart session)
# ---------------------------------------------------------------------------


def _make_smart_mock(mock_session):
    """Create an execute_db_operation_smart mock that calls the callback with mock_session."""
    async def _smart(operation):
        return await operation(mock_session)
    return AsyncMock(side_effect=_smart)


class TestGetUserGroupMembershipsWithRoles:
    """Tests for _get_user_group_memberships_with_roles using smart session."""

    @pytest.mark.asyncio
    async def test_uses_execute_db_operation_smart(self):
        """Should delegate to execute_db_operation_smart, not _local_session_factory."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1", email="test@example.com", is_system_admin=False)

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_groups_with_roles = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.user_context.execute_db_operation_smart", smart_mock, create=True), \
             patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            user, groups = await GroupContext._get_user_group_memberships_with_roles("test@example.com")

        assert user is mock_user
        assert groups == []
        smart_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_user_and_groups_with_roles(self):
        """Should return (user, [(group, role)]) when user has group memberships."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1", email="alice@corp.com", is_system_admin=False)

        mock_group_a = Mock(id="team_alpha")
        mock_group_b = Mock(id="team_beta")
        groups_with_roles = [(mock_group_a, "admin"), (mock_group_b, "operator")]

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_groups_with_roles = AsyncMock(return_value=groups_with_roles)

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            user, result = await GroupContext._get_user_group_memberships_with_roles("alice@corp.com")

        assert user is mock_user
        assert len(result) == 2
        assert result[0] == (mock_group_a, "admin")
        assert result[1] == (mock_group_b, "operator")

    @pytest.mark.asyncio
    async def test_returns_none_tuple_when_user_not_found(self):
        """Should return (None, []) when get_or_create_user_by_email returns None."""
        mock_session = AsyncMock()

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=None)

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService"):
            user, groups = await GroupContext._get_user_group_memberships_with_roles("ghost@example.com")

        assert user is None
        assert groups == []

    @pytest.mark.asyncio
    async def test_commits_session_on_success(self):
        """Should commit the session after successful lookup."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1", email="test@example.com", is_system_admin=False)

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_groups_with_roles = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            await GroupContext._get_user_group_memberships_with_roles("test@example.com")

        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exception_returns_none_tuple(self):
        """When execute_db_operation_smart raises, return (None, []) gracefully."""
        async def _failing(operation):
            raise ConnectionError("DB unavailable")

        smart_mock = AsyncMock(side_effect=_failing)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock):
            user, groups = await GroupContext._get_user_group_memberships_with_roles("test@example.com")

        assert user is None
        assert groups == []

    @pytest.mark.asyncio
    async def test_does_not_update_last_login(self):
        """Should pass update_login=False to avoid DB lock contention."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1", email="test@example.com", is_system_admin=False)

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_groups_with_roles = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            await GroupContext._get_user_group_memberships_with_roles("test@example.com")

        mock_user_service.get_or_create_user_by_email.assert_awaited_once_with(
            "test@example.com", update_login=False
        )

    @pytest.mark.asyncio
    async def test_passes_user_id_to_group_service(self):
        """Should call get_user_groups_with_roles with the resolved user ID."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-42", email="test@example.com", is_system_admin=False)

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_groups_with_roles = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            await GroupContext._get_user_group_memberships_with_roles("test@example.com")

        mock_group_service.get_user_groups_with_roles.assert_awaited_once_with("user-42")


class TestGetUserGroupMemberships:
    """Tests for _get_user_group_memberships using smart session."""

    @pytest.mark.asyncio
    async def test_uses_execute_db_operation_smart(self):
        """Should delegate to execute_db_operation_smart."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1")

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group = Mock(id="group-1")
        mock_group_service = Mock()
        mock_group_service.get_user_group_memberships = AsyncMock(return_value=[mock_group])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            result = await GroupContext._get_user_group_memberships("test@example.com")

        assert result == ["group-1"]
        smart_mock.assert_called_once()

    @pytest.mark.asyncio
    async def test_returns_multiple_group_ids(self):
        """Should return list of group IDs when user belongs to multiple groups."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1")

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_groups = [Mock(id="alpha"), Mock(id="beta"), Mock(id="gamma")]
        mock_group_service = Mock()
        mock_group_service.get_user_group_memberships = AsyncMock(return_value=mock_groups)

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            result = await GroupContext._get_user_group_memberships("test@example.com")

        assert result == ["alpha", "beta", "gamma"]

    @pytest.mark.asyncio
    async def test_returns_empty_when_user_not_found(self):
        """Should return [] when user cannot be created/found."""
        mock_session = AsyncMock()

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=None)

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService"):
            result = await GroupContext._get_user_group_memberships("ghost@example.com")

        assert result == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_no_groups(self):
        """Should return [] when user has no group memberships."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1")

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_group_memberships = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            result = await GroupContext._get_user_group_memberships("loner@example.com")

        assert result == []

    @pytest.mark.asyncio
    async def test_commits_session_twice_on_success(self):
        """Should commit after user creation and after group lookup."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1")

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_group_memberships = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            await GroupContext._get_user_group_memberships("test@example.com")

        assert mock_session.commit.await_count == 2

    @pytest.mark.asyncio
    async def test_exception_returns_empty_list(self):
        """When execute_db_operation_smart raises, return [] gracefully."""
        async def _failing(operation):
            raise ConnectionError("DB unavailable")

        smart_mock = AsyncMock(side_effect=_failing)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock):
            result = await GroupContext._get_user_group_memberships("test@example.com")

        assert result == []

    @pytest.mark.asyncio
    async def test_passes_email_to_group_service(self):
        """Should call get_user_group_memberships with the user's email."""
        mock_session = AsyncMock()
        mock_user = Mock(id="user-1")

        mock_user_service = Mock()
        mock_user_service.get_or_create_user_by_email = AsyncMock(return_value=mock_user)

        mock_group_service = Mock()
        mock_group_service.get_user_group_memberships = AsyncMock(return_value=[])

        smart_mock = _make_smart_mock(mock_session)

        with patch("src.utils.asyncio_utils.execute_db_operation_smart", smart_mock), \
             patch("src.services.user_service.UserService", return_value=mock_user_service), \
             patch("src.services.group_service.GroupService", return_value=mock_group_service):
            await GroupContext._get_user_group_memberships("alice@corp.com")

        mock_group_service.get_user_group_memberships.assert_awaited_once_with("alice@corp.com")


class TestFromEmailUsesSmartSession:
    """Integration tests verifying from_email resolves groups via smart session,
    ensuring correct group_id assignment when Lakebase is active."""

    @pytest.mark.asyncio
    async def test_from_email_with_shared_group_sets_correct_primary_group_id(self):
        """When user selects a shared group, primary_group_id should be that group."""
        mock_group = Mock(id="energy_0380b619")
        groups_with_roles = [(mock_group, "operator")]
        mock_user = Mock(id="user-1", email="nehme@databricks.com", is_system_admin=False)

        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            new=AsyncMock(return_value=(mock_user, groups_with_roles))
        ):
            ctx = await GroupContext.from_email(
                email="nehme@databricks.com",
                access_token="tok",
                group_id="energy_0380b619"
            )

        assert ctx.primary_group_id == "energy_0380b619"
        assert "energy_0380b619" in ctx.group_ids
        # Personal workspace should also be included for data access
        personal = GroupContext.generate_individual_group_id("nehme@databricks.com")
        assert personal in ctx.group_ids

    @pytest.mark.asyncio
    async def test_from_email_with_personal_workspace_sets_personal_primary(self):
        """When user selects personal workspace, primary_group_id should be personal."""
        mock_group = Mock(id="energy_0380b619")
        groups_with_roles = [(mock_group, "operator")]
        mock_user = Mock(id="user-1", email="nehme@databricks.com", is_system_admin=False)
        personal = GroupContext.generate_individual_group_id("nehme@databricks.com")

        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            new=AsyncMock(return_value=(mock_user, groups_with_roles))
        ):
            ctx = await GroupContext.from_email(
                email="nehme@databricks.com",
                access_token="tok",
                group_id=personal
            )

        assert ctx.primary_group_id == personal

    @pytest.mark.asyncio
    async def test_from_email_no_groups_falls_back_to_individual(self):
        """When user has no group memberships, falls back to individual group ID."""
        mock_user = Mock(id="user-1", email="solo@example.com", is_system_admin=False)

        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            new=AsyncMock(return_value=(mock_user, []))
        ):
            ctx = await GroupContext.from_email(
                email="solo@example.com",
                access_token="tok",
                group_id="some_group"
            )

        # With no group memberships, should use individual group ID
        expected = GroupContext.generate_individual_group_id("solo@example.com")
        assert ctx.primary_group_id == expected

    @pytest.mark.asyncio
    async def test_from_email_unauthorized_group_raises(self):
        """When user tries to access a group they don't belong to, should raise ValueError."""
        mock_group = Mock(id="team_alpha")
        groups_with_roles = [(mock_group, "admin")]
        mock_user = Mock(id="user-1", email="test@example.com", is_system_admin=False)

        with patch.object(
            GroupContext, "_get_user_group_memberships_with_roles",
            new=AsyncMock(return_value=(mock_user, groups_with_roles))
        ):
            with pytest.raises(ValueError, match="Access denied"):
                await GroupContext.from_email(
                    email="test@example.com",
                    access_token="tok",
                    group_id="unauthorized_group"
                )
