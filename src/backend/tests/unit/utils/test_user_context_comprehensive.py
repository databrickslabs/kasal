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
