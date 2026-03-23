"""Tests for UserBase.email_validator in src/schemas/user.py."""
import sys
import os
import pytest

sys.path.insert(0, "/Users/nehme.tohme/workspace/kasal/src/backend")

from src.schemas.user import UserBase


def make_user(email, username="validuser"):
    """Helper to construct a UserBase."""
    return UserBase(username=username, email=email)


def test_email_validator_normal_email():
    """Normal email passes through unchanged."""
    u = make_user("test@example.com")
    assert u.email == "test@example.com"


def test_email_validator_empty_string():
    """Empty string is returned as empty string."""
    u = make_user("")
    assert u.email == ""


def test_email_validator_none_becomes_empty():
    """None is converted to empty string."""
    u = make_user(None)
    assert u.email == ""


def test_email_validator_partial_email():
    """Partial/invalid email is accepted (read-path tolerant)."""
    u = make_user("user@")
    assert u.email == "user@"


def test_email_validator_localhost_email():
    """@localhost email is accepted."""
    u = make_user("admin@localhost")
    assert u.email == "admin@localhost"


def test_email_validator_complex_email():
    """Complex valid email passes through."""
    u = make_user("user.name+tag@sub.domain.com")
    assert u.email == "user.name+tag@sub.domain.com"


def test_email_validator_preserves_value():
    """Validator returns the value as-is without modification (except None)."""
    u = make_user("ANY_VALUE")
    assert u.email == "ANY_VALUE"
