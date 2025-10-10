import pytest
from types import SimpleNamespace

from src.utils.databricks_auth import extract_user_token_from_request


def test_extract_user_token_from_forwarded_header():
    req = SimpleNamespace(headers={'X-Forwarded-Access-Token': 'tok123'})
    assert extract_user_token_from_request(req) == 'tok123'


def test_extract_user_token_from_authorization():
    req = SimpleNamespace(headers={'Authorization': 'Bearer abc'})
    assert extract_user_token_from_request(req) == 'abc'


def test_extract_user_token_none():
    req = SimpleNamespace(headers={})
    assert extract_user_token_from_request(req) is None

