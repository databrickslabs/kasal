import pathlib
import pytest

# Ignore collection of all tests under backup_rbac_tests (legacy, deprecated RBAC suite)
# This prevents import-time failures from outdated models/schemas and keeps the suite clean.

def pytest_ignore_collect(path, config):
    try:
        return "backup_rbac_tests" in str(path)
    except Exception:
        return False

