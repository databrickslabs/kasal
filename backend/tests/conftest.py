"""
Global test configuration for pytest.

This file is automatically loaded by pytest and contains global fixtures
and configuration settings that apply to all tests.
"""
import os
import sys
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime, UTC

# Add the backend directory to the Python path so that 'src' can be imported
backend_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
if backend_dir not in sys.path:
    sys.path.insert(0, backend_dir)

# Configure asyncio for pytest
@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    policy = asyncio.get_event_loop_policy()
    loop = policy.new_event_loop()
    yield loop
    loop.close()

# Global test database configuration
@pytest.fixture
def test_db_config():
    """Test database configuration."""
    return {
        "DATABASE_URI": "sqlite+aiosqlite:///:memory:",
        "DATABASE_TYPE": "sqlite",
        "SQLITE_DB_PATH": ":memory:"
    }

# Mock logger for testing
@pytest.fixture
def mock_logger():
    """Create a mock logger for testing."""
    logger = MagicMock()
    logger.info = MagicMock()
    logger.error = MagicMock()
    logger.warning = MagicMock()
    logger.debug = MagicMock()
    return logger

# Mock datetime for consistent testing
@pytest.fixture
def fixed_datetime():
    """Return a fixed datetime for consistent testing."""
    return datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)

# Common test data fixtures
@pytest.fixture
def sample_uuid():
    """Return a sample UUID string for testing."""
    return "12345678-1234-5678-9012-123456789012"

@pytest.fixture
def sample_execution_data():
    """Sample execution data for testing."""
    return {
        "job_id": "test-job-123",
        "status": "pending",
        "run_name": "Test Execution",
        "inputs": {"key": "value"},
        "planning": True,
        "created_at": datetime.now(UTC)
    }

@pytest.fixture
def sample_crew_data():
    """Sample crew data for testing."""
    return {
        "name": "Test Crew",
        "description": "A test crew",
        "agents_yaml": {"agent1": {"role": "researcher"}},
        "tasks_yaml": {"task1": {"description": "research task"}}
    }

@pytest.fixture
def sample_flow_data():
    """Sample flow data for testing."""
    return {
        "name": "Test Flow",
        "description": "A test flow",
        "nodes": [{"id": "node1", "type": "agent"}],
        "edges": [{"source": "node1", "target": "node2"}],
        "flow_config": {"setting": "value"}
    }

# Async mock helpers
@pytest.fixture
def async_mock():
    """Create an AsyncMock for testing async functions."""
    return AsyncMock()

@pytest.fixture
def mock_async_session():
    """Create a mock async database session."""
    session = AsyncMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.close = AsyncMock()
    session.execute = AsyncMock()
    return session

# Test environment setup
@pytest.fixture(autouse=True)
def setup_test_env(monkeypatch):
    """Setup test environment variables."""
    monkeypatch.setenv("DATABASE_TYPE", "sqlite")
    monkeypatch.setenv("SQLITE_DB_PATH", ":memory:")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")
    monkeypatch.setenv("DEBUG_MODE", "true")

# Cleanup fixtures
@pytest.fixture(autouse=True)
def cleanup_after_test():
    """Cleanup after each test."""
    yield
    # Clean up any global state if needed
    # For example, clear in-memory caches, reset singletons, etc.

# Skip integration tests marker
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )

# Test collection modifiers
def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location."""
    for item in items:
        # Add unit marker to tests in unit directory
        if "unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        # Add integration marker to tests in integration directory
        elif "integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)