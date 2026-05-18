"""
Tests for SyncUnitOfWork which is skipped in the main test file.
"""
import pytest
from unittest.mock import MagicMock, patch

from src.core.unit_of_work import SyncUnitOfWork


class TestSyncUnitOfWork:
    """Test the synchronous UnitOfWork implementation."""

    def setup_method(self):
        """Reset singleton before each test."""
        SyncUnitOfWork._instance = None

    def test_singleton_pattern(self):
        """SyncUnitOfWork follows singleton pattern."""
        uow1 = SyncUnitOfWork.get_instance()
        uow2 = SyncUnitOfWork.get_instance()
        assert uow1 is uow2

    def test_init_state(self):
        """SyncUnitOfWork initializes with all None repositories and not initialized."""
        uow = SyncUnitOfWork()
        assert uow._session is None
        assert uow._initialized is False
        assert uow.tool_repository is None
        assert uow.api_key_repository is None
        assert uow.model_config_repository is None
        assert uow.template_repository is None
        assert uow.schema_repository is None
        assert uow.mcp_server_repository is None
        assert uow.mcp_settings_repository is None
        assert uow.engine_config_repository is None

    def _make_mock_session(self):
        return MagicMock()

    def _initialize_with_mock(self, uow):
        """Helper to initialize UoW with mocked session factory."""
        mock_session = self._make_mock_session()
        with patch("src.db.session.sync_session_factory", return_value=mock_session):
            uow.initialize()
        return mock_session

    def test_initialize_creates_repositories(self):
        """initialize sets _initialized and creates all repositories."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        with patch("src.db.session.sync_session_factory", return_value=mock_session):
            uow.initialize()
        assert uow._initialized is True
        assert uow._session is mock_session
        assert uow.tool_repository is not None
        assert uow.api_key_repository is not None
        assert uow.model_config_repository is not None
        assert uow.template_repository is not None

    def test_initialize_idempotent(self):
        """initialize does nothing when already initialized."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        with patch("src.db.session.sync_session_factory", return_value=mock_session) as mock_factory:
            uow.initialize()
            uow.initialize()  # second call
        mock_factory.assert_called_once()

    def test_commit_success(self):
        """commit calls session.commit()."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        uow._session = mock_session
        uow._initialized = True
        uow.commit()
        mock_session.commit.assert_called_once()

    def test_commit_not_initialized_raises(self):
        """commit raises RuntimeError when not initialized."""
        uow = SyncUnitOfWork()
        with pytest.raises(RuntimeError, match="SyncUnitOfWork not initialized"):
            uow.commit()

    def test_commit_error_triggers_rollback(self):
        """commit rollback on session.commit error."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        mock_session.commit.side_effect = Exception("DB error")
        uow._session = mock_session
        uow._initialized = True

        with pytest.raises(Exception, match="DB error"):
            uow.commit()
        mock_session.rollback.assert_called_once()

    def test_rollback_success(self):
        """rollback calls session.rollback()."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        uow._session = mock_session
        uow._initialized = True
        uow.rollback()
        mock_session.rollback.assert_called_once()

    def test_rollback_not_initialized_raises(self):
        """rollback raises RuntimeError when not initialized."""
        uow = SyncUnitOfWork()
        with pytest.raises(RuntimeError, match="SyncUnitOfWork not initialized"):
            uow.rollback()

    def test_rollback_error_propagates(self):
        """rollback propagates exception from session.rollback."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        mock_session.rollback.side_effect = Exception("Rollback failed")
        uow._session = mock_session
        uow._initialized = True

        with pytest.raises(Exception, match="Rollback failed"):
            uow.rollback()

    def test_cleanup_closes_session(self):
        """cleanup closes the session and resets state."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        uow._session = mock_session
        uow._initialized = True

        uow.cleanup()
        mock_session.close.assert_called_once()
        assert uow._session is None
        assert uow._initialized is False

    def test_cleanup_when_not_initialized(self):
        """cleanup does nothing when not initialized."""
        uow = SyncUnitOfWork()
        uow.cleanup()  # Should not raise
        assert uow._session is None
        assert uow._initialized is False

    def test_cleanup_when_session_none_but_initialized(self):
        """cleanup handles case where initialized but session is None."""
        uow = SyncUnitOfWork()
        uow._initialized = True
        uow._session = None
        uow.cleanup()  # Should not raise

    def test_del_calls_cleanup(self):
        """__del__ triggers cleanup."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        uow._session = mock_session
        uow._initialized = True

        uow.__del__()
        mock_session.close.assert_called_once()
        assert uow._session is None

    def test_singleton_persists_across_cleanup(self):
        """Singleton instance is the same even after cleanup."""
        uow1 = SyncUnitOfWork.get_instance()
        uow1.cleanup()
        uow2 = SyncUnitOfWork.get_instance()
        assert uow1 is uow2

    def test_all_repositories_initialized(self):
        """All expected repositories are initialized after initialize()."""
        uow = SyncUnitOfWork()
        mock_session = self._make_mock_session()
        with patch("src.db.session.sync_session_factory", return_value=mock_session):
            uow.initialize()

        repo_attrs = [
            "tool_repository",
            "api_key_repository",
            "model_config_repository",
            "template_repository",
            "schema_repository",
            "databricks_config_repository",
            "powerbi_config_repository",
            "mcp_server_repository",
            "mcp_settings_repository",
            "engine_config_repository",
            "memory_backend_repository",
            "documentation_embedding_repository",
            "conversion_history_repository",
            "conversion_job_repository",
            "saved_converter_config_repository",
        ]
        for attr in repo_attrs:
            assert getattr(uow, attr) is not None, f"{attr} should be initialized"
