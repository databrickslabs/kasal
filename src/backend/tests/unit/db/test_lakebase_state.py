"""
Unit tests for src.db.lakebase_state module.

Tests the Lakebase activation state tracker that distinguishes startup
(fallback OK) from runtime (fallback = data loss).
"""

import importlib
import pytest
from datetime import datetime, timezone
from unittest.mock import patch

import src.db.lakebase_state as lakebase_state_mod


@pytest.fixture(autouse=True)
def _reset_state():
    """Reset module-level state before every test."""
    lakebase_state_mod._lakebase_ever_activated = False
    lakebase_state_mod._last_successful_connection = None
    yield
    lakebase_state_mod._lakebase_ever_activated = False
    lakebase_state_mod._last_successful_connection = None


# ---------------------------------------------------------------------------
# is_fallback_allowed / is_lakebase_activated — initial state
# ---------------------------------------------------------------------------

class TestInitialState:
    def test_fallback_allowed_initially(self):
        assert lakebase_state_mod.is_fallback_allowed() is True

    def test_lakebase_not_activated_initially(self):
        assert lakebase_state_mod.is_lakebase_activated() is False

    def test_last_successful_connection_none_initially(self):
        assert lakebase_state_mod.get_last_successful_connection() is None


# ---------------------------------------------------------------------------
# mark_lakebase_activated
# ---------------------------------------------------------------------------

class TestMarkLakebaseActivated:
    def test_mark_sets_activated(self):
        lakebase_state_mod.mark_lakebase_activated()
        assert lakebase_state_mod.is_lakebase_activated() is True

    def test_mark_disables_fallback(self):
        lakebase_state_mod.mark_lakebase_activated()
        assert lakebase_state_mod.is_fallback_allowed() is False

    def test_mark_is_idempotent(self):
        lakebase_state_mod.mark_lakebase_activated()
        lakebase_state_mod.mark_lakebase_activated()
        assert lakebase_state_mod.is_lakebase_activated() is True
        assert lakebase_state_mod.is_fallback_allowed() is False

    def test_mark_logs_info(self):
        with patch.object(lakebase_state_mod.logger, "info") as mock_info:
            lakebase_state_mod.mark_lakebase_activated()
            mock_info.assert_called_once()
            assert "activated" in mock_info.call_args[0][0].lower()


# ---------------------------------------------------------------------------
# record_successful_connection
# ---------------------------------------------------------------------------

class TestRecordSuccessfulConnection:
    def test_records_timestamp(self):
        before = datetime.now(timezone.utc)
        lakebase_state_mod.record_successful_connection()
        after = datetime.now(timezone.utc)

        ts = lakebase_state_mod.get_last_successful_connection()
        assert ts is not None
        assert before <= ts <= after

    def test_updates_on_subsequent_calls(self):
        lakebase_state_mod.record_successful_connection()
        first = lakebase_state_mod.get_last_successful_connection()

        lakebase_state_mod.record_successful_connection()
        second = lakebase_state_mod.get_last_successful_connection()

        assert second >= first

    def test_does_not_affect_activation_flag(self):
        lakebase_state_mod.record_successful_connection()
        assert lakebase_state_mod.is_lakebase_activated() is False
        assert lakebase_state_mod.is_fallback_allowed() is True


# ---------------------------------------------------------------------------
# Combined scenarios
# ---------------------------------------------------------------------------

class TestCombinedScenarios:
    def test_activation_then_connection(self):
        lakebase_state_mod.mark_lakebase_activated()
        lakebase_state_mod.record_successful_connection()

        assert lakebase_state_mod.is_lakebase_activated() is True
        assert lakebase_state_mod.is_fallback_allowed() is False
        assert lakebase_state_mod.get_last_successful_connection() is not None

    def test_connection_without_activation(self):
        lakebase_state_mod.record_successful_connection()

        assert lakebase_state_mod.is_lakebase_activated() is False
        assert lakebase_state_mod.is_fallback_allowed() is True
        assert lakebase_state_mod.get_last_successful_connection() is not None
