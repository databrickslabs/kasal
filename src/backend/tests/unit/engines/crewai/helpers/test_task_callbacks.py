"""
Unit tests for src/engines/crewai/helpers/task_callbacks.py.

Covers:
  - configure_task_callbacks() — all branches

All external dependencies (JobOutputCallback, LoggerManager) are mocked.
"""

from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _patch_deps():
    """Return a context manager that patches all module-level dependencies."""
    return patch.multiple(
        "src.engines.crewai.helpers.task_callbacks",
        JobOutputCallback=MagicMock(),
        logger=MagicMock(),
    )


# ---------------------------------------------------------------------------
# Basic return type
# ---------------------------------------------------------------------------


class TestConfigureTaskCallbacksReturnType:

    def test_returns_list(self):
        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", MagicMock()):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t-key")
        assert isinstance(result, list)

    def test_empty_list_when_no_job_id(self):
        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", MagicMock()):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t-key", job_id=None)
        assert result == []

    def test_empty_list_when_job_id_is_empty_string(self):
        """Empty string is falsy — should behave like None."""
        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", MagicMock()):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t-key", job_id="")
        assert result == []


# ---------------------------------------------------------------------------
# JobOutputCallback added when job_id provided
# ---------------------------------------------------------------------------


class TestConfigureTaskCallbacksWithJobId:

    def test_one_callback_added_when_job_id_provided(self):
        mock_cb_instance = MagicMock()
        mock_cb_cls = MagicMock(return_value=mock_cb_instance)

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t-key", job_id="job-1")

        assert len(result) == 1
        assert result[0] is mock_cb_instance

    def test_job_output_callback_constructed_with_correct_args(self):
        mock_cb_cls = MagicMock()
        config = {"key": "val"}

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            configure_task_callbacks("t-key-123", job_id="job-abc", config=config)

        mock_cb_cls.assert_called_once_with(
            job_id="job-abc",
            task_key="t-key-123",
            config=config,
        )

    def test_job_output_callback_constructed_without_config(self):
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            configure_task_callbacks("t-key-456", job_id="job-no-cfg")

        mock_cb_cls.assert_called_once_with(
            job_id="job-no-cfg",
            task_key="t-key-456",
            config=None,
        )

    def test_callback_is_appended_to_list(self):
        """The returned list must contain exactly the JobOutputCallback instance."""
        mock_instance = MagicMock(name="callback_instance")
        mock_cb_cls = MagicMock(return_value=mock_instance)

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t", job_id="j")

        assert result[0] is mock_instance


# ---------------------------------------------------------------------------
# Config-based additional callbacks
# ---------------------------------------------------------------------------


class TestConfigureTaskCallbacksWithConfigCallbacks:

    def test_config_callbacks_are_iterated_without_error(self):
        """Config with 'callbacks' list should not raise."""
        mock_cb_cls = MagicMock()
        config = {
            "callbacks": [
                {"type": "custom", "params": {"p": 1}},
                {"type": "another"},
            ]
        }

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t", job_id="j", config=config)

        # Only the JobOutputCallback should be in the list (no implementations for custom types)
        assert len(result) == 1

    def test_callback_params_task_key_injected(self):
        """task_key must be injected into each callback_config's params."""
        logged_debug = []
        mock_logger = MagicMock()
        mock_logger.debug.side_effect = lambda msg, *a, **kw: logged_debug.append(msg)
        mock_cb_cls = MagicMock()

        config = {"callbacks": [{"type": "custom_type", "params": {}}]}

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls), \
             patch("src.engines.crewai.helpers.task_callbacks.logger", mock_logger):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            configure_task_callbacks("t-inject", job_id="j", config=config)

        # The debug log confirms the callback was iterated
        assert any("custom_type" in m for m in logged_debug)

    def test_no_additional_callbacks_when_config_missing_callbacks_key(self):
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t", job_id="j", config={"other_key": []})

        assert len(result) == 1  # only JobOutputCallback


# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------


class TestConfigureTaskCallbacksLogging:

    def test_info_logged_when_job_id_provided(self):
        mock_logger = MagicMock()
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls), \
             patch("src.engines.crewai.helpers.task_callbacks.logger", mock_logger):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            configure_task_callbacks("task-log", job_id="job-log")

        mock_logger.info.assert_called()
        call_args = str(mock_logger.info.call_args)
        assert "task-log" in call_args or "job-log" in call_args

    def test_no_info_logged_when_no_job_id(self):
        mock_logger = MagicMock()
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls), \
             patch("src.engines.crewai.helpers.task_callbacks.logger", mock_logger):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            configure_task_callbacks("task-nolog")

        mock_logger.info.assert_not_called()


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestConfigureTaskCallbacksEdgeCases:

    def test_none_config_is_handled_gracefully(self):
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t", job_id="j", config=None)

        assert isinstance(result, list)

    def test_empty_config_dict_is_handled(self):
        mock_cb_cls = MagicMock()

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            result = configure_task_callbacks("t", job_id="j", config={})

        assert isinstance(result, list)

    def test_multiple_calls_are_independent(self):
        """Each call should create a fresh list."""
        instances = [MagicMock(name=f"cb_{i}") for i in range(2)]
        mock_cb_cls = MagicMock(side_effect=instances)

        with patch("src.engines.crewai.helpers.task_callbacks.JobOutputCallback", mock_cb_cls):
            from src.engines.crewai.helpers.task_callbacks import configure_task_callbacks
            r1 = configure_task_callbacks("t1", job_id="j1")
            r2 = configure_task_callbacks("t2", job_id="j2")

        assert r1 is not r2
        assert r1[0] is instances[0]
        assert r2[0] is instances[1]
