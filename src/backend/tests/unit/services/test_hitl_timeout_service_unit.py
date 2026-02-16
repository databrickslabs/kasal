"""
Unit tests for HITLTimeoutService.

Tests the background service responsible for monitoring and processing
expired HITL approvals, including singleton management, lifecycle control,
expiration processing, webhook notification dispatch, and error handling.
"""

import asyncio
import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.hitl_timeout_service import (
    HITLTimeoutService,
    HITL_TIMEOUT_CHECK_INTERVAL,
    start_hitl_timeout_service,
    stop_hitl_timeout_service,
)

# The imports in _check_expired_approvals are local (lazy), so we must
# patch them at their *origin* modules rather than on the timeout service module.
_PATCH_SESSION_FACTORY = "src.db.session.async_session_factory"
_PATCH_APPROVAL_REPO = "src.repositories.hitl_repository.HITLApprovalRepository"
_PATCH_HITL_SERVICE = "src.services.hitl_service.HITLService"
_PATCH_WEBHOOK_SERVICE = "src.services.hitl_webhook_service.HITLWebhookService"


@pytest.fixture(autouse=True)
def reset_singleton():
    """Reset singleton state before and after each test to ensure isolation."""
    HITLTimeoutService._instance = None
    HITLTimeoutService._task = None
    HITLTimeoutService._running = False
    yield
    HITLTimeoutService._instance = None
    HITLTimeoutService._task = None
    HITLTimeoutService._running = False


@pytest.fixture
def service():
    """Create a fresh HITLTimeoutService instance via get_instance."""
    return HITLTimeoutService.get_instance()


@pytest.fixture
def mock_session():
    """Create a mock async database session with context manager support."""
    session = AsyncMock()
    session.commit = AsyncMock()
    return session


@pytest.fixture
def mock_session_factory(mock_session):
    """Create a mock async_session_factory that yields the mock session.

    The real async_session_factory is used as an async context manager:
        async with async_session_factory() as session:
    So we build an object whose __call__ returns an async context manager
    that yields mock_session.
    """
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)

    factory = MagicMock(return_value=ctx)
    return factory


@pytest.fixture
def mock_approval_repo():
    """Create a mock HITLApprovalRepository."""
    repo = AsyncMock()
    repo.get_expired_pending = AsyncMock(return_value=[])
    repo.get_by_id = AsyncMock(return_value=None)
    return repo


@pytest.fixture
def mock_hitl_service():
    """Create a mock HITLService."""
    svc = AsyncMock()
    svc.process_expired_approvals = AsyncMock(return_value=[])
    return svc


@pytest.fixture
def mock_webhook_service():
    """Create a mock HITLWebhookService."""
    svc = AsyncMock()
    svc.send_gate_timeout_notification = AsyncMock(return_value=True)
    return svc


# ---------------------------------------------------------------------------
# Singleton behaviour
# ---------------------------------------------------------------------------


class TestSingleton:
    """Tests for the singleton pattern of HITLTimeoutService."""

    def test_get_instance_returns_singleton(self):
        """get_instance() must return the same object on consecutive calls."""
        first = HITLTimeoutService.get_instance()
        second = HITLTimeoutService.get_instance()
        assert first is second

    def test_get_instance_creates_instance_when_none(self):
        """get_instance() creates a new instance when _instance is None."""
        assert HITLTimeoutService._instance is None
        instance = HITLTimeoutService.get_instance()
        assert instance is not None
        assert isinstance(instance, HITLTimeoutService)

    def test_singleton_reset_between_tests(self):
        """Verify that the autouse fixture properly resets singleton state."""
        instance = HITLTimeoutService.get_instance()
        assert instance._running is False
        assert instance._task is None


# ---------------------------------------------------------------------------
# start() lifecycle
# ---------------------------------------------------------------------------


class TestStart:
    """Tests for the start() method."""

    @pytest.mark.asyncio
    async def test_start_sets_running_flag(self, service):
        """start() must set _running = True and enter the loop."""
        with patch(
            "src.services.hitl_timeout_service.HITLTimeoutService._check_expired_approvals",
            new_callable=AsyncMock,
        ) as mock_check:
            with patch("asyncio.sleep", side_effect=asyncio.CancelledError):
                await service.start()

            assert mock_check.await_count == 1
        # After CancelledError breaks the loop, _running remains True
        # because only stop() resets it.
        assert service._running is True

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self, service):
        """Calling start() when already running returns immediately."""
        service._running = True
        with patch(
            "src.services.hitl_timeout_service.HITLTimeoutService._check_expired_approvals",
            new_callable=AsyncMock,
        ) as mock_check:
            await service.start()
            mock_check.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_start_calls_check_and_sleeps(self, service):
        """start() calls _check_expired_approvals then sleeps in a loop."""
        call_count = 0

        async def counting_sleep(seconds):
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise asyncio.CancelledError()

        with patch(
            "src.services.hitl_timeout_service.HITLTimeoutService._check_expired_approvals",
            new_callable=AsyncMock,
        ) as mock_check:
            with patch("asyncio.sleep", side_effect=counting_sleep):
                await service.start()

            assert mock_check.await_count == 2

    @pytest.mark.asyncio
    async def test_start_continues_on_check_error(self, service):
        """If _check_expired_approvals raises, the loop continues."""
        call_count = 0

        async def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("transient failure")

        sleep_count = 0

        async def limited_sleep(seconds):
            nonlocal sleep_count
            sleep_count += 1
            if sleep_count >= 2:
                raise asyncio.CancelledError()

        with patch(
            "src.services.hitl_timeout_service.HITLTimeoutService._check_expired_approvals",
            side_effect=fail_then_succeed,
        ):
            with patch("asyncio.sleep", side_effect=limited_sleep):
                await service.start()

        assert call_count == 2


# ---------------------------------------------------------------------------
# stop() lifecycle
# ---------------------------------------------------------------------------


class TestStop:
    """Tests for the stop() method."""

    @pytest.mark.asyncio
    async def test_stop_cancels_running_task(self, service):
        """stop() cancels the background task and sets _running to False."""
        # Create a real Future so that ``await self._task`` works.
        loop = asyncio.get_event_loop()
        future = loop.create_future()
        future.cancel()  # Makes ``await future`` raise CancelledError

        # Wrap the future so we can spy on done() and cancel().
        cancel_called = False
        original_done = future.done

        class SpyTask:
            """Thin wrapper around a cancelled Future with call tracking."""

            def __init__(self, fut):
                self._fut = fut

            def done(self):
                return False  # Pretend not done so cancel branch is entered

            def cancel(self):
                nonlocal cancel_called
                cancel_called = True
                return self._fut.cancel()

            def __await__(self):
                return self._fut.__await__()

        spy = SpyTask(future)
        service._task = spy
        service._running = True

        await service.stop()

        assert service._running is False
        assert cancel_called is True

    @pytest.mark.asyncio
    async def test_stop_handles_no_task_gracefully(self, service):
        """stop() when no task is set does not raise."""
        service._task = None
        service._running = True

        await service.stop()

        assert service._running is False

    @pytest.mark.asyncio
    async def test_stop_handles_already_done_task(self, service):
        """stop() does not cancel a task that is already done."""
        mock_task = MagicMock()
        mock_task.done.return_value = True
        mock_task.cancel = MagicMock()

        service._task = mock_task
        service._running = True

        await service.stop()

        assert service._running is False
        mock_task.cancel.assert_not_called()


# ---------------------------------------------------------------------------
# _check_expired_approvals()
# ---------------------------------------------------------------------------


class TestCheckExpiredApprovals:
    """Tests for the _check_expired_approvals() method."""

    @pytest.mark.asyncio
    async def test_processes_expired_approvals(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """Expired approvals are fetched, processed, and committed."""
        approval_1 = SimpleNamespace(id=1, gate_name="gate_a")
        approval_2 = SimpleNamespace(id=2, gate_name="gate_b")

        mock_approval_repo.get_expired_pending.return_value = [approval_1, approval_2]
        mock_hitl_service.process_expired_approvals.return_value = [1, 2]
        mock_approval_repo.get_by_id.side_effect = [approval_1, approval_2]

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        await service._check_expired_approvals()

        mock_approval_repo.get_expired_pending.assert_awaited_once()
        mock_hitl_service.process_expired_approvals.assert_awaited_once()
        assert mock_webhook_service.send_gate_timeout_notification.await_count == 2
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_sends_webhook_for_each_processed_approval(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """A webhook notification is sent for each processed approval ID."""
        approval_obj = SimpleNamespace(id=42, gate_name="review_gate")
        mock_approval_repo.get_expired_pending.return_value = [approval_obj]
        mock_hitl_service.process_expired_approvals.return_value = [42]
        mock_approval_repo.get_by_id.return_value = approval_obj

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        await service._check_expired_approvals()

        mock_approval_repo.get_by_id.assert_awaited_once_with(42)
        mock_webhook_service.send_gate_timeout_notification.assert_awaited_once_with(
            approval_obj
        )

    @pytest.mark.asyncio
    async def test_handles_empty_expired_list(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """When no approvals are expired, no processing or commit happens."""
        mock_approval_repo.get_expired_pending.return_value = []

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        await service._check_expired_approvals()

        mock_hitl_service.process_expired_approvals.assert_not_awaited()
        mock_webhook_service.send_gate_timeout_notification.assert_not_awaited()
        mock_session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_handles_session_factory_error_gracefully(self, service):
        """If async_session_factory itself raises, the method logs and returns."""
        broken_ctx = AsyncMock()
        broken_ctx.__aenter__ = AsyncMock(side_effect=RuntimeError("db down"))
        broken_ctx.__aexit__ = AsyncMock(return_value=False)
        broken_factory = MagicMock(return_value=broken_ctx)

        with patch(_PATCH_SESSION_FACTORY, broken_factory):
            # Must not raise -- the outer try/except catches it
            await service._check_expired_approvals()

    @pytest.mark.asyncio
    async def test_handles_webhook_notification_error(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """If a single webhook notification fails, other approvals still process."""
        approval_1 = SimpleNamespace(id=10, gate_name="gate_x")
        approval_2 = SimpleNamespace(id=20, gate_name="gate_y")

        mock_approval_repo.get_expired_pending.return_value = [approval_1, approval_2]
        mock_hitl_service.process_expired_approvals.return_value = [10, 20]
        mock_approval_repo.get_by_id.side_effect = [approval_1, approval_2]
        mock_webhook_service.send_gate_timeout_notification.side_effect = [
            RuntimeError("webhook fail"),
            True,
        ]

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        await service._check_expired_approvals()

        # Both webhooks were attempted
        assert mock_webhook_service.send_gate_timeout_notification.await_count == 2
        # Commit still happens despite one webhook failing
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_webhook_when_approval_not_found(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """If get_by_id returns None for a processed ID, skip the webhook."""
        expired = SimpleNamespace(id=99, gate_name="gate_z")
        mock_approval_repo.get_expired_pending.return_value = [expired]
        mock_hitl_service.process_expired_approvals.return_value = [99]
        mock_approval_repo.get_by_id.return_value = None

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        await service._check_expired_approvals()

        mock_approval_repo.get_by_id.assert_awaited_once_with(99)
        mock_webhook_service.send_gate_timeout_notification.assert_not_awaited()
        mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_handles_process_expired_error_gracefully(
        self,
        service,
        mock_session_factory,
        mock_session,
        mock_approval_repo,
        mock_hitl_service,
        mock_webhook_service,
    ):
        """If process_expired_approvals raises, the outer handler catches it."""
        expired = SimpleNamespace(id=5, gate_name="gate_err")
        mock_approval_repo.get_expired_pending.return_value = [expired]
        mock_hitl_service.process_expired_approvals.side_effect = RuntimeError(
            "processing failed"
        )

        with patch(_PATCH_SESSION_FACTORY, mock_session_factory):
            with patch(_PATCH_APPROVAL_REPO, return_value=mock_approval_repo):
                with patch(_PATCH_HITL_SERVICE, return_value=mock_hitl_service):
                    with patch(_PATCH_WEBHOOK_SERVICE, return_value=mock_webhook_service):
                        # Must not raise
                        await service._check_expired_approvals()

        # Webhook should not be called since the error aborted the flow
        mock_webhook_service.send_gate_timeout_notification.assert_not_awaited()


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------


class TestModuleFunctions:
    """Tests for the module-level start/stop convenience functions."""

    @pytest.mark.asyncio
    async def test_start_hitl_timeout_service_creates_background_task(self):
        """start_hitl_timeout_service() creates an asyncio task from start()."""
        mock_task = MagicMock()

        with patch("asyncio.create_task", return_value=mock_task) as mock_create:
            with patch.object(
                HITLTimeoutService, "start", new_callable=AsyncMock
            ):
                await start_hitl_timeout_service()

                mock_create.assert_called_once()
                instance = HITLTimeoutService.get_instance()
                assert instance._task is mock_task

    @pytest.mark.asyncio
    async def test_stop_hitl_timeout_service_delegates_to_stop(self):
        """stop_hitl_timeout_service() calls stop() on the singleton."""
        with patch.object(
            HITLTimeoutService, "stop", new_callable=AsyncMock
        ) as mock_stop:
            await stop_hitl_timeout_service()
            mock_stop.assert_awaited_once()


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Tests for module-level constants."""

    def test_check_interval_is_positive_integer(self):
        """HITL_TIMEOUT_CHECK_INTERVAL must be a positive integer."""
        assert isinstance(HITL_TIMEOUT_CHECK_INTERVAL, int)
        assert HITL_TIMEOUT_CHECK_INTERVAL > 0

    def test_check_interval_default_value(self):
        """Default check interval is 60 seconds."""
        assert HITL_TIMEOUT_CHECK_INTERVAL == 60
