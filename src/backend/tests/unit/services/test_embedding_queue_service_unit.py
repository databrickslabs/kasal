"""
Unit tests for EmbeddingQueueService.

Tests the functionality of the embedding queue service including
background queue processing, batch insertion, retry logic, and
lifecycle management (start/stop).
"""
import asyncio
import pytest
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch, call

from src.services.embedding_queue_service import EmbeddingQueueService


# ---------------------------------------------------------------------------
# Patch path constants
# ---------------------------------------------------------------------------
# _batch_insert and _insert_with_retry import async_session_factory locally
# via ``from src.db.session import async_session_factory``, so patching must
# target the *source* module, not the service module namespace.
_SESSION_FACTORY = "src.db.session.async_session_factory"
# insert and DocumentationEmbedding are imported at module top-level, so they
# live in the service module namespace after import.
_INSERT = "src.services.embedding_queue_service.insert"
_DOC_EMBEDDING = "src.services.embedding_queue_service.DocumentationEmbedding"
_LOGGER = "src.services.embedding_queue_service.logger"


def _make_service(**overrides) -> EmbeddingQueueService:
    """Create a fresh EmbeddingQueueService instance with optional overrides.

    This bypasses the module-level singleton so each test gets isolated state.
    """
    svc = EmbeddingQueueService()
    for attr, value in overrides.items():
        setattr(svc, attr, value)
    return svc


def _mock_async_session_ctx(mock_session):
    """Build an async-context-manager mock that yields *mock_session*."""
    ctx = AsyncMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)
    return ctx


# ===================================================================
# start()
# ===================================================================

class TestEmbeddingQueueServiceStart:
    """Test cases for the start() lifecycle method."""

    @pytest.mark.asyncio
    async def test_start_creates_task_and_sets_running(self):
        """start() should set _running=True and create an asyncio task."""
        svc = _make_service()

        with patch.object(svc, "_process_queue", new_callable=AsyncMock):
            await svc.start()

            assert svc._running is True
            assert svc._task is not None
            assert isinstance(svc._task, asyncio.Task)

            # Clean up the background task
            svc._running = False
            svc._task.cancel()
            try:
                await svc._task
            except asyncio.CancelledError:
                pass

    @pytest.mark.asyncio
    async def test_start_is_idempotent(self):
        """Calling start() twice should not create a second task."""
        svc = _make_service()

        with patch.object(svc, "_process_queue", new_callable=AsyncMock):
            await svc.start()
            first_task = svc._task

            await svc.start()
            second_task = svc._task

            assert first_task is second_task
            assert svc._running is True

            # Clean up
            svc._running = False
            svc._task.cancel()
            try:
                await svc._task
            except asyncio.CancelledError:
                pass


# ===================================================================
# stop()
# ===================================================================

class TestEmbeddingQueueServiceStop:
    """Test cases for the stop() lifecycle method."""

    @pytest.mark.asyncio
    async def test_stop_sets_running_false_and_awaits_task(self):
        """stop() should set _running=False and await the background task."""
        svc = _make_service()

        with patch.object(svc, "_process_queue", new_callable=AsyncMock) as mock_process:
            async def loop_until_stopped():
                while svc._running:
                    await asyncio.sleep(0.01)

            mock_process.side_effect = loop_until_stopped

            await svc.start()
            assert svc._running is True
            assert svc._task is not None

            await svc.stop()
            assert svc._running is False

    @pytest.mark.asyncio
    async def test_stop_with_no_task_is_safe(self):
        """stop() should be safe to call even when no task is running."""
        svc = _make_service()
        assert svc._task is None
        assert svc._running is False

        # Should not raise
        await svc.stop()
        assert svc._running is False


# ===================================================================
# add_embedding()
# ===================================================================

class TestEmbeddingQueueServiceAddEmbedding:
    """Test cases for the add_embedding() method."""

    @pytest.mark.asyncio
    async def test_add_embedding_appends_to_queue(self):
        """add_embedding() should add an item to the internal queue."""
        svc = _make_service()
        svc._flush_queue = AsyncMock()

        await svc.add_embedding(
            source="test_source",
            title="Test Title",
            content="Test content body",
            embedding=[0.1, 0.2, 0.3],
            doc_metadata={"key": "value"},
        )

        assert len(svc.queue) == 1
        item = svc.queue[0]
        assert item["source"] == "test_source"
        assert item["title"] == "Test Title"
        assert item["content"] == "Test content body"
        assert item["embedding"] == [0.1, 0.2, 0.3]
        assert item["doc_metadata"] == {"key": "value"}
        assert isinstance(item["created_at"], datetime)

    @pytest.mark.asyncio
    async def test_add_embedding_defaults_metadata_to_empty_dict(self):
        """add_embedding() with None metadata should store an empty dict."""
        svc = _make_service()
        svc._flush_queue = AsyncMock()

        await svc.add_embedding(
            source="src",
            title="t",
            content="c",
            embedding=[1.0],
            doc_metadata=None,
        )

        assert svc.queue[0]["doc_metadata"] == {}

    @pytest.mark.asyncio
    async def test_add_embedding_triggers_flush_when_batch_size_reached(self):
        """add_embedding() should call _flush_queue when queue reaches batch_size."""
        svc = _make_service(batch_size=3)
        svc._flush_queue = AsyncMock()

        # Add items below threshold -- no flush expected
        await svc.add_embedding("s", "t", "c", [0.1])
        await svc.add_embedding("s", "t", "c", [0.2])
        svc._flush_queue.assert_not_called()

        # Third add reaches batch_size=3 and should trigger flush
        await svc.add_embedding("s", "t", "c", [0.3])
        svc._flush_queue.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_add_embedding_does_not_flush_below_batch_size(self):
        """add_embedding() should not flush when queue is below batch_size."""
        svc = _make_service(batch_size=10)
        svc._flush_queue = AsyncMock()

        for i in range(9):
            await svc.add_embedding("s", "t", "c", [float(i)])

        svc._flush_queue.assert_not_called()
        assert len(svc.queue) == 9


# ===================================================================
# _flush_queue()
# ===================================================================

class TestEmbeddingQueueServiceFlushQueue:
    """Test cases for the _flush_queue() internal method."""

    @pytest.mark.asyncio
    async def test_flush_queue_extracts_batch_and_inserts(self):
        """_flush_queue() should extract up to batch_size items and call _batch_insert."""
        svc = _make_service(batch_size=2)
        svc._batch_insert = AsyncMock()

        items = [
            {"source": "s1", "title": "t1", "content": "c1", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()},
            {"source": "s2", "title": "t2", "content": "c2", "embedding": [0.2], "doc_metadata": {}, "created_at": datetime.utcnow()},
            {"source": "s3", "title": "t3", "content": "c3", "embedding": [0.3], "doc_metadata": {}, "created_at": datetime.utcnow()},
        ]
        svc.queue = list(items)

        await svc._flush_queue()

        # Should have inserted the first 2 (batch_size=2)
        svc._batch_insert.assert_awaited_once()
        inserted_batch = svc._batch_insert.call_args[0][0]
        assert len(inserted_batch) == 2
        assert inserted_batch[0]["source"] == "s1"
        assert inserted_batch[1]["source"] == "s2"

        # Remaining item stays in the queue
        assert len(svc.queue) == 1
        assert svc.queue[0]["source"] == "s3"

    @pytest.mark.asyncio
    async def test_flush_queue_does_nothing_when_empty(self):
        """_flush_queue() should return immediately if the queue is empty."""
        svc = _make_service()
        svc._batch_insert = AsyncMock()

        await svc._flush_queue()

        svc._batch_insert.assert_not_called()
        assert len(svc.queue) == 0


# ===================================================================
# _batch_insert()
# ===================================================================

class TestEmbeddingQueueServiceBatchInsert:
    """Test cases for the _batch_insert() method."""

    @pytest.mark.asyncio
    async def test_batch_insert_performs_bulk_insert(self):
        """_batch_insert() should execute a bulk insert statement and commit."""
        svc = _make_service()
        batch = [
            {"source": "s1", "title": "t1", "content": "c1", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()},
            {"source": "s2", "title": "t2", "content": "c2", "embedding": [0.2], "doc_metadata": {}, "created_at": datetime.utcnow()},
        ]

        mock_session = AsyncMock()
        mock_session_ctx = _mock_async_session_ctx(mock_session)

        with patch(_SESSION_FACTORY, return_value=mock_session_ctx), \
             patch(_INSERT) as mock_insert, \
             patch(_DOC_EMBEDDING) as mock_model:
            mock_stmt = MagicMock()
            mock_insert.return_value.values.return_value = mock_stmt

            await svc._batch_insert(batch)

            mock_insert.assert_called_once_with(mock_model)
            mock_insert.return_value.values.assert_called_once_with(batch)
            mock_session.execute.assert_awaited_once_with(mock_stmt)
            mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_batch_insert_falls_back_to_individual_retry_on_failure(self):
        """_batch_insert() should call _insert_with_retry for each item on bulk failure."""
        svc = _make_service()
        svc._insert_with_retry = AsyncMock()

        batch = [
            {"source": "s1", "title": "t1", "content": "c1", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()},
            {"source": "s2", "title": "t2", "content": "c2", "embedding": [0.2], "doc_metadata": {}, "created_at": datetime.utcnow()},
        ]

        mock_session = AsyncMock()
        mock_session.execute.side_effect = Exception("Bulk insert failed")
        mock_session_ctx = _mock_async_session_ctx(mock_session)

        with patch(_SESSION_FACTORY, return_value=mock_session_ctx), \
             patch(_INSERT) as mock_insert, \
             patch(_DOC_EMBEDDING):
            mock_insert.return_value.values.return_value = MagicMock()

            await svc._batch_insert(batch)

            assert svc._insert_with_retry.await_count == 2
            svc._insert_with_retry.assert_any_await(batch[0])
            svc._insert_with_retry.assert_any_await(batch[1])


# ===================================================================
# _insert_with_retry()
# ===================================================================

class TestEmbeddingQueueServiceInsertWithRetry:
    """Test cases for the _insert_with_retry() method."""

    @pytest.mark.asyncio
    async def test_insert_with_retry_succeeds_on_first_attempt(self):
        """_insert_with_retry() should insert successfully on the first try."""
        svc = _make_service()
        item = {"source": "s", "title": "t", "content": "c", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()}

        mock_session = AsyncMock()
        mock_session_ctx = _mock_async_session_ctx(mock_session)

        with patch(_SESSION_FACTORY, return_value=mock_session_ctx), \
             patch(_DOC_EMBEDDING) as mock_model:
            mock_embedding_instance = MagicMock()
            mock_model.return_value = mock_embedding_instance

            await svc._insert_with_retry(item)

            mock_model.assert_called_once_with(**item)
            mock_session.add.assert_called_once_with(mock_embedding_instance)
            mock_session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_insert_with_retry_retries_on_failure_with_backoff(self):
        """_insert_with_retry() should retry with exponential backoff on failures."""
        svc = _make_service()
        item = {"source": "s", "title": "t", "content": "c", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()}

        # Fail twice, succeed on the third attempt
        mock_session_success = AsyncMock()
        mock_session_fail = AsyncMock()
        mock_session_fail.commit.side_effect = Exception("DB error")

        call_count = 0

        def session_factory_side_effect():
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                return _mock_async_session_ctx(mock_session_fail)
            return _mock_async_session_ctx(mock_session_success)

        with patch(_SESSION_FACTORY, side_effect=session_factory_side_effect), \
             patch(_DOC_EMBEDDING) as mock_model, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
            mock_model.return_value = MagicMock()

            await svc._insert_with_retry(item, max_retries=3)

            # Exponential backoff: 2^0=1s, 2^1=2s
            assert mock_sleep.await_count == 2
            mock_sleep.assert_any_await(1)
            mock_sleep.assert_any_await(2)

            # Third attempt succeeded
            mock_session_success.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_insert_with_retry_exhausts_all_retries(self):
        """_insert_with_retry() should log error after exhausting all retries."""
        svc = _make_service()
        item = {"source": "s", "title": "t", "content": "c", "embedding": [0.1], "doc_metadata": {}, "created_at": datetime.utcnow()}

        mock_session = AsyncMock()
        mock_session.commit.side_effect = Exception("Persistent DB error")
        mock_session_ctx = _mock_async_session_ctx(mock_session)

        with patch(_SESSION_FACTORY, return_value=mock_session_ctx), \
             patch(_DOC_EMBEDDING) as mock_model, \
             patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep, \
             patch(_LOGGER) as mock_logger:
            mock_model.return_value = MagicMock()

            await svc._insert_with_retry(item, max_retries=3)

            # 3 attempts total
            assert mock_model.call_count == 3
            # 2 sleeps (between attempts 1->2 and 2->3)
            assert mock_sleep.await_count == 2
            # Final error logged
            mock_logger.error.assert_called()


# ===================================================================
# _process_queue()
# ===================================================================

class TestEmbeddingQueueServiceProcessQueue:
    """Test cases for the _process_queue() background loop."""

    @pytest.mark.asyncio
    async def test_process_queue_loops_until_not_running(self):
        """_process_queue() should loop calling sleep + flush until _running is False."""
        svc = _make_service(flush_interval=0.01)
        flush_mock = AsyncMock()
        iteration_count = 0

        async def counting_flush():
            nonlocal iteration_count
            iteration_count += 1
            if iteration_count >= 3:
                svc._running = False
            await flush_mock()

        svc._flush_queue = counting_flush
        svc._running = True

        await svc._process_queue()

        assert svc._running is False
        assert iteration_count >= 3
        assert flush_mock.await_count >= 3

    @pytest.mark.asyncio
    async def test_process_queue_handles_exceptions_gracefully(self):
        """_process_queue() should catch exceptions and continue looping."""
        svc = _make_service(flush_interval=0.01)
        call_count = 0

        async def failing_flush():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("Simulated flush failure")
            if call_count >= 3:
                svc._running = False

        svc._flush_queue = failing_flush
        svc._running = True

        with patch(_LOGGER):
            await svc._process_queue()

        # Continued past the exception
        assert call_count >= 3
        assert svc._running is False


# ===================================================================
# _handle_task_error()
# ===================================================================

class TestEmbeddingQueueServiceHandleTaskError:
    """Test cases for the _handle_task_error() callback."""

    def test_handle_task_error_with_cancelled_error(self):
        """_handle_task_error() should log info for CancelledError."""
        svc = _make_service()
        mock_task = MagicMock()
        mock_task.result.side_effect = asyncio.CancelledError()

        with patch(_LOGGER) as mock_logger:
            svc._handle_task_error(mock_task)
            mock_logger.info.assert_called_once()
            assert "cancelled" in mock_logger.info.call_args[0][0].lower()

    def test_handle_task_error_with_general_exception(self):
        """_handle_task_error() should log error for unexpected exceptions."""
        svc = _make_service()
        mock_task = MagicMock()
        mock_task.result.side_effect = RuntimeError("task crashed")

        with patch(_LOGGER) as mock_logger:
            svc._handle_task_error(mock_task)
            mock_logger.error.assert_called_once()
            assert "task crashed" in mock_logger.error.call_args[0][0]

    def test_handle_task_error_with_successful_task(self):
        """_handle_task_error() should do nothing for a successfully completed task."""
        svc = _make_service()
        mock_task = MagicMock()
        mock_task.result.return_value = None

        with patch(_LOGGER) as mock_logger:
            svc._handle_task_error(mock_task)
            mock_logger.error.assert_not_called()
            mock_logger.info.assert_not_called()
