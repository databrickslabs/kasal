"""Parsing an uploaded knowledge file must not freeze the event loop.

pdfminer and openpyxl are pure-Python and CPU-bound: a 300-page PDF or a large
workbook takes seconds. The deployed app is one uvicorn process on one loop, so
parsing on the loop stalls every other user's request and SSE stream for that
long. The parse runs in a worker thread; a coroutine running alongside the
upload must keep getting turns.
"""

import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.knowledge.databricks_service import DatabricksKnowledgeService

PARSE_SECONDS = 0.6


def _svc():
    with (
        patch("src.services.knowledge.databricks_service.DatabricksVolumeRepository"),
        patch(
            "src.services.knowledge.embedding_service.KnowledgeEmbeddingService",
            MagicMock(return_value=AsyncMock()),
        ),
        patch(
            "src.services.knowledge.search_service.KnowledgeSearchService",
            MagicMock(return_value=AsyncMock()),
        ),
    ):
        svc = DatabricksKnowledgeService(
            session=AsyncMock(), group_id="g1", created_by_email="u@example.com"
        )
    svc.embedding_service = AsyncMock()
    svc.embedding_service.file_already_embedded = AsyncMock(return_value=False)
    svc.embedding_service.forget_file = AsyncMock(return_value=0)
    svc.embedding_service.embed_file = AsyncMock(return_value={"status": "success"})
    svc._ensure_knowledge_tool_in_group = AsyncMock()
    return svc


def _upload(filename):
    f = AsyncMock()
    f.filename = filename
    f.size = None
    f.read = AsyncMock(return_value=b"raw bytes of a large file")
    return f


def _slow_pdf_text(fp, *args, **kwargs):
    time.sleep(PARSE_SECONDS)  # stands in for pdfminer's CPU-bound walk
    return "page text " * 100


def _slow_workbook(fp, *args, **kwargs):
    time.sleep(PARSE_SECONDS)  # stands in for openpyxl reading every row
    sheet = MagicMock(title="Sheet1")
    sheet.iter_rows.return_value = [("a", "b"), (1, 2)]
    book = MagicMock(worksheets=[sheet])
    return book


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "filename, target, slow",
    [
        ("big.pdf", "pdfminer.high_level.extract_text", _slow_pdf_text),
        ("big.xlsx", "openpyxl.load_workbook", _slow_workbook),
    ],
)
async def test_a_large_parse_leaves_the_loop_responsive(filename, target, slow):
    svc = _svc()
    ticks = []
    stop = asyncio.Event()

    async def _heartbeat():
        while not stop.is_set():
            ticks.append(time.monotonic())
            await asyncio.sleep(0.01)

    with patch(target, slow):
        beat = asyncio.create_task(_heartbeat())
        await asyncio.sleep(0)  # let the heartbeat start
        try:
            result = await svc.upload_knowledge_file(
                file=_upload(filename),
                execution_id="exec-1",
                group_id="g1",
                volume_config={},
            )
            finished = time.monotonic()
        finally:
            stop.set()
            await beat

    assert result["status"] == "success"
    beats = [t for t in ticks if t <= finished] + [finished]
    longest_gap = max(b - a for a, b in zip(beats, beats[1:]))
    assert (
        longest_gap < PARSE_SECONDS / 2
    ), f"event loop stalled {longest_gap:.2f}s while parsing {filename}"
    # The parsed text is what got embedded.
    embedded = svc.embedding_service.embed_file.await_args.kwargs["file_content"]
    assert embedded
