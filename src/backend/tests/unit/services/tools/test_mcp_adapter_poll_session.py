"""Tests for MCPAdapter.poll_session — one MCP connection reused for many
tool calls (the polling companion to the stateless execute_tool).

A separate file: test_mcp_adapter.py is over the size ceiling and must
not grow.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.services.tools.mcp_adapter import MCPAdapter


class TestPollSession:
    """adapter.poll_session(): one handshake, many calls (the polling
    companion to the stateless execute_tool)."""

    def _params(self):
        return {
            "url": "https://example.com/mcp",
            "timeout_seconds": 30,
            "max_retries": 3,
            "rate_limit": 60,
            "headers": {"Authorization": "Bearer test-token"},
        }

    @pytest.mark.asyncio
    async def test_one_handshake_many_calls(self):
        adapter = MCPAdapter(self._params())
        adapter._transport = "streamable_http"

        mock_session = AsyncMock()
        mock_session.initialize = AsyncMock()
        mock_session.call_tool = AsyncMock(side_effect=["r1", "r2", "r3"])

        with patch("mcp.client.streamable_http.streamablehttp_client") as mock_connect:
            with patch("mcp.ClientSession") as mock_client_session:
                mock_connect.return_value.__aenter__.return_value = (
                    Mock(),
                    Mock(),
                    None,
                )
                mock_client_session.return_value.__aenter__.return_value = mock_session

                async with adapter.poll_session() as session:
                    assert await session.call("poll_tool", {"id": 1}) == "r1"
                    assert await session.call("poll_tool", {"id": 2}) == "r2"
                    assert await session.call("poll_tool", {"id": 3}) == "r3"

        mock_connect.assert_called_once()  # ONE transport handshake
        assert mock_session.initialize.await_count == 1
        assert mock_session.call_tool.await_count == 3
        # Rate-limit bookkeeping still applies per call.
        assert len(adapter._call_timestamps) == 3

    @pytest.mark.asyncio
    async def test_call_outside_context_raises(self):
        adapter = MCPAdapter(self._params())
        adapter._transport = "streamable_http"
        session = adapter.poll_session()
        with pytest.raises(RuntimeError):
            await session.call("poll_tool", {})

    @pytest.mark.asyncio
    async def test_open_failure_propagates(self):
        adapter = MCPAdapter(self._params())
        adapter._transport = "streamable_http"

        with patch("mcp.client.streamable_http.streamablehttp_client") as mock_connect:
            mock_connect.return_value.__aenter__.side_effect = ConnectionError(
                "refused"
            )
            with pytest.raises(ConnectionError):
                async with adapter.poll_session():
                    pass
