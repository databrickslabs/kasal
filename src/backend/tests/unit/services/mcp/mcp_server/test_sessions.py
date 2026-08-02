"""The server-initiated stream, and what makes a stale tool list impossible.

This is the machinery that let `list_crews`/`start_crew` be retired: without a
way to tell a connected client that the published set moved, its tool list is a
snapshot from `initialize` and a capability published a minute later is
invisible to it. A workspace hit exactly that — ten tools for crews that had
been deleted, and the freshly published one missing.
"""

import asyncio

import pytest

from src.services.mcp.mcp_server import sessions
from src.services.publications import signals


@pytest.fixture(autouse=True)
def clean_registry():
    for session_id in sessions.active_sessions():
        sessions.close_session(session_id)
    yield
    for session_id in sessions.active_sessions():
        sessions.close_session(session_id)


class TestSessionRegistry:
    def test_opening_a_session_returns_an_id_that_is_tracked(self):
        session_id = sessions.open_session(["acme_corp"])
        assert session_id in sessions.active_sessions()

    def test_closing_forgets_it(self):
        session_id = sessions.open_session(["acme_corp"])
        assert sessions.close_session(session_id) is True
        assert session_id not in sessions.active_sessions()

    def test_closing_an_unknown_id_is_not_an_error(self):
        assert sessions.close_session("never-existed") is False

    def test_a_client_supplied_id_is_adopted(self):
        """A client may present an id from before a reload. Nothing is
        authorised by it — the caller is resolved from headers on every request
        — so adopting beats refusing, which would leave it with no stream."""
        adopted = sessions.adopt_session("from-a-previous-process", ["acme_corp"])

        assert adopted == "from-a-previous-process"
        assert adopted in sessions.active_sessions()


class TestNotification:
    @pytest.mark.asyncio
    async def test_a_session_is_told_the_tool_list_changed(self):
        session_id = sessions.open_session(["acme_corp"])

        assert sessions.notify_tools_changed(["acme_corp"]) == 1

        agen = sessions.stream(session_id)
        frame = await asyncio.wait_for(agen.__anext__(), timeout=1)
        assert b"notifications/tools/list_changed" in frame
        await agen.aclose()

    def test_another_workspace_is_not_woken(self):
        """A publish in one tenant must not make every other tenant's client
        refetch its whole tool list."""
        sessions.open_session(["globex_inc"])

        assert sessions.notify_tools_changed(["acme_corp"]) == 0

    def test_a_session_with_no_known_groups_hears_everything(self):
        """'We do not know which tenant this stream belongs to' is not a reason
        to withhold a message whose only effect is a refetch."""
        sessions.open_session([])

        assert sessions.notify_tools_changed(["acme_corp"]) == 1

    def test_notifying_with_no_sessions_is_a_no_op(self):
        assert sessions.notify_tools_changed(["acme_corp"]) == 0

    def test_a_backlog_does_not_grow_without_bound(self):
        """Every message is the same 'refetch'; a client that misses one and
        receives the next is in exactly the same state."""
        session_id = sessions.open_session(["acme_corp"])

        for _ in range(200):
            sessions.notify_tools_changed(["acme_corp"])

        queued = sessions._sessions[session_id].queue.qsize()
        assert queued <= sessions._QUEUE_LIMIT


class TestWiredToThePublicationRegistry:
    @pytest.mark.asyncio
    async def test_a_catalogue_change_reaches_the_stream(self):
        """The registry stays protocol-neutral: it announces, and this module
        listens. Nothing in `publication.py` imports the MCP session table."""
        session_id = sessions.open_session(["acme_corp"])

        await signals.catalogue_changed(["acme_corp"])

        assert sessions._sessions[session_id].queue.qsize() == 1

    @pytest.mark.asyncio
    async def test_a_listener_that_raises_does_not_fail_the_publish(self):
        @signals.on_catalogue_changed
        def _boom(group_ids):
            raise RuntimeError("editor went away")

        try:
            await signals.catalogue_changed(["acme_corp"])
        finally:
            signals._listeners.remove(_boom)


class TestStream:
    @pytest.mark.asyncio
    async def test_an_unknown_session_yields_nothing(self):
        frames = [frame async for frame in sessions.stream("never-opened")]

        assert frames == []

    @pytest.mark.asyncio
    async def test_the_session_is_forgotten_when_the_stream_ends(self):
        """A disconnect is the only close signal an HTTP stream gives us."""
        session_id = sessions.open_session(["acme_corp"])

        agen = sessions.stream(session_id)
        sessions.notify_tools_changed(["acme_corp"])
        await asyncio.wait_for(agen.__anext__(), timeout=1)
        await agen.aclose()

        assert session_id not in sessions.active_sessions()
