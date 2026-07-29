"""The exported app talks to MCP servers without crewai_tools.

``MCPServerAdapter`` is replaced by ``agent_server/mcp_tools.py`` on the raw
``mcp`` SDK. Since that is a reimplementation of the async/sync bridge the
adapter provided, the tests that matter run against a REAL MCP server started
in-process: connect, list, call, call again on the same session, call from a
worker thread (which is where a crew runs), and shut down.

Mocking the SDK here would test the mock. The whole risk in this phase is
whether a background event loop can hold a session open for synchronous
callers — and only a real server answers that.
"""

import importlib
import socket
import threading
import time
from contextlib import ExitStack

import pytest

pytest.importorskip("mcp")


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def mcp_server():
    """A real streamable-HTTP MCP server, in a background thread."""
    from mcp.server.fastmcp import FastMCP

    port = _free_port()
    server = FastMCP("test", host="127.0.0.1", port=port, stateless_http=True)

    @server.tool()
    def search(query: str, limit: int = 3) -> str:
        """Search a fake corpus."""
        return f"results for {query!r} (limit={limit}, type={type(limit).__name__})"

    @server.tool()
    def explode() -> str:
        """Always fails."""
        raise RuntimeError("tool blew up")

    thread = threading.Thread(
        target=lambda: server.run(transport="streamable-http"), daemon=True
    )
    thread.start()

    deadline = time.time() + 20
    while time.time() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                break
        except OSError:
            time.sleep(0.1)
    else:  # pragma: no cover - only on a broken environment
        pytest.fail("the test MCP server never came up")
    return f"http://127.0.0.1:{port}/mcp"


@pytest.fixture
def mcp_tools(app_bundle):
    return importlib.import_module("agent_server.mcp_tools")


class TestAgainstARealServer:
    def test_connect_and_list_tools(self, mcp_tools, mcp_server):
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            names = sorted(t.name for t in conn.tools)
        assert names == ["explode", "search"]

    def test_tools_are_kasal_tools_a_runtime_agent_accepts(self, mcp_tools, mcp_server):
        """``runtime.Agent.tools`` is typed ``list[BaseTool]`` — MCP tools have to
        BE Kasal tools, not be adapted into them at the last moment."""
        runtime = importlib.import_module(
            "agent_server.kasal_runtime.services.execution.runtime"
        )
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            agent = runtime.Agent(
                role="R", goal="g", backstory="b", llm=None, tools=conn.tools
            )
            assert {t.name for t in agent.tools} == {"search", "explode"}

    def test_calling_a_tool_synchronously(self, mcp_tools, mcp_server):
        """The crew is synchronous; this is the whole point of the bridge."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            search = next(t for t in conn.tools if t.name == "search")
            assert "results for 'kasal'" in search._run(query="kasal", limit=2)

    def test_the_session_survives_multiple_calls(self, mcp_tools, mcp_server):
        """Reconnecting per call would re-run the MCP handshake every time."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            search = next(t for t in conn.tools if t.name == "search")
            for i in range(3):
                assert f"'q{i}'" in search._run(query=f"q{i}")

    def test_calls_work_from_a_worker_thread(self, mcp_tools, mcp_server):
        """A crew kickoff runs under ``asyncio.to_thread``, so every real tool
        call comes from a thread that is not the one that connected."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            search = next(t for t in conn.tools if t.name == "search")
            result = {}
            worker = threading.Thread(
                target=lambda: result.update(out=search._run(query="threaded"))
            )
            worker.start()
            worker.join(timeout=30)
            assert "'threaded'" in result.get("out", "")

    def test_a_failing_tool_returns_text_rather_than_raising(
        self, mcp_tools, mcp_server
    ):
        """A tool error is information the agent can act on — it must not end
        the run."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            explode = next(t for t in conn.tools if t.name == "explode")
            out = explode._run()
        assert out.startswith("Tool error:")
        assert "blew up" in out

    def test_arguments_are_coerced_to_the_servers_types(self, mcp_tools, mcp_server):
        """Models emit ``"2"`` for an integer parameter constantly; a strict
        server rejects the call unless it is coerced first."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            search = next(t for t in conn.tools if t.name == "search")
            out = search._run(query="x", limit="2")
        assert "limit=2" in out and "type=int" in out

    def test_the_generated_args_schema_matches_the_server(self, mcp_tools, mcp_server):
        """The args schema is what the agent is shown, so it decides whether the
        model can call the tool at all."""
        with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
            search = next(t for t in conn.tools if t.name == "search")
            fields = search.args_schema.model_fields
        assert set(fields) == {"query", "limit"}
        assert fields["query"].is_required()
        assert not fields["limit"].is_required()

    def test_open_mcp_server_closes_with_the_stack(self, mcp_tools, mcp_server):
        with ExitStack() as stack:
            tools = mcp_tools.open_mcp_server(stack, "test", {"url": mcp_server})
            assert tools
            search = next(t for t in tools if t.name == "search")
            assert "results for" in search._run(query="x")
        # After the stack closes, the connection is gone and a call says so
        # instead of hanging or raising into the crew.
        assert "not connected" in search._run(query="x")


class TestTransportChoice:
    def test_the_deprecated_client_is_not_used(self, mcp_tools, mcp_server):
        """``streamablehttp_client`` warns on every connect. Connecting through
        the modern ``streamable_http_client`` instead keeps the app's logs clean
        — assert it by turning that warning into an error."""
        import warnings

        with warnings.catch_warnings():
            warnings.filterwarnings(
                "error",
                message=".*streamable_http_client.*",
                category=DeprecationWarning,
            )
            with mcp_tools.MCPServerConnection("test", {"url": mcp_server}) as conn:
                assert conn.tools

    def test_auth_headers_reach_the_server(self, mcp_tools, mcp_server, monkeypatch):
        """Headers moved onto the httpx client in the modern API — getting this
        wrong means every Databricks-managed MCP server 401s, and the connection
        still succeeds against an unauthenticated one like this fixture."""
        import httpx

        captured = {}
        real_client = httpx.AsyncClient  # capture BEFORE patching

        class _Recorder(real_client):
            def __init__(self, *args, **kwargs):
                captured.update(kwargs)
                super().__init__(*args, **kwargs)

        monkeypatch.setattr(httpx, "AsyncClient", _Recorder)
        with mcp_tools.MCPServerConnection(
            "test", {"url": mcp_server, "headers": {"Authorization": "Bearer tok"}}
        ) as conn:
            assert conn.tools
        assert captured["headers"]["Authorization"] == "Bearer tok"


class TestFailureIsolation:
    def test_an_unreachable_server_raises_promptly(self, mcp_tools):
        """The caller catches this per server, so one dead server does not take
        down every other server's tools."""
        port = _free_port()  # nothing listening
        with pytest.raises((ConnectionError, TimeoutError)) as excinfo:
            with mcp_tools.MCPServerConnection(
                "dead", {"url": f"http://127.0.0.1:{port}/mcp"}, timeout=15
            ):
                pass
        # The message must name the server; anyio's raw ExceptionGroup
        # ("unhandled errors in a TaskGroup") tells an operator nothing.
        assert "dead" in str(excinfo.value)

    def test_one_dead_server_does_not_stop_a_healthy_one(self, mcp_tools, mcp_server):
        dead = f"http://127.0.0.1:{_free_port()}/mcp"
        healthy = []
        with ExitStack() as stack:
            for name, url in (("dead", dead), ("live", mcp_server)):
                try:
                    healthy.extend(
                        mcp_tools.open_mcp_server(stack, name, {"url": url}, timeout=15)
                    )
                except Exception:  # noqa: BLE001 — what agent.py does
                    continue
        assert {t.name for t in healthy} == {"search", "explode"}


class TestPureHelpers:
    """No server needed — these are the parts easiest to get subtly wrong."""

    def test_schema_generation_handles_an_empty_schema(self, mcp_tools):
        model = mcp_tools.args_model_from_schema("noargs", {})
        assert model.model_fields == {}

    def test_unknown_json_types_become_any_rather_than_a_guess(self, mcp_tools):
        """Advertising a wrong type is worse than advertising none: the model
        formats an argument the server then rejects."""
        model = mcp_tools.args_model_from_schema(
            "t", {"properties": {"weird": {"type": "tuple"}}}
        )
        assert "weird" in model.model_fields

    def test_a_tool_name_with_punctuation_still_makes_a_valid_model(self, mcp_tools):
        model = mcp_tools.args_model_from_schema("my-tool.v2", {})
        assert model.__name__ == "my_tool_v2Args"

    @pytest.mark.parametrize(
        "value,wanted,expected",
        [
            ("3", "integer", 3),
            ("3.5", "number", 3.5),
            ("true", "boolean", True),
            ("no", "boolean", False),
            ("plain", "string", "plain"),
            ("3.9", "integer", 3),
        ],
    )
    def test_argument_coercion(self, mcp_tools, value, wanted, expected):
        schema = {"properties": {"v": {"type": wanted}}}
        assert mcp_tools.coerce_arguments({"v": value}, schema) == {"v": expected}

    def test_empty_arguments_are_dropped_not_sent(self, mcp_tools):
        """An absent optional is what a server expects; ``""`` is not."""
        schema = {"properties": {"v": {"type": "string"}}}
        assert mcp_tools.coerce_arguments({"v": "", "w": None}, schema) == {}

    def test_an_uncoercible_value_is_passed_through(self, mcp_tools):
        """Let the server say what it does not like — dropping the argument
        turns a clear error into a confusing one."""
        schema = {"properties": {"v": {"type": "integer"}}}
        assert mcp_tools.coerce_arguments({"v": "abc"}, schema) == {"v": "abc"}

    def test_result_rendering(self, mcp_tools):
        class _Item:
            def __init__(self, text):
                self.text = text

        class _Result:
            isError = False
            content = [_Item("one"), _Item("two")]

        assert mcp_tools.render_result(_Result()) == "one\ntwo"

    def test_an_empty_result_says_so(self, mcp_tools):
        class _Result:
            isError = False
            content = []

        assert "no content" in mcp_tools.render_result(_Result())

    def test_exception_groups_are_unwrapped(self, mcp_tools):
        inner = ConnectionRefusedError("connection refused")
        group = BaseExceptionGroup("unhandled errors in a TaskGroup", [inner])
        assert mcp_tools._unwrap(group) is inner


class TestNoCrewaiTools:
    def test_the_app_no_longer_imports_mcpserveradapter(self, app_bundle):
        """Checked as code, not prose — the docstring explaining the change
        names the class it replaced, and should keep doing so."""
        agent_py = (app_bundle / "agent_server" / "agent.py").read_text("utf-8")
        used = [
            line.strip()
            for line in agent_py.splitlines()
            if "MCPServerAdapter" in line
            and not line.strip().startswith(("#", '"'))
            and "``" not in line
        ]
        assert not used, f"MCPServerAdapter is still used: {used}"
        assert "from agent_server.mcp_tools import open_mcp_server" in agent_py

    def test_nothing_in_the_bundle_imports_crewai_tools(self, app_bundle):
        offenders = []
        for path in sorted((app_bundle).rglob("*.py")):
            for line in path.read_text(encoding="utf-8").splitlines():
                if line.strip().startswith(("from crewai", "import crewai")):
                    offenders.append(f"{path.name}: {line.strip()}")
        assert not offenders, "crewai is still imported:\n" + "\n".join(offenders)
