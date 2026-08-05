"""The exported app ships Kasal's runtime, and it actually imports standalone.

The point of this file is the import test: it writes the emitted bundle to a
temp dir and imports the vendored runtime with the backend's own ``src`` package
made *unavailable*, so any leaked ``src.`` dependency fails here rather than on a
customer's cluster. String assertions cannot catch that.

There is deliberately no "vendor in sync with source" drift guard: the copy is
read from live backend source at export time, so drift is not possible. See
runtime_vendor.py.
"""

import ast
import importlib
import sys

import pytest
import pytest_asyncio

from src.services.export.databricks_app_exporter import DatabricksAppExporter
from src.services.export.runtime_vendor import (
    STUB_INITS,
    VENDOR_PKG,
    VENDOR_ROOT,
    rewrite_import_root,
)


@pytest.fixture
def exporter():
    return DatabricksAppExporter()


@pytest.fixture
def crew_data():
    return {
        "id": "crew-123",
        "name": "Research Crew",
        "agents": [
            {
                "id": "a1",
                "name": "Researcher",
                "role": "Researcher",
                "goal": "Find things",
                "backstory": "Seasoned",
            }
        ],
        "tasks": [
            {
                "id": "t1",
                "name": "research",
                "description": "Research the topic",
                "expected_output": "A report",
                "agent_id": "a1",
            }
        ],
    }


@pytest_asyncio.fixture
async def runtime_files(exporter, crew_data):
    """Emitted path → content, for the vendored runtime only."""
    result = await exporter.export(crew_data, {})
    return {
        f["path"]: f["content"]
        for f in result["files"]
        if f["path"].startswith(f"{VENDOR_ROOT}/")
    }


class TestRuntimeShipped:
    @pytest.mark.asyncio
    async def test_the_runtime_is_emitted(self, runtime_files):
        for required in [
            f"{VENDOR_ROOT}/__init__.py",
            f"{VENDOR_ROOT}/services/execution/runtime/__init__.py",
            f"{VENDOR_ROOT}/services/execution/runtime/agent.py",
            f"{VENDOR_ROOT}/services/execution/runtime/crew.py",
            f"{VENDOR_ROOT}/services/execution/runtime/task.py",
            f"{VENDOR_ROOT}/services/execution/runtime/executor.py",
            f"{VENDOR_ROOT}/services/execution/runtime/guardrail.py",
            f"{VENDOR_ROOT}/core/events/bus.py",
            f"{VENDOR_ROOT}/core/events/types.py",
            f"{VENDOR_ROOT}/core/llm/transport/completion.py",
            f"{VENDOR_ROOT}/core/llm/json_extraction.py",
            f"{VENDOR_ROOT}/services/tools/base.py",
        ]:
            assert required in runtime_files, f"missing {required}"

    @pytest.mark.asyncio
    async def test_every_package_dir_has_an_init(self, runtime_files):
        """A missing __init__.py imports fine locally and breaks under a stale
        __pycache__ — the failure reads as 'the module vanished'."""
        dirs = {p.rsplit("/", 1)[0] for p in runtime_files}
        for d in dirs:
            assert f"{d}/__init__.py" in runtime_files, f"{d} has no __init__.py"

    @pytest.mark.asyncio
    async def test_core_llm_init_is_stubbed(self, runtime_files):
        """Upstream ``core/llm/__init__.py`` imports usage_telemetry, which
        reaches into src.utils.*. The vendored one must not."""
        init = runtime_files[f"{VENDOR_ROOT}/core/llm/__init__.py"]
        tree = ast.parse(init)
        assert not [
            n for n in ast.walk(tree) if isinstance(n, (ast.Import, ast.ImportFrom))
        ], "the vendored core/llm package must import nothing"

    @pytest.mark.asyncio
    async def test_no_src_import_leaks(self, runtime_files):
        """Nothing may still import Kasal's backend package."""
        offenders = []
        for path, content in runtime_files.items():
            for lineno, line in enumerate(content.splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith(("from src.", "import src.")):
                    offenders.append(f"{path}:{lineno}: {stripped}")
        assert (
            not offenders
        ), "vendored runtime still imports Kasal's backend:\n" + "\n".join(offenders)

    @pytest.mark.asyncio
    async def test_every_file_parses(self, runtime_files):
        """The template tree carries {{TOKEN}} placeholders; the vendored runtime
        must NOT be touched by that substitution walk."""
        for path, content in runtime_files.items():
            try:
                ast.parse(content)
            except SyntaxError as exc:  # pragma: no cover - failure detail
                pytest.fail(f"{path} is not valid Python: {exc}")

    @pytest.mark.asyncio
    async def test_copies_differ_from_source_only_in_the_import_root(
        self, runtime_files
    ):
        """Every vendored line either matches its original byte for byte, or is
        an import line whose ONLY change is the package root. This is what makes
        'copied verbatim' a checkable claim rather than a comment."""
        from src.services.export.runtime_vendor import BACKEND_SRC

        checked = 0
        for path, emitted in runtime_files.items():
            rel = path[len(VENDOR_ROOT) + 1 :]
            if rel in STUB_INITS:
                continue  # synthesised, not copied — see runtime_vendor.STUB_INITS
            source = BACKEND_SRC / rel
            assert source.is_file(), f"{path} has no source at {source}"
            original = source.read_text(encoding="utf-8")
            checked += 1
            for got, want in zip(emitted.splitlines(), original.splitlines()):
                if got == want:
                    continue
                assert got == rewrite_import_root(want) and want != got, (
                    f"{path}: vendored line is not a pure import re-root\n"
                    f"  source: {want!r}\n  export: {got!r}"
                )
            assert len(emitted.splitlines()) == len(
                original.splitlines()
            ), f"{path}: line count changed — the copy is not verbatim"
        assert checked >= 15, f"only {checked} files compared against source"

    @pytest.mark.asyncio
    async def test_every_intra_runtime_import_actually_ships(self, runtime_files):
        """A module the runtime imports from itself must BE in the bundle.

        The vendor list mixes whole trees with individually-named modules, so a
        new module added next to a vendored one — same package, not inside a
        vendored tree — is picked up by nothing. That is not hypothetical:
        ``core/llm/model_capabilities.py`` was added beside the already-vendored
        ``core/llm/transport/`` and imported by ``transport/completion.py``, and
        because it never shipped, EVERY exported app died at import with
        ``No module named 'agent_server.kasal_runtime.core.llm.model_capabilities'``.

        The 21 errors that surfaced it were all fixture setup, so they read as one
        broken test file rather than a broken product.
        """
        shipped = {
            path[len(VENDOR_ROOT) + 1 :].removesuffix(".py").replace("/", ".")
            for path in runtime_files
        }
        # A package is importable via its __init__, so record the package too.
        shipped |= {m.removesuffix(".__init__") for m in shipped}

        missing = []
        for path, content in runtime_files.items():
            for node in ast.walk(ast.parse(content)):
                if not isinstance(node, ast.ImportFrom) or node.level:
                    continue  # relative imports are covered by the tree copy
                module = node.module or ""
                if not module.startswith(f"{VENDOR_PKG}."):
                    continue  # third-party or stdlib
                target = module[len(VENDOR_PKG) + 1 :]
                if target in shipped:
                    continue
                # `from pkg import name` where name is itself a module.
                if any(f"{target}.{a.name}" in shipped for a in node.names):
                    continue
                missing.append(f"{path}: imports {module}")

        assert not missing, (
            "the vendored runtime imports modules that are not in the bundle — "
            "add them to _TREES or _MODULES in runtime_vendor.py:\n"
            + "\n".join(sorted(missing))
        )


class TestRuntimeImportsStandalone:
    """Write the bundle to disk and import it with ``src`` unavailable."""

    @pytest.mark.asyncio
    async def test_runtime_imports_and_exposes_the_crew_api(
        self, runtime_files, tmp_path, monkeypatch
    ):
        for rel, content in runtime_files.items():
            dest = tmp_path / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content, encoding="utf-8")
        (tmp_path / "agent_server").mkdir(exist_ok=True)
        (tmp_path / "agent_server" / "__init__.py").write_text("", encoding="utf-8")

        # Hide the backend so a leaked ``import src.x`` cannot resolve. The
        # already-imported ``src`` modules stay in sys.modules, so also block
        # fresh ones via a meta-path finder that raises for the ``src`` root.
        class _BlockSrc:
            def find_module(self, fullname, path=None):  # pragma: no cover
                return None

            def find_spec(self, fullname, path=None, target=None):
                if fullname == "src" or fullname.startswith("src."):
                    raise ImportError(
                        f"vendored runtime reached back into Kasal's backend: {fullname}"
                    )
                return None

        blocker = _BlockSrc()
        monkeypatch.syspath_prepend(str(tmp_path))
        sys.meta_path.insert(0, blocker)
        touched = [
            m for m in sys.modules if m.startswith(f"{VENDOR_PKG.split('.')[0]}.")
        ]
        for m in touched:
            sys.modules.pop(m, None)
        try:
            runtime = importlib.import_module(
                f"{VENDOR_PKG}.services.execution.runtime"
            )
            events = importlib.import_module(f"{VENDOR_PKG}.core.events")
            transport = importlib.import_module(f"{VENDOR_PKG}.core.llm.transport")
            tools = importlib.import_module(f"{VENDOR_PKG}.services.tools.base")
        finally:
            sys.meta_path.remove(blocker)
            for m in [m for m in sys.modules if m.startswith(VENDOR_PKG)]:
                sys.modules.pop(m, None)

        # The exact surface agent.py will swap onto in Phase 3.
        for name in ("Agent", "Task", "Crew", "Process", "LLMGuardrail", "TaskOutput"):
            assert hasattr(runtime, name), f"runtime.{name} missing"
        assert hasattr(events, "EventsBus") or hasattr(events, "event_bus")
        assert hasattr(transport, "OpenAICompletion")
        assert hasattr(tools, "BaseTool")

    @pytest.mark.asyncio
    async def test_a_crew_can_be_constructed_from_the_vendored_copy(
        self, runtime_files, tmp_path, monkeypatch
    ):
        """Constructing Agent/Task/Crew exercises the Pydantic validators, which
        is where a half-vendored model would blow up."""
        for rel, content in runtime_files.items():
            dest = tmp_path / rel
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(content, encoding="utf-8")
        (tmp_path / "agent_server" / "__init__.py").write_text("", encoding="utf-8")
        monkeypatch.syspath_prepend(str(tmp_path))
        for m in [m for m in sys.modules if m.startswith(VENDOR_PKG)]:
            sys.modules.pop(m, None)
        try:
            rt = importlib.import_module(f"{VENDOR_PKG}.services.execution.runtime")
            agent = rt.Agent(
                role="Researcher", goal="Find things", backstory="Seasoned", llm=None
            )
            task = rt.Task(
                description="Research the topic",
                expected_output="A report",
                agent=agent,
            )
            crew = rt.Crew(agents=[agent], tasks=[task], process=rt.Process.sequential)
        finally:
            for m in [m for m in sys.modules if m.startswith(VENDOR_PKG)]:
                sys.modules.pop(m, None)
        assert crew.agents[0].role == "Researcher"
        assert crew.tasks[0].expected_output == "A report"
