"""Vendor Kasal's agent runtime into an exported standalone app.

The exported Databricks App has no Kasal backend to import from — no database,
no session, no ``GroupContext``. It gets a *copy* of the runtime instead, under
``agent_server/kasal_runtime/``, so an exported crew runs on the same engine
Kasal itself runs, rather than on a second engine kept in agreement by hand.

**Read from the live source, never from a committed copy.** The A2UI composer
already does this (``SHARED_A2UI_DIR``), and it is why there is no drift guard
here to go stale: the bytes shipped are the bytes Kasal runs, resolved at export
time. ``src/deploy.py`` ships ``backend/src`` wholesale, so this works from the
deployed app too. (The A2UI *frontend* renderer is a committed copy only because
the deployed app does not ship the frontend source tree.)

What is copied, and why that is all of it: the runtime is plain Pydantic models
with no DI, no session and no DB. Its entire first-party dependency set is the
event bus, the LLM transport, ``json_extraction`` and ``BaseTool``; its entire
third-party footprint is ``pydantic``, ``pydantic_core`` and a lazy ``openai``.

The only edit made to any copied file is rewriting the ``src.`` import root to
the vendored package root — a single-token substitution on import lines, which
keeps the copies readable next to their originals.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiofiles

# ``src/backend/src`` — this file is at ``src/backend/src/services/export/``.
BACKEND_SRC = Path(__file__).parent.parent.parent

# Where the runtime lands in the export, as a path and as a package.
VENDOR_ROOT = "agent_server/kasal_runtime"
VENDOR_PKG = "agent_server.kasal_runtime"

# Whole packages copied verbatim (source dir → path under VENDOR_ROOT). The
# layout MIRRORS the backend so the import rewrite stays a single substitution
# and a vendored file diffs cleanly against its original.
_TREES: List[Tuple[Path, str]] = [
    (BACKEND_SRC / "core" / "events", "core/events"),
    (BACKEND_SRC / "core" / "llm" / "transport", "core/llm/transport"),
    (BACKEND_SRC / "services" / "execution" / "runtime", "services/execution/runtime"),
]

# Individual modules the trees above import.
_MODULES: List[Tuple[Path, str]] = [
    # runtime/executor.py and transport/instructor.py
    (
        BACKEND_SRC / "core" / "llm" / "json_extraction.py",
        "core/llm/json_extraction.py",
    ),
    # runtime/{agent,task,executor}.py — the tool contract. Itself has zero
    # ``src.`` imports, which is why the runtime can depend on it standalone.
    (BACKEND_SRC / "services" / "tools" / "base.py", "services/tools/base.py"),
    # The general-purpose tools an exported crew may be configured with. These
    # used to map onto crewai_tools' SerperDevTool / ScrapeWebsiteTool /
    # DallETool because the export could not ship a Kasal BaseTool; now that it
    # can, the exported app runs the SAME tool implementations Kasal runs
    # instead of a lookalike from another library.
    #
    # All four are stdlib + pydantic (``web_fetch`` uses urllib, not requests or
    # beautifulsoup) and import only ``.base``/``.web_fetch``, so their relative
    # imports need no rewriting at all.
    (
        BACKEND_SRC / "services" / "tools" / "web_fetch.py",
        "services/tools/web_fetch.py",
    ),
    (
        BACKEND_SRC / "services" / "tools" / "serper_search.py",
        "services/tools/serper_search.py",
    ),
    (
        BACKEND_SRC / "services" / "tools" / "scrape_website.py",
        "services/tools/scrape_website.py",
    ),
    (
        BACKEND_SRC / "services" / "tools" / "image_generation.py",
        "services/tools/image_generation.py",
    ),
]

# Package ``__init__`` files we SYNTHESISE rather than copy. Each canonical one
# reaches beyond the vendored surface, so copying it would drag the backend in:
#
#   core/llm/__init__.py    imports ``usage_telemetry`` → ``src.utils.user_context``
#                           and ``src.utils.telemetry``. Function-scope lazy, so
#                           it never fires standalone — but the module-level
#                           import of it does. This is THE gotcha of the whole
#                           vendoring exercise.
#   services/tools/__init__ a PEP 562 ``__getattr__`` over ~40 tool modules,
#                           none of which ship. Importing ``.base`` directly
#                           executes nothing else, so an empty init is correct.
#   services/execution/     a docstring describing the three execution paths,
#                           none of which exist in an exported app.
#
# ``core/__init__.py`` and ``services/__init__.py`` are already inert upstream;
# they are synthesised too so the vendored tree has no partial-copy ambiguity.
STUB_INITS: Dict[str, str] = {
    "__init__.py": (
        '"""Kasal\'s agent runtime, vendored into this standalone app.\n\n'
        "Copied verbatim from Kasal's backend at export time (only the ``src.``\n"
        "import root is rewritten). Do not edit these files in place — changes\n"
        "belong upstream in Kasal and arrive on the next export.\n"
        '"""\n'
    ),
    "core/__init__.py": '"""Vendored from Kasal ``src/core``."""\n',
    "core/llm/__init__.py": (
        '"""Vendored from Kasal ``src/core/llm`` — deliberately EMPTY.\n\n'
        "Upstream this module imports ``usage_telemetry``, which reaches into\n"
        "``src.utils.user_context`` and ``src.utils.telemetry``. Nothing in a\n"
        "standalone app calls ``register_usage_telemetry()`` (only Kasal's\n"
        "``services/llm/manager.py`` does), so the import would pull the backend\n"
        "in to provide something that never runs.\n"
        '"""\n'
    ),
    "services/__init__.py": '"""Vendored from Kasal ``src/services``."""\n',
    "services/tools/__init__.py": (
        '"""Vendored from Kasal ``src/services/tools`` — deliberately EMPTY.\n\n'
        "Upstream this is a lazy ``__getattr__`` over the full tool catalogue.\n"
        "Only ``base`` is vendored; importing it directly executes nothing else.\n"
        '"""\n'
    ),
    "services/execution/__init__.py": (
        '"""Vendored from Kasal ``src/services/execution``."""\n'
    ),
}

# ``from src.x`` / ``import src.x``, at any indentation (the transport has
# function-scope imports). Anchored on the dot so a variable named ``src`` or
# the word in a docstring is never touched.
_IMPORT_ROOT_RE = re.compile(r"(?m)^(?P<indent>[ \t]*)(?P<kw>from|import) +src\.")

_SKIP_NAMES = {".DS_Store", "Thumbs.db", "CLAUDE.md"}


def rewrite_import_root(code: str) -> str:
    """Re-root ``src.`` imports onto the vendored package.

    The one and only edit made to a vendored file.
    """
    return _IMPORT_ROOT_RE.sub(
        lambda m: f"{m.group('indent')}{m.group('kw')} {VENDOR_PKG}.", code
    )


def _is_vendorable(path: Path) -> bool:
    return (
        path.is_file()
        and path.suffix == ".py"
        and path.name not in _SKIP_NAMES
        and "__pycache__" not in path.parts
        and not path.name.endswith("_test.py")
        and ".test." not in path.name
    )


async def _read(path: Path) -> str:
    async with aiofiles.open(path, "r", encoding="utf-8") as f:
        return str(await f.read())


async def kasal_runtime_files(
    logger: Optional[logging.Logger] = None,
) -> List[Dict[str, str]]:
    """The full vendored runtime as export ``files`` entries.

    Raises ``FileNotFoundError`` if a source tree is missing. That is deliberate:
    an export that silently omits the runtime produces an app that fails at
    import time on the customer's cluster, which is a far worse place to find out.
    """
    files: List[Dict[str, str]] = []

    for rel, content in STUB_INITS.items():
        files.append(
            {"path": f"{VENDOR_ROOT}/{rel}", "content": content, "type": "python"}
        )

    for source_dir, dest in _TREES:
        if not source_dir.is_dir():
            raise FileNotFoundError(
                f"Kasal runtime source missing: {source_dir}. The export cannot "
                "produce a runnable app without it."
            )
        for path in sorted(source_dir.rglob("*")):
            if not _is_vendorable(path):
                continue
            rel = path.relative_to(source_dir).as_posix()
            files.append(
                {
                    "path": f"{VENDOR_ROOT}/{dest}/{rel}",
                    "content": rewrite_import_root(await _read(path)),
                    "type": "python",
                }
            )

    for source_file, dest in _MODULES:
        if not source_file.is_file():
            raise FileNotFoundError(f"Kasal runtime source missing: {source_file}")
        files.append(
            {
                "path": f"{VENDOR_ROOT}/{dest}",
                "content": rewrite_import_root(await _read(source_file)),
                "type": "python",
            }
        )

    if logger is not None:
        logger.info(f"Vendored {len(files)} Kasal runtime files into the export")
    return files
