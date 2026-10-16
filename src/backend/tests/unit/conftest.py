"""Unit-test isolation for process-wide state the root conftest does not cover."""

import sys

import pytest

_EXECUTOR = "src.services.execution.runtime.executor"
_HOOK_LISTS = ("_TOOL_PRE_HOOKS", "_TOOL_POST_HOOKS")


def _hook_lists():
    module = sys.modules.get(_EXECUTOR)
    found = [getattr(module, name, None) for name in _HOOK_LISTS]
    return [hooks if isinstance(hooks, list) else None for hooks in found]


@pytest.fixture(autouse=True)
def restore_tool_hook_lists():
    """Put the process-wide tool pre/post hook lists back after each test.

    The run subprocess entry points (``run_flow_in_process``,
    ``run_crew_in_process``) install the run's approval hook and, like a real
    subprocess that then exits, never remove it. Tests drive them in-process, so
    without this their hooks gated every later tool call on the same xdist
    worker: a test that checks which hooks fire saw 21 strangers.

    Reads the module from ``sys.modules`` rather than importing it, so test
    files that stub modules at import time are not handed a real executor.
    """
    saved = [list(hooks) if hooks is not None else [] for hooks in _hook_lists()]
    yield
    for hooks, before in zip(_hook_lists(), saved):
        if hooks is not None:
            hooks[:] = before


@pytest.fixture
def isolated_encryption_keys(tmp_path, monkeypatch):
    """Point EncryptionUtils at a fresh key pair under ``tmp_path``.

    Opt-in: a test that really encrypts (the tool-config column type does on
    every save) must not read or create the developer's key files in HOME.
    """
    from src.utils.encryption_utils import EncryptionUtils

    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    monkeypatch.setattr(EncryptionUtils, "get_key_directory", lambda: key_dir)
    return key_dir
