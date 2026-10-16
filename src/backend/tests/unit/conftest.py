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


#: The real ``EncryptionUtils.get_key_directory``, while the session swaps it.
_REAL_KEY_DIRECTORY = {}


@pytest.fixture(scope="session", autouse=True)
def isolated_encryption_keys(tmp_path_factory):
    """Point EncryptionUtils at a key pair under the session's temp directory.

    Every save of a tool-config column encrypts, so without this a unit test
    would read, or create, the key files in the real HOME. Tests that need a
    pair of their own patch ``get_key_directory`` themselves; the tests of
    ``get_key_directory`` itself use ``real_key_directory``.
    """
    from src.utils.encryption_utils import EncryptionUtils

    key_dir = tmp_path_factory.mktemp("encryption-keys")
    original = EncryptionUtils.__dict__["get_key_directory"]
    _REAL_KEY_DIRECTORY["function"] = original
    EncryptionUtils.get_key_directory = staticmethod(lambda: key_dir)
    try:
        yield key_dir
    finally:
        EncryptionUtils.get_key_directory = original


@pytest.fixture
def real_key_directory(monkeypatch):
    """Undo the isolation, for a test of ``get_key_directory`` itself."""
    from src.utils.encryption_utils import EncryptionUtils

    monkeypatch.setattr(
        EncryptionUtils, "get_key_directory", _REAL_KEY_DIRECTORY["function"]
    )


@pytest.fixture(autouse=True)
def forget_decrypted_values():
    """Never serve one test a plaintext another test decrypted (or mocked)."""
    module = sys.modules.get("src.utils.sensitive_data_utils")
    forget = getattr(module, "forget_decrypted_values", None)
    if forget is not None:
        forget()
    yield
