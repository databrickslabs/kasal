"""Which server process an in-run approval gate belongs to.

A tool-call or task-review gate is decided by a worker thread that lives in the
server process (a chat turn) or in a run subprocess that server started, and
neither outlives that server. So a gate's owner is the SERVER, named by a boot
id: its host, pid and process start time. The server exports its boot id to the
environment its run subprocesses inherit, so the gates they raise name the
same owner.

At startup a gate counts as abandoned only when its owner is not this server
and is provably gone: on this host, and no live process with that pid and start
time. Another deployment sharing the database runs on another host, so its
gates are never taken for abandoned.
"""

import hashlib
import os
import socket

import psutil

from src.config.settings import settings
from src.core.process_role import in_run_subprocess

#: Where the boot id is kept in an approval's gate_config.
BOOT_ID_KEY = "boot_id"


def _host() -> str:
    # Hashed: gate_config is shown to approvers, and the host name is not theirs
    # to know.
    return hashlib.sha256(socket.gethostname().encode()).hexdigest()[:16]


def boot_id(pid: int, started: float) -> str:
    """The boot id of the process ``pid`` on this host, started at ``started``."""
    return f"{_host()}:{pid}:{started:.3f}"


def current_boot_id() -> str:
    """The boot id of the server this process belongs to."""
    if in_run_subprocess() and settings.KASAL_BOOT_ID:
        return settings.KASAL_BOOT_ID
    process = psutil.Process()
    return boot_id(process.pid, process.create_time())


def export_boot_id() -> None:
    """Hand this server's boot id to the run subprocesses it starts from now on.

    Read back there through ``settings.KASAL_BOOT_ID``.
    """
    os.environ["KASAL_BOOT_ID"] = current_boot_id()


def owner_gone(gate_boot_id: str) -> bool:
    """Whether the server named by ``gate_boot_id`` is provably not running.

    False whenever that cannot be shown: another host, or a process this one
    may not inspect.
    """
    host, _, rest = gate_boot_id.partition(":")
    pid_text, _, started_text = rest.partition(":")
    if host != _host():
        return False
    try:
        pid, started = int(pid_text), float(started_text)
    except ValueError:
        return False
    try:
        running = psutil.Process(pid).create_time()
    except psutil.NoSuchProcess:
        return True
    except psutil.Error:
        return False
    # The pid was reused by a process that started later.
    return f"{running:.3f}" != f"{started:.3f}"
