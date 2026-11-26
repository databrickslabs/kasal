"""One reconciliation run at a time per SQL warehouse, across processes.

The warehouse and the Power BI service account are shared, so two reconciliations
on the same warehouse would compete for both. Kasal runs reconciliations in the
server process and in subprocesses, so the lock is a file, not an in-memory
object.

The lock is one file per warehouse under the system temp directory, named by a
hash of the warehouse id. It is created with ``O_CREAT | O_EXCL`` (atomic), and
holds the PID, the start time and a random token. Holders that are gone are
stale: the lock is taken over. A PID that now belongs to a process started after
the lock was written is a reused PID, so that lock is stale too. Only the holder
(matching token) removes the file on release.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import secrets
import tempfile
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import psutil

LOCK_DIR_NAME = "kasal-ucmv-reconciliation-locks"
POLL_SECONDS = 5.0
# A lock file with no readable content yet is being written by another process.
_UNREADABLE_GRACE_S = 60.0
_ATTEMPTS = 3


class RunLockHeldError(RuntimeError):
    """The warehouse is locked by another live reconciliation run."""


@dataclass(frozen=True)
class LockHolder:
    """What the lock file says about its holder. ``pid`` 0: unreadable."""

    pid: int
    started: str
    started_epoch: float
    token: str
    warehouse_hash: str

    def describe(self) -> str:
        if self.pid == 0:
            return "reconciliation lock is being written by another process"
        return f"reconciliation locked by PID {self.pid} since {self.started}"


def lock_path_for(warehouse_id: str, base_dir: Optional[Path] = None) -> Path:
    """The lock file of one warehouse. The id is hashed: it never reaches a path."""
    digest = hashlib.sha256(warehouse_id.encode("utf-8")).hexdigest()[:32]
    root = base_dir or Path(tempfile.gettempdir()) / LOCK_DIR_NAME
    return root / f"warehouse-{digest}.lock"


def process_alive(holder: LockHolder) -> bool:
    """True while the holder's process exists and is the process that took the
    lock (a process started after the lock was written has a reused PID)."""
    if holder.pid <= 0:
        return False
    try:
        process = psutil.Process(holder.pid)
    except psutil.NoSuchProcess:
        return False
    except psutil.AccessDenied:
        return True  # exists, owned by someone else
    return process.create_time() <= holder.started_epoch + 1.0


def _read_holder(path: Path) -> Optional[LockHolder]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(raw, dict):
        return None
    try:
        return LockHolder(
            pid=int(raw["pid"]),
            started=str(raw["started"]),
            started_epoch=float(raw["started_epoch"]),
            token=str(raw["token"]),
            warehouse_hash=str(raw.get("warehouse_hash", "")),
        )
    except (KeyError, TypeError, ValueError):
        return None


def _remove_if_unchanged(path: Path, judged: Optional[LockHolder]) -> None:
    """Remove a stale lock only while it is still the one judged stale: another
    run may have replaced it in the meantime."""
    if _read_holder(path) == judged:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()


def _try_create(
    path: Path,
    token: str,
    warehouse_hash: str,
    alive: Callable[[LockHolder], bool],
    now: Callable[[], float],
) -> Optional[LockHolder]:
    """Take the lock. Returns None when taken, or the live holder that blocks it."""
    for _ in range(_ATTEMPTS):
        try:
            fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except FileExistsError:
            holder = _read_holder(path)
            if holder is None:
                if now() - path.stat().st_mtime < _UNREADABLE_GRACE_S:
                    return LockHolder(0, "", 0.0, "", warehouse_hash)
                _remove_if_unchanged(path, None)  # unreadable and old: stale
                continue
            if alive(holder):
                return holder
            _remove_if_unchanged(path, holder)
            continue
        epoch = now()
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(
                {
                    "pid": os.getpid(),
                    "started": datetime.fromtimestamp(epoch, timezone.utc).isoformat(
                        timespec="seconds"
                    ),
                    "started_epoch": epoch,
                    "token": token,
                    "warehouse_hash": warehouse_hash,
                },
                handle,
            )
        return None
    raise RunLockHeldError("could not take the reconciliation lock")


@contextlib.contextmanager
def warehouse_run_lock(
    warehouse_id: str,
    *,
    wait_seconds: float = 0.0,
    base_dir: Optional[Path] = None,
    alive: Callable[[LockHolder], bool] = process_alive,
    now: Callable[[], float] = time.time,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> Iterator[None]:
    """Hold the reconciliation lock of ``warehouse_id`` for the ``with`` block.

    A live holder stops the run with :class:`RunLockHeldError`. With
    ``wait_seconds`` the lock is polled every few seconds until that many seconds
    have passed. Other locks are not touched.
    """
    path = lock_path_for(warehouse_id, base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    token = secrets.token_hex(16)
    warehouse_hash = path.stem.removeprefix("warehouse-")
    deadline = clock() + max(0.0, wait_seconds)
    while True:
        blocker = _try_create(path, token, warehouse_hash, alive, now)
        if blocker is None:
            break
        remaining = deadline - clock()
        if remaining <= 0:
            raise RunLockHeldError(f"{blocker.describe()}; not waited")
        sleep(min(POLL_SECONDS, remaining))
    try:
        yield
    finally:
        _release(path, token)


def _release(path: Path, token: str) -> None:
    holder = _read_holder(path)
    if holder is not None and holder.token == token:
        with contextlib.suppress(FileNotFoundError):
            path.unlink()
