"""Terminate the process trees this server spawned, and nothing else.

Both subprocess paths (Agent Builder and Flow Builder) used to "clean up" by
scanning the WHOLE HOST: every python process whose parent is PID 1, anything
whose ``ps`` line contained the first 8 characters of an execution id, anything
whose command line contained ``multiprocessing.spawn``. That matched uvicorn's
reload worker, a nohup'd ``mlflow server`` and the pytest controller.

The rule here is ownership, decided by the process table rather than by a
string match:

- a run's subprocess is the ``multiprocessing.Process`` the executor started
  and still holds, and everything below it is found with
  ``psutil.Process(pid).children(recursive=True)``;
- when that handle is gone, the only fallback is a DESCENDANT of this server
  whose ``KASAL_EXECUTION_ID`` equals the full execution id exactly.

Nothing in this module iterates ``psutil.process_iter()``.

This runs in the parent only. The functions block (they wait for exit), so
async callers run them off the event loop.

Errors are narrowed to ``psutil.Error`` on purpose. ``psutil`` is imported at
module level so a broken install fails loudly at import instead of every
lookup quietly returning "nothing to stop", and a stop that did not happen is
reported as not having happened.
"""

import logging
import os
from typing import Any, List

import psutil  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)

# A run's subprocess records its id under this name (see run_crew_in_process /
# run_flow_in_process); the parent also sets it for the spawn so the child
# inherits it from its very first instruction.
EXECUTION_ID_ENV = "KASAL_EXECUTION_ID"


def _descendants(pid: int) -> List[Any]:
    """Snapshot every descendant of ``pid``, which must be OUR child.

    Must be taken BEFORE ``pid`` is signalled: once it exits, its children
    are re-parented and can no longer be found from it.

    A pid that is not a direct child of this process yields nothing. A tracked
    ``multiprocessing.Process`` always is one; anything else (a stale pid the
    OS has since reused, a test double's made-up pid) is not ours to walk.
    """
    if not isinstance(pid, int):
        return []
    try:
        root = psutil.Process(pid)
        if root.ppid() != os.getpid():
            return []
        return list(root.children(recursive=True))
    except psutil.NoSuchProcess:
        return []  # already exited: there is no tree left to walk
    except psutil.Error as e:
        logger.warning("[process_tree] cannot list descendants of %s: %s", pid, e)
        return []


def _signal_all(procs: List[Any], graceful: bool) -> None:
    for proc in procs:
        try:
            proc.terminate() if graceful else proc.kill()
        except (psutil.NoSuchProcess, ProcessLookupError):
            pass  # already gone
        except psutil.Error as e:
            logger.warning(
                "[process_tree] could not signal process %s: %s",
                getattr(proc, "pid", "?"),
                e,
            )


def _reap_descendants(procs: List[Any], timeout: float) -> None:
    """Wait for ``procs`` to exit, SIGKILLing whatever outlives ``timeout``."""
    if not procs:
        return
    try:
        _, alive = psutil.wait_procs(procs, timeout=timeout)
        _signal_all(alive, graceful=False)
        if alive:
            _, alive = psutil.wait_procs(alive, timeout=1)
        if alive:
            logger.warning(
                "[process_tree] %d descendant(s) survived SIGKILL: %s",
                len(alive),
                [p.pid for p in alive],
            )
    except psutil.Error as e:
        logger.warning("[process_tree] waiting for descendants failed: %s", e)


def terminate_process_tree(
    process: Any,
    *,
    graceful: bool = True,
    grace_timeout: float = 2.0,
    wait: bool = True,
) -> bool:
    """Stop a subprocess this server started, together with its descendants.

    ``process`` is the ``multiprocessing.Process`` handle. The root is stopped
    through that handle rather than through psutil: psutil's ``wait`` reaps
    a direct child with ``waitpid``, after which the handle can never observe
    the exit and ``is_alive()`` stays ``True`` forever.

    Args:
        process: The tracked ``multiprocessing.Process``.
        graceful: SIGTERM first (and SIGKILL after ``grace_timeout``); when
            False, SIGKILL immediately.
        grace_timeout: Seconds to wait for a graceful exit.
        wait: When False, signal and return without waiting (shutdown with
            ``wait=False``).

    Returns:
        True once the root has exited (SIGKILL as the last resort). False when
        it is still alive: ``wait=False`` was passed, or it outlived SIGKILL
        (a process stuck in uninterruptible I/O).
    """
    pid = getattr(process, "pid", None)
    if not process.is_alive():
        return True

    descendants = _descendants(pid) if pid else []
    if graceful:
        process.terminate()
    else:
        process.kill()
    _signal_all(descendants, graceful)
    if not wait:
        return False

    process.join(timeout=grace_timeout if graceful else 1)
    if graceful and process.is_alive():
        logger.warning("[process_tree] force killing process %s", pid)
        process.kill()
        process.join(timeout=1)
    stopped = not process.is_alive()
    if not stopped:
        logger.error("[process_tree] process %s is still alive after SIGKILL", pid)

    _reap_descendants(descendants, grace_timeout)
    return stopped


def find_owned_processes(execution_id: str) -> List[Any]:
    """Descendants of THIS process that belong to ``execution_id``.

    Matching is exact on the full id — never a prefix, never a command-line
    substring — and limited to this server's own process tree. An empty or
    missing id matches nothing.
    """
    if not execution_id or not isinstance(execution_id, str):
        return []
    try:
        own_tree = psutil.Process(os.getpid()).children(recursive=True)
    except psutil.Error as e:
        logger.warning("[process_tree] cannot list this server's children: %s", e)
        return []

    matches = []
    for proc in own_tree:
        try:
            if proc.environ().get(EXECUTION_ID_ENV) == execution_id:
                matches.append(proc)
        except psutil.NoSuchProcess:
            continue  # exited meanwhile
        except psutil.Error as e:
            logger.warning(
                "[process_tree] cannot read the environment of %s: %s", proc.pid, e
            )
    # A match's own children inherit the variable; keep only the topmost,
    # since terminating it takes its subtree with it.
    matched_pids = {p.pid for p in matches}
    return [p for p in matches if _parent_pid(p) not in matched_pids]


def _parent_pid(proc: Any) -> Any:
    try:
        return proc.ppid()
    except psutil.Error:
        return None


def terminate_owned_processes(
    execution_id: str, *, graceful: bool = True, grace_timeout: float = 2.0
) -> int:
    """Terminate this server's descendants that run ``execution_id``.

    The fallback for a stop request whose ``Process`` handle is no longer
    tracked. Returns how many top-level matches were terminated.
    """
    matches = find_owned_processes(execution_id)
    terminated = 0
    for proc in matches:
        try:
            subtree = proc.children(recursive=True)
        except psutil.NoSuchProcess:
            continue  # exited between the lookup and now: nothing to stop
        except psutil.Error as e:
            logger.warning("[process_tree] cannot list children of %s: %s", proc.pid, e)
            subtree = []
        _signal_all(subtree + [proc], graceful)
        _reap_descendants(subtree + [proc], grace_timeout)
        if _still_running(proc):
            logger.error(
                "[process_tree] process %s for %s survived termination",
                proc.pid,
                execution_id,
            )
            continue
        terminated += 1
        logger.info(
            "[process_tree] terminated process %s (+%d descendants) for %s",
            proc.pid,
            len(subtree),
            execution_id,
        )
    return terminated


def _still_running(proc: Any) -> bool:
    """True while ``proc`` exists and is not a zombie awaiting its reaper."""
    try:
        return bool(proc.is_running()) and proc.status() != psutil.STATUS_ZOMBIE
    except psutil.NoSuchProcess:
        return False
    except psutil.Error:
        return True  # cannot tell: do not report a stop that may not have happened
