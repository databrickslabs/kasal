"""Put the redactor on a tool instance, so every route into the tool passes it.

A tool reaches the agent through ``run`` (the engine calls it, and so does a
replayed or approved call), through ``_run`` (``BaseTool.run`` and the native
structured-tool adapter both call it), and through the async pair ``arun`` /
``_arun``. The guard wraps all four on the instance. Only the OUTERMOST call
redacts: a context variable marks "already inside a guarded call", so the
``_run`` a ``run`` calls passes through untouched and the value is redacted
once, at the boundary the agent sees.

Exceptions are scrubbed too. A tool that raises ``ValueError("row 4 has
12.5")`` reaches the trace and the agent as ``ValueError("row <n> has <n>")``.
The original is dropped rather than chained, so its message is not in the
traceback either.

Tools with a value-free mode (``VALUE_FREE_MODES``) get that mode forced on
instead of redaction: their output is by construction counts and verdicts, and
redacting it would strip the very verdicts they exist to produce.

Known limit: the wrappers live on the instance. A copy made by ``copy.copy`` or
``model_copy`` shares them, which is what the factory's cache relies on; a
tool that is re-built from the class gets its own guard from the factory.
"""

import functools
from contextvars import ContextVar
from typing import Awaitable, Callable, Optional, Tuple

from src.services.tools.confidential.redactor import redact_output, scrub_error_text

#: Tool class name -> config keys forced on. The class name is the key because
#: the class itself is heavy to import from here (pandas, the reconciliation
#: package); a class renamed without updating this table simply loses the force,
#: and the guard then falls back to redaction.
VALUE_FREE_MODES: dict[str, dict[str, object]] = {
    "UCMVReconciliationTool": {"statistics_only": True},
    "GenieSpaceSyncTool": {"statistics_only": True},
}

#: Set on a guarded instance so a second stamp does not wrap twice.
GUARD_MARK = "_confidential_guarded"

_GUARD_DEPTH: ContextVar[int] = ContextVar("confidential_guard_depth", default=0)

SyncCall = Callable[..., object]
AsyncCall = Callable[..., Awaitable[object]]


def install_confidential_guard(tool: object) -> bool:
    """Wrap the tool's run paths. Returns False when the tool has no run path to
    wrap (the caller logs it; such an object never reaches an agent)."""
    if getattr(tool, GUARD_MARK, False):
        return True
    run_sync = getattr(tool, "_run", None)
    if not callable(run_sync):
        return False
    forced = _force_value_free(tool)
    forced_keys = tuple(VALUE_FREE_MODES[type(tool).__name__]) if forced else ()
    redact = not forced

    object.__setattr__(tool, "_run", _guard_sync(run_sync, redact, forced_keys))
    public_run = getattr(tool, "run", None)
    if callable(public_run):
        object.__setattr__(tool, "run", _guard_sync(public_run, redact, forced_keys))
    for name in ("_arun", "arun"):
        call = getattr(tool, name, None)
        if callable(call):
            object.__setattr__(tool, name, _guard_async(call, redact, forced_keys))
    object.__setattr__(tool, GUARD_MARK, True)
    return True


def is_value_free_mode_active(tool: object) -> bool:
    """True when the tool runs in its forced value-free mode (no redaction)."""
    return bool(getattr(tool, "_confidential_value_free", False))


def _force_value_free(tool: object) -> bool:
    """Set the tool's value-free config. False when the tool has no such mode, or
    its config is not where the mode is read from (then redaction applies)."""
    forced = VALUE_FREE_MODES.get(type(tool).__name__)
    if not forced:
        return False
    config = getattr(tool, "_default_config", None)
    if not isinstance(config, dict):
        return False
    config.update(forced)
    object.__setattr__(tool, "_confidential_value_free", True)
    return True


def scrubbed_exception(error: Exception) -> Exception:
    """An exception whose message has its numeric literals replaced.

    The original is returned unchanged when its message has no numbers. Otherwise
    a new exception of the same type is built from the scrubbed message; when that
    type cannot be built from one message, a RuntimeError is.
    """
    original = str(error)
    message = scrub_error_text(original)
    if message == original:
        return error
    try:
        rebuilt = type(error)(message)
    except Exception:  # noqa: BLE001 - any constructor failure falls back below
        return RuntimeError(message)
    return rebuilt if str(rebuilt) == message else RuntimeError(message)


def _guard_sync(call: SyncCall, redact: bool, forced_keys: Tuple[str, ...]) -> SyncCall:
    @functools.wraps(call)
    def guarded(*args: object, **kwargs: object) -> object:
        if _GUARD_DEPTH.get():
            return call(*args, **kwargs)
        for key in forced_keys:
            kwargs.pop(key, None)
        output: object = None
        failure: Optional[Exception] = None
        token = _GUARD_DEPTH.set(1)
        try:
            output = call(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - re-raised scrubbed, below
            failure = exc
        finally:
            _GUARD_DEPTH.reset(token)
        if failure is not None:
            # Raised outside the except block, so the original is not chained.
            raise scrubbed_exception(failure) from None
        return redact_output(output) if redact else output

    return guarded


def _guard_async(
    call: AsyncCall, redact: bool, forced_keys: Tuple[str, ...]
) -> AsyncCall:
    @functools.wraps(call)
    async def guarded(*args: object, **kwargs: object) -> object:
        if _GUARD_DEPTH.get():
            return await call(*args, **kwargs)
        for key in forced_keys:
            kwargs.pop(key, None)
        output: object = None
        failure: Optional[Exception] = None
        token = _GUARD_DEPTH.set(1)
        try:
            output = await call(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - re-raised scrubbed, below
            failure = exc
        finally:
            _GUARD_DEPTH.reset(token)
        if failure is not None:
            raise scrubbed_exception(failure) from None
        return redact_output(output) if redact else output

    return guarded
