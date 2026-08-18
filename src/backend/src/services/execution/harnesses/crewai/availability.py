"""Is CrewAI usable in this process, and which version?

One module so the answer is given once, in one voice. ``import crewai`` costs
~2.6 seconds and pulls chromadb, so nothing here runs at package import: an
installation on the Kasal harness must never pay for a library it does not use.

The failure is deliberately LOUD and EARLY. ``binding_for`` raises
``HarnessUnavailableError`` at resolution time — before an execution row reaches
RUNNING — so an operator reads "CrewAI is not installed" rather than watching a
crew die halfway through with an ImportError in a subprocess log.
"""

from __future__ import annotations

import importlib
from typing import Any, Optional, Tuple

from src.services.execution.harnesses.binding import HarnessUnavailableError

#: The version this integration was written against. A different one is not
#: refused — pinning the binding to an exact release would mean a dependency
#: bump breaks the harness rather than merely being untested — but it IS
#: reported, because "which CrewAI produced this run?" is the first question
#: asked when two harnesses disagree.
EXPECTED_VERSION = "1.15.16"

_cached: Optional[Tuple[Any, str]] = None

#: Set BEFORE ``import crewai``, because it reads them at import time.
#:
#: CrewAI ships usage telemetry ON by default, phoning home from the process
#: that just executed a tenant's crew. On a multi-tenant platform handling
#: customer data that is not a preference, it is a data-egress path nobody
#: opted into, so the harness turns it off rather than documenting how to.
#:
#: ``CREWAI_TRACING_ENABLED`` is left FALSE for the same reason — CrewAI's own
#: cloud tracing would duplicate, off-platform, what ``OTelEventBridge`` already
#: records in the customer's own database.
#:
#: NOTE the one that is deliberately absent: ``OTEL_SDK_DISABLED``. CrewAI's
#: telemetry check accepts it, but it is a GLOBAL OpenTelemetry kill switch —
#: setting it would silently disable Kasal's own span export, which is the
#: entire trace timeline. Two narrow switches, never the broad one.
_PRIVACY_ENV = {
    "CREWAI_DISABLE_TELEMETRY": "true",
    "CREWAI_DISABLE_TRACKING": "true",
    "CREWAI_TRACING_ENABLED": "false",
    # A version check is an outbound request on a path that must work offline
    # and inside a locked-down workspace.
    "CREWAI_DISABLE_VERSION_CHECK": "true",
}


def harden_environment() -> None:
    """Turn CrewAI's outbound reporting off. Idempotent; safe to call twice.

    An explicitly-set value is left alone: an operator who deliberately enabled
    something in their own deployment should not have it overwritten by an
    import.
    """
    import os

    for key, value in _PRIVACY_ENV.items():
        os.environ.setdefault(key, value)


def require_crewai() -> Tuple[Any, str]:
    """The ``crewai`` module and its version, or a reason it cannot be had.

    Cached: the import is expensive and idempotent, and a binding is built once
    per process anyway.
    """
    global _cached
    if _cached is not None:
        return _cached

    harden_environment()
    try:
        crewai = importlib.import_module("crewai")
        _reject_a_stub(crewai)
    except Exception as e:  # noqa: BLE001 — ANY import failure is the same answer
        raise HarnessUnavailableError(
            f"The CrewAI harness needs the 'crewai' package, which could not be "
            f"imported: {e}. Install it (it is declared in pyproject.toml) and "
            f"restart, or switch the harness back to Kasal in "
            f"Configuration → Engines."
        ) from e

    version = str(getattr(crewai, "__version__", "unknown"))
    _cached = (crewai, version)
    return _cached


def _reject_a_stub(module: Any) -> None:
    """Refuse to cache a module that is not the real library.

    A test that puts a ``MagicMock`` in ``sys.modules["crewai"]`` — several do,
    to exercise "crewai is not installed" branches — would otherwise be captured
    by the cache above and served to every later caller in the process. Under
    xdist that is one test poisoning an entire worker, and the symptom is
    ``AttributeError: __spec__`` raised from somewhere with no visible
    connection to the test that caused it.

    A real module has a ``__spec__``; a ``MagicMock`` raises ``AttributeError``
    for it, which is precisely the signal. Raising here means the caller sees
    "CrewAI is not installed", which is exactly what such a test is simulating,
    and the cache stays empty so the next caller re-checks.
    """
    try:
        spec = module.__spec__
    except AttributeError as e:
        raise HarnessUnavailableError(
            "The 'crewai' module in sys.modules is not the real package (it has "
            "no __spec__ — most likely a test stub). Not caching it."
        ) from e
    if spec is None:
        raise HarnessUnavailableError(
            "The 'crewai' module in sys.modules has no import spec; refusing to "
            "treat it as the installed package."
        )


def crewai_symbols() -> dict:
    """The classes the binding builds with, resolved once.

    Imported through ``require_crewai`` rather than at module scope so the cost
    and the failure both land in one place. ``BaseLLM`` and ``BaseTool`` come
    from their own modules because ``crewai`` re-exports them conditionally.
    """
    crewai, _ = require_crewai()
    base_llm = importlib.import_module("crewai.llms.base_llm")
    tools = importlib.import_module("crewai.tools")
    return {
        "Agent": crewai.Agent,
        "Task": crewai.Task,
        "Crew": crewai.Crew,
        "Process": crewai.Process,
        "LLMGuardrail": crewai.LLMGuardrail,
        "BaseLLM": base_llm.BaseLLM,
        "BaseTool": tools.BaseTool,
    }
