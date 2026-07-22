"""Model-fallback policy: classify an LLM failure and choose the next enabled
model to try, so a crew falls back to another model instead of dying.

Pure and dependency-light (no litellm / crewai / DB imports) so it can be unit
tested in isolation. The DB candidate loading lives in ``LLMManager`` and the
actual model rebuild + delegation lives in ``DatabricksRetryLLM``.
"""

from dataclasses import dataclass
from typing import List, Optional, Set

# Fallback reasons — a model swap can plausibly help for these.
CONTEXT_WINDOW = "context_window"  # prompt exceeded the model's context window
FATAL_4XX = "fatal_4xx"  # model-incompatibility 4xx (e.g. Gemini thought_signature)
RATE_LIMIT = "rate_limit"  # sustained 429 after same-model backoff
ENDPOINT_MISSING = "endpoint_missing"  # 404: the model's serving endpoint isn't deployed here

_CONTEXT_MARKERS = (
    "context length",
    "context_length_exceeded",
    "context window",
    "prompt is too long",
    "maximum context",
    "too many tokens",
    "reduce the length",
    "exceeds the maximum",
    "input is too long",
)
_RATE_LIMIT_MARKERS = ("rate limit", "too many requests", "quota exceeded")
# Markers that identify a missing serving endpoint (the model isn't deployed in
# THIS workspace) — a different, deployed model may work. Distinct from a generic
# 404 so we can route it to "try another model" instead of dying.
_ENDPOINT_MISSING_MARKERS = (
    "endpoint_not_found",
    "does not exist, please retry after checking",
    "model and version deployment",
)
# Markers that identify a model-incompatibility 4xx (a different model may work),
# as opposed to a generic bad request from malformed user input.
_FATAL_4XX_MARKERS = (
    "thought_signature",
    "invalid_argument",
    "does not support",
    "not supported",
    "unsupported parameter",
    "unsupported value",
    "invalid_request_error",
)

# Process-wide memory of serving endpoints that returned ENDPOINT_NOT_FOUND in
# THIS workspace. A model can be enabled in config (e.g. databricks-gpt-5) yet
# have no serving endpoint deployed here — falling back to it 404s. Once we've
# seen that, stop offering it as a fallback target (this run AND later runs in
# the same process) so a transient rate-limit on the primary model doesn't
# cascade into a fatal ENDPOINT_NOT_FOUND. Process-scoped on purpose: it resets
# when the flow/crew subprocess restarts, so a later deployment re-learns cleanly.
_KNOWN_MISSING_ENDPOINTS: Set[str] = set()


def mark_endpoint_missing(model_name: Optional[str]) -> None:
    """Record that a model's serving endpoint isn't deployed in this workspace."""
    if model_name:
        _KNOWN_MISSING_ENDPOINTS.add(model_name.split("/")[-1])


def is_endpoint_missing(model_name: Optional[str]) -> bool:
    """True if this model was previously seen to have no serving endpoint here."""
    return bool(model_name) and model_name.split("/")[-1] in _KNOWN_MISSING_ENDPOINTS


def reset_known_missing_endpoints() -> None:
    """Clear the known-missing set (test helper / manual re-probe)."""
    _KNOWN_MISSING_ENDPOINTS.clear()


@dataclass(frozen=True)
class ModelCandidate:
    """An enabled model usable as a fallback target."""

    name: str  # model key, e.g. "databricks-claude-sonnet-4-5" (no provider prefix)
    context_window: int = 0


def _iter_exc(exc) -> List[BaseException]:
    """All exceptions in the tree: the exc, ExceptionGroup sub-exceptions, and
    __cause__ chain (litellm/anyio wrap the real error several layers deep)."""
    seen_ids: Set[int] = set()
    out: List[BaseException] = []
    stack = [exc]
    while stack:
        e = stack.pop()
        if e is None or id(e) in seen_ids:
            continue
        seen_ids.add(id(e))
        out.append(e)
        for sub in getattr(e, "exceptions", None) or []:
            stack.append(sub)
        cause = getattr(e, "__cause__", None)
        if cause is not None:
            stack.append(cause)
    return out


def _status_code(exc) -> Optional[int]:
    """The HTTP status buried anywhere in the exception tree, if any."""
    for e in _iter_exc(exc):
        for attr in ("status_code", "status"):
            v = getattr(e, attr, None)
            if isinstance(v, int):
                return v
        v = getattr(getattr(e, "response", None), "status_code", None)
        if isinstance(v, int):
            return v
    return None


def _text(exc) -> str:
    """Lowercased class names + messages across the whole exception tree."""
    parts: List[str] = []
    for e in _iter_exc(exc):
        parts.append(type(e).__name__)
        try:
            parts.append(str(e))
        except Exception:  # pragma: no cover - defensive
            pass
    return " ".join(parts).lower()


def classify_llm_error(exc) -> Optional[str]:
    """Classify an LLM exception into a fallback reason, or None when a model
    swap won't help (auth, user-stop, transient, malformed input, unknown).

    Context-window is checked first because those arrive as 400 BadRequestError
    too — and the right remedy (a bigger model) differs from a generic 4xx.
    """
    text = _text(exc)
    status = _status_code(exc)

    if (
        "contextwindowexceeded" in text
        or "llmcontextlengthexceeded" in text
        or any(m in text for m in _CONTEXT_MARKERS)
    ):
        return CONTEXT_WINDOW

    # A missing serving endpoint (404 ENDPOINT_NOT_FOUND) — the model isn't
    # deployed in this workspace. Another enabled model might be, so try one.
    # Checked before RATE_LIMIT/FATAL_4XX because it is unambiguous and terminal
    # for THIS model (no amount of ret/backoff makes a nonexistent endpoint appear).
    if (
        "notfounderror" in text
        or any(m in text for m in _ENDPOINT_MISSING_MARKERS)
        or (status == 404 and "endpoint" in text)
    ):
        return ENDPOINT_MISSING

    if (
        status == 429
        or "ratelimiterror" in text
        or any(m in text for m in _RATE_LIMIT_MARKERS)
    ):
        return RATE_LIMIT

    if (status in (400, 422) or "badrequest" in text) and any(
        m in text for m in _FATAL_4XX_MARKERS
    ):
        return FATAL_4XX

    return None


def _model_family(name) -> str:
    """A coarse model-family token from a model key, used to avoid bouncing
    between models that share a family-wide incompatibility (e.g. all gemini-*
    reject multi-turn tool calls the same way). Drops a leading provider
    segment: 'databricks-gemini-3-5-flash' -> 'gemini'."""
    parts = [p for p in str(name or "").lower().replace("/", "-").split("-") if p]
    if parts and parts[0] in ("databricks", "azure", "openai"):
        parts = parts[1:]
    return parts[0] if parts else ""


def select_fallback(
    candidates: List[ModelCandidate],
    current_window: int,
    reason: str,
    tried: Set[str],
    current_model: Optional[str] = None,
) -> Optional[ModelCandidate]:
    """Pick the next model to try.

    - context_window: the replacement must have a LARGER window (else it just
      fails again); when the current window is unknown, try the roomiest.
    - fatal_4xx: model-incompatibilities (e.g. Gemini thought_signature) are
      usually family-wide, so prefer a DIFFERENT family than the one that
      failed, falling back to same-family only if nothing else is left.
    - otherwise (rate_limit): any untried model, roomiest first.

    Returns None when nothing suitable remains.
    """
    avail = [c for c in candidates if c.name not in tried]
    if not avail:
        return None

    if reason == CONTEXT_WINDOW:
        bigger = [c for c in avail if c.context_window > (current_window or 0)]
        if bigger:
            return max(bigger, key=lambda c: c.context_window)
        if not current_window:  # unknown current window — try the roomiest
            return max(avail, key=lambda c: c.context_window)
        return None  # nothing bigger — let the caller summarize instead

    if reason == FATAL_4XX:
        cur_family = _model_family(current_model)
        cross_family = [
            c for c in avail if cur_family and _model_family(c.name) != cur_family
        ]
        pool = cross_family or avail
        return max(pool, key=lambda c: c.context_window)

    if reason == ENDPOINT_MISSING:
        # The current model's endpoint isn't deployed here. Any OTHER untried
        # model might be — prefer a different family (the missing one may be a
        # whole family that's not provisioned, e.g. no gemini-* endpoints),
        # roomiest first, else any untried model.
        cur_family = _model_family(current_model)
        cross_family = [
            c for c in avail if cur_family and _model_family(c.name) != cur_family
        ]
        pool = cross_family or avail
        return max(pool, key=lambda c: c.context_window)

    return max(avail, key=lambda c: c.context_window)


def candidates_from_model_configs(models, current_model_key) -> List[ModelCandidate]:
    """Filter enabled model-config rows into fallback ModelCandidates.

    Keeps Databricks-served, non-codex models (those can be rebuilt and swapped
    through the same auth/endpoint) and drops the current model. Pure — takes
    objects exposing ``.key`` / ``.provider`` / ``.context_window``.
    """
    current = (current_model_key or "").split("/")[-1]
    out: List[ModelCandidate] = []
    for m in models or []:
        key = getattr(m, "key", None)
        if not key or key == current:
            continue
        provider = (getattr(m, "provider", "") or "").lower()
        if provider and provider != "databricks":
            continue
        if "codex" in key.lower():
            continue
        # Skip models whose serving endpoint 404'd here before — offering them
        # again just re-triggers ENDPOINT_NOT_FOUND.
        if is_endpoint_missing(key):
            continue
        out.append(
            ModelCandidate(
                name=key, context_window=getattr(m, "context_window", 0) or 0
            )
        )
    return out
