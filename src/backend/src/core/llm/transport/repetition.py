"""Catching a model that has stopped answering and started repeating itself.

A model can fall into degenerate decoding: it emits a phrase, then emits it
again, and keeps going until it hits ``max_tokens``. The run does not fail —
there is no error anywhere — it just returns tens of thousands of tokens that
say one thing. Downstream everything treats that as the answer.

This is NOT the loop the execution budget catches. ``MAX_TOOL_ROUNDS`` and
``Agent.max_iter`` bound how many times the agent goes round the think→tool
cycle; every framework has some version of that (LangGraph's
``recursion_limit``, CrewAI's ``max_iter``). All of them count STEPS, and this
happens inside a single step — one request, one response, one very long string.
A round cap cannot see it, and an agent with no tools never enters the cycle
those caps guard in the first place.

So the check is on the text itself: has the tail of what the model is writing
become one short unit repeated over and over?

Why a trailing WINDOW rather than the whole output
==================================================

Cost and honesty. Checking the whole accumulated text on every chunk is
quadratic, and a long answer legitimately repeats things near the start (a
preamble restating the task). What is never legitimate is the LAST few thousand
characters being nothing but one unit on a cycle. Looking only at the tail keeps
each check O(WINDOW) no matter how long the answer runs.

The cost of the window is detection latency: the loop is only visible once it
has filled the window, so roughly WINDOW characters are generated before the
abort. That is the price of not guessing — and it is still ~4KB rather than the
197KB a real incident produced.
"""

import logging
from typing import Any, NoReturn, Optional

from .exceptions import LLMRepetitionLoopError

logger = logging.getLogger(__name__)

#: How much of the tail to examine. Large enough that ordinary structural
#: repetition (table rows, a run of similar bullets) cannot fill it with a
#: single exact unit; small enough that the check stays cheap and the abort
#: happens early.
WINDOW = 4000

#: The longest repeating unit treated as a loop. A model looping on a sentence
#: or a paragraph is the observed failure; something longer than this repeating
#: verbatim is more likely to be genuine structure.
MAX_UNIT = 600

#: How many times the unit must repeat back-to-back. Five consecutive verbatim
#: copies of the same text with nothing between them is not prose.
MIN_REPEATS = 5

#: Characters of new output between checks. The detector is cheap, but not free,
#: and a loop that has run WINDOW characters will still be running 1000 later.
CHECK_EVERY = 1000


def _smallest_period(text: str) -> int:
    """The shortest unit ``text`` could be built from by repetition.

    KMP's failure function in O(n): the longest proper border of the string is
    ``fail[-1]``, and ``n - fail[-1]`` is the smallest ``p`` with
    ``text[i] == text[i + p]`` everywhere both exist. That is a NECESSARY
    condition for periodicity, not a sufficient one — "abcabcab" reports 3 and
    is only a partial third copy — so the caller still verifies.
    """
    n = len(text)
    if n == 0:
        return 0
    failure = [0] * n
    k = 0
    for i in range(1, n):
        while k and text[i] != text[k]:
            k = failure[k - 1]
        if text[i] == text[k]:
            k += 1
        failure[i] = k
    return n - failure[-1]


def looping_unit(text: str) -> Optional[str]:
    """The unit the model is stuck on, or None if it is still writing.

    None on every uncertain case. A false positive kills a legitimate answer
    mid-generation, which is worse than the loop it would have prevented — so
    the tail must be EXACTLY the unit repeated, not merely similar.
    """
    if len(text) < WINDOW:
        return None

    window = text[-WINDOW:]
    period = _smallest_period(window)
    if period <= 0 or period > MAX_UNIT:
        return None
    if len(window) // period < MIN_REPEATS:
        return None

    # Verify rather than trust the period: the failure function proves a border,
    # and only this proves the window is that unit over and over.
    repeated = window[:period] * (len(window) // period + 1)
    if not repeated.startswith(window):
        return None
    return window[:period]


def without_loop(text: str, unit: str) -> str:
    """What the model wrote before it started repeating.

    Keeps the FIRST copy of the unit: it is usually a real sentence, and the
    fault is that it was said again, not that it was said. Everything from the
    second copy on is dropped — returning it would hand the caller the same
    garbage the detector exists to stop.
    """
    if not unit:
        return text
    first = text.find(unit)
    if first < 0:
        return text
    second = text.find(unit, first + len(unit))
    if second < 0:
        return text
    return text[:second]


def stop_on_loop(model: str, written: str, unit: str, stream: Any = None) -> NoReturn:
    """End a degenerate decode and report it as a terminal failure.

    Closes the stream first — it is still open and the model is still being paid
    to say the same thing — then raises with the work that preceded the loop.

    Lives here rather than on the LLM class so the transport keeps a two-line
    hook and this stays testable without constructing a client.
    """
    if stream is not None:
        try:
            stream.close()
        except Exception:  # noqa: BLE001 — already failing; closing is best effort
            logger.debug("could not close the looping stream")

    kept = without_loop(written, unit)
    repeats = written.count(unit)
    logger.warning(
        "[llm-repetition] %s repeated a %d-char phrase %d times; stopped after "
        "%d chars (kept %d)",
        model,
        len(unit),
        repeats,
        len(written),
        len(kept),
    )
    raise LLMRepetitionLoopError(
        f"{model} stopped answering and repeated the same {len(unit)}-character "
        f"phrase {repeats} times. The call was stopped after {len(written)} "
        f"characters rather than run to the token limit. "
        f"Repeated text: {unit[:120]!r}",
        partial=kept,
    )


def check_response(model: str, content: Optional[str]) -> None:
    """Backstop for a response that arrived whole (no stream to watch).

    Also catches a loop that began too late in a streamed response for the
    periodic check to have seen it.
    """
    if not content:
        return
    unit = looping_unit(content)
    if unit:
        stop_on_loop(model, content, unit)


class RepetitionWatch:
    """Streaming-side detector: fed deltas, says when to stop.

    Holds only the tail, so memory and per-chunk cost stay constant however long
    the model runs.
    """

    def __init__(self) -> None:
        self._tail = ""
        self._since_check = 0

    def feed(self, delta: str) -> Optional[str]:
        """Add a streamed delta. Returns the looping unit once detected."""
        if not delta:
            return None
        self._tail = (self._tail + delta)[-WINDOW:]
        self._since_check += len(delta)
        if self._since_check < CHECK_EVERY:
            return None
        self._since_check = 0
        return looping_unit(self._tail)
