"""Persist — the write side of a run's memory.

``remember_async`` is the single boundary where run-produced content enters
memory: chat turns (``format_turn_for_memory``) and task outputs
(``make_memory_output_sink``) both arrive here. It screens the text, hands
the durable write to a small daemon pool and returns immediately; the record
is readable in the meantime through the ``pending`` overlay. Crew and flow
subprocesses call ``flush_memory_writes`` before exiting so the last save is
not lost with the interpreter.

Everything is best-effort — a failed write is logged, never raised."""

from __future__ import annotations

import concurrent.futures
import contextvars
import logging
import threading
from typing import Any

from src.services.execution.logs.context import current_execution_id
from src.services.memory.run.pending import (
    _add_pending,
    _drop_pending,
    _PendingMemory,
    pending_memory_for,
)
from src.services.memory.run.recall import _task_event_context, _usable_memory
from src.services.memory.run.write_hygiene import screen_memory_write
from src.services.memory.text import (
    normalized_text,
    says_the_same,
    strip_run_boilerplate,
)

logger = logging.getLogger(__name__)


# One shared writer for fire-and-forget persistence. In the long-lived server
# process writes always complete; short-lived crew SUBPROCESSES must call
# flush_memory_writes() before exiting or the last task's save (and its
# "Memory Write" trace span) dies with the interpreter.
_WRITE_POOL = concurrent.futures.ThreadPoolExecutor(
    max_workers=2, thread_name_prefix="kasal-memory-write"
)


_PENDING_WRITES: set = set()


_PENDING_LOCK = threading.Lock()


def flush_memory_writes(timeout: float = 15.0) -> int:
    """Wait (bounded) for all in-flight memory writes to finish.

    Call at the end of a crew subprocess, after kickoff and before OTel
    shutdown, so the final task's save lands in storage AND its
    MemorySaveCompleted span is emitted while the exporter is still alive.
    Returns the number of writes still pending after the timeout (0 = drained).
    """
    with _PENDING_LOCK:
        pending = set(_PENDING_WRITES)
    if not pending:
        return 0
    done, not_done = concurrent.futures.wait(pending, timeout=timeout)
    if not_done:
        logger.warning(
            "flush_memory_writes: %d write(s) still pending after %.1fs",
            len(not_done),
            timeout,
        )
    return len(not_done)


#: Stricter, because a skipped WRITE cannot be undone while a suppressed snippet
#: costs one prompt. Graphiti uses the same 0.9 for its LLM-free fuzzy tier.
_WRITE_REDUNDANCY = 0.9


def _already_remembered(mem: Any, text: str, own: Any = None) -> bool:
    """Whether this scope already holds what we are about to write.

    Every reference system reads before it writes, and Kasal was the one that
    did not: LangMem hands ``store.asearch(namespace, query, limit=5)`` hits to
    its extractor as ``existing``; Mem0 searches five neighbours per candidate
    fact and lets an LLM emit ADD/UPDATE/DELETE/NOOP; Graphiti fetches candidate
    nodes and settles most of them with an LLM-FREE MinHash/Jaccard tier at 0.9
    before any model is consulted. Kasal's write path never touched
    ``storage.search``, so two runs of one task produced two records saying the
    same thing, and the exact-hash consolidation could not see them because they
    differed by two swapped list entries.

    This is Graphiti's cheap tier and nothing more: the five nearest records in
    the same scope, compared by token-set Jaccard. No LLM call — the module's
    design rule is no model on these paths — and no embedding beyond the one the
    recall itself needs.

    **NOOP, never overwrite.** A skipped write is recoverable in effect: what it
    would have said is already stored. Rewriting or deleting a neighbour is not,
    and Kasal already has the honest mechanism for supersession
    (``supersession.py``: ``valid_to``/``superseded_by``), which keeps the
    replaced copy auditable.

    **Fails open.** Any error writes as before — deduplication must never be the
    reason a memory is lost.
    """
    try:
        neighbours = int(getattr(mem, "consolidation_limit", 5) or 5)
        for record in mem.recall(text[:2000], limit=neighbours, mode="raw") or []:
            if says_the_same(normalized_text(record), text, _WRITE_REDUNDANCY):
                logger.info(
                    "[memory] already remembered in this scope; skipping the write"
                )
                return True
        # The in-flight overlay too: the writer pool has two workers, so two
        # tasks finishing together can both miss storage and both insert. Mem0
        # guards the same race with its per-batch `seen_hashes`.
        for entry in pending_memory_for(getattr(mem, "root_scope", None)):
            # Not this write's own overlay entry: remember_async adds it BEFORE
            # submitting, so without this the gate matches the record against
            # itself and no write ever happens.
            if entry is own:
                continue
            if says_the_same(normalized_text(entry), text, _WRITE_REDUNDANCY):
                logger.info("[memory] an identical write is already in flight")
                return True
    except Exception as exc:  # noqa: BLE001 — never lose a memory over this
        logger.debug("[memory] duplicate check unavailable (%s); writing", exc)
    return False


def remember_async(
    memory: Any,
    content: str,
    *,
    source: str | None = None,
    agent_role: str | None = None,
    categories: list[str] | None = None,
    metadata: dict[str, Any] | None = None,
    importance: float | None = None,
) -> None:
    """Persist ``content`` off the hot path. Fire-and-forget; never raises.

    ``Memory.remember`` internally serializes writes through its own
    single-worker pool (and blocks on the result) — running it on the shared
    writer pool keeps that wait entirely off the caller's critical path while
    still emitting the MemorySave* events (contextvars propagate via the
    engine's ``copy_context`` submit).

    This is also the write BOUNDARY: it is the one place run-produced content
    enters memory (chat turns and task outputs both arrive here), so it is where
    content is screened for prompt injection before it can be replayed into a
    later run. See ``write_hygiene``.
    """
    mem = _usable_memory(memory)
    text = " ".join((content or "").split())
    if mem is None or not text:
        return

    verdict = screen_memory_write(text, source=source)
    if not verdict.persist:
        return  # quarantined — write_hygiene has already logged why
    findings = verdict.as_metadata()
    if findings:
        metadata = {**(metadata or {}), **findings}

    def _write() -> None:
        try:
            if _already_remembered(mem, text, own=pending):
                return
            mem.remember(
                text[:4000],
                categories=categories,
                metadata=metadata,
                importance=importance,
                source=source,
                agent_role=agent_role,
            )
        except Exception as exc:  # noqa: BLE001 — persistence is best-effort
            logger.warning("Memory persist failed (source=%s): %s", source, exc)

    # Readable before it is durable. Added BEFORE the submit so there is no
    # window in which the record is invisible to both paths, and carrying the
    # writing memory's root scope so the overlay honours the tenant boundary.
    write_scope = getattr(mem, "root_scope", None)
    pending = _PendingMemory(
        content=text[:4000],
        source=source,
        agent_role=agent_role,
        # Only a real scope string counts; see pending_memory_for. Anything else
        # makes the entry unreadable rather than globally readable.
        scope=write_scope if isinstance(write_scope, str) and write_scope else None,
    )
    _add_pending(pending)

    try:
        # copy_context: the caller's ambient event attribution (agent/task set
        # via src.core.events.event_context) rides into the writer thread,
        # so MemorySave* events land under the right task in the trace.
        ctx = contextvars.copy_context()
        future = _WRITE_POOL.submit(ctx.run, _write)
        with _PENDING_LOCK:
            _PENDING_WRITES.add(future)

        def _settled(f: concurrent.futures.Future) -> None:
            _PENDING_WRITES.discard(f)  # set.discard is thread-safe
            # The row is in storage now, so recall finds it there; keeping the
            # overlay entry would surface it twice.
            _drop_pending(pending)

        future.add_done_callback(_settled)
    except RuntimeError:  # pool shut down (interpreter exit) — drop silently
        _drop_pending(pending)


def format_turn_for_memory(prompt: str, answer: str) -> str:
    """Compact single-record representation of one exchange. The record's
    ``source`` field carries the kind ("chat"/"crew_task") — no prefix here.

    The run-grounding scaffold is STRIPPED before storing: every run shares
    those phrases, so records that carry them all embed close to each other —
    and to every scaffolded query — which is how an off-topic prompt recalled
    a store full of news at 0.84 cosine. The record should embed what the USER
    asked, not the wrapper."""
    user = strip_run_boilerplate(prompt)[:600]
    assistant = " ".join((answer or "").split())[:1400]
    return f"User: {user}\nAssistant: {assistant}"


def _persist_task_output(
    mem: Any, task: Any, output: Any, execution_id: str | None = None
) -> None:
    """Shared persist body for the output sink and the legacy bus listener.

    Scoped attribution: ``remember_async`` snapshots contextvars at submit, so
    the MemorySave* events carry this task's identity into the trace even
    though the write completes later.

    The record itself is stamped with the run that wrote it (``execution_id``
    in its metadata), the way the chat path stamps its turns. The trace's
    ``record_id`` says the same thing from the other side; a run's memory view
    resolves on either, so a pruned trace or a lost record still leaves one
    exact answer to "what did this run save".
    """
    raw = str(getattr(output, "raw", "") or "")
    if not raw.strip():
        return
    task_name = getattr(task, "name", None) or "task"
    description = " ".join(str(getattr(task, "description", "") or "").split())[:300]
    metadata: dict[str, Any] = {
        "task_name": str(task_name),
        "task_description": description,
    }
    run_id = execution_id or current_execution_id()
    if run_id:
        metadata["execution_id"] = str(run_id)
    with _task_event_context(task):
        remember_async(
            mem,
            # The ANSWER, alone. This used to be
            # f"[crew task: {name}] {description}\nResult: {raw}" — the retrieval
            # KEY stored inside the retrieved document. Recall queries with
            # `task.description`, and on a saved crew that description is
            # byte-identical every run, so the stored vector contained the query
            # as a literal substring: cosine ~0.98, the 0.35 relevance floor
            # never fires, and every previous run of a task was retrieved into
            # its own next prompt BY CONSTRUCTION. A model shown its own prior
            # answer to the same question repeats it (Xu et al., >90%).
            #
            # It also broke maintenance: the invariant prefix is why the
            # exact-hash consolidation could not see two near-identical results,
            # and why merge_similar_memories — which truncates at 300 chars —
            # was comparing two identical prefixes and never reaching the answers.
            #
            # No reference system fuses request and answer: Mem0 persists the
            # extracted fact and keeps raw turns in a separate table; Graphiti
            # embeds the one-sentence fact and keeps the episode in its own
            # scope. Provenance belongs in metadata, where it can filter and
            # label but cannot score.
            " ".join(raw.split())[:1400],
            source="crew_task",
            agent_role=getattr(output, "agent", None),
            metadata=metadata,
        )


def make_memory_output_sink(memory: Any, execution_id: str | None = None) -> Any:
    """Build a ``Crew.output_sinks`` callable persisting each task's output.

    The engine invokes sinks from ``_finish_task`` for every finished task
    (sequential, async, and hierarchical alike), so no bus registration or
    unregister dance is needed — the sink dies with the crew instance.
    Returns ``None`` when memory is not attached.

    ``execution_id`` is the run the records are stamped with. Pass it where
    the wiring site has it (the crew paths do); otherwise the subprocess's
    execution context supplies it, which is how the flow path is covered.
    """
    mem = _usable_memory(memory)
    if mem is None:
        return None

    def _sink(task: Any = None, output: Any = None) -> None:
        _persist_task_output(mem, task, output, execution_id=execution_id)

    return _sink


def register_task_output_persistence(crew: Any) -> Any:
    """Persist each completed task's output to crew memory via the event bus.

    Legacy path — new wiring uses ``make_memory_output_sink`` on
    ``Crew.output_sinks``. Registers a ``TaskCompletedEvent`` handler scoped
    to THIS crew's task ids (concurrent runs in one process never
    cross-write). Returns a zero-arg ``unregister`` callable — call it in a
    ``finally`` around kickoff.
    """
    mem = _usable_memory(getattr(crew, "memory", None))
    if mem is None:
        return lambda: None
    try:
        from src.core.events import TaskCompletedEvent, event_bus
    except ImportError:  # pragma: no cover - engine always present in app
        return lambda: None

    task_ids = {
        str(getattr(task, "id", "")) for task in getattr(crew, "tasks", []) or []
    }

    def _on_task_completed(source: Any, event: Any) -> None:
        try:
            task = getattr(event, "task", None) or source
            if task_ids and str(getattr(task, "id", "")) not in task_ids:
                return
            _persist_task_output(mem, task, getattr(event, "output", None))
        except Exception as exc:  # noqa: BLE001 — never disturb the run
            logger.debug("Task memory persistence skipped: %s", exc)

    event_bus.register_handler(TaskCompletedEvent, _on_task_completed)

    def _unregister() -> None:
        try:
            event_bus.off(TaskCompletedEvent, _on_task_completed)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Task memory handler unregister skipped: %s", exc)

    return _unregister
