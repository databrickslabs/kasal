"""The replay cassette — serve a tool call from an earlier run instead of paying for it again.

A tool whose config carries ``replayable: true`` gets stamped with
``_replay_policy`` by the ToolFactory. The pre-hook installed here intercepts
the call in ``wrap_tool`` — the single choke point all three execution paths
share — and answers it with the result the same call produced in an earlier
run, so the tool is never invoked and the API is never billed.

Why this exists in the shape it does
------------------------------------
Measured on this repo's own traces before it was written: across 36 runs, the
same workload re-run issued the SAME query text only 11% of the time. The model
rephrases ("ABC News top headlines today 2026" becomes "...today August 2026"),
and the number of searches drifts too — one task went 16, then 8, then 3. So a
cache keyed on arguments alone would have missed roughly nine calls in ten,
which is the whole reason a plain cache was not built.

Hence two ways to match, in order:

1. **The same arguments.** Exact, safe, and what you want when it is available.
2. **The same position.** The Nth call to this tool within this task, matched
   against the Nth recording of that tool in that task of the source run.

(2) is what the HTTP-fixture libraries do — vcrpy's ``record_mode``/``match_on``,
betamax, nock — and it is the half that actually saves money on a re-run. It
also means the answer may not correspond to the question asked, which is why
this is opt-in per tool, why the trace marks every replayed row ``from_cache``,
and why it is a testing and development tool rather than a production cache.

Consumed once
-------------
Each recording answers at most one call. Without that, an agent that loops on a
tool would be handed the same recording forever, and a re-run would converge on
a fixed point rather than reproducing the shape of the original.
"""

import logging
import threading
from typing import Any, Callable, Dict, List, Optional

from src.services.execution.runtime import (
    ToolCallAnswered,
    register_tool_hooks,
    unregister_tool_hooks,
)
from src.services.tools.tool_policies import replay_policy
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class _Cassette:
    """Recordings for one run, matched by arguments then by position.

    Not thread-safe by accident: tool calls run on LLM worker threads, and two
    agents calling the same tool at once must not be handed the same recording.
    """

    def __init__(self, recordings: List[Any]) -> None:
        self._lock = threading.Lock()
        self._by_args: Dict[str, List[Any]] = {}
        self._by_position: Dict[tuple, List[Any]] = {}
        for rec in recordings:
            self._by_args.setdefault(
                _args_bucket(rec.tool_name, rec.args_key), []
            ).append(rec)
            self._by_position.setdefault((rec.tool_name, rec.task_name), []).append(rec)
        self._spent: set = set()
        self.hits = 0
        self.misses = 0

    def __len__(self) -> int:
        return sum(len(v) for v in self._by_position.values())

    def take(self, tool_name: str, args_key: str, task_name: str) -> Optional[Any]:
        with self._lock:
            match = self._first_unspent(
                self._by_args.get(_args_bucket(tool_name, args_key))
            ) or self._first_unspent(self._by_position.get((tool_name, task_name)))
            if match is None:
                self.misses += 1
                return None
            self._spent.add(id(match))
            self.hits += 1
            return match

    def _first_unspent(self, candidates: Optional[List[Any]]) -> Optional[Any]:
        for candidate in candidates or []:
            if id(candidate) not in self._spent:
                return candidate
        return None


def _args_bucket(tool_name: str, args_key: str) -> str:
    return f"{tool_name}\x00{args_key}"


#: Same cap the OTel bridge applies when it writes ``kasal.extra.task_name``.
_TASK_NAME_MAX = 200


def task_key(task: Any) -> str:
    """The task identity a recording is filed under, as the RECORDING spells it.

    Both sides have to agree or positional matching silently never fires, and
    the first version of this got it wrong: it read ``task.name``, while the
    recorded value comes from the bridge's ``_get_task_name`` — which prefers
    the event's ``task_name`` (that same ``task.name``, usually None here) and
    then falls back to the task DESCRIPTION, capped at 200 characters. So every
    recording was filed under "Search for and scrape the latest news…" and
    every live call looked for "", matching nothing. Observed on a real run:
    the cassette loaded, the hook installed, and not one call was replayed.
    """
    name = getattr(task, "name", None) or getattr(task, "description", None) or ""
    return str(name)[:_TASK_NAME_MAX]


def install_tool_replay_hook(
    execution_id: str,
    group_context: Optional[GroupContext],
    *,
    turn_key: str = "",
    load: Optional[Callable[..., Any]] = None,
) -> Optional[Callable[[], None]]:
    """Install the replay pre-hook for this run. Returns an uninstall callable.

    Returns None when there is nothing to replay — no group, no replayable
    tool with recordings, or the read failed. The run then behaves exactly as
    it did before this module existed.

    ``turn_key`` is the bucket positional matching uses when a call has no
    Task, which is every call on the chat path — ``Agent.kickoff_async`` runs
    the agent directly, so there is no Task to name. Chat passes the hash of
    its prompt, the same value its rows are stamped with.

    The cassette is loaded ONCE here, not per call: a tool call runs on a
    worker thread deep inside the LLM loop, and reaching for a database session
    there is how the approval hook has to work (it waits for a human) but not
    how this one should — the recordings are already final.
    """
    group_ids = list(getattr(group_context, "group_ids", None) or [])
    if not group_ids:
        return None

    recordings = (load or _load_cassette)(execution_id, group_context, group_ids)
    if not recordings:
        return None

    cassette = _Cassette(recordings)
    logger.info(
        "[replay] %d recording(s) loaded for execution %s", len(cassette), execution_id
    )

    def _pre_hook(tool: Any, kwargs: Dict[str, Any], agent: Any, task: Any) -> Any:
        policy = replay_policy(tool)
        if policy is None:
            return None

        from src.services.trace.recordings import canonical_args

        match = cassette.take(
            tool_name=getattr(tool, "name", ""),
            args_key=canonical_args(kwargs),
            task_name=task_key(task) or turn_key,
        )
        if match is None:
            return None
        return ToolCallAnswered(output=match.output, source="replay")

    register_tool_hooks(pre=_pre_hook)

    def _uninstall() -> None:
        unregister_tool_hooks(pre=_pre_hook)
        logger.info(
            "[replay] %s: %d call(s) replayed, %d went out for real",
            execution_id,
            cassette.hits,
            cassette.misses,
        )

    return _uninstall


def _load_cassette(
    execution_id: str, group_context: GroupContext, group_ids: List[str]
) -> List[Any]:
    """Read the source run's recordings for every replayable tool.

    Runs the async read on whatever loop this path has — the hook is installed
    from setup code in all three paths, before the worker threads exist.
    """
    try:
        from src.services.tools.tool_policies import DEFAULT_REPLAY_TTL_SECONDS

        return _run_async(
            _load_cassette_async(
                execution_id, group_context, group_ids, DEFAULT_REPLAY_TTL_SECONDS
            )
        )
    except Exception:  # noqa: BLE001
        # A cassette that cannot be read means the calls go out for real, which
        # is what would have happened anyway. Never fail a run over it.
        logger.debug("[replay] could not load recordings", exc_info=True)
        return []


async def _load_cassette_async(
    execution_id: str,
    group_context: GroupContext,
    group_ids: List[str],
    max_age_seconds: int,
) -> List[Any]:
    from src.db.session import routed_scoped_session
    from src.services.tools.tool_service import ToolService
    from src.services.trace.recordings import ToolRecordingsService

    async with routed_scoped_session() as session:
        if not await _workspace_has_a_replayable_tool(
            ToolService(session), group_context
        ):
            return []
        return await ToolRecordingsService(session).cassette_for(
            group_ids=group_ids,
            exclude_job_id=execution_id,
            max_age_seconds=max_age_seconds,
        )


async def _workspace_has_a_replayable_tool(
    tool_service: Any, group_context: GroupContext
) -> bool:
    """Is anything in this workspace marked replayable?

    A gate, not a filter — it only decides whether reading a cassette is worth
    a query. WHICH calls may be replayed is settled per call by the policy
    stamped on the tool instance; matching tool titles against recordings was
    the bug that emptied the cassette.

    Through the tool domain's own SERVICE, and specifically the method that
    merges the teamspace mapping over the global row (group wins) — the
    checkbox writes to ``group_tools.config``, so reading the base table would
    report every tool as not replayable.
    """
    try:
        listing = await tool_service.get_enabled_tools_for_group(group_context)
    except Exception:  # noqa: BLE001
        logger.debug("[replay] could not list tools", exc_info=True)
        return False

    return any(
        isinstance(getattr(tool, "config", None), dict)
        and (tool.config.get("replayable") or tool.config.get("replay"))
        for tool in getattr(listing, "tools", None) or []
    )


def _run_async(coro: Any) -> Any:
    """Run a coroutine from sync setup code, loop or no loop."""
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)

    # Already on a loop (the chat path installs from async setup): hand the
    # work to a thread with its own loop rather than blocking this one.
    import concurrent.futures

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        return pool.submit(asyncio.run, coro).result()
