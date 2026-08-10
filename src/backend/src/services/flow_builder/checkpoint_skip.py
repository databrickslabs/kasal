"""Which completed crews a resuming flow may replay instead of running.

Two conditions, and both must hold:

1. the crew is BEFORE the resume point (``sequence < resume_from_crew_sequence``,
   which names the crew to RUN, so ``<`` and not ``<=``); and
2. nothing it depends on has changed — including the crew itself.

The second condition is what this module exists for. Checking each crew's
identity independently catches an edited crew but not what an edited crew
invalidates: a DOWNSTREAM crew whose own tasks are untouched still holds a
stale output, because the input it was computed from just changed. Its identity
matches, so it was skipped, and the edit vanished one hop later.

**Why this follows the graph and not the sequence.** Sequence is assigned in
BUILD order — starting points in config order, then listeners in config order —
which is not execution order. A real flow made that painfully clear: `black`
(seq 1) → `white` (seq 3) → `green` (seq 2), because `green` was simply
declared before `white` in the config. Treating "before in sequence" as "not
downstream" then skipped `green` while re-running `white`, replaying an output
computed from the previous `white` and reporting success. The sequence prefix
was justified here as conservative; against a graph whose declaration order
differs from its dependency order it is not conservative, it is wrong.

So the graph is reconstructed from the same ``listen_to_task_ids`` the builder
uses, and invalidation propagates along it.

**Every decision is made up front**, before any crew is built, because the
build loop cannot answer the question in the order it needs: `green` is decided
before `white`, yet depends on it. Deciding lazily would consult a
not-yet-computed verdict, which is the bug above wearing a different hat.
"""

import logging
from typing import Any, Dict, Iterable, List, Optional, Set

logger = logging.getLogger(__name__)


def _crew_name(
    default: Optional[str],
    configs: Iterable[Dict[str, Any]],
    *,
    task_ids: Optional[Iterable[Any]] = None,
    crew_id: Optional[Any] = None,
) -> Optional[str]:
    """The crew's USER-FACING name, as the flow config knows it.

    The database name may be an agent role; the checkpoint stores what the
    frontend calls the crew, so identity lookups have to use the same one. The
    builder resolves this inline for its log lines and this must agree with it
    — matching a starting point by task id and a listener by crew id, exactly
    as it does.
    """
    wanted = {str(t) for t in (task_ids or [])}
    for config in configs or []:
        if crew_id is not None and config.get("crewId") == crew_id:
            return config.get("name") or config.get("crewName") or default
        task_id = config.get("taskId")
        if task_id and str(task_id) in wanted:
            return config.get("crewName") or default
    return default


class CrewSkipPolicy:
    """What a resuming flow may replay, decided once for the whole flow."""

    def __init__(
        self,
        resume_from_crew_sequence: Optional[int],
        rerunning: Optional[Set[int]] = None,
    ):
        self._resume_from = resume_from_crew_sequence
        self._rerunning = rerunning or set()

    @classmethod
    def decide(
        cls,
        resume_from_crew_sequence: Optional[int],
        checkpoint_identities: Optional[Dict[str, Any]],
        starting_points: Optional[List[Any]] = None,
        listener_crews: Optional[List[Any]] = None,
        flow_config: Optional[Dict[str, Any]] = None,
    ) -> "CrewSkipPolicy":
        """Work out which crews must re-run, before any of them is built.

        Args:
            resume_from_crew_sequence: The crew to RUN; lower sequences are
                candidates for replay. None disables skipping entirely.
            checkpoint_identities: ``{crew_name: identity}`` from the checkpoint.
            starting_points: The builder's starting-point tuples,
                ``(method_name, task_ids, tasks, db_crew_name, crew_data)``.
            listener_crews: The builder's listener tuples, ``(method_name,
                crew_id, task_ids, tasks, db_crew_name, listen_to_task_ids,
                condition_type, crew_data)``.
            flow_config: For the user-facing crew names.

        Sequence numbers are assigned here exactly as the build loop assigns
        them — starting points first, then listeners, one per entry in list
        order — because that is the handle the builder asks with.
        """
        if resume_from_crew_sequence is None:
            return cls(None)

        identities = checkpoint_identities or {}
        sp_configs = (flow_config or {}).get("startingPoints", []) or []
        listener_configs = (flow_config or {}).get("listeners", []) or []

        owner_of_task: Dict[str, int] = {}
        upstream: Dict[int, Set[int]] = {}
        changed: Set[int] = set()
        names: Dict[int, Optional[str]] = {}
        sequence = 0

        def note(seq, name, task_ids, tasks):
            names[seq] = name
            for task_id in task_ids or []:
                owner_of_task[str(task_id)] = seq
            if _identity_changed(name, tasks, identities):
                changed.add(seq)

        for method_name, task_ids, tasks, db_name, _crew_data in starting_points or []:
            sequence += 1
            note(
                sequence,
                _crew_name(db_name, sp_configs, task_ids=task_ids),
                task_ids,
                tasks,
            )

        listens: Dict[int, List[Any]] = {}
        for entry in listener_crews or []:
            _m, crew_id, task_ids, tasks, db_name, listen_to, _c, _d = entry
            sequence += 1
            note(
                sequence,
                _crew_name(db_name, listener_configs, crew_id=crew_id),
                task_ids,
                tasks,
            )
            listens[sequence] = listen_to or []

        for seq, listen_to in listens.items():
            upstream[seq] = {
                owner_of_task[str(t)]
                for t in listen_to
                if str(t) in owner_of_task and owner_of_task[str(t)] != seq
            }

        rerunning = _propagate(changed, upstream)

        for seq in sorted(rerunning):
            if seq in changed:
                logger.warning(
                    f"  🔄 crew '{names.get(seq)}' (sequence {seq}) has changed "
                    f"since the checkpoint — it and everything downstream will re-run"
                )
            else:
                logger.warning(
                    f"  🔄 crew '{names.get(seq)}' (sequence {seq}) will re-run: it "
                    f"is downstream of a crew that changed, so its stored output "
                    f"was computed from an input that no longer exists"
                )

        return cls(resume_from_crew_sequence, rerunning)

    def may_skip(self, crew_name: str, crew_tasks: Any, sequence: int) -> bool:
        """Whether this crew's stored output may stand in for running it.

        ``crew_name`` and ``crew_tasks`` are accepted for the log line and to
        keep the call site self-describing; the verdict itself was reached in
        :meth:`decide`, where the whole graph is visible.
        """
        if self._resume_from is None or sequence >= self._resume_from:
            return False

        if sequence in self._rerunning:
            logger.info(
                f"  ▶️  RUNNING crew '{crew_name}' (sequence {sequence}) rather "
                f"than replaying it"
            )
            return False

        return True


def _identity_changed(
    crew_name: Optional[str], crew_tasks: Any, identities: Dict[str, Any]
) -> bool:
    """Whether this crew is verifiably different from the one checkpointed.

    "Cannot verify" is NOT "changed". Every checkpoint written before
    identities existed has no stored value, and refusing those would make
    existing checkpoints worthless overnight for the exact workflow this
    protects — so they skip as they always did, and say that nothing was
    checked.
    """
    from src.services.flow_builder.checkpoint_identity import (
        CHANGED,
        MATCH,
        compute_crew_identity,
        verify_crew_identity,
    )

    verdict = verify_crew_identity(
        compute_crew_identity(crew_name, crew_tasks), identities.get(crew_name)
    )
    if verdict != MATCH and verdict != CHANGED:
        logger.warning(
            f"  ⚠️  Crew '{crew_name}' is UNVERIFIED — the checkpoint carries no "
            f"identity, so an edit to it cannot be detected"
        )
    return verdict == CHANGED


def _propagate(changed: Set[int], upstream: Dict[int, Set[int]]) -> Set[int]:
    """``changed`` plus everything reachable from it, following the edges.

    Iterated to a fixpoint rather than walked once: the graph is keyed by
    sequence, and sequence is not topological order, so a single pass in
    numeric order would miss a crew whose ancestor sorts after it — which is
    the whole reason this module stopped trusting sequence.

    Bounded by the node count, so a cycle in a malformed config terminates
    instead of hanging the build.
    """
    invalid = set(changed)
    for _ in range(len(upstream) + 1):
        grown = {
            seq
            for seq, parents in upstream.items()
            if seq not in invalid and parents & invalid
        }
        if not grown:
            break
        invalid |= grown
    return invalid
