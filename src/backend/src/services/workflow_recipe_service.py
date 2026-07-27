"""Mine completed crew executions into reusable workflow recipes.

Phase 1 of workflow reuse: a WRITE-ONLY path that turns finished runs into
recipes. Nothing reads them yet and no user-facing behaviour changes, so this
can run for a while and build a corpus before anything depends on it.

Why a periodic sweeper and not a hook on the status write:
``ExecutionStatusService.update_status`` is tempting, but the crew path writes
its terminal status from INSIDE the spawned subprocess
(``paths/crew/execution_runner.py``). A hook there would run in the child
interpreter — the same trap that made a decided HITL gate leave its badge stuck,
because the subprocess's announcements never reached the parent. A parent-side
sweep sidesteps it entirely: idempotent, cannot fail a run, decoupled from the
status write, and it back-fills history on first pass.
"""

import hashlib
import logging
import os
import random
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace
from src.models.workflow_recipe_trial import (
    ARM_CONTROL,
    ARM_EXEMPLAR,
    ARM_NONE,
    ARMS,
)
from src.repositories.workflow_recipe_repository import WorkflowRecipeRepository
from src.repositories.workflow_recipe_trial_repository import (
    WorkflowRecipeTrialRepository,
)

logger = logging.getLogger(__name__)

# Only crews carry a reusable graph. A light-agent ("agent") run is a single
# agent with one task — there is no structure to reuse — and flows are authored
# by hand rather than derived by an LLM, so neither has a derivation cost to
# remove.
_MINEABLE_EXECUTION_TYPES = {"crew"}

# Mining reads only runs that reached this state. It is NOT a quality claim:
# COMPLETED means the crew finished, not that its output was right.
_MINEABLE_STATUS = "COMPLETED"

_BATCH = int(os.getenv("WORKFLOW_RECIPE_MINE_BATCH", "100"))

# Minimum cosine similarity for a recipe to be offered for reuse.
#
# Measured on the dev corpus (51 recipes, local nomic-embed-text): genuinely
# matching prompts scored 0.818 / 0.826 / 0.831, while an unrelated prompt
# ("build me a snake game in python") topped out at 0.456. 0.75 sits in that gap
# with margin on the noise side, because the costs are asymmetric — missing a
# reusable crew just means generating it as before, whereas offering the wrong
# crew wastes the user's attention and erodes trust in every later suggestion.
#
# The absolute scale is MODEL-DEPENDENT: swapping the embedder (dev Ollama vs
# production databricks-gte-large-en) shifts these numbers, so re-measure before
# assuming this default transfers. Hence the env override.
MIN_SIMILARITY = float(os.getenv("WORKFLOW_RECIPE_MIN_SIMILARITY", "0.75"))

# Kill-switch for feeding past crews into generation as few-shot examples. The
# real gate is curation (only human-blessed recipes are ever used, so this is
# inert until a workspace curates), but a single env var to turn the whole
# behaviour off is worth having when diagnosing a generation regression.
EXEMPLARS_ENABLED = os.getenv(
    "WORKFLOW_RECIPE_EXEMPLARS", "true"
).strip().lower() not in (
    "0",
    "false",
    "no",
)

# Fraction of ELIGIBLE generations (those that found a blessed, above-threshold
# match) that are deliberately denied their exemplars and recorded as controls.
#
# This is the only thing that makes the effectiveness report mean anything.
# Without it the comparison available is "generations that matched a past crew"
# vs "generations that did not" — but matching a past crew IS the definition of
# repeat work, and repeat work succeeds more often on its own merits. That
# comparison would credit the feature for the familiarity of the request and
# would look good even if the exemplar text were replaced with lorem ipsum.
#
# Default 0.0 (off): a holdout is a real cost — some fraction of users get a
# deliberately worse-informed generation — so it is opted into for a measurement
# window, not left running. 0.2 for a few hundred generations is enough to see a
# large effect; small effects need more than a single workspace can produce.
HOLDOUT_FRACTION = max(
    0.0, min(1.0, float(os.getenv("WORKFLOW_RECIPE_HOLDOUT", "0.0") or 0.0))
)


@dataclass
class ExemplarDecision:
    """What the recipe library contributed to ONE crew generation.

    Returned instead of a bare string because the string is the only part
    generation needs, and everything else is the part measurement needs — the
    candidates considered, the arm assigned, and (later) the ids of what was
    produced. Bundling them keeps the caller from having to re-run retrieval to
    find out what it was given.
    """

    text: str = ""
    arm: str = ARM_NONE
    candidates: List[Dict[str, Any]] = field(default_factory=list)
    injected_recipe_ids: List[int] = field(default_factory=list)
    # First line of each injected recipe's intent, for telling the user AFTER
    # the fact that this build drew on crews they approved. Deliberately a
    # notification, not a prompt: an approval step on every generation would
    # tax the common case to confirm something the user already said yes to
    # when they marked the recipe good.
    injected_labels: List[str] = field(default_factory=list)
    prompt: str = ""

    @property
    def blessed_count(self) -> int:
        return sum(1 for c in self.candidates if c.get("curation") == "good")

    @property
    def best_similarity(self) -> Optional[float]:
        scores = [c.get("similarity") for c in self.candidates if c.get("similarity")]
        return max(scores) if scores else None


def _roles(agents_yaml: Optional[Dict[str, Any]]) -> List[str]:
    """Agent roles, for a compact exemplar line."""
    return [
        str(a.get("role") or name)
        for name, a in (agents_yaml or {}).items()
        if isinstance(a, dict)
    ]


def _task_names(tasks_yaml: Optional[Dict[str, Any]]) -> List[str]:
    """Short task labels — the first clause of each description, not the whole
    prose, so a two-exemplar block stays a handful of lines rather than pages."""
    names = []
    for name, task in (tasks_yaml or {}).items():
        if not isinstance(task, dict):
            continue
        text = str(task.get("name") or task.get("description") or name)
        names.append(text.split(".")[0][:70])
    return names


def _normalize_intent(text: str) -> str:
    """Collapse whitespace/case so trivially different phrasings hash alike."""
    return " ".join((text or "").lower().split())


def intent_hash(intent_text: str) -> str:
    return hashlib.sha256(_normalize_intent(intent_text).encode("utf-8")).hexdigest()


def build_intent_text(run_name: Optional[str], tasks_yaml: Dict[str, Any]) -> str:
    """What this crew was asked to do, as one string.

    Run name plus each task's description — the run name alone is too coarse
    (several unrelated crews share "Direct User Helper"), and descriptions alone
    lose the user's own framing.
    """
    parts: List[str] = []
    if run_name:
        parts.append(str(run_name))
    for task in (tasks_yaml or {}).values():
        if isinstance(task, dict) and task.get("description"):
            parts.append(str(task["description"]))
    return "\n".join(parts).strip()


class WorkflowRecipeService:
    """Distils finished crew runs into recipes."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repository = WorkflowRecipeRepository(session)
        self.trial_repository = WorkflowRecipeTrialRepository(session)

    # ------------------------------------------------------------------ mining

    async def _candidate_executions(self) -> List[ExecutionHistory]:
        """Completed crew runs, newest first.

        Already-mined runs are filtered per-candidate against the matching
        recipe's ``mined_job_ids`` rather than here — a bulk pre-filter on
        ``source_job_id`` would miss every run that dedup folded in and rewrote
        away, which is exactly how the sweep failed to converge.
        """
        stmt = (
            select(ExecutionHistory)
            .where(ExecutionHistory.status == _MINEABLE_STATUS)
            .order_by(ExecutionHistory.created_at.desc())
            .limit(_BATCH)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def _trace_shape(self, job_id: str) -> Dict[str, Any]:
        """Descriptive stats for a run, read from what it ACTUALLY did.

        Tools are taken from trace spans rather than the configured tool list: a
        tool that was bound but never called is not part of what made this crew
        work, and shipping it in a recipe would propagate dead configuration.
        """
        stmt = select(
            ExecutionTrace.event_type,
            ExecutionTrace.output,
            ExecutionTrace.created_at,
        ).where(ExecutionTrace.job_id == job_id)
        result = await self.session.execute(stmt)
        rows = list(result.all())

        tools: set = set()
        tool_calls = 0
        errors = 0
        stamps = []
        for event_type, output, created_at in rows:
            if created_at is not None:
                stamps.append(created_at)
            if event_type and "error" in str(event_type):
                errors += 1
            name = None
            if isinstance(output, dict):
                name = output.get("tool_name")
            if name:
                tools.add(str(name))
                tool_calls += 1

        duration_ms = None
        if len(stamps) >= 2:
            duration_ms = int((max(stamps) - min(stamps)).total_seconds() * 1000)

        return {
            "tool_names": sorted(tools),
            "tool_call_count": tool_calls,
            "error_span_count": errors,
            "span_count": len(rows),
            "duration_ms": duration_ms,
        }

    @staticmethod
    def _mcp_servers(agents_yaml: Dict, tasks_yaml: Dict) -> List[str]:
        """MCP servers selected anywhere in the crew, deduplicated."""
        servers: set = set()
        for holder in list((agents_yaml or {}).values()) + list(
            (tasks_yaml or {}).values()
        ):
            if not isinstance(holder, dict):
                continue
            configs = holder.get("tool_configs")
            if not isinstance(configs, dict):
                continue
            mcp = configs.get("MCP_SERVERS")
            names = mcp.get("servers") if isinstance(mcp, dict) else mcp
            if isinstance(names, list):
                servers.update(str(n) for n in names if n)
        return sorted(servers)

    @staticmethod
    def _distil(execution: ExecutionHistory) -> Optional[Tuple[Dict[str, Any], str]]:
        """Turn one execution row into recipe fields, or None if not mineable.

        Static and side-effect free: everything it needs is on the row, which
        keeps the mineability rules directly testable.
        """
        inputs = execution.inputs if isinstance(execution.inputs, dict) else {}

        if inputs.get("execution_type") not in _MINEABLE_EXECUTION_TYPES:
            return None
        # PROVENANCE GUARD: a run that was itself started from a recipe must not
        # be mined back in, or the corpus starts learning from its own output.
        if inputs.get("source_recipe_id") is not None:
            return None

        agents_yaml = inputs.get("agents_yaml")
        tasks_yaml = inputs.get("tasks_yaml")
        if not isinstance(agents_yaml, dict) or not isinstance(tasks_yaml, dict):
            return None
        if not agents_yaml or not tasks_yaml:
            return None

        text = build_intent_text(execution.run_name, tasks_yaml)
        if not text:
            return None

        return (
            {
                "group_id": execution.group_id,
                "group_email": getattr(execution, "group_email", None),
                "intent_text": text,
                "intent_hash": intent_hash(text),
                "agents_yaml": agents_yaml,
                "tasks_yaml": tasks_yaml,
                "mcp_servers": WorkflowRecipeService._mcp_servers(
                    agents_yaml, tasks_yaml
                ),
                "source_job_id": execution.job_id,
            },
            text,
        )

    # -------------------------------------------------------------- embedding

    @staticmethod
    async def embed(text: str, group_id: Optional[str] = None) -> Optional[List[float]]:
        """Embed one string with the SAME resolver knowledge ingest/search use.

        Write and read must embed with the same model or the vectors never
        match, so both sides go through ``resolve_knowledge_embedder_config``
        (Databricks in production, local Ollama in dev).

        Returns None rather than raising when no embedder is reachable: a recipe
        without a vector is simply not retrievable yet, which must never be
        allowed to fail a mining sweep or a crew generation.
        """
        try:
            from src.core.llm_manager import LLMManager
            from src.services.knowledge_embedder import (
                resolve_knowledge_embedder_config,
            )

            config = await resolve_knowledge_embedder_config(group_id=group_id)
            return await LLMManager.get_embedding(text, embedder_config=config)
        except Exception as embed_err:  # noqa: BLE001
            logger.warning(f"[WorkflowRecipes] Embedding unavailable: {embed_err}")
            return None

    async def backfill_embeddings(self, limit: int = 100) -> int:
        """Embed recipes that have no vector yet.

        Separate from mining so an embedder outage degrades to "not retrievable
        yet" instead of losing the recipe: the structure is captured on the
        sweep, and the vector is filled in whenever the embedder returns.
        """
        embedded = 0
        for recipe in await self.repository.list_missing_embeddings(limit=limit):
            vector = await self.embed(recipe.intent_text, recipe.group_id)
            if vector is None:
                break  # embedder is down; try again next sweep
            recipe.embedding = vector
            embedded += 1
        if embedded:
            await self.session.commit()
        return embedded

    # ---------------------------------------------------------------- retrieval

    async def find_similar_for_prompt(
        self,
        prompt: str,
        group_ids: List[str],
        limit: int = 3,
    ) -> List[Tuple[Any, float]]:
        """Recipes most similar to a user's generation prompt, best first.

        NOTE the asymmetry this matches across: a recipe's ``intent_text`` is
        built from the GENERATED run name and task descriptions, while the query
        is the user's own phrasing. They describe the same job in different
        registers, so scores run lower than a like-for-like comparison would —
        the threshold is calibrated for that, not for prose-vs-prose.
        """
        if not prompt or not group_ids:
            return []
        vector = await self.embed(prompt, group_ids[0] if group_ids else None)
        if vector is None:
            return []
        return await self.repository.find_similar(vector, group_ids, limit=limit)

    async def suggest_for_prompt(
        self,
        prompt: str,
        group_ids: List[str],
        limit: int = 3,
        min_similarity: Optional[float] = None,
    ) -> List[Dict[str, Any]]:
        """Reuse candidates for a prompt, already thresholded.

        Returns [] when nothing clears the bar — deliberately, rather than
        "closest anyway". A ranked list always has a best row, so a caller that
        forgets to check the score would happily propose an unrelated crew.
        """
        threshold = MIN_SIMILARITY if min_similarity is None else min_similarity
        hits = await self.find_similar_for_prompt(prompt, group_ids, limit=limit)
        relevant = [(r, s) for r, s in hits if s >= threshold]

        # Relevance first, then human judgement. Everything left has already
        # cleared the similarity floor, so all of it is genuinely on-topic; among
        # equals, an explicit "this is the good one" outranks a few hundredths of
        # cosine distance. Stable sort, so similarity order survives within each
        # group. Ordering only — a 'bad'/'hidden' recipe never reaches this point,
        # the retrieval query already dropped it.
        relevant.sort(key=lambda pair: getattr(pair[0], "curation", None) != "good")

        return [
            self.to_summary(recipe, similarity=round(score, 4))
            for recipe, score in relevant
        ]

    # --------------------------------------------------------------- exemplars

    async def prepare_exemplars(
        self,
        prompt: str,
        group_ids: List[str],
        limit: int = 2,
    ) -> ExemplarDecision:
        """Decide what the library contributes to one generation, and record why.

        Exemplars are sourced ONLY from recipes a human marked 'good'. That
        restriction is the entire safety story: mining can say a crew FINISHED but
        never that it was correct, so learning from merely-completed runs would
        teach the generator whatever shape happened to survive — and, because
        generated crews get mined in turn, reinforce it each round until the
        library agrees only with itself. A blessed recipe is the one claim in the
        system backed by a person looking at the result.

        Consequence worth stating plainly: with nothing curated the text is "" and
        generation is byte-for-byte unchanged. The feature switches itself on as
        the workspace curates, rather than shipping enabled and hoping.

        Returns the decision rather than just the text so the caller can record a
        trial — including the CONTROL case, where blessed matches existed and were
        withheld on purpose. A control that is not recorded is indistinguishable
        from a generation that never had a match, which would collapse the two
        populations the report needs to keep apart.
        """
        decision = ExemplarDecision(prompt=prompt or "")
        if not EXEMPLARS_ENABLED or not prompt or not group_ids:
            return decision
        try:
            hits = await self.find_similar_for_prompt(prompt, group_ids, limit=8)
        except Exception as retrieval_err:  # noqa: BLE001
            # Generation must never fail because reuse lookup did.
            logger.warning(
                f"[WorkflowRecipes] Exemplar lookup skipped: {retrieval_err}"
            )
            return decision

        # Every candidate considered, not just the ones used: a report showing
        # "no exemplars ever injected" is unreadable without knowing whether
        # retrieval found nothing or found things nobody had curated.
        decision.candidates = [
            {
                "recipe_id": r.id,
                "similarity": round(float(s), 4),
                "curation": getattr(r, "curation", None),
            }
            for r, s in hits
        ]

        blessed = [
            (r, s)
            for r, s in hits
            if getattr(r, "curation", None) == "good" and s >= MIN_SIMILARITY
        ][:limit]
        if not blessed:
            return decision  # arm stays ARM_NONE

        if self._assign_to_holdout():
            decision.arm = ARM_CONTROL
            logger.info(
                f"[WorkflowRecipes] Holdout: withholding {len(blessed)} exemplar(s) "
                "from this generation (control arm)"
            )
            return decision

        decision.arm = ARM_EXEMPLAR
        decision.injected_recipe_ids = [r.id for r, _ in blessed]
        decision.injected_labels = [
            (r.intent_text or "").splitlines()[0][:120] for r, _ in blessed
        ]
        decision.text = self._format_exemplars(blessed)
        return decision

    @staticmethod
    def _assign_to_holdout() -> bool:
        """Randomly, per generation, at ``HOLDOUT_FRACTION``.

        Per generation rather than per prompt: assigning by prompt hash would be
        stable, but a workspace repeats a handful of intents, so a hash split
        would put whole intents permanently in one arm and compare different
        WORK rather than different treatment.
        """
        return HOLDOUT_FRACTION > 0.0 and random.random() < HOLDOUT_FRACTION

    @staticmethod
    def _format_exemplars(blessed: List[Tuple[Any, float]]) -> str:
        """The few-shot block itself — shape and tools, never the full graph."""
        blocks = []
        for recipe, score in blessed:
            blocks.append(
                f"--- Previously built for: {recipe.intent_text.splitlines()[0]}\n"
                f"Agents ({len(recipe.agents_yaml or {})}): "
                f"{', '.join(_roles(recipe.agents_yaml))}\n"
                f"Tasks ({len(recipe.tasks_yaml or {})}): "
                f"{', '.join(_task_names(recipe.tasks_yaml))}\n"
                f"Tools actually used: {', '.join(recipe.tool_names or []) or 'none'}\n"
                f"MCP servers: {', '.join(recipe.mcp_servers or []) or 'none'}"
            )
            logger.info(
                f"[WorkflowRecipes] Exemplar recipe={recipe.id} similarity={score:.3f}"
            )

        return (
            "\n\nPREVIOUSLY SUCCESSFUL CREWS IN THIS WORKSPACE\n"
            "These were built for similar requests and a human marked them good. "
            "Treat them as evidence of what works here — the shape of the team, "
            "which tools mattered — not as a template to copy. The current "
            "request takes precedence wherever they differ.\n\n" + "\n\n".join(blocks)
        )

    async def exemplars_for_prompt(
        self,
        prompt: str,
        group_ids: List[str],
        limit: int = 2,
    ) -> str:
        """Just the few-shot text. Thin wrapper over :meth:`prepare_exemplars`
        for callers that do not record a trial."""
        decision = await self.prepare_exemplars(prompt, group_ids, limit=limit)
        return decision.text

    # ---------------------------------------------------------------- curation

    async def curate(
        self,
        recipe_id: int,
        curation: Optional[str],
        group_ids: List[str],
        curated_by: Optional[str] = None,
    ) -> Optional[Any]:
        """Record a human judgement on a recipe, or clear it with ``None``.

        This is the ONLY trustworthy quality signal the system has. Everything
        mined is merely "a crew that finished" — which says nothing about whether
        the right rows landed in postgres — so 'good' here is what a later phase
        is allowed to learn from, and 'bad'/'hidden' immediately take a recipe
        out of circulation.

        Returns None when the recipe does not exist in these workspaces, so the
        caller can 404 rather than silently no-op.
        """
        from datetime import datetime

        from src.repositories.workflow_recipe_repository import VALID_CURATIONS

        if curation is not None and curation not in VALID_CURATIONS:
            raise ValueError(
                f"Invalid curation {curation!r}; expected one of "
                f"{VALID_CURATIONS} or null"
            )

        recipe = await self.repository.get_by_id(recipe_id, group_ids)
        if recipe is None:
            return None

        recipe.curation = curation
        recipe.curated_by = curated_by if curation else None
        recipe.curated_at = datetime.utcnow() if curation else None
        await self.session.commit()
        await self.session.refresh(recipe)
        return recipe

    async def record_reuse(self, recipe_id: int, group_ids: List[str]) -> Optional[Any]:
        """Note that a suggestion was actually taken up.

        The accept rate is the cheapest honest signal available: it is a human
        choosing this crew over generating a fresh one, captured without asking
        anyone to rate anything. It also measures whether the similarity floor is
        set sensibly — suggestions that are never accepted mean it is too low.
        """
        recipe = await self.repository.get_by_id(recipe_id, group_ids)
        if recipe is None:
            return None
        recipe.times_reused = (recipe.times_reused or 0) + 1
        await self.session.commit()
        await self.session.refresh(recipe)
        return recipe

    async def delete(self, recipe_id: int, group_ids: List[str]) -> bool:
        """Remove one recipe from the workspace's library.

        A recipe is not a record of what happened — the run history is — so
        deleting one loses nothing but the reuse candidate itself. Curating it
        'bad' or 'hidden' takes it out of circulation while keeping the row;
        this is for when it should not exist at all.

        Returns False when it does not exist in these workspaces, so the caller
        can 404 rather than report a delete that never happened.
        """
        deleted = await self.repository.delete_by_id(recipe_id, group_ids)
        if deleted:
            await self.session.commit()
        return deleted

    async def delete_for_groups(self, group_ids: Optional[List[str]] = None) -> Dict[str, int]:
        """Drop the recipe library and its trial ledger for these workspaces.

        Called when run history is deleted: recipes are distilled FROM runs and
        keep pointing at ``source_job_id`` values that no longer exist, and they
        keep feeding exemplars into crew generation from crews the user believes
        they erased. Trials go with them for the same reason — they measure runs.

        Does not commit: run deletion owns the transaction, so the recipes and
        the runs they came from disappear together or not at all.
        """
        recipes = await self.repository.delete_by_groups(group_ids)
        trials = await self.trial_repository.delete_by_groups(group_ids)
        return {"recipe_count": recipes, "trial_count": trials}

    @staticmethod
    def to_summary(recipe: Any, similarity: Optional[float] = None) -> Any:
        """Recipe row -> API summary. One mapping, so the library listing and a
        curation/reuse response cannot drift apart.

        Callers that just committed must ``refresh`` first: commit expires the
        instance, so reading these attributes would lazy-load and raise
        MissingGreenlet on the async session — the write lands and only the
        response fails, which is a confusing 500 to debug.
        """
        from src.schemas.workflow_recipe import RecipeSummary

        return RecipeSummary(
            recipe_id=recipe.id,
            intent_text=recipe.intent_text,
            run_count=recipe.run_count,
            agent_count=len(recipe.agents_yaml or {}),
            task_count=len(recipe.tasks_yaml or {}),
            tool_names=recipe.tool_names or [],
            mcp_servers=recipe.mcp_servers or [],
            source_job_id=recipe.source_job_id,
            similarity=similarity,
            curation=recipe.curation,
            times_reused=recipe.times_reused,
            updated_at=recipe.updated_at.isoformat() if recipe.updated_at else None,
        )

    async def list_for_group(self, group_ids: List[str], limit: int = 50) -> List[Any]:
        """The workspace's recipe library, as summaries."""
        recipes = await self.repository.list_by_group(group_ids, limit=limit)
        return [self.to_summary(r) for r in recipes]

    async def recipes_by_job(
        self, group_ids: List[str], limit: int = 500
    ) -> Dict[str, Dict[str, Any]]:
        """Map every mined job_id to the recipe it belongs to.

        Exists for the run list, which needs to show recipe state on each row.
        Returning the whole index in one call rather than exposing a per-job
        lookup keeps that list at one request instead of one per visible run.

        Keyed on EVERY id in ``mined_job_ids``, not just ``source_job_id``:
        dedup rewrites source_job_id to the newest run of an intent, so keying
        on it alone would leave the 28 earlier runs of a repeated intent looking
        as though they had never been mined.
        """
        index: Dict[str, Dict[str, Any]] = {}
        for recipe in await self.repository.list_by_group(group_ids, limit=limit):
            entry = {
                "recipe_id": recipe.id,
                "curation": recipe.curation,
                "intent_text": (recipe.intent_text or "").splitlines()[0][:200],
                "run_count": recipe.run_count,
                "times_reused": recipe.times_reused,
            }
            for job_id in recipe.mined_job_ids or []:
                index[str(job_id)] = entry
        return index

    async def mine_new_executions(self) -> int:
        """Distil any completed crew runs that have no recipe yet.

        Returns how many recipes were created or refreshed. Idempotent: an
        execution already represented is skipped, so repeated sweeps are cheap.
        """
        created = 0
        for execution in await self._candidate_executions():
            try:
                distilled = self._distil(execution)
                if distilled is None:
                    continue
                fields, _text = distilled
                existing = await self.repository.get_for_mining(
                    fields["intent_hash"], fields["group_id"]
                )

                if existing is not None and execution.job_id in (
                    existing.mined_job_ids or []
                ):
                    continue  # already folded in — nothing to do

                fields.update(await self._trace_shape(execution.job_id))

                if existing is not None:
                    # Same intent as a recipe we already hold. Refresh it to the
                    # NEWEST run rather than inserting a near-duplicate — 29
                    # repeats of one intent should occupy one slot, not 29.
                    # Recency is the tiebreaker on purpose: with no trustworthy
                    # quality signal yet, "the version you most recently ran" is
                    # honest, where "fewest tool calls / shortest duration"
                    # would actively reward a crew that did less work.
                    seen = list(existing.mined_job_ids or [])
                    seen.append(execution.job_id)
                    for key, value in fields.items():
                        if key not in ("group_id", "group_email"):
                            setattr(existing, key, value)
                    # Reassign (not mutate) so SQLAlchemy marks the JSON dirty.
                    existing.mined_job_ids = seen
                    existing.run_count = len(seen)
                else:
                    fields["mined_job_ids"] = [execution.job_id]
                    fields["run_count"] = 1
                    await self.repository.create(fields)
                created += 1
            except (
                Exception
            ) as distil_err:  # noqa: BLE001 — one bad run must not stop the sweep
                logger.warning(
                    f"[WorkflowRecipes] Skipped {execution.job_id}: {distil_err}"
                )

        if created:
            await self.session.commit()
        return created

    # ------------------------------------------------------------- measurement

    async def record_trial(
        self,
        decision: "ExemplarDecision",
        generated: Optional[Dict[str, Any]] = None,
        group_id: Optional[str] = None,
        group_email: Optional[str] = None,
    ) -> Optional[Any]:
        """Write one row of the measurement ledger. Never raises.

        Called after generation succeeds, so the row carries the ids of what was
        actually produced — those ids are what later links this generation to the
        run it became. Failure here is logged and dropped: a measurement that
        breaks the thing it measures is worse than a gap in the data.

        Trials are recorded for EVERY generation that consulted the library,
        including the ``none_available`` case. Those rows are the denominator —
        without them "18% of generations got exemplars" has no 100%.
        """
        try:
            agents = (generated or {}).get("agents") or []
            tasks = (generated or {}).get("tasks") or []
            agent_ids = [
                str(a.get("id")) for a in agents if isinstance(a, dict) and a.get("id")
            ]
            task_ids = [
                str(t.get("id")) for t in tasks if isinstance(t, dict) and t.get("id")
            ]

            trial = await self.trial_repository.create(
                {
                    "group_id": group_id,
                    "group_email": group_email,
                    "prompt_hash": intent_hash(decision.prompt),
                    "prompt_text": (decision.prompt or "")[:2000],
                    "candidates": decision.candidates,
                    "candidate_count": len(decision.candidates),
                    "blessed_count": decision.blessed_count,
                    "best_similarity": decision.best_similarity,
                    "arm": decision.arm,
                    "injected_recipe_ids": decision.injected_recipe_ids,
                    "agent_ids": agent_ids,
                    "task_ids": task_ids,
                    "agent_count": len(agents),
                    "task_count": len(tasks),
                }
            )
            await self.session.commit()
            logger.info(
                f"[WorkflowRecipes] Trial recorded arm={decision.arm} "
                f"candidates={len(decision.candidates)} agents={len(agents)}"
            )
            return trial
        except Exception as trial_err:  # noqa: BLE001 — never disturb generation
            logger.warning(f"[WorkflowRecipes] Trial not recorded: {trial_err}")
            return None

    async def link_trials(self, limit: int = 200) -> int:
        """Attach each trial to the run its generated crew produced, if any.

        The join is exact, not fuzzy: a crew execution stores its agents under
        ``agents_yaml`` keys of the form ``agent_<database id>``, and the trial
        holds the ids the generation created. Anything else — matching on prompt
        text, or on timing — would silently attribute the wrong run and quietly
        corrupt the very comparison this exists to make.

        Most trials never link, and that is expected: generated crews get edited,
        abandoned, or merged into a bigger canvas. Returns how many were linked.
        """
        trials = await self.trial_repository.list_unlinked(limit=limit)
        if not trials:
            return 0

        # One scan of recent crew runs, reused across every pending trial —
        # cheaper than a query per trial, and the candidate window is small.
        executions = await self._recent_crew_executions(limit=_BATCH)
        by_agent_id: Dict[str, Any] = {}
        for execution in executions:
            inputs = execution.inputs if isinstance(execution.inputs, dict) else {}
            for key in inputs.get("agents_yaml") or {}:
                agent_id = str(key).removeprefix("agent_")
                # Keep the EARLIEST run for an agent id: that is the first time
                # the generated crew was exercised, before any later editing.
                if agent_id not in by_agent_id:
                    by_agent_id[agent_id] = execution

        linked = 0
        for trial in trials:
            execution = next(
                (by_agent_id[a] for a in (trial.agent_ids or []) if a in by_agent_id),
                None,
            )
            if execution is None:
                continue
            shape = await self._trace_shape(execution.job_id)
            trial.linked_job_id = execution.job_id
            trial.linked_at = datetime.utcnow()
            trial.outcome_status = execution.status
            trial.outcome_duration_ms = shape.get("duration_ms")
            trial.outcome_error_spans = shape.get("error_span_count")
            trial.outcome_tool_calls = shape.get("tool_call_count")
            linked += 1

        if linked:
            await self.session.commit()
        return linked

    async def _recent_crew_executions(self, limit: int = 100) -> List[ExecutionHistory]:
        """Recent runs in any terminal or running state.

        Unlike mining, this does NOT filter to COMPLETED — a generation that
        produced a crew which then failed is exactly the outcome the report needs
        to count, and dropping it would leave only successes in both arms.
        """
        stmt = (
            select(ExecutionHistory)
            .order_by(ExecutionHistory.created_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def effectiveness(
        self, group_ids: List[str], days: int = 30
    ) -> Dict[str, Any]:
        """Did injecting exemplars change anything? Reported per arm.

        Read the arms as three populations, not two:
        ``none_available`` is every generation the library had nothing blessed
        for — the baseline the workspace lives with today. ``exemplar`` and
        ``control`` are the comparison, and ONLY that pair is a fair one: both
        had a blessed match available, so they differ in treatment rather than in
        how familiar the request was.

        With no holdout configured the control arm is empty and the report says
        so. That is the honest state — coverage and injection rate are still
        real, but any outcome difference against ``none_available`` is
        confounded by repeat-vs-novel work and is labelled as such.
        """
        since = datetime.utcnow() - timedelta(days=days) if days else None
        trials = await self.trial_repository.list_for_report(group_ids, since=since)

        arms = {
            arm: self._summarize_arm([t for t in trials if t.arm == arm])
            for arm in ARMS
        }
        total = len(trials)
        with_candidates = sum(1 for t in trials if (t.candidate_count or 0) > 0)
        with_blessed = sum(1 for t in trials if (t.blessed_count or 0) > 0)

        return {
            "window_days": days,
            "generations": total,
            # Coverage answers "does the library know anything about what people
            # ask?" — the ceiling on how much this feature could ever matter.
            "with_candidates": with_candidates,
            "with_blessed_candidates": with_blessed,
            "coverage_rate": round(with_candidates / total, 4) if total else None,
            "injection_rate": (
                round(arms[ARM_EXEMPLAR]["generations"] / total, 4) if total else None
            ),
            "holdout_fraction": HOLDOUT_FRACTION,
            "min_similarity": MIN_SIMILARITY,
            "arms": arms,
            "comparable": (
                arms[ARM_EXEMPLAR]["generations"] > 0
                and arms[ARM_CONTROL]["generations"] > 0
            ),
            "note": (
                "exemplar vs control is the only unconfounded comparison; "
                "none_available is repeat-vs-novel work and differs for reasons "
                "that have nothing to do with exemplars"
            ),
        }

    @staticmethod
    def _summarize_arm(trials: List[Any]) -> Dict[str, Any]:
        """Outcome rates for one arm. Rates are over LINKED trials only.

        Dividing completions by all trials would count "never run" as "did not
        complete", which is not a failure of the crew — the user simply never
        pressed go. The linked count is reported alongside so a rate computed on
        three runs is visibly a rate computed on three runs.
        """
        linked = [t for t in trials if t.linked_job_id]
        completed = [
            t for t in linked if (t.outcome_status or "").upper() == "COMPLETED"
        ]
        durations = [t.outcome_duration_ms for t in linked if t.outcome_duration_ms]
        errors = [
            t.outcome_error_spans for t in linked if t.outcome_error_spans is not None
        ]
        agents = [t.agent_count for t in trials if t.agent_count is not None]
        tasks = [t.task_count for t in trials if t.task_count is not None]

        def _median(values: List[Any]) -> Optional[float]:
            return round(statistics.median(values), 2) if values else None

        return {
            "generations": len(trials),
            "linked_runs": len(linked),
            "completed": len(completed),
            "completion_rate": (
                round(len(completed) / len(linked), 4) if linked else None
            ),
            "median_duration_ms": _median(durations),
            "median_error_spans": _median(errors),
            "median_agents": _median(agents),
            "median_tasks": _median(tasks),
        }

    # ------------------------------------------------------------------ sweeper

    @staticmethod
    async def sweep() -> int:
        """Entry point for the periodic parent-side task.

        Mines first, then fills in any missing vectors — so a recipe is always
        captured even when the embedder is unavailable, and becomes retrievable
        on a later pass. Finally links pending trials to the runs their generated
        crews produced, which can only happen here: the generation that wrote the
        trial finished long before the user pressed run.
        """
        from src.db.session import async_session_factory

        async with async_session_factory() as session:
            service = WorkflowRecipeService(session)
            mined = await service.mine_new_executions()
            await service.backfill_embeddings()
            try:
                await service.link_trials()
            except Exception as link_err:  # noqa: BLE001 — mining must still count
                logger.warning(f"[WorkflowRecipes] Trial linking skipped: {link_err}")
            return mined
