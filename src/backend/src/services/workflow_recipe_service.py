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
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.execution_history import ExecutionHistory
from src.models.execution_trace import ExecutionTrace
from src.repositories.workflow_recipe_repository import WorkflowRecipeRepository

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
        return [
            {
                "recipe_id": recipe.id,
                "similarity": round(score, 4),
                "intent_text": recipe.intent_text,
                "run_count": recipe.run_count,
                "agent_count": len(recipe.agents_yaml or {}),
                "task_count": len(recipe.tasks_yaml or {}),
                "tool_names": recipe.tool_names or [],
                "mcp_servers": recipe.mcp_servers or [],
                "source_job_id": recipe.source_job_id,
            }
            for recipe, score in hits
            if score >= threshold
        ]

    async def list_for_group(self, group_ids: List[str], limit: int = 50) -> List[Any]:
        """The workspace's recipe library, as summaries."""
        from src.schemas.workflow_recipe import RecipeSummary

        recipes = await self.repository.list_by_group(group_ids, limit=limit)
        return [
            RecipeSummary(
                recipe_id=r.id,
                intent_text=r.intent_text,
                run_count=r.run_count,
                agent_count=len(r.agents_yaml or {}),
                task_count=len(r.tasks_yaml or {}),
                tool_names=r.tool_names or [],
                mcp_servers=r.mcp_servers or [],
                source_job_id=r.source_job_id,
                curation=r.curation,
                times_reused=r.times_reused,
                updated_at=r.updated_at.isoformat() if r.updated_at else None,
            )
            for r in recipes
        ]

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

    # ------------------------------------------------------------------ sweeper

    @staticmethod
    async def sweep() -> int:
        """Entry point for the periodic parent-side task.

        Mines first, then fills in any missing vectors — so a recipe is always
        captured even when the embedder is unavailable, and becomes retrievable
        on a later pass.
        """
        from src.db.session import async_session_factory

        async with async_session_factory() as session:
            service = WorkflowRecipeService(session)
            mined = await service.mine_new_executions()
            await service.backfill_embeddings()
            return mined
