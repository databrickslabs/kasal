"""Repository for workflow recipes. Group scoping enforced on all reads.

Every read takes ``group_ids`` and returns nothing when it is empty, rather than
falling back to unscoped results — a recipe carries a crew's full structure and
tool bindings, so leaking one across workspaces would leak how another tenant
builds their crews.
"""

from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.workflow_recipe import WorkflowRecipe

# Curations that take a recipe out of circulation entirely. "bad" and "hidden"
# differ in meaning — one is a judgement, the other a preference — but both mean
# "do not offer this", and conflating them at the query is what guarantees they
# behave the same.
SUPPRESSED_CURATIONS = ("bad", "hidden")

# Every value the curation column accepts. Anything else is rejected at the API
# rather than silently stored, since an unrecognised value would read as
# "uncurated" to the filter above and quietly resurrect a rejected recipe.
VALID_CURATIONS = ("good", "bad", "hidden")


class WorkflowRecipeRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def create(self, data: Dict[str, Any]) -> WorkflowRecipe:
        record = WorkflowRecipe(**data)
        self.session.add(record)
        await self.session.flush()
        return record

    async def get_for_mining(
        self, intent_hash: str, group_id: Optional[str]
    ) -> Optional[WorkflowRecipe]:
        """The existing recipe for this intent in this workspace, if any.

        Used by mining to collapse repeats: 29 runs of one intent become one row
        that is refreshed, not 29 near-identical rows that would each compete
        for the same retrieval slot (and each cost an embedding call) later.

        Takes a single ``group_id`` rather than a list because this is the
        writer's exact-match lookup, and it matches NULL against NULL — some
        executions carry no group and must still dedup against each other
        instead of inserting a fresh row per run.
        """
        stmt = select(WorkflowRecipe).where(
            WorkflowRecipe.intent_hash == intent_hash,
            (
                WorkflowRecipe.group_id.is_(None)
                if group_id is None
                else WorkflowRecipe.group_id == group_id
            ),
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def list_by_group(
        self, group_ids: List[str], limit: int = 50
    ) -> List[WorkflowRecipe]:
        if not group_ids:
            return []
        stmt = (
            select(WorkflowRecipe)
            .where(WorkflowRecipe.group_id.in_(group_ids))
            .order_by(WorkflowRecipe.updated_at.desc())
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def count_by_group(self, group_ids: List[str]) -> int:
        if not group_ids:
            return 0
        stmt = select(WorkflowRecipe.id).where(WorkflowRecipe.group_id.in_(group_ids))
        result = await self.session.execute(stmt)
        return len(list(result.scalars().all()))

    async def list_missing_embeddings(self, limit: int = 100) -> List[WorkflowRecipe]:
        """Recipes with no vector yet. Not group-scoped: this is the writer's
        backfill queue, not a tenant-visible read."""
        stmt = (
            select(WorkflowRecipe)
            .where(WorkflowRecipe.embedding.is_(None))
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    # ---------------------------------------------------------------- retrieval

    async def _is_sqlite(self) -> bool:
        return "sqlite" in str(self.session.bind.dialect.name).lower()

    async def find_similar(
        self,
        query_embedding: List[float],
        group_ids: List[str],
        limit: int = 3,
    ) -> List[Tuple[WorkflowRecipe, float]]:
        """Recipes ranked by cosine similarity to ``query_embedding``.

        Returns (recipe, similarity) pairs so the caller can apply a threshold —
        a ranked list alone would always yield a "best" match even when nothing
        is actually close, which is how a cache serves the wrong plan.

        Hidden recipes are excluded here rather than at the caller so no reuse
        path can forget to.
        """
        if not group_ids or not query_embedding:
            return []
        if await self._is_sqlite():
            return await self._find_similar_sqlite(query_embedding, group_ids, limit)
        return await self._find_similar_postgres(query_embedding, group_ids, limit)

    def _base_query(self, group_ids: List[str]):
        """Retrievable recipes for these workspaces.

        Suppressed curations are filtered HERE rather than at each caller, so no
        present or future reuse path can forget to honour a human's "never
        suggest this again" — the one signal in the system that is unambiguous.
        """
        return select(WorkflowRecipe).where(
            WorkflowRecipe.group_id.in_(group_ids),
            WorkflowRecipe.embedding.is_not(None),
            (WorkflowRecipe.curation.is_(None))
            | (WorkflowRecipe.curation.not_in(SUPPRESSED_CURATIONS)),
        )

    async def get_by_id(
        self, recipe_id: int, group_ids: List[str]
    ) -> Optional[WorkflowRecipe]:
        """One recipe, scoped — used by curation and reuse-recording."""
        if not group_ids:
            return None
        stmt = select(WorkflowRecipe).where(
            WorkflowRecipe.id == recipe_id,
            WorkflowRecipe.group_id.in_(group_ids),
        )
        result = await self.session.execute(stmt)
        return result.scalars().first()

    async def _find_similar_sqlite(
        self, query_embedding: List[float], group_ids: List[str], limit: int
    ) -> List[Tuple[WorkflowRecipe, float]]:
        """Rank in Python: SQLite stores the vector as JSON TEXT, so there is no
        distance operator. Recipe counts are small (one row per distinct intent
        per workspace), so this is milliseconds.
        """
        import json as _json
        import math

        result = await self.session.execute(self._base_query(group_ids))
        rows = list(result.scalars().all())

        q = [float(x) for x in query_embedding]
        q_norm = math.sqrt(sum(x * x for x in q)) or 1.0

        scored: List[Tuple[WorkflowRecipe, float]] = []
        for row in rows:
            emb = row.embedding
            if isinstance(emb, (str, bytes)):
                try:
                    emb = _json.loads(emb)
                except (TypeError, ValueError):
                    continue
            if not emb or len(emb) != len(q):
                # A dimension mismatch means the row was embedded with a
                # different model; comparing them would produce a meaningless
                # score, so skip rather than silently mis-rank.
                continue
            dot = sum(a * b for a, b in zip(q, (float(x) for x in emb)))
            norm = math.sqrt(sum(float(x) * float(x) for x in emb)) or 1.0
            scored.append((row, dot / (q_norm * norm)))

        scored.sort(key=lambda pair: pair[1], reverse=True)
        return scored[:limit]

    async def _find_similar_postgres(
        self, query_embedding: List[float], group_ids: List[str], limit: int
    ) -> List[Tuple[WorkflowRecipe, float]]:
        """pgvector: rank with the cosine-distance operator and convert to
        similarity so both backends return the same scale."""
        distance = WorkflowRecipe.embedding.cosine_distance(query_embedding)
        stmt = (
            self._base_query(group_ids)
            .add_columns(distance.label("distance"))
            .order_by(distance)
            .limit(limit)
        )
        result = await self.session.execute(stmt)
        return [(row[0], 1.0 - float(row[1])) for row in result.all()]
