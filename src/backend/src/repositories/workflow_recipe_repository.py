"""Repository for workflow recipes. Group scoping enforced on all reads.

Every read takes ``group_ids`` and returns nothing when it is empty, rather than
falling back to unscoped results — a recipe carries a crew's full structure and
tool bindings, so leaking one across workspaces would leak how another tenant
builds their crews.
"""

from typing import Any, Dict, List, Optional

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.models.workflow_recipe import WorkflowRecipe


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
