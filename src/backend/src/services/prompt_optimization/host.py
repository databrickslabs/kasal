"""What ``PromptOptimizationService`` provides to the mixins it is built from.

The mixins (judges, alignment, runs) read ``self.session`` /
``self.run_repository`` and call hooks defined on the service or on a sibling
mixin. Declaring them here lets each mixin be type-checked on its own; the
declarations are annotation-only, so nothing here shadows the real attributes
at runtime.
"""

from typing import TYPE_CHECKING, Optional, Tuple

from sqlalchemy.ext.asyncio import AsyncSession

from src.repositories.prompt_optimization_run_repository import (
    PromptOptimizationRunRepository,
)
from src.utils.user_context import GroupContext


class PromptOptimizationHost:
    """Annotation-only contract between the service and its mixins."""

    session: AsyncSession
    run_repository: PromptOptimizationRunRepository

    if TYPE_CHECKING:

        async def _resolve_registry(
            self, template_name: str, group_context: Optional[GroupContext]
        ) -> Tuple[str, str]: ...

        @staticmethod
        def _crew_judge_prefix(crew_id: str) -> str: ...

        async def _judge_registry_target(
            self, group_context: Optional[GroupContext]
        ) -> Tuple[str, Optional[str]]: ...
