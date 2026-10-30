"""What the crew-generation mixins read from ``self``, for the type checker.

The strategies in this package are mixed into ``CrewGenerationService`` and
reach attributes and methods that only exist on the assembled class (the
session, the repositories, the shared helpers in ``crews.py``) or on a sibling
mixin. This class declares that surface once so each mixin can be checked on
its own.

It is imported ONLY under ``TYPE_CHECKING``: at runtime every mixin still
derives from ``object``, so the MRO of ``CrewGenerationService`` is unchanged.
The method bodies never run; the real implementations live in ``crews.py`` and
the sibling mixins, with identical signatures.
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from src.utils.user_context import GroupContext

if TYPE_CHECKING:
    from src.repositories.crew_generator_repository import CrewGeneratorRepository
    from src.schemas.crew import CrewGenerationRequest, CrewStreamingRequest
    from src.services.execution.logs.llm_log_service import LLMLogService
    from src.services.tools.tool_service import ToolService


class CrewGenerationHost:
    """The ``self`` every crew-generation mixin runs against."""

    session: Any
    log_service: "LLMLogService"
    crew_generator_repository: "CrewGeneratorRepository"

    # -- crews.py (CrewGenerationService) ------------------------------------
    async def _log_llm_interaction(
        self,
        endpoint: str,
        prompt: str,
        response: str,
        model: str,
        status: str = "success",
        error_message: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ) -> None:
        raise NotImplementedError

    async def _prepare_prompt_template(
        self,
        tools: List[Dict[str, Any]],
        group_context: Optional[GroupContext],
        prompt: Optional[str] = None,
    ) -> str:
        raise NotImplementedError

    def _create_tool_name_to_id_map(
        self, tools: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        raise NotImplementedError

    async def _get_tool_details(
        self, tool_identifiers: List[Any], tool_service: "ToolService"
    ) -> List[Dict[str, Any]]:
        raise NotImplementedError

    @staticmethod
    async def _has_persistent_memory_backend(
        session: Any, group_context: Optional[GroupContext]
    ) -> bool:
        raise NotImplementedError

    @staticmethod
    def build_crew_config_from_generated(
        request: "CrewStreamingRequest",
        agent_results: List[Dict[str, Any]],
        clean_tasks: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        raise NotImplementedError

    # -- sibling mixins -------------------------------------------------------
    async def create_crew_complete(
        self,
        request: "CrewGenerationRequest",
        group_context: Optional[GroupContext] = None,
        fast_planning: bool = True,
    ) -> Dict[str, Any]:
        raise NotImplementedError

    async def _run_chat_fast_path(
        self,
        request: "CrewStreamingRequest",
        group_context: Optional[GroupContext],
        generation_id: str,
        root_span: Any,
    ) -> None:
        raise NotImplementedError

    async def _prepare_exemplars(
        self, request: Any, group_context: Optional[GroupContext], session: Any = None
    ) -> Optional[Any]:
        raise NotImplementedError

    async def _record_recipe_trial(
        self,
        decision: Optional[Any],
        result: Dict[str, Any],
        group_context: Optional[GroupContext],
        session: Any = None,
    ) -> None:
        raise NotImplementedError

    async def _recipe_decision_isolated(
        self, request: Any, group_context: Optional[GroupContext]
    ) -> Optional[Any]:
        raise NotImplementedError

    async def _record_recipe_trial_isolated(
        self,
        decision: Optional[Any],
        agents: List[Dict[str, Any]],
        tasks: List[Dict[str, Any]],
        group_context: Optional[GroupContext],
    ) -> None:
        raise NotImplementedError


# The base each mixin declares. The host for the type checker; ``object`` at
# runtime, so the assembled service's MRO is exactly what it was.
if TYPE_CHECKING:
    CrewGenerationBase = CrewGenerationHost
else:
    CrewGenerationBase = object
