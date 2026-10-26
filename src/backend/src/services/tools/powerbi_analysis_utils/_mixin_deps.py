"""Type-only declarations of the cross-mixin methods each mixin calls on ``self``.

The PowerBIAnalysisTool mixins call each other's methods through ``self``; the
concrete tool class composes them all. These bases carry no runtime behaviour
(the stubs exist only under ``TYPE_CHECKING``), they just tell the type checker
what the composed class provides.
"""

from typing import TYPE_CHECKING, Any, Dict, List


class DaxGenerationDeps:
    """What ``PowerBIDaxGenerationMixin`` needs from the other mixins."""

    if TYPE_CHECKING:

        def _build_enriched_semantic_context(
            self, model_context: Dict[str, Any], config: Dict[str, Any]
        ) -> str: ...

        def _auto_wrap_with_report_filters(
            self, dax_query: str, config: Dict[str, Any]
        ) -> str: ...


class ModelFetchDeps:
    """What ``PowerBIModelFetchMixin`` needs from the other mixins."""

    if TYPE_CHECKING:

        async def _execute_dax_query(
            self, workspace_id: str, dataset_id: str, access_token: str, dax_query: str
        ) -> Dict[str, Any]: ...

        def _parse_tmdl_for_measures_and_tables(
            self, tmdl_parts: List[Dict[str, Any]], config: Dict[str, Any]
        ) -> tuple: ...
