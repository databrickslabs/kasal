"""Apply Chat's output contract per task, without changing saved crew specs."""

from typing import Any, Dict, Optional

from src.core.logger import LoggerManager
from src.services.a2ui.compose import deck_intent, html_owned_intent, resolve_themes
from src.services.a2ui.output_directive import (
    app_rendered_kind,
    apply_diagram_directive,
)

logger = LoggerManager.get_instance().crew


async def apply_task_presentation(
    task_args: Dict[str, Any],
    task_config: Dict[str, Any],
    config: Optional[Dict[str, Any]] = None,
) -> None:
    # Explicit machine-readable contracts must retain their schema. This is a
    # presentation hint for free-text deliverables, never an output converter.
    if any(
        task_config.get(key)
        for key in ("output_schema", "output_json", "output_pydantic")
    ):
        return
    prompt = "\n".join(
        str(task_config.get(key) or "")
        for key in ("name", "description", "expected_output")
    )
    if not app_rendered_kind(prompt) and not html_owned_intent(prompt):
        return

    themes = None
    if deck_intent(prompt):
        # Match Chat's configured presentation palette; only deck tasks pay for
        # this lookup. It runs in the routed subprocess context, not a new DB.
        try:
            from src.db.session import routed_scoped_session
            from src.services.settings.ui import UIConfigService

            group_id = (config or {}).get("group_id")
            async with routed_scoped_session() as session:
                ui_config = await UIConfigService(
                    session, group_id=group_id if group_id != "default" else None
                ).get_config()
            if ui_config.enabled:
                themes = resolve_themes({"style_json": ui_config.style_json})
        except Exception as exc:  # noqa: BLE001 — branding must not stop a task
            logger.debug(f"[task] presentation palette unavailable: {exc}")

    # Instructions belong to this task, not its agent: a single agent may
    # research plain facts, create a quiz, then produce a presentation.
    directive = apply_diagram_directive({}, prompt, themes=themes)["backstory"]
    task_args["description"] += "\n\nKasal output rendering instructions:" + directive
    task_args["expected_output"] += directive
