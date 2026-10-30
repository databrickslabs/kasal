"""The built-in judges an evaluation run scores with, called through LLMManager.

Evaluation builds its judges from the same catalog as Optimize and routes them
through the same bridge: each scorer gets the placeholder model
``openai:/kasal-judge--<key>``, so its calls go to LLMManager (the Kasal model
key, with the workspace's group auth) rather than to MLflow's own provider
routing.

Which model judges (:func:`resolve_judge_model`):

1. Configuration → MLflow's judge model (inside Databricks Apps, the installed
   model when none is set; see ``MLflowService.configured_judge_model``);
2. otherwise the installation's default model (``DEFAULT_ENGINE_MODEL``).

The key must name a Kasal model. If it does not, the run's judges are skipped
with a warning, and the evaluation still logs its dataset and baseline metrics.

``mlflow.genai.evaluate`` scores on its own thread pool, while the bridge reads
its route from a ContextVar. :func:`judging` arms the route and opts the block
into sp_auth's thread-pool context copying, so evaluate's workers see it.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import contextmanager
from typing import TYPE_CHECKING, Iterator, List, Optional

from src.utils.user_context import GroupContext

if TYPE_CHECKING:
    from mlflow.genai.scorers import Scorer

    from src.services.prompt_optimization.builtin_judges.bridge import JudgeRoute
    from src.services.settings.models import ModelConfigService

logger = logging.getLogger(__name__)


def _candidate_keys(stored: str) -> List[str]:
    """The model keys a stored judge setting may mean, most specific first.

    Legacy values carry a scheme (``endpoints://x``) or a provider prefix
    (``databricks/x``); the key is then the last path segment.
    """
    text = stored.strip().split("://", 1)[-1]
    return list(dict.fromkeys(key for key in (text, text.rsplit("/", 1)[-1]) if key))


async def resolve_judge_model(
    configured: Optional[str], model_configs: ModelConfigService
) -> Optional[str]:
    """The Kasal model key evaluation judges use, or None (judges skipped)."""
    from src.utils.model_config import DEFAULT_ENGINE_MODEL

    stored = (configured or "").strip() or DEFAULT_ENGINE_MODEL
    for key in _candidate_keys(stored):
        if await model_configs.find_by_key(key) is not None:
            return key
    logger.warning(
        "Evaluation judges skipped: judge model '%s' is not a Kasal model. "
        "Choose one in Configuration -> MLflow.",
        stored,
    )
    return None


def current_judge_route(
    group_context: Optional[GroupContext], group_id: Optional[str]
) -> JudgeRoute:
    """The route for this request's judge calls: the running (main) loop, the
    caller's group, and its user token, as Optimize's judges use.

    Call on the event loop: the judges run on worker threads and submit their
    LLMManager calls back to it.
    """
    from src.services.prompt_optimization.builtin_judges.bridge import JudgeRoute

    if group_context is None and group_id:
        group_context = GroupContext(group_ids=[group_id])
    return JudgeRoute(
        asyncio.get_running_loop(),
        group_context,
        getattr(group_context, "access_token", None),
    )


def judge_scorers(
    judge_model: Optional[str],
    route: Optional[JudgeRoute],
    has_reference: bool,
    has_context: bool,
) -> List[Scorer]:
    """The catalog's evaluation scorers on the bridge, or [] without a judge."""
    if not judge_model or route is None:
        logger.warning(
            "Evaluation judges skipped: no judge model could be resolved; the "
            "run logs its dataset and baseline metrics only"
        )
        return []
    from src.services.prompt_optimization.builtin_judges.bridge import (
        placeholder_uri,
    )
    from src.services.prompt_optimization.builtin_judges.catalog import (
        evaluation_scorers,
    )

    return evaluation_scorers(
        placeholder_uri(judge_model),
        has_reference=has_reference,
        has_context=has_context,
    )


@contextmanager
def judging(route: Optional[JudgeRoute]) -> Iterator[None]:
    """Arm the judge route for ``mlflow.genai.evaluate`` and its workers."""
    if route is None:
        yield
        return
    from src.services.mlflow.sp_auth import propagate_context
    from src.services.prompt_optimization.builtin_judges.bridge import judge_route

    with judge_route(route), propagate_context():
        yield
