"""Prompt optimization, split by concern.

``PromptOptimizationService`` was one 3,031-line file doing four unrelated
jobs. Each is now its own module, mixed into the service rather than composed —
so this is pure movement: every method still reads ``self`` exactly as before
and the public surface is unchanged.

  template_runner  the TEMPLATE optimization worker (runs in a thread)
  crew_runner      the CREW optimization worker (runs in a thread)
  judges           judge lifecycle + eval feedback
  runs             the run registry: read / cancel / apply / revert

Shared, non-duplicable pieces live beside them: ``run_state`` (the process-wide
``_RUNS`` cache and its DB mirror) and ``config`` (model defaults + the task
catalogue). The GEPA thread bridge lives in ``services/gepa/reflection.py``.
"""

from src.services.prompt_optimization.alignment import JudgeAlignmentMixin
from src.services.prompt_optimization.crew_runner import CrewRunnerMixin
from src.services.prompt_optimization.judges import JudgeOperationsMixin
from src.services.prompt_optimization.runs import RunRegistryMixin
from src.services.prompt_optimization.template_runner import TemplateRunnerMixin

__all__ = [
    "CrewRunnerMixin",
    "JudgeOperationsMixin",
    "JudgeAlignmentMixin",
    "RunRegistryMixin",
    "TemplateRunnerMixin",
]
