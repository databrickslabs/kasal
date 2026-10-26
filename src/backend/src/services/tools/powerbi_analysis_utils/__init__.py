"""PowerBIAnalysisTool, split by concern.

The tool was one 3,506-line file — the largest source file in the repo — doing
seven unrelated jobs behind a single class. Each is now its own module, mixed
into ``PowerBIAnalysisTool`` rather than composed, so this is pure movement:
every method still reads ``self`` exactly as before and every existing
``tool._method(...)`` call site (including ~300 in the test suite) is unchanged.

  model_fetch       tokens + the three semantic-model sources + metadata
  tmdl              TMDL text/JSON parsing (measures, tables, default filters)
  semantic_context  the context handed to the DAX-generating LLM
  dax_generation    the LLM call, its trace, extraction, self-correction, execution
  dax_filters       wrapping generated DAX in the report's filter context
  report_refs       which report pages/visuals reference a measure
  output            rendering the result for the agent

Most of these methods never touch ``self`` — they are pure functions that
happened to live on a class. Now that they sit in cohesive modules, converting
them to free functions is a small per-module change rather than one large one.
"""

from src.services.tools.powerbi_analysis_utils.dax_filters import (
    PowerBIDaxFilterMixin,
)
from src.services.tools.powerbi_analysis_utils.dax_generation import (
    PowerBIDaxGenerationMixin,
)
from src.services.tools.powerbi_analysis_utils.model_fetch import (
    PowerBIModelFetchMixin,
)
from src.services.tools.powerbi_analysis_utils.output import (
    PowerBIOutputMixin,
)
from src.services.tools.powerbi_analysis_utils.report_refs import (
    PowerBIReportReferenceMixin,
)
from src.services.tools.powerbi_analysis_utils.semantic_context import (
    PowerBISemanticContextMixin,
)
from src.services.tools.powerbi_analysis_utils.tmdl import (
    PowerBITmdlParsingMixin,
)

__all__ = [
    "PowerBIDaxFilterMixin",
    "PowerBIDaxGenerationMixin",
    "PowerBIModelFetchMixin",
    "PowerBIOutputMixin",
    "PowerBIReportReferenceMixin",
    "PowerBISemanticContextMixin",
    "PowerBITmdlParsingMixin",
]
