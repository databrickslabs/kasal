"""Confidential-values mode: tool results reach the agent without fractional values.

Switched on per tool with ``confidential_values: true`` in the tool's teamspace
config (see ``src/services/tools/tool_policies.py``). ``guard`` wraps the tool's
run paths, ``redactor`` rewrites what they return, ``cells`` decides what a
single value may show.
"""

from src.services.tools.confidential.cells import (
    PLACEHOLDER,
    classify_text,
    classify_value,
)
from src.services.tools.confidential.guard import (
    VALUE_FREE_MODES,
    install_confidential_guard,
    is_value_free_mode_active,
    scrubbed_exception,
)
from src.services.tools.confidential.redactor import (
    NOTICE,
    redact_output,
    redact_text,
    scrub_error_text,
)

__all__ = [
    "NOTICE",
    "PLACEHOLDER",
    "VALUE_FREE_MODES",
    "classify_text",
    "classify_value",
    "install_confidential_guard",
    "is_value_free_mode_active",
    "redact_output",
    "redact_text",
    "scrub_error_text",
    "scrubbed_exception",
]
