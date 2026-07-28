"""Guardrails: policies that decide whether an output is acceptable.

Validating an output is a CAPABILITY, not orchestration. It lived under
``engines/kasal/`` because a crew task was the first thing to call it, and that
accident made it unreachable from anywhere a crew is not already running — crew
generation, a planning pass, an exported app — all of which have outputs worth
checking.

Layout:
- root: the contract (``BaseGuardrail``, ``is_task_output``), the config model,
  and the registry. Build one via ``GuardrailFactory.create_guardrail``.
- core/: reusable guardrails (minimum_number, self_reflection, prompt-injection,
  human_review).
- demo/: domain demo guardrails coupled to the ``data_processing`` table.

``wrapper.GuardrailWrapper`` gives a built guardrail a stable class identity so
a run can label it in events and trace rows (``Task._guardrail_label`` reads the
wrapper's inner type).
"""

from src.services.guardrails.base_guardrail import BaseGuardrail, is_task_output
from src.services.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail
from src.services.guardrails.core.minimum_number_guardrail import MinimumNumberGuardrail
from src.services.guardrails.core.self_reflection_guardrail import (
    SelfReflectionGuardrail,
)
from src.services.guardrails.demo.company_count_guardrail import CompanyCountGuardrail
from src.services.guardrails.demo.company_name_not_null_guardrail import (
    CompanyNameNotNullGuardrail,
)
from src.services.guardrails.demo.data_processing_count_guardrail import (
    DataProcessingCountGuardrail,
)
from src.services.guardrails.demo.data_processing_guardrail import (
    DataProcessingGuardrail,
)
from src.services.guardrails.demo.empty_data_processing_guardrail import (
    EmptyDataProcessingGuardrail,
)
from src.services.guardrails.guardrail_factory import GuardrailFactory
from src.services.guardrails.guardrail_model import resolve_guardrail_model
from src.services.guardrails.wrapper import GuardrailWrapper

__all__ = [
    "BaseGuardrail",
    "CompanyCountGuardrail",
    "CompanyNameNotNullGuardrail",
    "DataProcessingCountGuardrail",
    "DataProcessingGuardrail",
    "EmptyDataProcessingGuardrail",
    "GuardrailFactory",
    "GuardrailWrapper",
    "LLMInjectionGuardrail",
    "MinimumNumberGuardrail",
    "SelfReflectionGuardrail",
    "is_task_output",
    "resolve_guardrail_model",
]
