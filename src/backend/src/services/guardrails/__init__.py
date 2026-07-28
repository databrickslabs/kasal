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

What stayed in the engine is ``GuardrailWrapper``: it exists to give a built
guardrail a stable class identity so the engine can label it in events and trace
rows. That is an orchestration concern and nothing else needs it.
"""

from src.services.guardrails.base_guardrail import BaseGuardrail, is_task_output
from src.services.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail
from src.services.guardrails.core.minimum_number_guardrail import MinimumNumberGuardrail
from src.services.guardrails.core.self_reflection_guardrail import SelfReflectionGuardrail
from src.services.guardrails.demo.company_count_guardrail import CompanyCountGuardrail
from src.services.guardrails.demo.company_name_not_null_guardrail import (
    CompanyNameNotNullGuardrail,
)
from src.services.guardrails.demo.data_processing_count_guardrail import (
    DataProcessingCountGuardrail,
)
from src.services.guardrails.demo.data_processing_guardrail import DataProcessingGuardrail
from src.services.guardrails.demo.empty_data_processing_guardrail import (
    EmptyDataProcessingGuardrail,
)
from src.services.guardrails.guardrail_factory import GuardrailFactory
from src.services.guardrails.guardrail_model import resolve_guardrail_model

__all__ = [
    "BaseGuardrail",
    "CompanyCountGuardrail",
    "CompanyNameNotNullGuardrail",
    "DataProcessingCountGuardrail",
    "DataProcessingGuardrail",
    "EmptyDataProcessingGuardrail",
    "GuardrailFactory",
    "LLMInjectionGuardrail",
    "MinimumNumberGuardrail",
    "SelfReflectionGuardrail",
    "is_task_output",
    "resolve_guardrail_model",
]
