"""
Guardrails package for validating task output.

Layout:
- root: framework pieces — BaseGuardrail, GuardrailFactory, GuardrailWrapper.
  Build guardrails via ``GuardrailFactory.create_guardrail``.
- core/: reusable guardrails (minimum_number, self_reflection, prompt-injection).
- demo/: domain demo guardrails coupled to the ``data_processing`` table.
"""

from src.engines.crewai.guardrails.base_guardrail import BaseGuardrail
from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory
from src.engines.crewai.guardrails.guardrail_wrapper import GuardrailWrapper

# Reusable framework guardrails
from src.engines.crewai.guardrails.core.minimum_number_guardrail import MinimumNumberGuardrail
from src.engines.crewai.guardrails.core.self_reflection_guardrail import SelfReflectionGuardrail
from src.engines.crewai.guardrails.core.llm_injection_guardrail import LLMInjectionGuardrail

# Domain demo guardrails (data_processing table family)
from src.engines.crewai.guardrails.demo.company_count_guardrail import CompanyCountGuardrail
from src.engines.crewai.guardrails.demo.data_processing_guardrail import DataProcessingGuardrail
from src.engines.crewai.guardrails.demo.empty_data_processing_guardrail import EmptyDataProcessingGuardrail
from src.engines.crewai.guardrails.demo.data_processing_count_guardrail import DataProcessingCountGuardrail
from src.engines.crewai.guardrails.demo.company_name_not_null_guardrail import CompanyNameNotNullGuardrail

__all__ = [
    # Framework
    'BaseGuardrail',
    'GuardrailFactory',
    'GuardrailWrapper',
    # Reusable (core)
    'MinimumNumberGuardrail',
    'SelfReflectionGuardrail',
    'LLMInjectionGuardrail',
    # Demo
    'CompanyCountGuardrail',
    'DataProcessingGuardrail',
    'EmptyDataProcessingGuardrail',
    'DataProcessingCountGuardrail',
    'CompanyNameNotNullGuardrail',
]