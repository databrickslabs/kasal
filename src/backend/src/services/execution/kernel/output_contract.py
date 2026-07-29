"""The per-task JSON contract: an inline schema, and the gate over it.

Split out of ``task_builder`` rather than appended to it — that module is the
single source of truth for task-args assembly across the crew AND flow paths,
and this is a self-contained concern with a clean seam: config in, guardrail
out.
"""

import json

from src.core.logger import LoggerManager
from src.services.guardrails.wrapper import GuardrailWrapper

logger = LoggerManager.get_instance().crew
guardrail_logger = LoggerManager.get_instance().guardrails


def apply_output_schema(task_args, task_config, task_key):
    """Resolve an INLINE JSON Schema onto the task, and return its parse gate.

    Two deliberate choices here.

    **Inline, not a DB row.** ``output_pydantic`` resolves a schema by NAME from
    the ``schemas`` table, which means a per-run schema is impossible — there is
    nothing to name it. Deep research mints an envelope per task, so the schema
    travels in the task config. (It travels as a JSON Schema dict, not a class:
    the config is serialized into the crew subprocess.)

    **``output_json``, not ``output_pydantic``.** These are not interchangeable.
    Downstream context is assembled from ``TaskOutput.raw`` only, and the engine
    leaves ``raw`` untouched when ``output_pydantic`` is set — the validated
    object lands on ``.pydantic`` and is dropped from the context chain, so the
    next task still receives prose. Only ``output_json`` rewrites ``raw`` to the
    JSON dump. Since the entire point is that each task hands the next one clean
    JSON, this routes to ``output_json`` on purpose rather than as a
    provider-compatibility fallback.
    """
    from src.services.agent_builder.schema_converter import build_model_from_schema

    schema = task_config["output_schema"]
    if not isinstance(schema, dict):
        logger.warning(
            f"Task {task_key}: output_schema is {type(schema).__name__}, not an "
            "object — ignoring"
        )
        return None

    model_name = str(task_config.get("output_schema_name") or f"{task_key}_output")
    model = build_model_from_schema(model_name, schema)
    if model is None:
        logger.warning(
            f"Task {task_key}: could not build a model from output_schema — "
            "task will run without a JSON contract"
        )
        return None

    task_args["output_json"] = model
    logger.info(f"Task {task_key}: JSON contract set from inline schema ({model_name})")

    from src.services.guardrails.core.schema_gate_guardrail import SchemaGateGuardrail

    return GuardrailWrapper(SchemaGateGuardrail(model), task_key)


def build_detection_gate(task_config, task_key):
    """A DetectionRuleGuardrail from the task's ``gate``, or None."""
    gate = task_config["gate"]
    if isinstance(gate, str):
        try:
            gate = json.loads(gate)
        except (json.JSONDecodeError, TypeError):
            guardrail_logger.warning(f"Task {task_key}: gate is not valid JSON")
            return None
    if not isinstance(gate, dict) or not gate.get("require"):
        return None

    from src.services.guardrails.core.detection_rule_guardrail import (
        DetectionRuleGuardrail,
    )

    guardrail_logger.info(
        f"Task {task_key}: detection rule with {len(gate['require'])} requirement(s)"
    )
    return GuardrailWrapper(DetectionRuleGuardrail(gate), task_key)
