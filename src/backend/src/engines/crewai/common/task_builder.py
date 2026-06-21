"""Shared task-args assembly used by BOTH the crew path
(``task_adapter.create_task``) and the flow path
(``flow.modules.task_adapter.configure_task``).

Single source of truth for the kwargs passed to ``crewai.Task``: base fields,
markdown instructions, Genie MCP formatting, code-based + LLM guardrails, and
``output_pydantic`` resolution.

Path-specific plumbing stays in each caller: tool sourcing, DatabricksVolume
callback auto-add, callback string→callable resolution, task ``context`` 2-pass,
and ``_kasal_task_id`` / ``Task(**task_args)`` construction.
"""
import json
import traceback
from typing import Any, Dict, List, Optional

from src.core.logger import LoggerManager
from src.engines.crewai.common.genie_formatting import append_genie_mcp_formatting
from src.engines.crewai.guardrails.guardrail_wrapper import GuardrailWrapper

logger = LoggerManager.get_instance().crew
guardrail_logger = LoggerManager.get_instance().guardrails

# Code-based guardrail factory types that judge output with an LLM.
_LLM_FACTORY_GUARDRAILS = {'self_reflection', 'prompt_injection_check'}


async def build_task_args(
    task_config: Dict[str, Any],
    agent: Any,
    tools: List[Any],
    *,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Assemble the kwargs dict for ``crewai.Task`` from a normalized task spec.

    Returns ``task_args``; the caller constructs ``Task(**task_args)`` and applies
    its own callbacks / ``_kasal_task_id`` / context. When a guardrail is
    successfully attached, ``'guardrail'`` is present in the result so callers can
    decide whether to wire a fallback callback (a crew-only concern).
    """
    task_key = task_config.get('name') or task_config.get('id') or 'task'

    task_args: Dict[str, Any] = {
        'description': str(task_config.get('description', '')),
        'expected_output': (
            str(task_config['expected_output'])
            if task_config.get('expected_output') else ''
        ),
        'tools': tools or [],
        'agent': agent,
        'async_execution': task_config.get('async_execution', False),
        'retry_on_fail': task_config.get('retry_on_fail', False),
        'max_retries': task_config.get('max_retries', 3),
        'markdown': task_config.get('markdown', False),
    }

    # Markdown instructions
    if task_args['markdown']:
        task_args['description'] += "\n\nPlease format your response using markdown syntax."
        task_args['expected_output'] += "\n\nYour response should be formatted in markdown."

    # Genie MCP output formatting (shared with the agent/crew paths)
    task_args['expected_output'] = append_genie_mcp_formatting(
        task_args['expected_output'], task_config.get('tool_configs', {}) or {}
    )

    # Code-based guardrail (may re-route to llm_guardrail when it is actually an
    # LLM guardrail stored under the 'guardrail' key).
    if task_config.get('guardrail'):
        _apply_code_guardrail(task_args, task_config, agent, config, task_key)

    # LLM guardrail (takes priority over a code-based guardrail if both exist).
    if task_config.get('llm_guardrail'):
        await _apply_llm_guardrail(task_args, task_config, agent, config, task_key)

    # output_pydantic resolution (DB model lookup + converter compatibility).
    if task_config.get('output_pydantic'):
        await _apply_output_pydantic(task_args, task_config, agent, task_key)

    # Other optional Task fields.
    for field in ('async_execution', 'context', 'human_input', 'converter_cls', 'output_json'):
        if field in task_config:
            # output_json must be a BaseModel class, never a legacy string.
            if field == 'output_json' and isinstance(task_config[field], str):
                continue
            task_args[field] = task_config[field]

    return task_args


def _apply_code_guardrail(task_args, task_config, agent, config, task_key):
    """Code-based guardrail via GuardrailFactory. Mirrors the crew path; the
    crew-only callback fallback on failure is left to the caller."""
    guardrail_config = task_config['guardrail']
    guardrail_logger.info(f"Task {task_key} has guardrail configuration: {guardrail_config}")

    _parsed_config = guardrail_config
    if isinstance(_parsed_config, str):
        try:
            _parsed_config = json.loads(_parsed_config)
        except (json.JSONDecodeError, TypeError):
            _parsed_config = {}

    # Promote a known factory type stored under 'description' to the 'type' field.
    if (
        isinstance(_parsed_config, dict)
        and 'type' not in _parsed_config
        and _parsed_config.get('description') in _LLM_FACTORY_GUARDRAILS
    ):
        _parsed_config = dict(_parsed_config)
        _parsed_config['type'] = _parsed_config.pop('description')
        guardrail_logger.info(
            f"Task {task_key} guardrail: promoted description '{_parsed_config['type']}' to type field"
        )

    _is_llm_guardrail = (
        isinstance(_parsed_config, dict)
        and ('description' in _parsed_config or 'llm_model' in _parsed_config)
        and 'type' not in _parsed_config
    )

    if _is_llm_guardrail:
        # Actually an LLM guardrail stored under 'guardrail' — re-route it.
        guardrail_logger.info(
            f"Task {task_key} guardrail config detected as LLM guardrail "
            f"(has description/llm_model, no type) — routing to LLM guardrail handler"
        )
        task_config['llm_guardrail'] = _parsed_config
        return

    try:
        from src.engines.crewai.guardrails.guardrail_factory import GuardrailFactory

        # LLM-backed factory guardrails get the run's model stamped in.
        if isinstance(_parsed_config, dict) and _parsed_config.get('type') in _LLM_FACTORY_GUARDRAILS:
            from src.engines.crewai.guardrails.guardrail_model import resolve_guardrail_model
            _parsed_config = {**_parsed_config, 'llm_model': resolve_guardrail_model(_parsed_config.get('llm_model'), agent, config)}

        factory_config = json.dumps(_parsed_config) if isinstance(_parsed_config, dict) else _parsed_config
        guardrail = GuardrailFactory.create_guardrail(factory_config)
        if guardrail:
            # GuardrailWrapper (not a closure) so CrewAI's inspect.getsource() works.
            guardrail_wrapper = GuardrailWrapper(guardrail, task_key)
            task_args['guardrail'] = guardrail_wrapper.__call__
            if 'retry_on_fail' not in task_config:
                task_args['retry_on_fail'] = True
            guardrail_logger.info(f"Added guardrail validation to task {task_key}")
        else:
            guardrail_logger.warning(
                f"Failed to create guardrail for task {task_key}, guardrail will not be applied"
            )
    except Exception as e:
        guardrail_logger.error(f"Error setting up guardrail for task {task_key}: {str(e)}")
        guardrail_logger.error(f"Stack trace: {traceback.format_exc()}")


async def _apply_llm_guardrail(task_args, task_config, agent, config, task_key):
    """LLM guardrail via CrewAI's OSS LLMGuardrail. Mirrors the crew path,
    including the multi-tenant group_id requirement (raises if missing)."""
    llm_guardrail_config = task_config['llm_guardrail']
    guardrail_logger.info(f"Task {task_key} has LLM guardrail configuration: {llm_guardrail_config}")

    try:
        from crewai.tasks.llm_guardrail import LLMGuardrail

        if isinstance(llm_guardrail_config, dict):
            description = llm_guardrail_config.get('description', 'Validate the task output')
            explicit_model = llm_guardrail_config.get('llm_model')
        else:
            description = getattr(llm_guardrail_config, 'description', 'Validate the task output')
            explicit_model = getattr(llm_guardrail_config, 'llm_model', None)
        from src.engines.crewai.guardrails.guardrail_model import resolve_guardrail_model
        llm_model = resolve_guardrail_model(explicit_model, agent, config)

        # Proactively inject validation criteria into the description so the agent
        # aligns on the first attempt (CrewAI's native guardrail is reactive).
        if description and description != 'Validate the task output':
            validation_augmentation = (
                f"\n\n=== VALIDATION REQUIREMENTS ===\n"
                f"Your output will be validated against these criteria: {description}\n"
                f"Ensure your response satisfies them before finishing."
            )
            task_args['description'] = task_args['description'] + validation_augmentation
            guardrail_logger.info(f"Augmented task {task_key} description with guardrail criteria for proactive alignment")

        from src.core.llm_manager import LLMManager
        from src.utils.user_context import UserContext
        gc = UserContext.get_group_context()
        group_id = (gc.primary_group_id if gc else None) or (config.get('group_id') if config else None)
        if not group_id:
            # Mirror agent_adapter: never silently fall back to a shared group.
            raise ValueError("group_id is REQUIRED for LLM guardrail configuration")
        guardrail_llm = await LLMManager.configure_crewai_llm(llm_model, group_id)

        task_args['guardrail'] = LLMGuardrail(description=description, llm=guardrail_llm)
        guardrail_logger.info(f"Configured LLM guardrail for task {task_key} using model {llm_model}")
        if not task_args.get('retry_on_fail'):
            task_args['retry_on_fail'] = True
    except ImportError as e:
        guardrail_logger.error(f"Could not import LLMGuardrail for task {task_key}: {str(e)}")
    except ValueError:
        # Missing group_id — surface the multi-tenant violation.
        raise
    except Exception as e:
        guardrail_logger.error(f"Error configuring LLM guardrail for task {task_key}: {str(e)}")
        guardrail_logger.error(f"Stack trace: {traceback.format_exc()}")


async def _apply_output_pydantic(task_args, task_config, agent, task_key):
    """Resolve output_pydantic to a Pydantic class + converter compatibility."""
    from src.engines.crewai.helpers.task_adapter import get_pydantic_class_from_name

    output_pydantic_name = task_config['output_pydantic']
    logger.info(f"Task {task_key} has output_pydantic: {output_pydantic_name}")
    pydantic_class = await get_pydantic_class_from_name(output_pydantic_name)
    if not pydantic_class:
        logger.warning(
            f"Could not resolve Pydantic model '{output_pydantic_name}', removing output_pydantic"
        )
        return

    from src.engines.crewai.helpers.model_conversion_handler import (
        get_compatible_converter_for_model,
        configure_output_json_approach,
    )
    converter_cls, pydantic_cls, use_output_json, is_compatible = get_compatible_converter_for_model(
        agent, pydantic_class
    )
    if is_compatible and use_output_json:
        updated = configure_output_json_approach(task_args, pydantic_class)
        if isinstance(updated, dict) and updated is not task_args:
            task_args.clear()
            task_args.update(updated)
    elif is_compatible and converter_cls:
        task_args['converter_cls'] = converter_cls
        task_args['output_pydantic'] = pydantic_cls
    else:
        task_args['output_pydantic'] = pydantic_class
