"""Static configuration for prompt optimization: model defaults, the task
catalogue GEPA can optimize, and the MLflow experiment pin.

Separate from the service so the mixins can read it without importing the
service back (which would be a cycle)."""

import asyncio
import hashlib
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from src.core.exceptions import BadRequestError
from src.repositories.log_repository import LLMLogRepository
from src.repositories.model_config_repository import ModelConfigRepository
from src.services.prompt_optimization.gepa.grading import (  # noqa: E402
    _judge_value_to_grade,
    _checklist_grade,
    _grade_judge_verdict,
    _parse_grade_from_text,
    _job_name_score,
    _intent_format_score,
    _json_keys_score,
    _median_sample,
    _to_float,
    _CATEGORICAL_GRADES,
    JUDGE_SPREAD_WARN,
    VALID_INTENTS,
)
from src.services.prompt_optimization.gepa.crew_doc import (  # noqa: E402
    _serialize_crew_doc,
    _parse_crew_doc,
    _parse_requirement_lines,
    _distill_requirements,
    _extract_user_from_log,
    _CREW_DOC_FIELD_LABELS,
)

logger = logging.getLogger(__name__)


# Same fallback chain as the dispatcher: intent classification rides a fast model.
DEFAULT_TARGET_MODEL = os.getenv(
    "DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick"
)


MIN_EXAMPLES = 5


# Per-template task wiring: where training inputs come from in the LLM log and
# how outputs are scored. Adding an entry here (plus a schema/UI listing) is
# all it takes to make another seeded template optimizable.
TEMPLATE_TASKS: Dict[str, Dict[str, Any]] = {
    "detect_intent": {
        # dispatcher logs the raw user message as `prompt` under this endpoint
        "log_endpoint": "detect-intent",
        "input_key": "message",
        "extract": None,
    },
    "generate_agent": {
        "log_endpoint": "generate-agent",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("name", "role", "goal", "backstory"),
        "judge_system": (
            "You judge an AI-agent generator. Given a user's request and the generated "
            "agent JSON (name/role/goal/backstory), decide if the agent is a faithful, "
            "specific, well-formed configuration for that request: the role matches the "
            "domain, the goal is concrete with an action verb, and the backstory is "
            "relevant professional expertise. Answer with EXACTLY one word: CORRECT or WRONG."
        ),
    },
    "generate_task": {
        "log_endpoint": "generate-task",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("name", "description", "expected_output"),
        "judge_system": (
            "You judge an AI-task generator. Given a user's request and the generated "
            "task JSON (name/description/expected_output), decide if the task is a "
            "faithful, specific, well-formed configuration for that request: the "
            "description covers context/objective/method and the expected output names "
            "a checkable deliverable and its structure. Answer with EXACTLY one word: "
            "CORRECT or WRONG."
        ),
    },
    "generate_crew": {
        "log_endpoint": "generate-crew",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("agents", "tasks"),
        "judge_system": (
            "You judge an AI-crew generator. Given a user's goal and the generated crew "
            "JSON (agents + tasks), decide if the crew is a faithful, minimal, "
            "well-formed plan for that goal: agents have specific roles matching the "
            "domain, every task is assigned to an existing agent, dependencies make "
            "sense, and together the tasks accomplish the goal. Answer with EXACTLY "
            "one word: CORRECT or WRONG."
        ),
    },
    "generate_crew_plan": {
        "log_endpoint": "generate-crew-plan",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "required_keys": ("complexity", "process_type", "agents", "tasks"),
        "judge_system": (
            "You judge an AI-crew PLANNER that outputs a skeleton only (complexity, "
            "process_type, agent names/roles, task names with assignments). Given the "
            "user's goal and the plan JSON, decide if the outline is faithful and "
            "right-sized: the minimum agents needed, each task assigned to a listed "
            "agent, and the tasks together covering the goal's distinct actions. "
            "Answer with EXACTLY one word: CORRECT or WRONG."
        ),
    },
    "generate_job_name": {
        "log_endpoint": "generate-execution-name",
        "input_key": "request",
        "extract": _extract_user_from_log,
        "format_fn": _job_name_score,
        "judge_system": (
            "You judge an AI job-run NAMER. Given a description of the agents/tasks "
            "involved and the generated name, decide if the name is a concise (2-4 "
            "word), descriptive title for that work — specific to the subject matter, "
            "no generic filler like 'AI Job' or 'Crew Run'. Answer with EXACTLY one "
            "word: CORRECT or WRONG."
        ),
    },
}


def _pin_local_experiment() -> None:
    """Pin the MLflow experiment for judge/scorer operations.

    Scorers are PER-EXPERIMENT. The optimization runs pin the launch
    experiment ('kasal' by default), but a fresh worker's active experiment
    is Default/0 — a judge registered or listed there silently diverges from
    everything else (risk observed live while chasing a judge that never
    appeared). Every judge CRUD body must call this after set_tracking_uri.
    """
    import mlflow

    exp_name = os.environ.get("MLFLOW_EXPERIMENT_NAME") or "kasal"
    try:
        mlflow.set_experiment(exp_name)
    except Exception as exp_err:
        logger.warning(f"Could not pin experiment '{exp_name}': {exp_err}")
