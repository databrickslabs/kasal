"""Static configuration for prompt optimization: model defaults, the task
catalogue GEPA can optimize.

Separate from the service so the mixins can read it without importing the
service back (which would be a cycle)."""

import logging
from typing import Any, Dict

from src.services.prompt_optimization.gepa.crew_doc import (
    _extract_user_from_log,
)
from src.services.prompt_optimization.gepa.grading import (
    _job_name_score,
)
from src.utils.model_config import DEFAULT_ENGINE_MODEL

logger = logging.getLogger(__name__)


# The engine default (the installed model inside Databricks Apps).
DEFAULT_TARGET_MODEL = DEFAULT_ENGINE_MODEL


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
    "route_capability": {
        # capability_dispatch logs the raw user message under this endpoint
        "log_endpoint": "capability-route",
        "input_key": "message",
        "extract": None,
        "required_keys": ("capability", "confidence", "inputs"),
        "judge_system": (
            "You judge a capability router. Given a user's message and the router's "
            "JSON output, decide if it is correct. It is WRONG if it picked a "
            "capability whose description does not cover the request, if it returned "
            "null when one clearly did, or — most importantly — if it bound a value "
            "the user never stated. A null input for something the user did not say "
            "is CORRECT: the user is asked for it. Answer with EXACTLY one word: "
            "CORRECT or WRONG."
        ),
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
