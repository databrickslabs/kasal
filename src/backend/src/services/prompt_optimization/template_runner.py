"""The TEMPLATE optimization worker — runs inside a thread, no event loop.

Mixed into ``PromptOptimizationService`` rather than composed, so this is pure
movement: every method still reads ``self`` exactly as it did in the single
3,031-line file, and the public surface is unchanged.
"""

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
from src.services.prompt_optimization.config import TEMPLATE_TASKS
from src.services.prompt_optimization.gepa import reflection
from src.services.prompt_optimization.gepa.grading import (  # noqa: E402
    _CATEGORICAL_GRADES,
    JUDGE_SPREAD_WARN,
    VALID_INTENTS,
    _checklist_grade,
    _grade_judge_verdict,
    _intent_format_score,
    _job_name_score,
    _json_keys_score,
    _judge_value_to_grade,
    _median_sample,
    _parse_grade_from_text,
    _to_float,
)
from src.services.prompt_optimization.gepa.reflection import (
    _GEPA_REFLECTION_STATE,
    _JUDGE_SYSTEM,
    _install_gepa_reflection_bridge,
    _make_reflection_fn,
    _sync_llm_completion,
)
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)


class TemplateRunnerMixin:
    @staticmethod
    def _execute_optimization_sync(
        loop: asyncio.AbstractEventLoop,
        template_name: str,
        baseline: str,
        examples: List[str],
        input_key: str,
        target_model: str,
        judge_model: str,
        reflection_model: str,
        max_metric_calls: int,
        registry_uri: str,
        prompt_name: str,
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """The blocking optimization body — runs in a worker thread."""
        user_token = (
            getattr(group_context, "access_token", None) if group_context else None
        )
        # Quiet MLflow's background telemetry consumer — it resolves the
        # tracking scheme over HTTP with lazy imports, one of the threads in
        # the import-lock deadlock below. Must be set before mlflow import.
        os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")

        import mlflow
        from mlflow.genai import optimize_prompts
        from mlflow.genai.optimize import GepaPromptOptimizer
        from mlflow.genai.scorers import scorer

        # Pre-import every module that MLflow imports LAZILY from background
        # threads during this flow: register_prompt spawns an async
        # link-prompt-to-experiment thread and optimize_prompts enables openai
        # autologging via import hooks — concurrent lazy imports across those
        # threads deadlock on importlib module locks (observed live via
        # py-spy). With the modules already in sys.modules, nothing imports.
        # The openai submodules matter most: mlflow/openai/autolog.py imports
        # them lazily inside its import-hook critical section, and the openai
        # package itself lazy-loads submodules (so importing the parent is NOT
        # enough — py-spy showed successive deadlocks marching through beta,
        # then responses). This list mirrors autolog's actual imports; with
        # everything already in sys.modules our thread takes no import locks
        # while MLflow's background threads run their own lazy imports.
        for _mod in (
            "openai",
            "openai.resources",
            "openai.resources.chat.completions",
            "openai.resources.completions",
            "openai.resources.embeddings",
            "openai.resources.images",
            "openai.resources.beta.chat.completions",
            "openai.resources.responses",
            "litellm",
            "databricks.sdk",
            "mlflow.openai",
        ):
            try:
                __import__(_mod)
            except ImportError:
                pass

        # Prompt-registry destination (resolved by _resolve_registry: managed
        # UC by default, local server only when explicitly enabled). Only the
        # REGISTRY is pointed there — the app's global tracking config for
        # tracing is left untouched. Belt and braces: set the module global
        # AND the env var (for optimize_prompts internals), and additionally
        # pin an explicit client for our own calls — in-process something can
        # reset the global between our set and the call (observed live), and a
        # client carries its registry_uri immutably.
        os.environ["MLFLOW_REGISTRY_URI"] = registry_uri
        mlflow.set_registry_uri(registry_uri)
        logger.info(
            f"Prompt optimization registry: requested={registry_uri} "
            f"effective={mlflow.get_registry_uri()}"
        )
        client = mlflow.MlflowClient(registry_uri=registry_uri)

        # In LOCAL mode, redirect TRACKING to the local server for the whole
        # register→optimize span and restore after. This is not just for the
        # optimizer: register_prompt itself resolves the default experiment
        # (MLFLOW_EXPERIMENT_NAME) against the TRACKING store, which is
        # globally "databricks" (main.py) and unauthenticated in local dev.
        # Side benefit: the optimization is visible in the local MLflow UI.
        local_mode = not registry_uri.startswith("databricks")
        prev_tracking = mlflow.get_tracking_uri()
        if local_mode:
            mlflow.set_tracking_uri(registry_uri)

        # Suppress experiment resolution for the whole span: register_prompt
        # spawns an async link-prompt-to-experiment thread when
        # MLFLOW_EXPERIMENT_NAME/_ID resolve, and that thread deadlocks with
        # autologging's import hooks on importlib module locks (py-spy
        # verified twice). Prompt↔experiment linking isn't needed here.
        saved_exp_env = {
            k: os.environ.pop(k, None)
            for k in ("MLFLOW_EXPERIMENT_NAME", "MLFLOW_EXPERIMENT_ID")
        }

        def _restore_span_env() -> None:
            if local_mode:
                mlflow.set_tracking_uri(prev_tracking)
            for k, old in saved_exp_env.items():
                if old is not None:
                    os.environ[k] = old

        logger.info(f"Prompt optimization stage=register registry={registry_uri}")
        try:
            prompt_version = client.register_prompt(
                name=prompt_name,
                template=baseline,
                commit_message="baseline registered by Kasal prompt optimization",
            )
        except Exception:
            _restore_span_env()
            raise
        prompt_uri = prompt_version.uri
        logger.info(f"Prompt optimization stage=optimize prompt_uri={prompt_uri}")

        def predict_fn(**inputs) -> str:
            # Loading via the registry URI is what lets the optimizer inject
            # candidate templates without knowing our LLM stack.
            candidate = client.load_prompt(prompt_uri)
            return reflection._sync_llm_completion(
                loop,
                messages=[
                    {"role": "system", "content": candidate.template},
                    {"role": "user", "content": str(inputs[input_key])},
                ],
                model=target_model,
                max_tokens=1000,
                group_context=group_context,
                user_token=user_token,
            )

        task_cfg = TEMPLATE_TASKS[template_name]

        @scorer
        def output_format(outputs) -> float:
            if template_name == "detect_intent":
                return _intent_format_score(outputs)
            if task_cfg.get("format_fn"):
                return task_cfg["format_fn"](outputs)
            return _json_keys_score(outputs, task_cfg["required_keys"])

        @scorer
        def output_correct(inputs, outputs) -> float:
            if template_name == "detect_intent":
                try:
                    parsed = robust_json_parser(str(outputs))
                    predicted = (
                        parsed.get("intent") if isinstance(parsed, dict) else None
                    )
                except Exception:
                    predicted = None
                if predicted not in VALID_INTENTS:
                    return 0.0
                judge_system = _JUDGE_SYSTEM
                judge_user = (
                    f"User message: {inputs[input_key]}\nPredicted intent: {predicted}"
                )
            else:
                judge_system = task_cfg["judge_system"]
                judge_user = (
                    f"User request: {inputs[input_key]}\nGenerated output: {outputs}"
                )
            verdict = reflection._sync_llm_completion(
                loop,
                messages=[
                    {"role": "system", "content": judge_system},
                    {"role": "user", "content": judge_user},
                ],
                model=judge_model,
                # Room for forced-thinking judges (Kimi K2.x): 300 tokens of
                # allowance was consumed entirely by reasoning, leaving empty
                # visible content (observed live).
                max_tokens=1500,
                group_context=group_context,
                user_token=user_token,
            )
            # Check the verdict's TAIL so reasoning text mentioning "correct"
            # doesn't count — the instructed final word is what matters.
            tail = (verdict or "").strip().upper()[-40:]
            negative = "WRONG" in tail or "INCORRECT" in tail
            return 1.0 if "CORRECT" in tail and not negative else 0.0

        def aggregation(scores: Dict[str, Any]) -> float:
            fmt = float(scores.get("output_format") or 0.0)
            correct = float(scores.get("output_correct") or 0.0)
            return 0.4 * fmt + 0.6 * correct

        train_data = [
            {"inputs": {input_key: ex}, "expectations": {}} for ex in examples
        ]

        # Mark every autolog flavor disabled for the span: optimize_prompts'
        # evaluation wrapper otherwise registers import hooks that lazily
        # import each installed flavor module (openai, crewai, ...) inside a
        # lock-guarded critical section — which deadlocks against MLflow's own
        # background threads on importlib locks (py-spy verified across five
        # runs, each stuck one flavor further). With disable=True the wrapper
        # skips the flavor entirely: no hooks, no imports, nothing to deadlock.
        from mlflow.models.evaluation.utils.trace import FLAVOR_TO_MODULE_NAME
        from mlflow.utils.autologging_utils import AUTOLOGGING_INTEGRATIONS

        saved_autolog_flags: Dict[str, Any] = {}
        for _flavor in FLAVOR_TO_MODULE_NAME:
            cfg = AUTOLOGGING_INTEGRATIONS.setdefault(_flavor, {})
            saved_autolog_flags[_flavor] = cfg.get("disable")
            cfg["disable"] = True

        # Quiet the JSON-recovery parser for the span: candidate templates that
        # break the output contract are EXPECTED here (that's what the format
        # scorer is for) and each one otherwise emits a burst of parse-recovery
        # ERROR logs.
        _parser_logger = logging.getLogger("src.utils.prompt_utils")
        _prev_parser_level = _parser_logger.level
        _parser_logger.setLevel(logging.CRITICAL)

        reflection_fn = _make_reflection_fn(
            loop, reflection_model, group_context, user_token
        )
        _install_gepa_reflection_bridge()

        # Tracking is already redirected in local mode (see above); this
        # try/finally owns restoring it once the optimization ends.
        try:
            _GEPA_REFLECTION_STATE.reflection_fn = reflection_fn
            result = optimize_prompts(
                predict_fn=predict_fn,
                train_data=train_data,
                prompt_uris=[prompt_uri],
                optimizer=GepaPromptOptimizer(
                    # Inert placeholder — the bridge swaps in the LLMManager
                    # -backed callable; this string is parsed but never called.
                    reflection_model="openai:/kasal-llm-manager",
                    max_metric_calls=max_metric_calls,
                ),
                scorers=[output_format, output_correct],
                aggregation=aggregation,
                # Local mode: track into the local server (visible in its UI).
                # Managed mode: registry artifacts suffice; skip tracking writes.
                enable_tracking=local_mode,
            )
        finally:
            _GEPA_REFLECTION_STATE.reflection_fn = None
            _parser_logger.setLevel(_prev_parser_level)
            _restore_span_env()
            for _flavor, old_flag in saved_autolog_flags.items():
                if old_flag is None:
                    AUTOLOGGING_INTEGRATIONS.get(_flavor, {}).pop("disable", None)
                else:
                    AUTOLOGGING_INTEGRATIONS[_flavor]["disable"] = old_flag

        optimized = result.optimized_prompts[0]
        return {
            "optimized_template": optimized.template,
            "initial_score": _to_float(getattr(result, "initial_eval_score", None)),
            "final_score": _to_float(getattr(result, "final_eval_score", None)),
            "prompt_uri": getattr(optimized, "uri", None),
        }
