"""The CREW optimization worker — runs inside a thread, no event loop.

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
from src.utils.user_context import GroupContext
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
from src.services.prompt_optimization.gepa.judge_model import (  # noqa: E402
    _stored_judge_model_to_key,
    _crew_target_model,
    _resolve_judge_model,
)
from src.services.prompt_optimization.gepa.reflection import _GEPA_REFLECTION_STATE, _install_gepa_reflection_bridge, _judge_sample_count, _make_reflection_fn, _preflight_reflection, _sync_llm_completion, _sync_run_crew
from src.services.prompt_optimization.run_state import _RUNS

from src.services.prompt_optimization.gepa import reflection

logger = logging.getLogger(__name__)


class CrewRunnerMixin:
    @staticmethod
    def _execute_crew_optimization_sync(
        loop: asyncio.AbstractEventLoop,
        baseline_doc: str,
        field_keys: List[str],
        objective: str,
        rubric: str,
        agents_yaml: Dict[str, Any],
        tasks_yaml: Dict[str, Any],
        target_model: str,
        judge_model: str,
        reflection_model: str,
        max_metric_calls: int,
        execution_timeout: int,
        registry_uri: str,
        prompt_name: str,
        crew_id: str = "",
        cancel_run_id: str = "",
        group_context: Optional[GroupContext] = None,
    ) -> Dict[str, Any]:
        """Blocking crew-optimization body (worker thread). Mirrors the
        template body's MLflow span setup; predict = execute the crew."""
        import copy

        user_token = (
            getattr(group_context, "access_token", None) if group_context else None
        )
        os.environ.setdefault("MLFLOW_DISABLE_TELEMETRY", "true")
        import mlflow
        from mlflow.entities import Feedback
        from mlflow.genai import optimize_prompts
        from mlflow.genai.optimize import GepaPromptOptimizer
        from mlflow.genai.scorers import scorer
        from mlflow.models.evaluation.utils.trace import FLAVOR_TO_MODULE_NAME
        from mlflow.utils.autologging_utils import AUTOLOGGING_INTEGRATIONS

        os.environ["MLFLOW_REGISTRY_URI"] = registry_uri
        mlflow.set_registry_uri(registry_uri)
        client = mlflow.MlflowClient(registry_uri=registry_uri)

        local_mode = not registry_uri.startswith("databricks")
        prev_tracking = mlflow.get_tracking_uri()
        if local_mode:
            mlflow.set_tracking_uri(registry_uri)
        saved_exp_env = {
            k: os.environ.pop(k, None)
            for k in ("MLFLOW_EXPERIMENT_NAME", "MLFLOW_EXPERIMENT_ID")
        }
        saved_autolog_flags: Dict[str, Any] = {}
        for _flavor in FLAVOR_TO_MODULE_NAME:
            cfg = AUTOLOGGING_INTEGRATIONS.setdefault(_flavor, {})
            saved_autolog_flags[_flavor] = cfg.get("disable")
            cfg["disable"] = True

        try:
            # Fail fast on a dead reflection model — before ANY crew execution.
            _preflight_reflection(loop, reflection_model, group_context, user_token)

            prompt_version = client.register_prompt(
                name=prompt_name,
                template=baseline_doc,
                commit_message="crew baseline registered by Kasal prompt optimization",
            )
            prompt_uri = prompt_version.uri
            logger.info(f"Crew optimization stage=optimize prompt_uri={prompt_uri}")

            # Pin the experiment AFTER register_prompt (the experiment lookup
            # before registration is what spawned the deadlocking link thread).
            # Traces, registered judges, and assessments are all per-experiment,
            # and the request-context endpoints resolve MLFLOW_EXPERIMENT_NAME —
            # the run must land on the SAME experiment or judges/evals become
            # invisible to each other (observed: scorers registered on 'kasal',
            # run searching the default experiment).
            if local_mode:
                exp_name = saved_exp_env.get("MLFLOW_EXPERIMENT_NAME") or "kasal"
                try:
                    mlflow.set_experiment(exp_name)
                except Exception as exp_err:
                    logger.warning(f"Could not pin experiment '{exp_name}': {exp_err}")

            # HUMAN JUDGMENT via MLflow Assessments: every evaluation logs its
            # deliverable as a trace (tagged kasal_crew_id); Feedback and
            # Expectations the user adds on those traces in the MLflow UI are
            # harvested here and folded into the judge's rubric on the NEXT run.
            judge_rubric = rubric
            objective_for_training = objective
            train_expectations: Dict[str, str] = {}
            human_requirements: List[str] = []
            if local_mode and crew_id:
                try:
                    prior = mlflow.search_traces(
                        filter_string=f"tags.kasal_crew_id = '{crew_id}'",
                        max_results=50,
                        return_type="list",
                    )
                    # Oldest-first so the "keep the last 12" slice below keeps
                    # the NEWEST notes (search order is not guaranteed).
                    prior.sort(key=lambda t: t.info.request_time or 0)
                    notes: List[str] = []
                    req_texts: List[str] = []
                    for trace in prior:
                        for assessment in trace.search_assessments() or []:
                            name = getattr(assessment, "name", "") or ""
                            value = getattr(
                                getattr(assessment, "feedback", None), "value", None
                            )
                            exp_value = getattr(
                                getattr(assessment, "expectation", None),
                                "value",
                                None,
                            )
                            if exp_value is not None:
                                req_texts.append(str(exp_value))
                            if value is None:
                                value = exp_value
                            rationale = getattr(assessment, "rationale", None) or ""
                            if rationale:
                                req_texts.append(rationale)
                            if value is not None or rationale:
                                notes.append(
                                    f"- {name}: {value if value is not None else ''} {rationale}".strip()
                                )
                    # Deduplicated constraints, NOT the grade litany: repeating
                    # "human_grade: 0.0 ..." thirteen times anchored the judge
                    # to zero even for a compliant answer (verified live A/B).
                    human_requirements = _distill_requirements(req_texts)
                    harvest_entry = _RUNS.get(cancel_run_id) if cancel_run_id else None
                    if harvest_entry is not None:
                        harvest_entry["human_feedback_count"] = len(notes)
                except Exception as assess_err:
                    logger.warning(
                        f"Could not harvest MLflow assessments: {assess_err}"
                    )

            if human_requirements:
                # LLM-refine the raw complaints into testable imperatives —
                # verified live: a 30B judge fed the raw complaint sentences
                # ("it is giving french side...") as checklist items failed
                # EVERY mark by quoting the requirement itself as evidence;
                # the same judge with cleanly phrased requirements graded a
                # compliant answer 0.96 and a Geneva-containing one 0.60 with
                # correct verbatim quotes. One cheap call per run.
                try:
                    refined_text = reflection._sync_llm_completion(
                        loop,
                        messages=[
                            {
                                "role": "system",
                                "content": (
                                    "You convert raw human review notes about an "
                                    "AI crew's answers into a clean requirements "
                                    "checklist for FUTURE answers.\n"
                                    "- Merge duplicate and overlapping notes into "
                                    "one requirement.\n"
                                    "- Phrase each as a positive, testable "
                                    "requirement about the answer content.\n"
                                    "- Notes describing one-off failures (e.g. "
                                    "'nothing delivered') become a standing "
                                    "requirement only if sensible.\n"
                                    "- Output ONLY numbered lines 'R1. ...', "
                                    "'R2. ...' — at most 5 requirements, nothing "
                                    "else."
                                ),
                            },
                            {
                                "role": "user",
                                "content": "\n".join(
                                    f"- {r}" for r in human_requirements
                                ),
                            },
                        ],
                        model=judge_model,
                        max_tokens=800,
                        group_context=group_context,
                        user_token=user_token,
                    )
                    refined = _parse_requirement_lines(refined_text)
                    if refined:
                        human_requirements = refined[:5]
                except Exception as distill_err:
                    logger.warning(
                        f"Requirement distillation failed; using raw notes: {distill_err}"
                    )
                req_block = "\n".join(f"- {r}" for r in human_requirements)
                # Ground truth rides GEPA's expectations channel too — the
                # reflective dataset surfaces it to the mutator as explicit
                # targets, not just prose inside the request.
                train_expectations = {"human_requirements": req_block[:2000]}
                # The requirements must ALSO reach GEPA's reflection model,
                # which only sees training inputs and scorer feedback — a
                # judge that grades 0 "because wrong region" is useless to a
                # mutator that never learns the region requirement (observed
                # live: flat 0-scores with mutations blind to the human's why).
                objective_for_training = (
                    objective
                    + "\nHard requirements from human review of past answers:\n"
                    + req_block
                )
                logger.info(
                    "Crew optimization: using "
                    f"{len(human_requirements)} distilled human requirements"
                )

            # CUSTOM JUDGES: LLM judges registered on the local MLflow
            # experiment ("Create LLM judge" in the MLflow UI) participate in
            # scoring alongside the built-in judge — users author evaluation
            # criteria there and optimization honors them automatically.
            registered_scorers: List[Any] = []
            if local_mode:
                try:
                    from mlflow.genai.scorers import list_scorers

                    # Only judges ASSIGNED to this crew (scoped by name prefix)
                    # participate — the shared library is inert until assigned.
                    # Called on the mixin that DEFINES it. This used to say
                    # PromptOptimizationService._crew_judge_prefix, a name never
                    # imported here, so it raised NameError the moment a crew_id
                    # was present — silently disabling per-crew judge scoping.
                    # `self` is not an option either: the enclosing
                    # _execute_crew_optimization_sync is a @staticmethod.
                    from src.services.prompt_optimization.judges import (
                        JudgeOperationsMixin,
                    )

                    crew_prefix = (
                        JudgeOperationsMixin._crew_judge_prefix(crew_id)
                        if crew_id
                        else None
                    )
                    registered_scorers = [
                        s
                        for s in (list_scorers() or [])
                        if crew_prefix
                        and str(getattr(s, "name", "")).startswith(crew_prefix)
                    ]
                    if registered_scorers:
                        logger.info(
                            "Crew optimization: using registered MLflow judges: "
                            + ", ".join(
                                getattr(s, "name", "?") for s in registered_scorers
                            )
                        )
                except Exception as scorer_err:
                    logger.warning(
                        f"Could not load registered MLflow judges: {scorer_err}"
                    )

            expected_keys = set(field_keys)

            def _apply_fields(fields: Dict[str, str]):
                agents_over = copy.deepcopy(agents_yaml)
                tasks_over = copy.deepcopy(tasks_yaml)
                for cfg_map in (agents_over, tasks_over):
                    for entity in cfg_map.values():
                        prefix = entity.pop("_field_prefix", None)
                        if not prefix:
                            continue
                        for field in (
                            "role",
                            "goal",
                            "backstory",
                            "description",
                            "expected_output",
                        ):
                            key = f"{prefix}.{field}"
                            if key in fields and fields[key].strip():
                                entity[field] = fields[key].strip()
                return agents_over, tasks_over

            # Result caches, keyed by content. GEPA re-evaluates the SAME
            # candidate doc many times (upfront smoke test, baseline valset
            # pass, and a fresh reflective-minibatch pass EVERY iteration).
            # Uncached, those re-runs burned most of a small execution budget
            # re-measuring the baseline — a 4-execution run bought exactly ONE
            # distinct candidate (observed live: total_metric_calls=7,
            # candidates=1). Worse, the stochastic judge re-grading identical
            # prompts drew 0.0 and then 4/10 two minutes apart, so accept/
            # reject was a coin flip. With caching, each DISTINCT candidate
            # costs exactly one execution and one judgment, and comparisons
            # against the baseline are stable within the run.
            deliverable_cache: Dict[str, str] = {}
            judge_cache: Dict[str, Any] = {}
            # Serializes check-then-execute: mlflow's eval harness runs batch
            # records through a thread pool, and concurrent calls for the SAME
            # candidate all missed the cache and each ran the crew (observed
            # live: two executions of one candidate finishing in the same
            # second). GEPA itself steps sequentially, so the lock costs
            # nothing in wall-clock.
            execute_lock = threading.Lock()

            # ANSWER-FIRST JUDGING. A reference-free judge — one that only ever
            # sees a candidate answer and decides whether it looks good — is
            # measurably exploitable by an optimizer: optimizing against one
            # drove judge pass rate 0.72 -> 0.94 while true accuracy stayed at
            # 0.20, and making the judge COMMIT to its own answer before it sees
            # the candidate cut the false-positive rate 0.719 -> 0.012
            # (arXiv:2607.05904). So before grading anything, the judge writes
            # the answer IT would give for this objective + rubric, and every
            # candidate is then graded against that fixed reference.
            #
            # Cached by (objective, rubric): one extra judge call per RUN, not
            # per candidate. Computed lazily so a run that never reaches
            # grading pays nothing.
            reference_cache: Dict[str, str] = {}

            def _judge_reference() -> str:
                key = hashlib.sha256(
                    f"{objective}\x00{judge_rubric}".encode("utf-8")
                ).hexdigest()
                if key in reference_cache:
                    return reference_cache[key]
                reference = ""
                try:
                    reference = (
                        reflection._sync_llm_completion(
                            loop,
                            messages=[
                                {
                                    "role": "system",
                                    "content": (
                                        "You are establishing the GROUND TRUTH used "
                                        "to grade an AI crew's work. You have NOT "
                                        "seen any candidate answer and must not ask "
                                        "for one.\n"
                                        "Write, in at most 400 words:\n"
                                        "1. The answer YOU would give for the "
                                        "objective below, at the level of detail the "
                                        "expectations demand.\n"
                                        "2. Then a short 'MUST INCLUDE:' list of the "
                                        "concrete, checkable things any acceptable "
                                        "answer has to contain (specific facts, "
                                        "scope, structure).\n"
                                        "Output nothing else."
                                    ),
                                },
                                {
                                    "role": "user",
                                    "content": (
                                        f"Objective: {objective}\n\n"
                                        f"Per-task expectations:\n{judge_rubric}"
                                    ),
                                },
                            ],
                            model=judge_model,
                            max_tokens=1500,
                            group_context=group_context,
                            user_token=user_token,
                        )
                        or ""
                    ).strip()
                except Exception as ref_err:
                    # Degrade to reference-free grading rather than failing the
                    # run — but say so loudly, because that is the exploitable
                    # mode this whole step exists to remove.
                    logger.warning(
                        "Could not establish the judge's reference answer "
                        f"({ref_err}); grading falls back to REFERENCE-FREE, "
                        "which is exploitable by the optimizer."
                    )
                reference_cache[key] = reference
                if reference:
                    logger.info(
                        "Crew optimization: judge committed a reference answer "
                        f"({len(reference)} chars) before seeing any candidate"
                    )
                return reference

            def predict_fn(**inputs) -> str:
                run_entry = _RUNS.get(cancel_run_id, {}) if cancel_run_id else {}
                # User-requested stop: abort BEFORE spending a crew execution.
                if run_entry.get("cancel_requested"):
                    raise RuntimeError("Cancelled by user")
                candidate = client.load_prompt(prompt_uri)
                doc_key = hashlib.sha256(candidate.template.encode("utf-8")).hexdigest()
                with execute_lock:
                    return _predict_locked(doc_key, candidate, run_entry, inputs)

            def _predict_locked(doc_key, candidate, run_entry, inputs) -> str:
                # Cache lookup BEFORE the cap check: re-evaluations of an
                # already-executed candidate (usually the baseline) stay
                # truthful even after the budget is spent.
                if doc_key in deliverable_cache:
                    return deliverable_cache[doc_key]
                # HARD execution cap: the user's budget is a promise about crew
                # executions, but GEPA overshoots (parallel batches are only
                # budget-checked between iterations, plus the upfront smoke
                # test). Once the cap is spent, further NEW candidates get a
                # free empty result — they score 0, never win, and GEPA wraps
                # up returning the best already-evaluated candidate.
                if run_entry.get("executions_used", 0) >= max_metric_calls:
                    logger.info(
                        "Crew optimization execution cap reached "
                        f"({max_metric_calls}); skipping further executions"
                    )
                    return ""
                fields = _parse_crew_doc(candidate.template)
                # Malformed candidates never execute — free rejection.
                if fields is None or set(fields) != expected_keys:
                    return ""
                if run_entry:
                    run_entry["executions_used"] = (
                        run_entry.get("executions_used", 0) + 1
                    )
                agents_over, tasks_over = _apply_fields(fields)
                deliverable = reflection._sync_run_crew(
                    loop,
                    agents_yaml=agents_over,
                    tasks_yaml=tasks_over,
                    model=target_model,
                    timeout=execution_timeout,
                    group_context=group_context,
                    user_token=user_token,
                )
                deliverable_cache[doc_key] = deliverable
                if run_entry:
                    run_entry["candidates_tried"] = len(deliverable_cache)
                # Log this evaluation as an MLflow trace so the user can attach
                # Feedback/Expectations (Assessments panel) that steer the judge
                # on the next run. Advisory only — never fail the eval over it.
                if local_mode and crew_id:
                    try:
                        with mlflow.start_span(name="crew_optimization_eval") as span:
                            span.set_inputs(
                                {
                                    "objective": inputs.get("request", objective),
                                    "candidate_prompts": candidate.template[:4000],
                                }
                            )
                            span.set_outputs({"deliverable": deliverable[:8000]})
                            mlflow.update_current_trace(tags={"kasal_crew_id": crew_id})
                    except Exception as trace_err:
                        # Warning, not debug: a lost trace means the user
                        # cannot grade that answer (a baseline eval vanished
                        # silently this way, observed live).
                        logger.warning(f"Eval trace logging failed: {trace_err}")
                return deliverable

            @scorer
            def output_format(outputs) -> float:
                text = str(outputs or "").strip()
                return 1.0 if len(text) > 50 else 0.0

            @scorer
            def output_correct(inputs, outputs):
                # GRADED, not binary: a pass/fail judge saturates at 1.0 for any
                # acceptable baseline, leaving GEPA no gradient to climb (observed
                # live: 1.00 -> 1.00 with zero exploration payoff). A harsh 0-10
                # rubric keeps ordinary output around 6-7 so better prompts can
                # actually outscore the baseline.
                #
                # Returns an mlflow Feedback (value + rationale), NOT a bare
                # float: rationales are the ONLY textual signal the GEPA
                # reflection model receives about WHY a candidate scored low
                # (mlflow folds Feedback.rationale into the reflective
                # dataset). With floats, mutations were blind guesses — the
                # judge knew "wrong region, rentals not sales" but the mutator
                # never heard it (observed live: 1 requirement-aware candidate
                # in ~10 runs).
                text = str(outputs or "").strip()
                if not text:
                    return Feedback(
                        name="output_correct",
                        value=0.0,
                        rationale="Empty deliverable — the crew produced no output.",
                    )
                text_key = hashlib.sha256(text.encode("utf-8")).hexdigest()
                cached = judge_cache.get(text_key)
                if cached is not None:
                    return Feedback(
                        name="output_correct", value=cached[0], rationale=cached[1]
                    )
                if human_requirements:
                    # CHECKLIST mode: per-requirement PASS/FAIL gives GEPA a
                    # gradient to climb — the all-or-nothing harsh grader
                    # produced a flat 0.0 landscape where a fully compliant
                    # candidate could only ever TIE the baseline. The verbatim
                    # -quote rule counters judge hallucination (observed live:
                    # a FAIL claiming Geneva rows in an answer containing
                    # none), and the objective line is deliberately withheld —
                    # the crew's own task text may contradict the human
                    # requirements (it said "cities like Zurich, Geneva" while
                    # the human demanded German-side only).
                    #
                    # This mode is deliberately NOT answer-first: it already
                    # grades against ground truth (the distilled HUMAN
                    # requirements), which beats a judge-written reference. And
                    # a reference derived from the objective would reintroduce
                    # exactly the contradiction the withheld objective avoids.
                    req_lines = "\n".join(
                        f"R{i + 1}. {r}" for i, r in enumerate(human_requirements)
                    )
                    judge_messages = [
                        {
                            "role": "system",
                            "content": (
                                "You are grading an AI crew's final deliverable "
                                "against a numbered requirements checklist distilled "
                                "from human review of PREVIOUS answers.\n"
                                "Rules:\n"
                                "- Judge ONLY the answer shown below. Failures of "
                                "previous answers are irrelevant.\n"
                                "- Each requirement states what the human demanded "
                                "(sometimes phrased as a complaint about an older "
                                "answer); decide whether THIS answer satisfies it.\n"
                                "- For EACH requirement output one line: "
                                "'R<n>: PASS' or 'R<n>: FAIL — ' followed by a "
                                "VERBATIM quote from the answer proving the "
                                "violation.\n"
                                "- If you cannot quote a violating passage from the "
                                "answer, the mark is PASS.\n"
                                "- Then output 'Q: <0-10>' rating base quality "
                                "(completeness, specificity, format) of the answer "
                                "against the task expectations. 10 is rare.\n"
                                "- Output nothing else."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Task expectations:\n{judge_rubric}\n\n"
                                f"Requirements from human review:\n{req_lines}\n\n"
                                f"Answer to grade:\n{text[:6000]}"
                            ),
                        },
                    ]
                else:
                    # ANSWER-FIRST: the judge already committed to its own
                    # reference answer for this objective (before it had seen
                    # any candidate), and grades against THAT. Without it the
                    # judge is reference-free and the optimizer can climb its
                    # preferences instead of answer quality.
                    reference = _judge_reference()
                    answer_first_rules = (
                        (
                            "\nBefore seeing this answer you committed to your OWN "
                            "reference answer and a MUST INCLUDE list for this "
                            "objective; both are given below.\n"
                            "- Grade the candidate against THAT reference and that "
                            "list. Do NOT revise the reference to fit the candidate.\n"
                            "- Credit only content the reference or the MUST INCLUDE "
                            "list actually calls for; a fluent answer that misses "
                            "them is not a good answer.\n"
                        )
                        if reference
                        else ""
                    )
                    judge_messages = [
                        {
                            "role": "system",
                            "content": (
                                "You are a HARSH grader of an AI crew's final deliverable. "
                                "Score 0-10 against the per-task expectations:\n"
                                "- completeness: every expectation addressed, none skipped\n"
                                "- specificity: concrete facts/sources/structure, no filler\n"
                                "- fidelity: matches the requested format and scope exactly\n"
                                "10 = flawless and exceptional (rare). 7 = solid with minor "
                                "gaps. 5 = acceptable but generic. 3 = major omissions. "
                                "0 = failed.\n"
                                f"{answer_first_rules}"
                                "First, in one or two sentences, name the SPECIFIC "
                                "failures, quoting the exact expectation that was "
                                "violated. Then write the final grade alone on the "
                                "LAST line as a bare number."
                            ),
                        },
                        {
                            "role": "user",
                            "content": (
                                f"Objective: {inputs.get('request', objective)}\n"
                                f"Expectations:\n{judge_rubric}\n"
                                + (
                                    "\nYour committed reference answer and required "
                                    f"content:\n{reference[:4000]}\n"
                                    if reference
                                    else ""
                                )
                                + f"\nFinal output:\n{text[:6000]}"
                            ),
                        },
                    ]

                # MULTI-SAMPLE + MEDIAN. One draw made accept/reject a coin
                # flip: the same prompt has been graded 0.0 and then 4/10 two
                # minutes apart. N samples reduced by median absorb that. The
                # per-candidate cost is bounded because judge_cache means a
                # DISTINCT deliverable is only ever sampled once per run.
                sample_count = _judge_sample_count()
                samples: List[tuple] = []
                judge_error: Optional[Exception] = None
                for index in range(sample_count):
                    messages = judge_messages
                    if sample_count > 1:
                        # Distinct requests: litellm's process-global cache
                        # replays a byte-identical prompt, which would make
                        # samples 2..N copies of sample 1 (the same trap the
                        # reflection bridge already works around). Skipped when
                        # sample_count == 1 so that path stays byte-identical
                        # to single-draw behavior.
                        messages = [
                            {
                                "role": "system",
                                "content": f"grading pass {uuid.uuid4().hex}",
                            }
                        ] + judge_messages
                    try:
                        verdict = reflection._sync_llm_completion(
                            loop,
                            messages=messages,
                            model=judge_model,
                            # Room for forced-thinking models (Kimi K2.x): even
                            # 300 tokens were consumed entirely by reasoning,
                            # leaving empty visible content (observed live).
                            max_tokens=1500,
                            group_context=group_context,
                            user_token=user_token,
                        )
                    except Exception as judge_err:
                        logger.error(
                            f"Crew optimization judge call failed "
                            f"(sample {index + 1}/{sample_count}): {judge_err}"
                        )
                        judge_error = judge_err
                        continue
                    sample = _grade_judge_verdict(verdict, len(human_requirements))
                    if sample is not None:
                        samples.append(sample)
                if not samples and judge_error is not None:
                    # A broken judge must be LOUD: silent zeros flatten the
                    # whole score landscape and make runs look like "no
                    # improvement possible" (observed live with an unsupported
                    # judge provider — every grade was an exception). Only
                    # raised when NO sample survived; a partial outage still
                    # yields a usable median.
                    raise judge_error

                grades: List[float] = []
                rationale_parts: List[str] = []
                if samples:
                    median_grade, median_rationale = _median_sample(samples)
                    grades.append(median_grade)
                    if median_rationale:
                        rationale_parts.append(median_rationale)
                # Registered judges grade the SAME deliverable here rather than
                # running as separate MLflow scorers — trace-based scorers were
                # each re-triggering their own crew execution (observed live as
                # bursts of one execution per judge), multiplying the budget.
                # Registered judges are RENDERED AND INVOKED HERE through
                # LLMManager — never via mlflow's own model client. The judge
                # entity contributes only its instructions and model key;
                # provider routing, keys and request quirks stay centralized
                # in the manager (invoking judges through mlflow's client is
                # what produced the retired-DeepSeek and Kimi failures).
                for judge in registered_scorers:
                    judge_name = getattr(judge, "name", "judge")
                    try:
                        instructions = getattr(judge, "instructions", "") or ""
                        rendered = (
                            instructions.replace("{{ outputs }}", text)
                            .replace("{{outputs}}", text)
                            .replace(
                                "{{ inputs }}", str(inputs.get("request", objective))
                            )
                            .replace(
                                "{{inputs}}", str(inputs.get("request", objective))
                            )
                        )
                        judge_reply = reflection._sync_llm_completion(
                            loop,
                            messages=[
                                {
                                    "role": "user",
                                    "content": (
                                        f"{rendered}\n\nEnd your reply with the "
                                        "numeric grade 0-10 alone on the LAST line."
                                    ),
                                }
                            ],
                            model=_stored_judge_model_to_key(
                                getattr(judge, "model", None)
                            )
                            or judge_model,
                            max_tokens=1500,
                            group_context=group_context,
                            user_token=user_token,
                        )
                        grade = _parse_grade_from_text(judge_reply)
                        if grade is None:
                            logger.warning(
                                f"Registered judge '{judge_name}' reply not "
                                f"numeric; skipping: {judge_reply!r:.200}"
                            )
                        else:
                            grades.append(grade)
                            if judge_reply and judge_reply.strip():
                                rationale_parts.append(
                                    f"[{judge_name}] {judge_reply.strip()}"
                                )
                    except Exception as judge_err:
                        logger.warning(
                            f"Registered judge '{judge_name}' failed: {judge_err}"
                        )
                grade_value = sum(grades) / len(grades) if grades else 0.0
                rationale = "\n".join(rationale_parts)[:4000]
                judge_cache[text_key] = (grade_value, rationale)
                return Feedback(
                    name="output_correct", value=grade_value, rationale=rationale
                )

            def aggregation(scores: Dict[str, Any]) -> float:
                # Registered judges are already averaged INSIDE output_correct.
                # Scores arrive RAW: a scorer that returned a Feedback shows up
                # here as the Feedback object, not its numeric value.
                def _num(value: Any) -> float:
                    value = getattr(value, "value", value)
                    try:
                        return float(value or 0.0)
                    except (TypeError, ValueError):
                        return 0.0

                fmt = _num(scores.get("output_format"))
                correct = _num(scores.get("output_correct"))
                return 0.3 * fmt + 0.7 * correct

            reflection_fn = _make_reflection_fn(
                loop, reflection_model, group_context, user_token
            )
            _install_gepa_reflection_bridge()
            _GEPA_REFLECTION_STATE.reflection_fn = reflection_fn
            result = optimize_prompts(
                predict_fn=predict_fn,
                train_data=[
                    {
                        "inputs": {"request": objective_for_training},
                        "expectations": train_expectations,
                    }
                ],
                prompt_uris=[prompt_uri],
                optimizer=GepaPromptOptimizer(
                    # Inert placeholder — the bridge swaps in the LLMManager
                    # -backed callable; this string is parsed, never called.
                    reflection_model="openai:/kasal-llm-manager",
                    # METRIC calls are decoupled from crew EXECUTIONS: with
                    # the caches (ours + gepa's) most metric calls are free
                    # re-scores, so the user's number stays a hard cap on real
                    # executions while GEPA gets iteration headroom. Observed
                    # live without this: a 10-execution budget stopped after 4
                    # executions because cached re-evaluations had consumed
                    # the metric budget.
                    max_metric_calls=max_metric_calls * 2 + 3,
                    gepa_kwargs={
                        # Default minibatch of 3 sampled our SINGLE training
                        # example three times per step — every candidate cost
                        # 3 crew executions racing the cache (two finished the
                        # same second, observed live).
                        "reflection_minibatch_size": 1,
                        # Strict improvement rejected TIES: a candidate that
                        # fully incorporated the human requirements scored
                        # 0.9 vs 0.9 on the minibatch and was discarded
                        # (proposals.json, observed live). Lateral moves must
                        # survive so the search can leave a flat region.
                        "acceptance_criterion": "improvement_or_equal",
                        # gepa-side (candidate, example) result cache: skips
                        # the metric call entirely on repeats, preserving the
                        # metric budget for NEW candidates.
                        "cache_evaluation": True,
                        # gepa's default template says "write a new
                        # instruction ... within ``` blocks" — an open
                        # invitation to restructure: the reflection model
                        # returned {"instruction": "..."} JSON blobs that
                        # lost the [AGENT]/[TASK] document structure and
                        # free-rejected every proposal (observed live, 11/11
                        # malformed). Pin the output contract to the crew-doc
                        # format instead.
                        "reflection_prompt_template": (
                            "I provided an assistant with the following "
                            "DOCUMENT of prompt fields that configures an AI "
                            "crew (agents and tasks):\n"
                            "```\n<curr_param>\n```\n\n"
                            "The following are examples of task inputs, the "
                            "crew's final answer, and feedback (score and "
                            "judge rationale) on how the answer could be "
                            "better:\n"
                            "```\n<side_info>\n```\n\n"
                            "Your task is to write an IMPROVED VERSION of the "
                            "document above so that a future answer satisfies "
                            "the feedback and every hard requirement stated "
                            "in the task input.\n\n"
                            "STRICT FORMAT RULES:\n"
                            "- Keep EXACTLY the same structure: the same "
                            "[AGENT <id>] and [TASK <id>] section headers "
                            "with the same ids, and the same field labels "
                            "(ROLE:, GOAL:, BACKSTORY:, DESCRIPTION:, "
                            "EXPECTED_OUTPUT:).\n"
                            "- Each field label starts its line, followed by "
                            "the improved text for that field on the same "
                            "line.\n"
                            "- Do NOT output JSON, commentary, or anything "
                            "except the document.\n"
                            "- Output ONLY the improved document, nothing "
                            "before or after it. Start your reply directly "
                            "with the first [AGENT line."
                        ),
                    },
                ),
                scorers=[output_format, output_correct],
                aggregation=aggregation,
                enable_tracking=local_mode,
            )
        finally:
            _GEPA_REFLECTION_STATE.reflection_fn = None
            if local_mode:
                mlflow.set_tracking_uri(prev_tracking)
            for k, old in saved_exp_env.items():
                if old is not None:
                    os.environ[k] = old
            for _flavor, old_flag in saved_autolog_flags.items():
                if old_flag is None:
                    AUTOLOGGING_INTEGRATIONS.get(_flavor, {}).pop("disable", None)
                else:
                    AUTOLOGGING_INTEGRATIONS[_flavor]["disable"] = old_flag

        optimized = result.optimized_prompts[0]
        optimized_fields = _parse_crew_doc(optimized.template) or {}
        return {
            "optimized_template": optimized.template,
            "optimized_fields": optimized_fields,
            "initial_score": _to_float(getattr(result, "initial_eval_score", None)),
            "final_score": _to_float(getattr(result, "final_eval_score", None)),
            "prompt_uri": getattr(optimized, "uri", None),
        }
