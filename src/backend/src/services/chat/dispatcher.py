"""
Service for dispatching natural language requests to appropriate generation services.

This module provides business logic for analyzing user messages and determining
whether they want to generate an agent, task, or crew, then calling the appropriate service.
"""

import asyncio
import hashlib
import logging
import os
import re
import time
from contextlib import nullcontext
from typing import Any, Dict, List, Optional, Set, Tuple

try:
    import mlflow as _mlflow

    _HAS_MLFLOW = True
except ImportError:
    _mlflow = None  # type: ignore[assignment]
    _HAS_MLFLOW = False


def _set_mlflow_tracing(enabled: bool) -> None:
    """Hard-toggle MLflow tracing to match the setup outcome.

    Importing mlflow (litellm/crewai integrations) can leave a trace exporter
    armed even when our setup decides tracing is off — every dispatcher LLM
    call then attempts a doomed export and logs
    'INVALID_PARAMETER_VALUE: experiment_id is missing'. Disable explicitly
    when setup is skipped/fails, re-enable on a successful setup.
    """
    if not _HAS_MLFLOW:
        return
    try:
        if enabled:
            _mlflow.tracing.enable()
        else:
            _mlflow.tracing.disable()
    except Exception:
        pass


from src.core.cache import intent_cache
from src.schemas.crew import (
    CrewGenerationRequest,
    CrewGenerationResponse,
    CrewStreamingRequest,
)
from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse

# The templates these prompts come from live in the database and are optimizable
# by GEPA; these constants are the rows' own source, used only when the row is
# missing. Imported rather than copied — see the fallbacks below.
from src.seeds.prompt_templates import DETECT_INTENT_TEMPLATE
from src.services.catalog.crews import CrewService
from src.services.catalog.templates import TemplateService
from src.services.chat.capability_dispatch import route_and_dispatch
from src.services.chat.slash_commands import detect_slash_command
from src.services.databricks.workspace.service import DatabricksService
from src.services.execution.logs.llm_log_service import LLMLogService
from src.services.flow_builder.flow_service import FlowService
from src.services.generation.agents import AgentGenerationService
from src.services.generation.crews import CrewGenerationService
from src.services.generation.tasks import TaskGenerationService
from src.services.llm.manager import LLMManager
from src.services.mlflow.service import MLflowService
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)

# Fast-model fallback chain for intent detection. Intent is a 6-way
# classification emitting fixed JSON — it wants small, fast, reliable instruct
# models, not a reasoning model. detect_intent tries the caller's preferred model
# first (the model picked in chat, else this chain's first entry), then walks the
# rest so a single gated or erroring endpoint can't drop intent to the dumb
# semantic fallback. Spread across providers (Anthropic / OpenAI / Google) to
# avoid a correlated outage. Override via env (comma-separated), e.g.
#   DISPATCHER_FALLBACK_MODELS="databricks-claude-haiku-4-5,databricks-gpt-5-nano"
DEFAULT_DISPATCHER_FALLBACK_MODELS = [
    "databricks-claude-haiku-4-5",
    "databricks-gpt-5-nano",
    "databricks-gemini-3-5-flash",
]
DISPATCHER_FALLBACK_MODELS = [
    m.strip()
    for m in os.getenv(
        "DISPATCHER_FALLBACK_MODELS", ",".join(DEFAULT_DISPATCHER_FALLBACK_MODELS)
    ).split(",")
    if m.strip()
]
# First chain entry doubles as the default when no model is selected in chat.
DEFAULT_DISPATCHER_MODEL = os.getenv(
    "DEFAULT_DISPATCHER_MODEL",
    (
        DISPATCHER_FALLBACK_MODELS[0]
        if DISPATCHER_FALLBACK_MODELS
        else "databricks-claude-haiku-4-5"
    ),
)


class DispatcherService:
    """Service for dispatching natural language requests to generation services."""

    # --- Confidence & scoring constants ---
    SEMANTIC_CONFIDENCE_NORMALIZER = 10.0
    SEMANTIC_FALLBACK_MIN_CONFIDENCE = 0.3
    SEMANTIC_OVERRIDE_THRESHOLD = 0.7
    LLM_CONFIDENCE_WEAK_THRESHOLD = 0.85
    DEFAULT_FALLBACK_CONFIDENCE = 0.5

    # --- Crew-first scoring constants ---
    CREW_BASE_SCORE = 6  # Crew is the default intent
    MULTI_STEP_BONUS = 4  # Bonus when multi-step workflow detected

    # --- Retry / timeout constants ---
    LLM_MAX_RETRIES = 3
    LLM_INITIAL_BACKOFF = 1.0  # exponential: 1s, 2s, 4s
    LLM_REQUEST_TIMEOUT = 30.0  # per-attempt timeout in seconds
    RETRYABLE_ERROR_TERMS: frozenset = frozenset(
        {
            "timeout",
            "connection",
            "rate limit",
            "ratelimit",
            "too many requests",
            "service unavailable",
            "503",
            "429",
            "502",
            "504",
            "gateway",
            "request_limit_exceeded",
        }
    )

    # --- Circuit breaker state (class-level, shared across instances) ---
    _intent_failures: Dict[str, Dict[str, Any]] = {}
    _failure_threshold = 5
    _circuit_reset_time = (
        60  # seconds (shorter than embedding's 300s; intent is interactive)
    )

    # --- Concurrency control ---
    _concurrency_semaphore: Optional[asyncio.Semaphore] = None
    _max_concurrent_detections = 10

    # General action verbs — used for multi-step and imperative detection.
    # These boost the crew score (not task score). Kept to core verbs only;
    # words that overlap with EXECUTE_KEYWORDS or CONFIGURE_KEYWORDS are
    # excluded to avoid false signals.
    TASK_ACTION_WORDS = {
        "find",
        "search",
        "locate",
        "discover",
        "identify",
        "get",
        "fetch",
        "retrieve",
        "collect",
        "gather",
        "analyze",
        "examine",
        "study",
        "investigate",
        "review",
        "assess",
        "evaluate",
        "compare",
        "contrast",
        "create",
        "make",
        "build",
        "generate",
        "produce",
        "develop",
        "write",
        "compose",
        "draft",
        "prepare",
        "document",
        "calculate",
        "compute",
        "determine",
        "measure",
        "summarize",
        "condense",
        "extract",
        "compile",
        "organize",
        "sort",
        "categorize",
        "classify",
        "check",
        "verify",
        "validate",
        "test",
        "inspect",
        "audit",
        "monitor",
        "track",
        "send",
        "deliver",
        "share",
        "distribute",
        "convert",
        "transform",
        "translate",
        "format",
        "parse",
    }

    # Agent-related keywords — ONLY explicit agent entity words
    # Role descriptors (expert, analyst, etc.) are NOT here; they indicate
    # specialisation which is better served by crew generation.
    AGENT_KEYWORDS = {
        "agent",
        "assistant",
        "bot",
        "robot",
        "chatbot",
    }

    # detect_intent sources where NO LLM produced the answer — the result came
    # from semantic analysis or the deterministic guardrail. Logging these as a
    # "success" against a model is a lie about what ran.
    DEGRADED_INTENT_SOURCES = frozenset(
        {"semantic_fallback", "circuit_breaker_fallback", "explicit_override"}
    )

    # Patterns that indicate explicit single-agent creation intent
    AGENT_CREATION_PATTERNS = [
        r"\b(create|make|build|generate|develop)\b.*\b(an?\s+)?(agent|bot|assistant|chatbot)\b",
        r"\b(i need|give me|set up)\b.*\b(an?\s+)?(agent|bot|assistant|chatbot)\b",
    ]

    # Signals that a "create ... agent" message asks for MORE THAN ONE agent, so
    # the single-agent guardrail must NOT fire. AGENT_CREATION_PATTERNS use a
    # greedy `.*`, so "create 4 agents ... the other agent ..." matched on the
    # later SINGULAR occurrence and force-routed a five-topic crew into the
    # single-agent generator (it produced one "Swiss Sports News Reporter").
    MULTI_AGENT_PATTERNS = [
        r"\bagents\b",  # plural entity
        r"\b(crew|team|squad|panel)\b",  # collective noun
        # explicit count before the entity, digit or spelled out, with a bounded
        # gap so "4 specialized agents" counts but a distant number does not
        r"\b(\d+|two|three|four|five|six|seven|eight|nine|ten)\s+"
        r"(?:\w+\s+){0,3}(agents?|bots?|assistants?)\b",
        # enumeration of distinct roles: "one agent does X, another agent does Y"
        r"\b(another|other|each)\s+(agent|bot|assistant)\b",
    ]

    # Patterns that indicate explicit single-task creation intent.
    # "task force"/"task list" are excluded — they connote broader work.
    TASK_CREATION_PATTERNS = [
        r"\b(create|add|make|generate|set up)\b\s+(a\s+|an\s+|the\s+|another\s+|new\s+)*task\b(?!\s+(force|list|board))",
        r"\b(i need|give me)\b\s+(a\s+|an\s+|the\s+|another\s+|new\s+)*task\b(?!\s+(force|list|board))",
    ]

    # Multi-step workflow indicators — boost crew score
    MULTI_STEP_PATTERNS = [
        r"\bthen\b",  # "research then write then present"
        r",\s*[a-z]+\s+(and|then)\b",  # comma-separated action chain
        r"\band\b.*\b(create|write|build|make|generate|analyze|review|produce|prepare)\b",
        r"\bstep\s*\d+\b",  # "step 1, step 2"
        r"\bfirst\b.*\bthen\b",  # "first X then Y"
        r"\b(after|before|once|finally)\b",  # sequential indicators
    ]

    # Crew-related keywords (includes plan/strategy terms since they're functionally the same)
    CREW_KEYWORDS = {
        "team",
        "crew",
        "group",
        "squad",
        "multiple",
        "several",
        "many",
        "workflow",
        "pipeline",
        "process",
        "collaboration",
        "together",
        "plan",
        "planning",
        "strategy",
        "roadmap",
        "blueprint",
        "scheme",
        "approach",
        "design",
        "outline",
        "proposal",
        "framework",
        "architecture",
    }

    # Execution-related keywords
    EXECUTE_KEYWORDS = {
        "execute",
        "run",
        "start",
        "launch",
        "begin",
        "proceed",
        "go",
        "ec",
    }

    # Configuration-related keywords
    CONFIGURE_KEYWORDS = {
        "configure",
        "config",
        "setup",
        "set",
        "change",
        "update",
        "modify",
        "settings",
        "preferences",
        "options",
        "parameters",
        "llm",
        "model",
        "maxr",
        "max",
        "rpm",
        "rate",
        "limit",
        "tools",
        "tool",
        "select",
        "choose",
        "pick",
        "adjust",
        "tune",
        "customize",
        "personalize",
    }

    # Catalog management keywords (for natural language fallback)
    CATALOG_KEYWORDS = {
        "catalog",
        "list",
        "browse",
        "show",
        "view",
        "plans",
        "saved",
        "library",
        "templates",
        "load",
        "open",
        "restore",
        "import",
        "save",
        "store",
        "persist",
        "export",
        "schedule",
        "cron",
        "recurring",
        "automate",
        "timer",
    }

    def __init__(
        self, log_service: LLMLogService, template_service: TemplateService, session
    ):
        """
        Initialize the service.

        Args:
            log_service: Service for logging LLM interactions
            template_service: Service for template management
            session: Database session for generation services
        """
        self.log_service = log_service
        self.template_service = template_service
        self.session = session
        self.agent_service = AgentGenerationService(session)
        self.task_service = TaskGenerationService(session)
        self.crew_service = CrewGenerationService(session)
        self.catalog_service = CrewService(session)
        self.flow_service = FlowService(session)

    @classmethod
    def create(cls, session) -> "DispatcherService":
        """
        Factory method to create a properly configured instance of the service.

        Args:
            session: Database session for repository operations

        Returns:
            An instance of DispatcherService with all required dependencies
        """
        log_service = LLMLogService.create(session)
        template_service = TemplateService(session)
        return cls(
            log_service=log_service, template_service=template_service, session=session
        )

    async def _log_llm_interaction(
        self,
        endpoint: str,
        prompt: str,
        response: str,
        model: str,
        status: str = "success",
        error_message: Optional[str] = None,
        group_context: Optional[GroupContext] = None,
    ):
        """
        Log LLM interaction using the log service.

        Args:
            endpoint: API endpoint name
            prompt: Input prompt
            response: Model response
            model: LLM model used
            status: Status of the interaction (success/error)
            error_message: Optional error message
            group_context: Optional group context for multi-group isolation
        """
        try:
            await self.log_service.create_log(
                endpoint=endpoint,
                prompt=prompt,
                response=response,
                model=model,
                status=status,
                error_message=error_message,
                group_context=group_context,
            )
            logger.info(f"Logged {endpoint} interaction to database")
        except Exception as e:
            logger.error(f"Failed to log LLM interaction: {str(e)}")

    async def _call_llm_with_retry(
        self,
        messages: list,
        model: str,
        temperature: float = 0.3,
        max_tokens: int = 4000,
        extra_headers: Optional[dict] = None,
    ) -> str:
        """Call LLMManager.completion with retry, timeout, and exponential backoff.

        Args:
            messages: Chat messages list
            model: LLM model identifier
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response
            extra_headers: Optional extra HTTP headers (e.g. User-Agent for telemetry)

        Returns:
            ``(content, served_model)`` — the response text and the model key
            that ACTUALLY answered. The two differ whenever `model` was
            substituted (a Databricks model on a deployment with no workspace),
            and the llmlog row has to name the one that ran.

        Raises:
            Last encountered exception after all retries are exhausted
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                completion_kwargs = dict(
                    messages=messages,
                    model=model,
                    temperature=temperature,
                    max_tokens=max_tokens,
                    with_served_model=True,
                )
                if extra_headers:
                    completion_kwargs["extra_headers"] = extra_headers
                out = await asyncio.wait_for(
                    LLMManager.completion(**completion_kwargs),
                    timeout=self.LLM_REQUEST_TIMEOUT,
                )
                # completion() returns (content, served_model) for
                # with_served_model=True. Tolerate a bare string from any
                # wrapper or stub that ignores the flag — an unknown served
                # model is a worse log label, not a failed classification.
                if isinstance(out, tuple):
                    return out[0], out[1]
                return out, None
            except Exception as e:
                last_error = e
                error_str = str(e).lower()
                is_retryable = any(
                    term in error_str for term in self.RETRYABLE_ERROR_TERMS
                )

                if not is_retryable:
                    logger.warning(
                        f"Non-retryable LLM error (attempt {attempt + 1}): {e}"
                    )
                    raise

                backoff = self.LLM_INITIAL_BACKOFF * (2**attempt)
                logger.warning(
                    f"Retryable LLM error (attempt {attempt + 1}/{self.LLM_MAX_RETRIES}), "
                    f"retrying in {backoff}s: {e}"
                )
                if attempt < self.LLM_MAX_RETRIES - 1:
                    await asyncio.sleep(backoff)

        raise last_error  # type: ignore[misc]

    # --- Circuit breaker helpers ---

    @classmethod
    def _check_circuit_breaker(cls, model: str) -> bool:
        """Return True if the circuit is open (should fail fast)."""
        if model not in cls._intent_failures:
            return False
        info = cls._intent_failures[model]
        if info.get("count", 0) >= cls._failure_threshold:
            if time.time() - info.get("last_failure", 0) < cls._circuit_reset_time:
                logger.warning(
                    f"Circuit breaker OPEN for intent detection model {model}. Failing fast."
                )
                return True
            # Reset after timeout
            logger.info(f"Resetting circuit breaker for intent detection model {model}")
            cls._intent_failures[model] = {"count": 0, "last_failure": 0}
        return False

    @classmethod
    def _record_failure(cls, model: str) -> None:
        """Record a failure for the given model."""
        if model not in cls._intent_failures:
            cls._intent_failures[model] = {"count": 0, "last_failure": 0}
        cls._intent_failures[model]["count"] += 1
        cls._intent_failures[model]["last_failure"] = time.time()
        count = cls._intent_failures[model]["count"]
        if count >= cls._failure_threshold:
            logger.error(
                f"Circuit breaker tripped for intent detection model {model} "
                f"after {count} failures"
            )

    @classmethod
    def _record_success(cls, model: str) -> None:
        """Reset failure counter on success."""
        if model in cls._intent_failures:
            cls._intent_failures[model] = {"count": 0, "last_failure": 0}

    # --- Concurrency helpers ---

    @classmethod
    def _get_semaphore(cls) -> asyncio.Semaphore:
        """Lazy-init the concurrency semaphore."""
        if cls._concurrency_semaphore is None:
            cls._concurrency_semaphore = asyncio.Semaphore(
                cls._max_concurrent_detections
            )
        return cls._concurrency_semaphore

    async def _walk_model_chain(
        self,
        messages: list,
        model: Optional[str],
        last_resort_model: Optional[str] = None,
        span_name: str = "intent_detection",
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str], int]:
        """Ask the model chain for JSON, returning the first usable object.

        Walks the candidates preferred-first: skips a model whose circuit breaker
        is open, and on error or empty/unparseable output moves to the next. Only
        when no candidate yields a usable result does the caller drop to its own
        fallback.

        Extracted so intent classification and capability routing share ONE
        implementation of it. They need the same retry, the same breaker
        bookkeeping, the same semaphore and the same tracing, and the way those
        stop being the same is a second copy of this loop.

        Returns ``(parsed, served_model, attempted)``. ``attempted`` counts models
        actually called — breaker-open skips do not count, which is what lets the
        caller distinguish "every breaker was open" (no LLM ran at all) from "all
        attempts failed".
        """
        from src.utils.telemetry import KasalProduct, get_user_agent_header

        extra_headers = get_user_agent_header(KasalProduct.INTENT_DETECTION)

        candidates = self._intent_model_candidates(model, last_resort_model)
        attempted = 0

        for candidate in candidates:
            if self._check_circuit_breaker(candidate):
                logger.info(f"Skipping intent model {candidate} (circuit breaker open)")
                continue
            attempted += 1
            # Reset per attempt so a previous candidate's resolved name can
            # never be attributed to this one.
            served: Optional[str] = None
            try:
                # Acquire concurrency semaphore to limit parallel LLM calls
                async with self._get_semaphore():
                    # Generate completion with optional MLflow span for tracing
                    if _HAS_MLFLOW and hasattr(_mlflow, "start_span"):
                        with _mlflow.start_span(
                            name=span_name, span_type="LLM"
                        ) as intent_span:
                            if hasattr(intent_span, "set_inputs"):
                                intent_span.set_inputs(
                                    {
                                        "model": candidate,
                                        "messages": messages,
                                        "temperature": 0.3,
                                    }
                                )
                            content, served = await self._call_llm_with_retry(
                                messages=messages,
                                model=candidate,
                                extra_headers=extra_headers,
                            )
                            if hasattr(intent_span, "set_outputs"):
                                intent_span.set_outputs(
                                    {"response": content[:500] if content else ""}
                                )
                    else:
                        content, served = await self._call_llm_with_retry(
                            messages=messages,
                            model=candidate,
                            extra_headers=extra_headers,
                        )
            except Exception as e:
                logger.warning(
                    f"Intent model {candidate} failed: {e}. Trying next fallback."
                )
                self._record_failure(candidate)
                continue

            if not content or not content.strip():
                logger.warning(
                    f"Intent model {candidate} returned an empty response. "
                    "Trying next fallback."
                )
                continue

            try:
                parsed = robust_json_parser(content)
            except Exception as e:
                logger.warning(
                    f"Intent model {candidate} returned unparseable output: {e}. "
                    "Trying next fallback."
                )
                continue
            if not isinstance(parsed, dict):
                logger.warning(
                    f"Intent model {candidate} did not return a JSON object. "
                    "Trying next fallback."
                )
                continue

            # Usable result — record success and stop walking the chain.
            self._record_success(candidate)
            # `served` is the key that actually answered; it differs from the
            # candidate whenever the model was substituted downstream.
            return parsed, served or candidate, attempted

        return None, None, attempted

    @classmethod
    def _intent_model_candidates(
        cls, preferred: Optional[str], last_resort: Optional[str] = None
    ) -> List[str]:
        """Ordered intent models: the preferred model first, then the fast
        fallback chain, then an optional last-resort model appended at the very
        end — deduped, order preserved. detect_intent walks this list so a single
        gated/erroring endpoint can't drop intent to the deterministic semantic
        fallback. The last-resort slot lets the dispatcher run intent on the fast
        chain by default while still falling back to the user's selected crew
        model on a workspace where none of the fast models are enabled.
        """
        ordered: List[str] = []
        for m in [preferred, *DISPATCHER_FALLBACK_MODELS, last_resort]:
            if m and m not in ordered:
                ordered.append(m)
        return ordered

    async def _maybe_enable_mlflow_tracing(
        self, group_context: Optional[GroupContext]
    ) -> bool:
        """Enable MLflow tracing for dispatcher intent.

        Delegates to the shared parent-process setup so dispatcher intent traces
        land in the SAME Unity Catalog experiment as crew execution and the
        crew/agent/task generation traces (``<base>-uc`` -> ``kasal_otel_*``).
        """
        from src.services.otel_tracing.mlflow_parent_setup import (
            configure_parent_mlflow_tracing,
        )

        return await configure_parent_mlflow_tracing(
            self.session, group_context, label="Dispatcher"
        )

    #: Slash-command parsing lives in ``slash_commands`` — it is a parser, not
    #: dispatch logic, and it took up more of this file than the classifier does.
    #: Still bound here because it is the name every caller and test uses.
    _detect_slash_command = staticmethod(detect_slash_command)

    def _analyze_message_semantics(self, message: str) -> Dict[str, Any]:
        """
        Perform semantic analysis on the message to extract intent hints.

        Uses a **crew-first** approach: generate_crew is the default intent
        and other intents must earn their score through explicit signals.

        Args:
            message: User's natural language message

        Returns:
            Dictionary containing semantic analysis results
        """
        # Extract factual observations from the message for the LLM.
        # NO scoring, NO intent suggestion — the LLM decides.
        msg_lower = message.lower()
        words = re.findall(r"\b\w+\b", msg_lower)
        word_set = set(words)

        # Extract keyword groups (factual, no scoring)
        task_actions = word_set.intersection(self.TASK_ACTION_WORDS)
        agent_keywords = word_set.intersection(self.AGENT_KEYWORDS)
        crew_keywords = word_set.intersection(self.CREW_KEYWORDS)
        execute_keywords = word_set.intersection(self.EXECUTE_KEYWORDS)
        configure_keywords = word_set.intersection(self.CONFIGURE_KEYWORDS)
        catalog_keywords = word_set.intersection(self.CATALOG_KEYWORDS)

        has_multiple_actions = len(task_actions) > 1

        return {
            "task_actions": list(task_actions),
            "agent_keywords": list(agent_keywords),
            "crew_keywords": list(crew_keywords),
            "execute_keywords": list(execute_keywords),
            "configure_keywords": list(configure_keywords),
            "catalog_keywords": list(catalog_keywords),
            "has_multi_step": has_multiple_actions,
            "has_explicit_agent": bool(agent_keywords),
            "has_explicit_task": "task" in word_set,
            "has_configure_structure": bool(configure_keywords),
            "semantic_hints": [],
            "suggested_intent": "generate_crew",
            "intent_scores": {"generate_crew": 1},
        }

    @staticmethod
    def _build_tool_catalog(available_tools: List[Dict[str, str]]) -> str:
        """Format available tools into a prompt section for the LLM.

        Args:
            available_tools: List of dicts with 'title' and 'description' keys.

        Returns:
            A string to append to the user message with tool catalog and instructions.
        """
        # Intent classification only needs to PICK tool names from a list —
        # full descriptions added ~3.5k prompt tokens to EVERY chat message
        # (downstream crew/task generation receives the full tool details
        # anyway). Keep a short hint per tool for disambiguation.
        max_desc = 100

        def _short(desc: str) -> str:
            desc = (desc or "").strip()
            return desc if len(desc) <= max_desc else desc[: max_desc - 1] + "…"

        lines = [
            f"- {t['title']}: {_short(t.get('description', ''))}"
            for t in available_tools
        ]
        return (
            "\n\nAvailable tools in the workspace:\n"
            + "\n".join(lines)
            + "\n\nBased on the user's request, include a 'suggested_tools' field in your JSON response "
            "containing a list of tool names (from the list above) that would be useful for this task. "
            "Only suggest tools that are directly relevant. Return an empty list if no tools apply."
        )

    def _explicit_creation_intent(self, message: str) -> Optional[str]:
        """Deterministic intent for UNAMBIGUOUS single-entity creation requests.

        "create a task" / "add a task"   -> generate_task
        "create an agent" / "make a bot" -> generate_agent

        Returns None for multi-step messages (those stay crew-first, even if the
        word "task"/"agent" appears) and for everything else (the LLM decides).
        This is a guardrail so a weak intent model can't misroute an explicit
        "create a task" into a crew plan — the chat default is heavily crew-biased,
        which small models over-apply.

        It is a SINGLE-entity guardrail only. A message asking for several agents
        ("create 4 agents…", "a crew with four specialized agents") must fall
        through to the LLM/crew default — forcing generate_agent there discards
        every agent but one.
        """
        msg = message.lower()
        # Multi-step workflows are crews even if "task"/"agent" appears.
        if any(re.search(p, msg) for p in self.MULTI_STEP_PATTERNS):
            return None
        if any(re.search(p, msg) for p in self.TASK_CREATION_PATTERNS):
            return "generate_task"
        if any(re.search(p, msg) for p in self.AGENT_CREATION_PATTERNS):
            # Plural / counted / enumerated agents are a crew, not one agent.
            if any(re.search(p, msg) for p in self.MULTI_AGENT_PATTERNS):
                return None
            return "generate_agent"
        return None

    def _resolve_surface_intent(
        self, message: str, current_intent: Optional[str], chat_mode: bool
    ) -> tuple[str, Optional[str]]:
        """Apply the per-surface intent rule. Returns (intent, override_reason).

        - ChatMode (chat_mode=True): single-entity creation is NOT available here —
          task/agent intents collapse to generate_crew. (Commands like
          execute/configure/catalog are left untouched.) ChatMode always builds a crew.
        - AgentBuilder / crew canvas (chat_mode=False): an explicit "create a task"
          / "create an agent" deterministically routes to that generator, even when
          a weak intent model defaulted elsewhere.
        """
        if chat_mode:
            if current_intent in ("generate_task", "generate_agent"):
                return "generate_crew", f"chat_mode forces crew (was {current_intent})"
            return current_intent or "generate_crew", None
        forced = self._explicit_creation_intent(message)
        if forced and forced != current_intent:
            return forced, f"explicit creation (was {current_intent})"
        return current_intent or "generate_crew", None

    @staticmethod
    def _resolve_effective_tools(
        requested: Optional[List[str]], enabled_titles
    ) -> List[str]:
        """Restrict the tools available to generation to the workspace's ENABLED set.

        - requested (client preference): keep only the ones actually enabled, so a
          stale/over-broad selection can't smuggle in a non-enabled tool. If the
          enabled set couldn't be resolved (empty, e.g. a transient error), fall
          back to the requested list rather than stripping everything.
        - no request: offer all enabled tools EXCEPT DatabricksKnowledgeSearchTool,
          which reads chat-uploaded docs — only meaningful with attachments (the
          frontend signals that by adding it to request.tools).
        """
        if requested:
            if not enabled_titles:
                return list(requested)
            return [t for t in requested if t in enabled_titles]
        return [t for t in enabled_titles if t != "DatabricksKnowledgeSearchTool"]

    async def detect_intent(
        self,
        message: str,
        model: str,
        group_context: Optional[GroupContext] = None,
        available_tools: Optional[List[Dict[str, str]]] = None,
        chat_mode: bool = False,
        last_resort_model: Optional[str] = None,
        prefer_existing: bool = False,
    ) -> Dict[str, Any]:
        """
        Detect the intent from the user's message using LLM enhanced with semantic analysis.

        Args:
            message: User's natural language message.
            model: LLM model to use (preferred; tried first in the fallback chain).
            last_resort_model: Optional model appended after the fast fallback
                chain — used only if every faster candidate is unavailable (e.g.
                the user's selected crew model, so intent never hard-fails on a
                workspace where the fast intent models aren't enabled).
            prefer_existing: True when the user picked "Use existing". Opens the
                ChatMode fast path so the capability router can run — the ONLY
                thing that does.

        Returns:
            Dictionary containing intent, confidence, and extracted information
        """
        # Check for slash commands first (instant, no LLM needed)
        slash_result = self._detect_slash_command(message)
        if slash_result is not None:
            return slash_result

        # "Use existing": the user asked to run something already published, so
        # classification is settled — the only question left is WHICH capability,
        # and dispatch() answers that with the route catalog in hand.
        if prefer_existing:
            return {
                "intent": IntentType.CATALOG_ROUTE.value,
                "confidence": 1.0,
                "extracted_info": {},
                "suggested_prompt": message,
                "source": "prefer_existing",
                "suggested_tools": [],
            }

        # ChatMode fast-path: this surface ONLY builds crews. generate_agent /
        # generate_task already collapse to generate_crew here (see
        # _resolve_surface_intent), and catalog/execute are reached via slash
        # commands (handled above) or intercepted client-side — so the intent
        # LLM always lands on generate_crew anyway. Skip the classification
        # round-trip entirely and route straight there, saving a full fast-model
        # call on every chat message.
        #
        # Gated on prefer_existing above, not removed: chat / research / deep
        # keep the per-message saving they have today.
        if chat_mode:
            return {
                "intent": "generate_crew",
                "confidence": 1.0,
                "extracted_info": {
                    "semantic_analysis": self._analyze_message_semantics(message)
                },
                "suggested_prompt": message,
                "source": "chat_mode_fast_path",
                "suggested_tools": [],
            }

        # Perform semantic analysis first
        semantic_analysis = self._analyze_message_semantics(message)
        # Get prompt template from database
        system_prompt = await self.template_service.get_template_content(
            "detect_intent"
        )

        if not system_prompt:
            # No DB row — a fresh workspace, or a seed that has not run yet.
            # Fall back to the SEED the row is created from, not to a copy: this
            # used to be sixty lines of prose inlined here, which had already
            # drifted from the seeded template it was supposed to mirror. A
            # prompt with two sources has one that is quietly wrong, and it is
            # always the one you are not reading.
            system_prompt = DETECT_INTENT_TEMPLATE

        # Send the raw message to the LLM — let it do all the analysis
        enhanced_user_message = f"""Message: {message}

Please analyze this message and provide your intent classification."""

        # Append tool catalog so the LLM can suggest relevant tools
        if available_tools:
            enhanced_user_message += self._build_tool_catalog(available_tools)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": enhanced_user_message},
        ]

        # Cache check — return cached result if available (group-scoped)
        group_id = (
            getattr(group_context, "primary_group_id", None) if group_context else None
        ) or "__default__"
        tools_hash = (
            hashlib.md5(
                ",".join(sorted(t["title"] for t in available_tools)).encode()
            ).hexdigest()[:8]
            if available_tools
            else ""
        )
        cache_key = hashlib.md5(
            f"{message.strip().lower()}:{model}:{tools_hash}".encode()
        ).hexdigest()
        cached = await intent_cache.get(group_id, cache_key)
        if cached is not None:
            logger.info(f"Intent cache hit for model {model}")
            cached["source"] = "cache"
            # Apply the per-surface rule to cached results too, so a result cached
            # before this guardrail existed (or from a weak model) can't keep
            # misrouting — and ChatMode hits still collapse task/agent to crew.
            new_intent, reason = self._resolve_surface_intent(
                message, cached.get("intent"), chat_mode
            )
            if reason:
                logger.info(f"Intent override (cache): {reason} -> '{new_intent}'.")
                cached["intent"] = new_intent
                cached["confidence"] = max(float(cached.get("confidence") or 0), 0.95)
                cached["source"] = "cache+surface_override"
            return cached

        result, used_model, attempted = await self._walk_model_chain(
            messages, model, last_resort_model
        )

        if result is None:
            # No candidate produced a usable result. Distinguish "every circuit
            # breaker was open" (no LLM was called) from "all attempts failed".
            forced = None if chat_mode else self._explicit_creation_intent(message)
            if not attempted:
                logger.warning(
                    "All intent models have open circuit breakers; failing fast."
                )
                source = "circuit_breaker_fallback"
            elif forced:
                source = "explicit_override"
            else:
                source = "semantic_fallback"
            return {
                "intent": forced or "generate_crew",
                "confidence": 0.95 if forced else self.DEFAULT_FALLBACK_CONFIDENCE,
                "extracted_info": {"semantic_analysis": semantic_analysis},
                "suggested_prompt": message,
                "source": source,
                # No LLM produced this answer — carry that through so the log
                # can't attribute the result to a model that never responded.
                "model": None,
                "suggested_tools": [],
            }

        logger.info(f"Intent resolved by model {used_model}")
        # The model that ACTUALLY answered, which is rarely the first candidate:
        # the chain walks preferred -> fast fallbacks -> last resort.
        result["model"] = used_model

        # Validate the response
        if "intent" not in result:
            result["intent"] = semantic_analysis["suggested_intent"]
        if "confidence" not in result:
            result["confidence"] = 0.5
        else:
            # Clamp confidence to valid range [0.0, 1.0]
            # LLMs sometimes return values > 1.0 (e.g., 1.2 for 120%)
            try:
                confidence_value = float(result["confidence"])
                result["confidence"] = max(0.0, min(1.0, confidence_value))
                if confidence_value != result["confidence"]:
                    logger.warning(
                        f"Clamped confidence from {confidence_value} to {result['confidence']}"
                    )
            except (ValueError, TypeError) as e:
                logger.warning(
                    f"Invalid confidence value: {result['confidence']}, defaulting to 0.5"
                )
                result["confidence"] = 0.5
        if "extracted_info" not in result:
            result["extracted_info"] = {}
        if "suggested_prompt" not in result:
            result["suggested_prompt"] = message

        # Extract and validate suggested tools
        raw_tools = result.get("suggested_tools", [])
        if available_tools and isinstance(raw_tools, list):
            valid_titles = {t["title"] for t in available_tools}
            result["suggested_tools"] = [t for t in raw_tools if t in valid_titles]
        else:
            result["suggested_tools"] = []

        # Enhance extracted_info with semantic analysis (factual only)
        result["extracted_info"]["semantic_analysis"] = semantic_analysis

        # Per-surface guardrail (keeps the LLM's extracted_info/suggested_prompt):
        #  - ChatMode: task/agent intents collapse to generate_crew (entity
        #    creation is only for the AgentBuilder/crew canvas).
        #  - Canvas/builder: an explicit "create a task/agent" deterministically
        #    routes there even when a weak intent model defaulted elsewhere
        #    (guarded against multi-step messages, which stay crew-first).
        new_intent, reason = self._resolve_surface_intent(
            message, result.get("intent"), chat_mode
        )
        if reason:
            logger.info(
                f"Intent override: LLM returned '{result.get('intent')}', {reason} "
                f"-> forcing '{new_intent}'."
            )
            result["intent"] = new_intent
            result["confidence"] = max(float(result.get("confidence") or 0), 0.95)
            result["source"] = "llm+surface_override"
        else:
            result["source"] = "llm"

        # Cache successful LLM results (never cache fallback/degraded)
        await intent_cache.set(group_id, cache_key, result)

        return result

    async def _route_to_capability(
        self,
        message: str,
        group_context: Optional[GroupContext],
        model: Optional[str],
        last_resort_model: Optional[str] = None,
        session_id: Optional[str] = None,
        allow_continuation: bool = True,
    ) -> Dict[str, Any]:
        """Run one prompt against the chat-published catalog and dispatch the winner.

        A thin binding: the run itself lives in ``capability_dispatch`` so this
        file does not carry it. What it hands over is the two collaborators the
        route needs and cannot build for itself — the model chain (with its
        retry, circuit breakers and tracing) and the llmlog writer.
        """
        return await route_and_dispatch(
            session=self.session,
            group_context=group_context,
            message=message,
            ask_models=lambda messages: self._walk_model_chain(
                messages,
                model or DEFAULT_DISPATCHER_MODEL,
                last_resort_model,
                span_name="capability_routing",
            ),
            log_llm=self._log_llm_interaction,
            catalog_service=self.catalog_service,
            flow_service=self.flow_service,
            # The conversation this turn belongs to. Without it the router reads
            # every message as though nothing came before it.
            session_id=session_id,
            # False when the user broke out of a held conversation. Stickiness
            # the user cannot refuse is a trap, so the refusal has to reach here.
            allow_continuation=allow_continuation,
        )

    async def detect_intent_logged(
        self,
        message: str,
        model: str,
        group_context: Optional[GroupContext] = None,
        available_tools: Optional[List[Dict[str, str]]] = None,
        chat_mode: bool = False,
        last_resort_model: Optional[str] = None,
        prefer_existing: bool = False,
    ) -> Dict[str, Any]:
        """detect_intent + the same llmlog record dispatch() writes.

        The preview endpoint used to classify silently, so a misroute showed up
        in llmlog as a bare generate-agent/generate-task call with no visible
        classification step — the reason a five-agent crew request that got
        force-routed to the single-agent generator looked inexplicable.
        """
        result = await self.detect_intent(
            message,
            model,
            group_context,
            available_tools,
            chat_mode=chat_mode,
            last_resort_model=last_resort_model,
            prefer_existing=prefer_existing,
        )
        # Skip when no LLM actually ran (ChatMode fast-path / slash command /
        # "Use existing", which settles the intent without asking anything),
        # otherwise every message records a phantom call against the model.
        # The capability router logs its OWN call, where one really happens.
        if result.get("source") in (
            "chat_mode_fast_path",
            "slash_command",
            "prefer_existing",
        ):
            return result

        # Report the model that ANSWERED, not the first candidate we tried. The
        # row used to always read DEFAULT_DISPATCHER_MODEL / "success" even when
        # that endpoint never responded and the answer came from the
        # deterministic fallback — which reads as "claude ran" on a workspace
        # where no Databricks model is reachable at all.
        source = result.get("source") or ""
        degraded = source in self.DEGRADED_INTENT_SOURCES
        await self._log_llm_interaction(
            endpoint="detect-intent",
            prompt=message,
            response=str(result),
            model=result.get("model") or model,
            status="error" if degraded else "success",
            error_message=(
                f"no intent model answered; result came from {source}"
                if degraded
                else None
            ),
            group_context=group_context,
        )
        return result

    async def dispatch(
        self,
        request: DispatcherRequest,
        group_context: GroupContext = None,
        available_tools: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Dispatch the user's request to the appropriate generation service.

        Args:
            request: Dispatcher request with user message and options
            group_context: Group context from headers for multi-group isolation

        Returns:
            Dictionary containing the intent detection result and generation response
        """
        model = request.model or DEFAULT_DISPATCHER_MODEL

        # Enable MLflow tracing (same experiment as Crew execution) if workspace toggle is on
        mlflow_enabled = await self._maybe_enable_mlflow_tracing(group_context)

        # Use mlflow_tracing_service for robust trace context creation
        if mlflow_enabled:
            try:
                from src.services.mlflow.tracing import (
                    get_last_active_trace_id,
                    start_root_trace,
                )

                trace_ctx = start_root_trace(
                    "dispatcher", inputs={"message": request.message}
                )
                logger.info(
                    "[Dispatcher] MLflow root trace started using mlflow_tracing_service"
                )
            except Exception as trace_e:
                logger.warning(f"[Dispatcher] Could not start root trace: {trace_e}")
                trace_ctx = nullcontext()
        else:
            trace_ctx = nullcontext()

        with trace_ctx as root_trace:
            # Explicitly set inputs on the trace if available
            if mlflow_enabled and root_trace is not None:
                try:
                    if hasattr(root_trace, "set_inputs"):
                        root_trace.set_inputs(
                            {"message": request.message, "model": model}
                        )
                        logger.info("[Dispatcher] Trace inputs set successfully")
                except Exception as input_e:
                    logger.warning(
                        f"[Dispatcher] Could not set trace inputs: {input_e}"
                    )

            # Try to log last active trace id for observability
            if mlflow_enabled:
                try:
                    trace_id = get_last_active_trace_id()
                    if trace_id:
                        logger.info(f"[Dispatcher] Active trace id: {trace_id}")
                except Exception:
                    pass

            # Detect intent. Intent classification ALWAYS rides the fast model
            # chain (DEFAULT_DISPATCHER_MODEL + fallbacks), decoupled from the
            # possibly-heavy/reasoning model the user picked for the crew —
            # request.model is passed only as a last-resort fallback so intent
            # never hard-fails on a workspace where the fast models aren't enabled.
            # detect_intent_logged writes the DB record (separate from MLflow),
            # skipping the no-LLM paths and attributing the row to the model that
            # actually answered — one implementation for both surfaces.
            intent_result = await self.detect_intent_logged(
                request.message,
                DEFAULT_DISPATCHER_MODEL,
                group_context,
                available_tools,
                chat_mode=request.chat_mode,
                last_resort_model=request.model,
                prefer_existing=request.prefer_existing,
            )

            # Create dispatcher response
            # Clamp confidence to [0.0, 1.0] range (LLM sometimes returns >1.0)
            confidence = min(max(float(intent_result["confidence"]), 0.0), 1.0)
            dispatcher_response = DispatcherResponse(
                intent=IntentType(intent_result["intent"]),
                confidence=confidence,
                extracted_info=intent_result["extracted_info"],
                suggested_prompt=intent_result["suggested_prompt"],
                source=intent_result.get("source"),
                suggested_tools=intent_result.get("suggested_tools", []),
            )

            # Resolve workspace tools against the group's ENABLED set (the
            # server-side source of truth). A client-supplied request.tools is
            # only a PREFERENCE — we intersect it with the enabled tools so a
            # stale or over-broad selection can't smuggle in a tool that isn't
            # enabled for this workspace (e.g. SerperDevTool/ScrapeWebsiteTool
            # leaking into generated crews when they were never enabled).
            # Order-preserving (dict keys keep insertion order) with O(1)
            # membership — a plain set made the resolved tool order depend on
            # PYTHONHASHSEED, producing non-deterministic tool lists.
            enabled_titles: dict = {}
            if available_tools:
                # The router already fetched this workspace's enabled tools for
                # this very request — reuse them instead of a second fetch
                # (each tool_list cache hit also deep-copies the response).
                enabled_titles = dict.fromkeys(
                    t.get("title") for t in available_tools if t.get("title")
                )
            else:
                try:
                    from src.services.tools.tool_service import ToolService

                    tool_svc = ToolService(self.session)
                    if group_context:
                        tools_resp = await tool_svc.get_enabled_tools_for_group(
                            group_context
                        )
                    else:
                        tools_resp = await tool_svc.get_enabled_tools()
                    enabled_titles = dict.fromkeys(t.title for t in tools_resp.tools)
                except Exception as e:
                    logger.warning(f"Failed to fetch enabled workspace tools: {e}")

            effective_tools = self._resolve_effective_tools(
                request.tools, enabled_titles
            )
            if request.tools:
                dropped = [
                    t
                    for t in request.tools
                    if enabled_titles and t not in enabled_titles
                ]
                if dropped:
                    logger.info(
                        f"Dropped non-enabled tools from request.tools: {dropped}"
                    )

            # "Use existing" routes BEFORE the dispatch chain, because the
            # answer can change which branch runs. A router that declines
            # mid-conversation is saying "this turn is a question about what is
            # already on screen" — which is answered, not run. Rewriting the
            # intent here lets the ordinary chat path answer it, instead of this
            # branch growing a second copy of the generation machinery.
            routed_result = None
            if dispatcher_response.intent == IntentType.CATALOG_ROUTE:
                routed_result = await self._route_to_capability(
                    request.message,
                    group_context,
                    model,
                    last_resort_model=request.model,
                    session_id=request.session_id,
                    allow_continuation=getattr(request, "allow_continuation", True),
                )
                if routed_result.get("answer_here"):
                    logger.info(
                        "[capability_router] declined mid-conversation; answering "
                        "the turn instead of running a capability"
                    )
                    dispatcher_response.intent = IntentType.GENERATE_CREW
                    # A light agent, whatever the answer-mode pill says. That
                    # pill is greyed out in this mode and its stored value could
                    # be 'deep' — building a full reasoning crew to answer
                    # "what is this Aviation sector" would spend minutes on a
                    # question the transcript already answers.
                    request.chat_mode_type = "chat"
                    # And no semantic recall for this turn. The answer is in the
                    # transcript, which the light agent gets either way
                    # (``_conversation_preamble`` is unconditional). Semantic
                    # memory would query a shared, topic-polluted pool with a
                    # short generic question — the exact shape that matches badly
                    # — and is the only mechanism that could drag a different
                    # subject into an answer about what is already on screen.
                    request.disable_memory = True
                    routed_result = None

            # Dispatch to appropriate service based on intent
            generation_result = None
            try:
                if dispatcher_response.intent == IntentType.GENERATE_AGENT:
                    generation_result = await self.agent_service.generate_agent(
                        prompt_text=dispatcher_response.suggested_prompt
                        or request.message,
                        model=request.model,
                        tools=effective_tools,
                        group_context=group_context,
                        fast_planning=True,
                    )

                elif dispatcher_response.intent == IntentType.GENERATE_TASK:
                    task_request = TaskGenerationRequest(
                        text=dispatcher_response.suggested_prompt or request.message,
                        model=request.model,
                    )
                    generation_result = await self.task_service.generate_and_save_task(
                        task_request, group_context, fast_planning=True
                    )

                elif dispatcher_response.intent == IntentType.GENERATE_CREW:
                    import uuid as _uuid

                    generation_id = str(_uuid.uuid4())
                    streaming_request = CrewStreamingRequest(
                        prompt=dispatcher_response.suggested_prompt or request.message,
                        # Ground the run with the user's CLEAN message when the
                        # frontend sent it (message may carry a steering prefix).
                        original_prompt=request.original_prompt or request.message,
                        model=request.model,
                        tools=effective_tools or [],
                        # ChatMode generates AND runs on the backend so the run
                        # survives a session switch before the plan completes. The
                        # crew canvas leaves this False (default) — it renders the
                        # plan and the user runs it via Play; auto-executing here
                        # too would double-run the crew.
                        auto_execute=request.auto_execute,
                        session_id=request.session_id,
                        memory_workspace_scope=request.memory_workspace_scope,
                        disable_memory=request.disable_memory,
                        mcp_servers=request.mcp_servers or [],
                        agentbricks_endpoints=request.agentbricks_endpoints or [],
                        # Files attached in this chat turn — scopes the knowledge
                        # search tool so the run grounds on the just-uploaded doc.
                        knowledge_file_paths=request.knowledge_file_paths or [],
                        # Skills picked in the chat "+" menu — attached to every
                        # agent of the run by the shared kernel builder.
                        skills=request.skills or [],
                        # ChatMode answer mode (chat|research|deep) → drives
                        # reasoning/execution_type at config-build time.
                        chat_mode_type=request.chat_mode_type or "chat",
                    )
                    # Spawn progressive generation in background
                    asyncio.create_task(
                        self.crew_service.create_crew_progressive(
                            streaming_request,
                            group_context,
                            generation_id,
                            mlflow_enabled=mlflow_enabled,
                        )
                    )
                    generation_result = {
                        "generation_id": generation_id,
                        "type": "streaming",
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_ROUTE:
                    # Decided above, so the result could rewrite the intent.
                    # Carries the SAME execute_* shape the slash-command path
                    # produces, so everything downstream is unchanged.
                    generation_result = routed_result

                elif dispatcher_response.intent == IntentType.EXECUTE_CREW:
                    run_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    crews = await self.catalog_service.find_by_group(group_context)

                    if not run_name:
                        # No name — execute whatever is on the canvas
                        generation_result = {
                            "type": "execute_crew",
                            "plan": None,
                            "message": "Executing crew on canvas...",
                        }
                    else:
                        matches = [
                            c for c in crews if run_name.lower() in c.name.lower()
                        ]
                        exact_matches = [
                            c for c in matches if c.name.lower() == run_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            crew = matches[0]
                            generation_result = {
                                "type": "execute_crew",
                                "plan": {
                                    "id": str(crew.id),
                                    "name": crew.name,
                                    "nodes": crew.nodes or [],
                                    "edges": crew.edges or [],
                                    "process": crew.process,
                                    "memory": crew.memory,
                                    "verbose": crew.verbose,
                                    "max_rpm": crew.max_rpm,
                                },
                                "message": f"Loading and executing crew '{crew.name}'...",
                            }
                        elif len(matches) > 1:
                            unique_names = {c.name.lower() for c in matches}
                            if len(unique_names) == 1:
                                crew = sorted(
                                    matches,
                                    key=lambda c: c.updated_at or c.created_at,
                                    reverse=True,
                                )[0]
                                generation_result = {
                                    "type": "execute_crew",
                                    "plan": {
                                        "id": str(crew.id),
                                        "name": crew.name,
                                        "nodes": crew.nodes or [],
                                        "edges": crew.edges or [],
                                        "process": crew.process,
                                        "memory": crew.memory,
                                        "verbose": crew.verbose,
                                        "max_rpm": crew.max_rpm,
                                    },
                                    "message": f"Loading and executing crew '{crew.name}'...",
                                }
                            else:
                                generation_result = {
                                    "type": "catalog_list",
                                    "plans": [
                                        {"id": str(c.id), "name": c.name}
                                        for c in matches
                                    ],
                                    "message": f"Multiple crews match '{run_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "execute_crew",
                                "plan": None,
                                "message": f"No crew found matching '{run_name}'.",
                            }

                elif dispatcher_response.intent == IntentType.EXECUTE_FLOW:
                    run_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    flows = await self.flow_service.get_all_flows_for_group(
                        group_context
                    )

                    if not run_name:
                        # No name — execute whatever is on the canvas
                        generation_result = {
                            "type": "execute_flow",
                            "flow": None,
                            "message": "Executing flow on canvas...",
                        }
                    else:
                        matches = [
                            f for f in flows if run_name.lower() in f.name.lower()
                        ]
                        exact_matches = [
                            f for f in matches if f.name.lower() == run_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            flow = matches[0]
                            generation_result = {
                                "type": "execute_flow",
                                "flow": {
                                    "id": str(flow.id),
                                    "name": flow.name,
                                    "nodes": flow.nodes or [],
                                    "edges": flow.edges or [],
                                    "flow_config": flow.flow_config or {},
                                },
                                "message": f"Loading and executing flow '{flow.name}'...",
                            }
                        elif len(matches) > 1:
                            unique_names = {f.name.lower() for f in matches}
                            if len(unique_names) == 1:
                                flow = sorted(
                                    matches,
                                    key=lambda f: f.updated_at or f.created_at,
                                    reverse=True,
                                )[0]
                                generation_result = {
                                    "type": "execute_flow",
                                    "flow": {
                                        "id": str(flow.id),
                                        "name": flow.name,
                                        "nodes": flow.nodes or [],
                                        "edges": flow.edges or [],
                                        "flow_config": flow.flow_config or {},
                                    },
                                    "message": f"Loading and executing flow '{flow.name}'...",
                                }
                            else:
                                generation_result = {
                                    "type": "flow_list",
                                    "flows": [
                                        {"id": str(f.id), "name": f.name}
                                        for f in matches
                                    ],
                                    "message": f"Multiple flows match '{run_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "execute_flow",
                                "flow": None,
                                "message": f"No flow found matching '{run_name}'.",
                            }

                elif dispatcher_response.intent == IntentType.CONFIGURE_CREW:
                    config_type = dispatcher_response.extracted_info.get(
                        "config_type", "general"
                    )
                    generation_result = {
                        "type": "configure_crew",
                        "config_type": config_type,
                        "message": f"Opening configuration dialog for {config_type} settings.",
                        "actions": {
                            "open_llm_dialog": config_type in ["llm", "general"],
                            "open_maxr_dialog": config_type in ["maxr", "general"],
                            "open_tools_dialog": config_type in ["tools", "general"],
                        },
                        "extracted_info": dispatcher_response.extracted_info,
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_LIST:
                    crews = await self.catalog_service.find_by_group(group_context)
                    generation_result = {
                        "type": "catalog_list",
                        "plans": [
                            {
                                "id": str(c.id),
                                "name": c.name,
                                "agent_count": len(c.agent_ids) if c.agent_ids else 0,
                                "task_count": len(c.task_ids) if c.task_ids else 0,
                                "created_at": (
                                    c.created_at.isoformat() if c.created_at else None
                                ),
                                "updated_at": (
                                    c.updated_at.isoformat() if c.updated_at else None
                                ),
                            }
                            for c in crews
                        ],
                        "message": f"Found {len(crews)} plan(s) in your catalog.",
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_LOAD:
                    search_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    crews = await self.catalog_service.find_by_group(group_context)

                    if not search_name:
                        # No name provided — return the list instead
                        generation_result = {
                            "type": "catalog_list",
                            "plans": [
                                {
                                    "id": str(c.id),
                                    "name": c.name,
                                    "agent_count": (
                                        len(c.agent_ids) if c.agent_ids else 0
                                    ),
                                    "task_count": len(c.task_ids) if c.task_ids else 0,
                                }
                                for c in crews
                            ],
                            "message": "No plan name specified. Here are your available plans:",
                        }
                    else:
                        # Search by name (case-insensitive partial match)
                        matches = [
                            c for c in crews if search_name.lower() in c.name.lower()
                        ]
                        # Prioritize exact name matches to avoid infinite loops
                        # when multiple items share the same name
                        exact_matches = [
                            c for c in matches if c.name.lower() == search_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            crew = matches[0]
                            generation_result = {
                                "type": "catalog_load",
                                "plan": {
                                    "id": str(crew.id),
                                    "name": crew.name,
                                    "nodes": crew.nodes or [],
                                    "edges": crew.edges or [],
                                    "process": crew.process,
                                    "memory": crew.memory,
                                    "verbose": crew.verbose,
                                    "max_rpm": crew.max_rpm,
                                },
                                "message": f"Loaded plan '{crew.name}' onto the canvas.",
                            }
                        elif len(matches) > 1:
                            # Multiple matches — check if they all share the
                            # same name (duplicates). If so, load the most
                            # recent one instead of showing an ambiguous list.
                            unique_names = {c.name.lower() for c in matches}
                            if len(unique_names) == 1:
                                # All duplicates — pick most recently updated
                                crew = sorted(
                                    matches,
                                    key=lambda c: c.updated_at or c.created_at,
                                    reverse=True,
                                )[0]
                                generation_result = {
                                    "type": "catalog_load",
                                    "plan": {
                                        "id": str(crew.id),
                                        "name": crew.name,
                                        "nodes": crew.nodes or [],
                                        "edges": crew.edges or [],
                                        "process": crew.process,
                                        "memory": crew.memory,
                                        "verbose": crew.verbose,
                                        "max_rpm": crew.max_rpm,
                                    },
                                    "message": f"Loaded plan '{crew.name}' (most recent) onto the canvas.",
                                }
                            else:
                                generation_result = {
                                    "type": "catalog_list",
                                    "plans": [
                                        {"id": str(c.id), "name": c.name}
                                        for c in matches
                                    ],
                                    "message": f"Multiple plans match '{search_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "catalog_load",
                                "plan": None,
                                "message": f"No plan found matching '{search_name}'.",
                            }

                elif dispatcher_response.intent == IntentType.CATALOG_SAVE:
                    save_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    generation_result = {
                        "type": "catalog_save",
                        "action": "open_save_dialog",
                        "suggested_name": save_name or None,
                        "message": (
                            f"Saving crew '{save_name}'..."
                            if save_name
                            else "Opening save dialog..."
                        ),
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_SCHEDULE:
                    generation_result = {
                        "type": "catalog_schedule",
                        "action": "open_schedule_dialog",
                        "message": "Opening schedule dialog...",
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_HELP:
                    # Command-specific usage help (e.g. bare /list without qualifier)
                    command_help = dispatcher_response.extracted_info.get(
                        "command_help", ""
                    )
                    # Invalid/unrecognized command prefix
                    invalid = dispatcher_response.extracted_info.get(
                        "invalid_command", False
                    )
                    invalid_cmd = dispatcher_response.extracted_info.get("command", "")
                    invalid_prefix = (
                        f"Unknown command `{invalid_cmd}`.\n\n" if invalid else ""
                    )

                    full_help = (
                        "**Crew Commands:**\n"
                        "- `/list crews` — List all saved crews in your catalog\n"
                        "- `/load crew <name>` — Load a saved crew onto the canvas\n"
                        "- `/save crew [name]` — Save the current canvas as a crew\n"
                        "- `/run crew` — Execute the current crew on the canvas\n"
                        "- `/delete crew <name>` — Delete a saved crew\n"
                        "- `/schedule crew` — Schedule the current crew for automatic execution\n"
                        "\n"
                        "**Flow Commands:**\n"
                        "- `/list flows` — List all saved flows\n"
                        "- `/load flow <name>` — Load a saved flow onto the canvas\n"
                        "- `/save flow [name]` — Save the current flow\n"
                        "- `/run flow` — Execute the current flow\n"
                        "- `/delete flow <name>` — Delete a saved flow\n"
                        "\n"
                        "**Other:**\n"
                        "- `/help` — Show this help message\n"
                        "\n"
                        "**Aliases:** `/plans` = `/list crews`, `/exec` = `/run`, `/flows` = `/list flows`"
                    )

                    if command_help:
                        # Show only the command-specific usage hint
                        message = command_help
                    elif invalid_prefix:
                        message = f"{invalid_prefix}{full_help}"
                    else:
                        message = full_help

                    generation_result = {
                        "type": "catalog_help",
                        "message": message,
                    }

                elif dispatcher_response.intent == IntentType.FLOW_LIST:
                    flows = await self.flow_service.get_all_flows_for_group(
                        group_context
                    )
                    generation_result = {
                        "type": "flow_list",
                        "flows": [
                            {
                                "id": str(f.id),
                                "name": f.name,
                                "node_count": (len(f.nodes) if f.nodes else 0),
                                "created_at": (
                                    f.created_at.isoformat() if f.created_at else None
                                ),
                                "updated_at": (
                                    f.updated_at.isoformat() if f.updated_at else None
                                ),
                            }
                            for f in flows
                        ],
                        "message": f"Found {len(flows)} flow(s) in your catalog.",
                    }

                elif dispatcher_response.intent == IntentType.FLOW_LOAD:
                    search_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    flows = await self.flow_service.get_all_flows_for_group(
                        group_context
                    )

                    if not search_name:
                        # No name provided — return the list instead
                        generation_result = {
                            "type": "flow_list",
                            "flows": [
                                {
                                    "id": str(f.id),
                                    "name": f.name,
                                    "node_count": (len(f.nodes) if f.nodes else 0),
                                }
                                for f in flows
                            ],
                            "message": "No flow name specified. Here are your available flows:",
                        }
                    else:
                        # Search by name (case-insensitive partial match)
                        matches = [
                            f for f in flows if search_name.lower() in f.name.lower()
                        ]
                        # Prioritize exact name matches to avoid infinite loops
                        # when multiple items share the same name
                        exact_matches = [
                            f for f in matches if f.name.lower() == search_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            flow = matches[0]
                            generation_result = {
                                "type": "flow_load",
                                "flow": {
                                    "id": str(flow.id),
                                    "name": flow.name,
                                    "nodes": flow.nodes or [],
                                    "edges": flow.edges or [],
                                    "flow_config": flow.flow_config or {},
                                },
                                "message": f"Loaded flow '{flow.name}' onto the canvas.",
                            }
                        elif len(matches) > 1:
                            # Multiple matches — check if they all share the
                            # same name (duplicates). If so, load the most
                            # recent one instead of showing an ambiguous list.
                            unique_names = {f.name.lower() for f in matches}
                            if len(unique_names) == 1:
                                # All duplicates — pick most recently updated
                                flow = sorted(
                                    matches,
                                    key=lambda f: f.updated_at or f.created_at,
                                    reverse=True,
                                )[0]
                                generation_result = {
                                    "type": "flow_load",
                                    "flow": {
                                        "id": str(flow.id),
                                        "name": flow.name,
                                        "nodes": flow.nodes or [],
                                        "edges": flow.edges or [],
                                        "flow_config": flow.flow_config or {},
                                    },
                                    "message": f"Loaded flow '{flow.name}' (most recent) onto the canvas.",
                                }
                            else:
                                generation_result = {
                                    "type": "flow_list",
                                    "flows": [
                                        {"id": str(f.id), "name": f.name}
                                        for f in matches
                                    ],
                                    "message": f"Multiple flows match '{search_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "flow_load",
                                "flow": None,
                                "message": f"No flow found matching '{search_name}'.",
                            }

                elif dispatcher_response.intent == IntentType.FLOW_SAVE:
                    save_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    generation_result = {
                        "type": "flow_save",
                        "action": "open_save_flow_dialog",
                        "suggested_name": save_name or None,
                        "message": (
                            f"Saving flow '{save_name}'..."
                            if save_name
                            else "Opening save flow dialog..."
                        ),
                    }

                elif dispatcher_response.intent == IntentType.CATALOG_DELETE:
                    delete_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    crews = await self.catalog_service.find_by_group(group_context)

                    if not delete_name:
                        generation_result = {
                            "type": "catalog_delete",
                            "message": "Please specify a crew name to delete. Usage: `/delete crew <name>`",
                        }
                    else:
                        matches = [
                            c for c in crews if delete_name.lower() in c.name.lower()
                        ]
                        exact_matches = [
                            c for c in matches if c.name.lower() == delete_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            crew = matches[0]
                            await self.catalog_service.delete_by_group(
                                crew.id, group_context
                            )
                            generation_result = {
                                "type": "catalog_delete",
                                "message": f"Crew '{crew.name}' has been deleted.",
                            }
                        elif len(matches) > 1:
                            unique_names = {c.name.lower() for c in matches}
                            if len(unique_names) == 1:
                                crew = sorted(
                                    matches,
                                    key=lambda c: c.updated_at or c.created_at,
                                    reverse=True,
                                )[0]
                                await self.catalog_service.delete_by_group(
                                    crew.id, group_context
                                )
                                generation_result = {
                                    "type": "catalog_delete",
                                    "message": f"Crew '{crew.name}' (most recent) has been deleted.",
                                }
                            else:
                                generation_result = {
                                    "type": "catalog_list",
                                    "plans": [
                                        {"id": str(c.id), "name": c.name}
                                        for c in matches
                                    ],
                                    "message": f"Multiple crews match '{delete_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "catalog_delete",
                                "message": f"No crew found matching '{delete_name}'.",
                            }

                elif dispatcher_response.intent == IntentType.FLOW_DELETE:
                    delete_name = dispatcher_response.extracted_info.get(
                        "args", ""
                    ).strip()
                    flows = await self.flow_service.get_all_flows_for_group(
                        group_context
                    )

                    if not delete_name:
                        generation_result = {
                            "type": "flow_delete",
                            "message": "Please specify a flow name to delete. Usage: `/delete flow <name>`",
                        }
                    else:
                        matches = [
                            f for f in flows if delete_name.lower() in f.name.lower()
                        ]
                        exact_matches = [
                            f for f in matches if f.name.lower() == delete_name.lower()
                        ]
                        if exact_matches:
                            matches = exact_matches
                        if len(matches) == 1:
                            flow = matches[0]
                            await self.flow_service.force_delete_flow_with_executions_with_group_check(
                                flow.id, group_context
                            )
                            generation_result = {
                                "type": "flow_delete",
                                "message": f"Flow '{flow.name}' has been deleted.",
                            }
                        elif len(matches) > 1:
                            unique_names = {f.name.lower() for f in matches}
                            if len(unique_names) == 1:
                                flow = sorted(
                                    matches,
                                    key=lambda f: f.updated_at or f.created_at,
                                    reverse=True,
                                )[0]
                                await self.flow_service.force_delete_flow_with_executions_with_group_check(
                                    flow.id, group_context
                                )
                                generation_result = {
                                    "type": "flow_delete",
                                    "message": f"Flow '{flow.name}' (most recent) has been deleted.",
                                }
                            else:
                                generation_result = {
                                    "type": "flow_list",
                                    "flows": [
                                        {"id": str(f.id), "name": f.name}
                                        for f in matches
                                    ],
                                    "message": f"Multiple flows match '{delete_name}'. Please be more specific:",
                                }
                        else:
                            generation_result = {
                                "type": "flow_delete",
                                "message": f"No flow found matching '{delete_name}'.",
                            }

                else:
                    logger.warning(
                        f"Unknown intent detected: {dispatcher_response.intent}"
                    )
                    generation_result = {
                        "type": "unknown",
                        "message": "I'm not sure what you'd like me to create. Could you please clarify if you want me to generate a task, agent, crew, or plan?",
                        "suggestions": [
                            "Create a task: 'I need a task to...'",
                            "Generate an agent: 'Create an agent that can...'",
                            "Build a crew: 'Build a team that can...'",
                            "Create a plan: 'Create a plan for...'",
                        ],
                    }
            except Exception as e:
                logger.error(f"Error in generation service: {str(e)}")
                await self._log_llm_interaction(
                    endpoint=f"dispatch-{dispatcher_response.intent}",
                    prompt=request.message,
                    response=str(e),
                    model=model,
                    status="error",
                    error_message=str(e),
                    group_context=group_context,
                )
                raise

            # Prepare the combined response
            combined_response = {
                "dispatcher": dispatcher_response.model_dump(),
                "generation_result": generation_result,
                "service_called": (
                    dispatcher_response.intent.value
                    if dispatcher_response.intent != IntentType.UNKNOWN
                    else None
                ),
            }

            # Set trace outputs if MLflow tracing is enabled
            if mlflow_enabled and root_trace is not None:
                try:
                    if hasattr(root_trace, "set_outputs"):
                        trace_outputs = {
                            "intent": dispatcher_response.intent.value,
                            "confidence": dispatcher_response.confidence,
                            "extracted_info": dispatcher_response.extracted_info,
                            "suggested_prompt": dispatcher_response.suggested_prompt,
                            "service_called": combined_response["service_called"],
                        }

                        # Add generation result summary (avoid large payloads)
                        if generation_result:
                            if isinstance(generation_result, dict):
                                # Include type and summary info, exclude large data
                                trace_outputs["generation_summary"] = {
                                    "type": generation_result.get("type"),
                                    "message": generation_result.get("message"),
                                    "has_result": bool(generation_result),
                                }

                        root_trace.set_outputs(trace_outputs)
                        logger.info("[Dispatcher] Trace outputs set successfully")
                except Exception as output_e:
                    logger.warning(
                        f"[Dispatcher] Could not set trace outputs: {output_e}"
                    )

            # Return combined response
            return combined_response
