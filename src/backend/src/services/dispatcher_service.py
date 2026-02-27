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
from typing import Any, Dict, List, Optional, Set

try:
    import mlflow as _mlflow

    _HAS_MLFLOW = True
except ImportError:
    _mlflow = None  # type: ignore[assignment]
    _HAS_MLFLOW = False

from src.core.cache import intent_cache
from src.core.llm_manager import LLMManager
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse, CrewStreamingRequest
from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.schemas.task_generation import TaskGenerationRequest, TaskGenerationResponse
from src.services.agent_generation_service import AgentGenerationService
from src.services.crew_generation_service import CrewGenerationService
from src.services.crew_service import CrewService
from src.services.databricks_service import DatabricksService
from src.services.flow_service import FlowService
from src.services.log_service import LLMLogService
from src.services.mlflow_service import MLflowService
from src.services.task_generation_service import TaskGenerationService
from src.services.template_service import TemplateService
from src.utils.prompt_utils import robust_json_parser
from src.utils.user_context import GroupContext

# Configure logging
logger = logging.getLogger(__name__)

# Default model for intent detection
DEFAULT_DISPATCHER_MODEL = os.getenv(
    "DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick"
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
        "find", "search", "locate", "discover", "identify",
        "get", "fetch", "retrieve", "collect", "gather",
        "analyze", "examine", "study", "investigate", "review",
        "assess", "evaluate", "compare", "contrast",
        "create", "make", "build", "generate", "produce", "develop",
        "write", "compose", "draft", "prepare", "document",
        "calculate", "compute", "determine", "measure",
        "summarize", "condense", "extract", "compile",
        "organize", "sort", "categorize", "classify",
        "check", "verify", "validate", "test", "inspect", "audit",
        "monitor", "track",
        "send", "deliver", "share", "distribute",
        "convert", "transform", "translate", "format", "parse",
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

    # Patterns that indicate explicit single-agent creation intent
    AGENT_CREATION_PATTERNS = [
        r"\b(create|make|build|generate|develop)\b.*\b(an?\s+)?(agent|bot|assistant|chatbot)\b",
        r"\b(i need|give me|set up)\b.*\b(an?\s+)?(agent|bot|assistant|chatbot)\b",
    ]

    # Multi-step workflow indicators — boost crew score
    MULTI_STEP_PATTERNS = [
        r"\bthen\b",                       # "research then write then present"
        r",\s*[a-z]+\s+(and|then)\b",      # comma-separated action chain
        r"\band\b.*\b(create|write|build|make|generate|analyze|review|produce|prepare)\b",
        r"\bstep\s*\d+\b",                 # "step 1, step 2"
        r"\bfirst\b.*\bthen\b",            # "first X then Y"
        r"\b(after|before|once|finally)\b", # sequential indicators
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
    ) -> str:
        """Call LLMManager.completion with retry, timeout, and exponential backoff.

        Args:
            messages: Chat messages list
            model: LLM model identifier
            temperature: Sampling temperature
            max_tokens: Maximum tokens in response

        Returns:
            Content string from the LLM response

        Raises:
            Last encountered exception after all retries are exhausted
        """
        last_error: Optional[Exception] = None

        for attempt in range(self.LLM_MAX_RETRIES):
            try:
                content = await asyncio.wait_for(
                    LLMManager.completion(
                        messages=messages,
                        model=model,
                        temperature=temperature,
                        max_tokens=max_tokens,
                    ),
                    timeout=self.LLM_REQUEST_TIMEOUT,
                )
                return content
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

    async def _maybe_enable_mlflow_tracing(
        self, group_context: Optional[GroupContext]
    ) -> bool:
        """Enable MLflow tracing for dispatcher/planner if workspace toggle is on.
        - Uses the same experiment as Crew execution traces so UI can link consistently.
        - Runs blocking MLflow setup in a background thread to keep API async.
        """
        try:
            group_id = (
                getattr(group_context, "primary_group_id", None)
                if group_context
                else None
            )
            svc = MLflowService(self.session, group_id=group_id)
            enabled = await svc.is_enabled()
            if not enabled:
                logger.info(
                    "[Dispatcher] MLflow disabled for this workspace; skipping tracing setup"
                )
                return False

            def _setup_mlflow_sync():
                import asyncio
                import os

                import mlflow

                # Enable OBO → PAT → SPN fallback chain for MLflow authentication
                # This matches the pattern used by LLM authentication
                try:
                    from src.utils.databricks_auth import (
                        get_auth_context,
                        is_scope_error,
                    )

                    # Extract user_token from group_context if available
                    user_token = (
                        getattr(group_context, "access_token", None)
                        if group_context
                        else None
                    )

                    # Try with OBO first (if user_token available)
                    auth = asyncio.run(get_auth_context(user_token=user_token))
                    if auth:
                        os.environ["DATABRICKS_HOST"] = auth.workspace_url
                        os.environ["DATABRICKS_TOKEN"] = auth.token
                        logger.info(
                            f"[Dispatcher] MLflow configured with {auth.auth_method} authentication"
                        )
                    else:
                        logger.warning(
                            "[Dispatcher] No Databricks authentication available for MLflow"
                        )
                        return

                    # Ensure Databricks tracking
                    mlflow.set_tracking_uri("databricks")

                    # Get MLflow experiment name from Databricks config via service
                    exp_name = "/Shared/kasal-crew-execution-traces"  # Default fallback
                    try:
                        databricks_service = DatabricksService(group_id=group_id)
                        db_config = asyncio.run(
                            databricks_service.get_databricks_config()
                        )
                        if db_config and db_config.mlflow_experiment_name:
                            # Ensure experiment name starts with /Shared/ for proper organization
                            if not db_config.mlflow_experiment_name.startswith("/"):
                                exp_name = f"/Shared/{db_config.mlflow_experiment_name}"
                            else:
                                exp_name = db_config.mlflow_experiment_name
                    except Exception as config_err:
                        logger.info(
                            f"[Dispatcher] Could not fetch MLflow experiment name from config: {config_err}, using default"
                        )

                    # Try to set experiment - this may fail with scope error if using OBO
                    try:
                        exp = mlflow.set_experiment(exp_name)
                        logger.info(
                            f"[Dispatcher] MLflow experiment set successfully with {auth.auth_method}"
                        )
                    except Exception as mlflow_e:
                        # Check if this is a scope error (OBO token lacks MLflow permissions)
                        if is_scope_error(mlflow_e) and user_token:
                            logger.warning(
                                f"[Dispatcher] OBO token lacks MLflow scopes, falling back to PAT/SPN: {mlflow_e}"
                            )
                            # Retry with PAT/SPN fallback (no user_token)
                            auth_fallback = asyncio.run(
                                get_auth_context(user_token=None)
                            )
                            if auth_fallback:
                                os.environ["DATABRICKS_HOST"] = (
                                    auth_fallback.workspace_url
                                )
                                os.environ["DATABRICKS_TOKEN"] = auth_fallback.token
                                logger.info(
                                    f"[Dispatcher] MLflow reconfigured with {auth_fallback.auth_method} authentication"
                                )
                                # Retry experiment creation with fallback auth
                                mlflow.set_tracking_uri("databricks")
                                exp = mlflow.set_experiment(exp_name)
                                logger.info(
                                    f"[Dispatcher] MLflow experiment set successfully with {auth_fallback.auth_method} fallback"
                                )
                            else:
                                logger.error(
                                    "[Dispatcher] PAT/SPN fallback auth also failed"
                                )
                                raise
                        else:
                            # Not a scope error or already using PAT/SPN, re-raise
                            raise

                except Exception as auth_e:
                    logger.warning(
                        f"[Dispatcher] Databricks auth setup failed: {auth_e}"
                    )
                    raise
                try:
                    logger.info(
                        f"[Dispatcher] MLflow experiment set: {exp_name} (ID: {getattr(exp, 'experiment_id', '')})"
                    )
                except Exception:
                    pass
                # Ensure OpenTelemetry SDK is enabled; otherwise MLflow traces won't record
                try:
                    import os as _otel_env

                    if _otel_env.environ.get("OTEL_SDK_DISABLED", "").lower() in (
                        "",
                        "true",
                        "1",
                    ):
                        _otel_env.environ["OTEL_SDK_DISABLED"] = "false"
                        logger.info(
                            "[Dispatcher] Set OTEL_SDK_DISABLED=false for MLflow tracing"
                        )
                except Exception as _ote:
                    logger.info(
                        f"[Dispatcher] Could not adjust OTEL_SDK_DISABLED: {_ote}"
                    )
                # Route tracing to the experiment when available (MLflow 3.x)
                try:
                    from mlflow.tracing.destination import Databricks as _Dest

                    mlflow.tracing.set_destination(
                        _Dest(experiment_id=str(getattr(exp, "experiment_id", "")))
                    )
                    mlflow.tracing.enable()
                except Exception as te:
                    # Older MLflow versions may not support tracing destination
                    logger.info(
                        f"[Dispatcher] MLflow tracing destination not set (version/availability): {te}"
                    )
                # Enable LiteLLM autolog to create child spans (not separate root traces)
                # log_traces=False creates spans that nest under the parent "dispatcher" trace
                try:
                    mlflow.litellm.autolog(log_traces=False)
                    logger.info(
                        "[Dispatcher] MLflow LiteLLM autolog enabled (spans only)"
                    )
                except Exception as ae:
                    logger.info(
                        f"[Dispatcher] MLflow LiteLLM autolog not available: {ae}"
                    )

            # Run setup off the event loop
            await asyncio.to_thread(_setup_mlflow_sync)
            logger.info(
                "[Dispatcher] MLflow tracing configured (experiment and autolog)"
            )
            return True
        except Exception as e:
            logger.warning(f"[Dispatcher] MLflow setup skipped: {e}")
            return False

    @staticmethod
    def _detect_slash_command(message: str) -> Optional[Dict[str, Any]]:
        """Detect and parse slash commands (e.g., /list, /load my-plan).

        Returns a fully formed intent result dict if the message is a recognized
        slash command, or None otherwise.
        """
        stripped = message.strip()
        if not stripped.startswith("/"):
            return None

        parts = stripped.split(None, 1)  # split into command + rest
        command = parts[0].lower()
        args = parts[1].strip() if len(parts) > 1 else ""

        COMMAND_MAP = {
            "/list": "catalog_list",
            "/plans": "catalog_list",
            "/flows": "flow_list",
            "/load": "catalog_load",
            "/save": "catalog_save",
            "/schedule": "catalog_schedule",
            "/help": "catalog_help",
            "/run": "execute_crew",
            "/exec": "execute_crew",
            "/delete": "catalog_delete",
        }

        intent = COMMAND_MAP.get(command)
        if intent is None:
            if stripped.startswith("/"):
                # Unrecognized slash command -> show help with error
                return {
                    "intent": "catalog_help",
                    "confidence": 1.0,
                    "extracted_info": {"command": command, "args": args, "invalid_command": True},
                    "suggested_prompt": stripped,
                    "source": "slash_command",
                    "suggested_tools": [],
                }
            return None

        # Check for flow qualifier in args (e.g. "/list flows", "/load flow my-flow")
        qualifier_found = False
        FLOW_INTENT_MAP = {
            "catalog_list": "flow_list",
            "catalog_load": "flow_load",
            "catalog_save": "flow_save",
            "execute_crew": "execute_flow",
            "catalog_delete": "flow_delete",
        }
        if args.lower().startswith(("flow", "flows")) and intent in FLOW_INTENT_MAP:
            intent = FLOW_INTENT_MAP[intent]
            qualifier_found = True
            # Strip "flow" or "flows" prefix from args
            remaining = args.split(None, 1)
            args = remaining[1].strip() if len(remaining) > 1 else ""

        # Check for crew/crews qualifier (e.g. "/list crews", "/save crew My Crew")
        CREW_QUALIFIABLE = {"catalog_list", "catalog_load", "catalog_save", "catalog_schedule", "execute_crew", "catalog_delete"}
        if not qualifier_found and args.lower().startswith(("crew", "crews")) and intent in CREW_QUALIFIABLE:
            qualifier_found = True
            remaining = args.split(None, 1)
            args = remaining[1].strip() if len(remaining) > 1 else ""

        # Commands that require a crew/flow qualifier (bare /list, /load etc. show usage help)
        # /plans and /flows are aliases that already imply the qualifier, so they're excluded.
        QUALIFIER_REQUIRED = {"/list", "/load", "/save", "/run", "/exec", "/schedule", "/delete"}
        if not qualifier_found and command in QUALIFIER_REQUIRED:
            COMMAND_USAGE = {
                "/list": "Usage: `/list crews` or `/list flows`",
                "/load": "Usage: `/load crew <name>` or `/load flow <name>`",
                "/save": "Usage: `/save crew [name]` or `/save flow [name]`",
                "/run": "Usage: `/run crew` or `/run flow`",
                "/exec": "Usage: `/run crew` or `/run flow`",
                "/schedule": "Usage: `/schedule crew`",
                "/delete": "Usage: `/delete crew <name>` or `/delete flow <name>`",
            }
            return {
                "intent": "catalog_help",
                "confidence": 1.0,
                "extracted_info": {
                    "command": command,
                    "args": args,
                    "command_help": COMMAND_USAGE.get(command, ""),
                },
                "suggested_prompt": stripped,
                "source": "slash_command",
                "suggested_tools": [],
            }

        return {
            "intent": intent,
            "confidence": 1.0,
            "extracted_info": {"command": command, "args": args},
            "suggested_prompt": stripped,
            "source": "slash_command",
            "suggested_tools": [],
        }

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
        # Normalize message for analysis
        msg_lower = message.lower()
        words = re.findall(r"\b\w+\b", msg_lower)
        word_set = set(words)

        # Count different types of keywords
        task_actions = word_set.intersection(self.TASK_ACTION_WORDS)

        agent_keywords = word_set.intersection(self.AGENT_KEYWORDS)
        crew_keywords = word_set.intersection(self.CREW_KEYWORDS)

        execute_keywords = word_set.intersection(self.EXECUTE_KEYWORDS)
        configure_keywords = word_set.intersection(self.CONFIGURE_KEYWORDS)
        catalog_keywords = word_set.intersection(self.CATALOG_KEYWORDS)

        # Analyze message structure patterns
        has_imperative = any(
            word in words[:3] for word in self.TASK_ACTION_WORDS
        )  # Action word in first 3 words
        has_question = message.strip().endswith("?") or any(
            word in words[:2] for word in ["what", "how", "why", "when", "where", "who"]
        )
        has_greeting = False  # Removed conversation detection

        # Detect configuration patterns
        configure_patterns = [
            r"(configure|config|setup|set up)",  # Configuration words
            r"(change|update|modify|adjust).*?(llm|model|tools|maxr|max|rpm)",  # Change configuration
            r"(select|choose|pick).*?(llm|model|tools)",  # Selection patterns
            r"(llm|model|tools|maxr).*?(setting|config)",  # Configuration contexts
        ]

        has_configure_structure = any(
            re.search(pattern, msg_lower) for pattern in configure_patterns
        )

        # Multi-step workflow detection — boosts crew
        has_multi_step = any(
            re.search(pattern, msg_lower) for pattern in self.MULTI_STEP_PATTERNS
        )
        has_multiple_actions = len(task_actions) > 1
        has_complex_task = has_multiple_actions or bool(
            re.search(r"multiple|several|all|various|different", msg_lower)
        )

        # Explicit agent creation — requires a creation verb + agent entity word
        has_explicit_agent = any(
            re.search(pattern, msg_lower) for pattern in self.AGENT_CREATION_PATTERNS
        )

        # Explicit task creation — user literally says "task"
        has_explicit_task = bool(
            re.search(r"\b(create|make|add|generate)\b.*\btask\b", msg_lower)
        )

        # Single atomic action check — ONLY true when message is clearly
        # one simple action with no workflow indicators
        is_single_atomic = (
            len(task_actions) <= 1
            and not has_multi_step
            and not has_multiple_actions
            and not crew_keywords
            and not has_explicit_agent
        )

        # ── Crew-first intent scoring ─────────────────────────────
        # generate_crew starts with a base score; others must earn it.

        crew_score = self.CREW_BASE_SCORE  # Default advantage
        crew_score += len(crew_keywords) * 3
        if has_multi_step or has_multiple_actions:
            crew_score += self.MULTI_STEP_BONUS
        if has_complex_task:
            crew_score += 2

        # generate_task only wins when clearly a single atomic action
        task_score = 0
        if has_explicit_task and not crew_keywords:
            task_score = 15  # User literally said "create a task" — overrides crew
        elif is_single_atomic and not has_question:
            task_score = 5  # Simple single action, might be a task

        # generate_agent only wins when user explicitly asks for one
        agent_score = 0
        if has_explicit_agent and not crew_keywords:
            agent_score = 15  # User literally said "create an agent" — overrides crew

        intent_scores = {
            "generate_crew": crew_score,
            "generate_task": task_score,
            "generate_agent": agent_score,
            "execute_crew": len(execute_keywords) * 4
            + (2 if execute_keywords.intersection({"execute", "ec"}) else 0),
            "configure_crew": len(configure_keywords) * 3
            + (2 if has_configure_structure else 0),
            "catalog_list": (
                3
                if word_set.intersection({"list", "browse", "show", "view"})
                and word_set.intersection({"plans", "catalog", "saved", "crews"})
                else 0
            ),
            "catalog_load": (
                3
                if word_set.intersection({"load", "open", "restore", "import"})
                and word_set.intersection({"plan", "crew", "saved"})
                else 0
            ),
            "catalog_save": (
                3
                if word_set.intersection({"save", "store", "persist", "export"})
                and word_set.intersection({"plan", "crew", "catalog"})
                else 0
            ),
            "catalog_schedule": (
                3
                if word_set.intersection({"schedule", "cron", "recurring", "automate"})
                and word_set.intersection({"plan", "crew", "this"})
                else 0
            ),
        }

        # Determine semantic hints
        semantic_hints = []
        if task_actions:
            semantic_hints.append(f"Action words detected: {', '.join(task_actions)}")
        if has_multi_step:
            semantic_hints.append("Multi-step workflow detected")
        if has_multiple_actions:
            semantic_hints.append("Multiple action words detected")
        if has_explicit_agent:
            semantic_hints.append("Explicit agent creation detected")
        if has_explicit_task:
            semantic_hints.append("Explicit task creation detected")
        if execute_keywords:
            semantic_hints.append(
                f"Execution words detected: {', '.join(execute_keywords)}"
            )
        if configure_keywords:
            semantic_hints.append(
                f"Configuration words detected: {', '.join(configure_keywords)}"
            )
        if catalog_keywords:
            semantic_hints.append(
                f"Catalog keywords detected: {', '.join(catalog_keywords)}"
            )
        if has_configure_structure:
            semantic_hints.append("Configuration structure detected")
        if has_question:
            semantic_hints.append("Question form detected")

        # Tie-breaking: prefer crew over task over agent
        suggested_intent = "generate_crew"
        max_score = max(intent_scores.values())
        if max_score > 0:
            # Among intents tied at max, prefer crew > execute > configure > task > agent
            priority_order = [
                "generate_crew", "execute_crew", "configure_crew",
                "catalog_list", "catalog_load", "catalog_save", "catalog_schedule",
                "generate_task", "generate_agent",
            ]
            for intent_name in priority_order:
                if intent_scores.get(intent_name, 0) == max_score:
                    suggested_intent = intent_name
                    break

        return {
            "task_actions": list(task_actions),
            "agent_keywords": list(agent_keywords),
            "crew_keywords": list(crew_keywords),
            "execute_keywords": list(execute_keywords),
            "configure_keywords": list(configure_keywords),
            "catalog_keywords": list(catalog_keywords),
            "has_imperative": has_imperative,
            "has_question": has_question,
            "has_greeting": has_greeting,
            "has_command_structure": False,  # Removed — no longer used
            "has_configure_structure": has_configure_structure,
            "has_complex_task": has_complex_task,
            "has_multi_step": has_multi_step,
            "has_explicit_agent": has_explicit_agent,
            "has_explicit_task": has_explicit_task,
            "intent_scores": intent_scores,
            "semantic_hints": semantic_hints,
            "suggested_intent": suggested_intent,
        }

    @staticmethod
    def _build_tool_catalog(available_tools: List[Dict[str, str]]) -> str:
        """Format available tools into a prompt section for the LLM.

        Args:
            available_tools: List of dicts with 'title' and 'description' keys.

        Returns:
            A string to append to the user message with tool catalog and instructions.
        """
        lines = [f"- {t['title']}: {t['description']}" for t in available_tools]
        return (
            "\n\nAvailable tools in the workspace:\n"
            + "\n".join(lines)
            + "\n\nBased on the user's request, include a 'suggested_tools' field in your JSON response "
            "containing a list of tool names (from the list above) that would be useful for this task. "
            "Only suggest tools that are directly relevant. Return an empty list if no tools apply."
        )

    async def detect_intent(
        self,
        message: str,
        model: str,
        group_context: Optional[GroupContext] = None,
        available_tools: Optional[List[Dict[str, str]]] = None,
    ) -> Dict[str, Any]:
        """
        Detect the intent from the user's message using LLM enhanced with semantic analysis.

        Args:
            message: User's natural language message
            model: LLM model to use

        Returns:
            Dictionary containing intent, confidence, and extracted information
        """
        # Check for slash commands first (instant, no LLM needed)
        slash_result = self._detect_slash_command(message)
        if slash_result is not None:
            return slash_result

        # Perform semantic analysis first
        semantic_analysis = self._analyze_message_semantics(message)
        # Get prompt template from database
        system_prompt = await self.template_service.get_template_content(
            "detect_intent"
        )

        if not system_prompt:
            # Use a crew-first default prompt if template not found
            system_prompt = """You are an intent detection system for a CrewAI workflow designer.

CRITICAL DEFAULT RULE: The default intent is ALWAYS "generate_crew" with confidence 0.95.
A crew can contain a single agent with a single task, making it the safest and most flexible choice.
Only use a different intent when there is EXPLICIT evidence for it.

The ONLY cases where you should NOT return generate_crew:

1. **generate_agent**: User EXPLICITLY says "create an agent", "make me a bot", "build an assistant".
   Must contain the word "agent", "bot", "assistant", or "chatbot" as the entity being created.

2. **generate_task**: User EXPLICITLY says "create a task" or "add a task". The word "task" must appear.

3. **execute_crew**: User says "execute", "run", "start", "launch", or "ec".

4. **configure_crew**: User wants to change LLM model, max RPM, tools, or settings.

5. **catalog/flow operations**: list, load, save, schedule, or delete plans/flows/crews.

For ALL other messages return generate_crew with confidence 0.95.

Return a JSON object with:
{
    "intent": "generate_task" | "generate_agent" | "generate_crew" | "execute_crew" | "configure_crew" | "catalog_list" | "catalog_load" | "catalog_save" | "catalog_schedule" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        "action_words": ["detected", "action", "words"],
        "entities": ["extracted", "entities"],
        "goal": "what the user wants to accomplish",
        "config_type": "llm|maxr|tools|general"
    },
    "suggested_prompt": "Enhanced version optimized for the specific service",
    "suggested_tools": ["ToolName1", "ToolName2"]
}

Examples of generate_crew (the DEFAULT):
- "get me the latest news from switzerland" -> generate_crew
- "analyze market trends and create a report" -> generate_crew
- "find the best flights and hotels" -> generate_crew
- "gather news from cnn.com and create a dashboard" -> generate_crew
- "Build a team of agents to handle customer support" -> generate_crew
- "Create a plan for market analysis" -> generate_crew

Examples of other intents (ONLY with explicit signals):
- "Create an agent that can analyze data" -> generate_agent
- "create a task to check server status" -> generate_task
- "execute crew" -> execute_crew
- "ec" -> execute_crew
- "configure crew" -> configure_crew
- "setup llm" -> configure_crew
- "change model" -> configure_crew
- "list my plans" -> catalog_list
- "load the research plan" -> catalog_load
- "save this plan" -> catalog_save
- "schedule this crew" -> catalog_schedule

"""

        # Enhance the user message with factual observations only
        # NOTE: We intentionally do NOT inject intent scores or suggested intent
        # to avoid anchoring the LLM toward a biased classification.
        observations = []
        if semantic_analysis.get("has_multi_step"):
            observations.append("Multi-step workflow indicators detected")
        if semantic_analysis.get("has_explicit_agent"):
            observations.append("Explicit agent creation pattern detected")
        if semantic_analysis.get("has_explicit_task"):
            observations.append("Explicit task creation pattern detected")
        if semantic_analysis.get("execute_keywords"):
            observations.append(f"Execution words: {', '.join(semantic_analysis['execute_keywords'])}")
        if semantic_analysis.get("configure_keywords"):
            observations.append(f"Configuration words: {', '.join(semantic_analysis['configure_keywords'])}")
        if semantic_analysis.get("has_configure_structure"):
            observations.append("Configuration structure detected")

        enhanced_user_message = f"""Message: {message}

Observations:
{chr(10).join(f'- {obs}' for obs in observations) if observations else '- No special patterns detected (default: generate_crew)'}

Please analyze this message and provide your intent classification."""

        # Append tool catalog so the LLM can suggest relevant tools
        if available_tools:
            enhanced_user_message += self._build_tool_catalog(available_tools)

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": enhanced_user_message},
        ]

        # Circuit breaker check — fail fast if model is in open state
        if self._check_circuit_breaker(model):
            semantic_confidence = (
                max(semantic_analysis["intent_scores"].values())
                / self.SEMANTIC_CONFIDENCE_NORMALIZER
            )
            # Default to crew when circuit breaker is open
            suggested = semantic_analysis["suggested_intent"]
            if suggested in ("generate_task", "generate_agent") and semantic_confidence < 0.8:
                suggested = "generate_crew"
            return {
                "intent": (
                    suggested
                    if semantic_confidence > self.SEMANTIC_FALLBACK_MIN_CONFIDENCE
                    else "generate_crew"
                ),
                "confidence": min(1.0, max(
                    self.DEFAULT_FALLBACK_CONFIDENCE, semantic_confidence
                )),
                "extracted_info": {"semantic_analysis": semantic_analysis},
                "suggested_prompt": message,
                "source": "circuit_breaker_fallback",
                "suggested_tools": [],
            }

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
            return cached

        try:
            # Acquire concurrency semaphore to limit parallel LLM calls
            async with self._get_semaphore():
                # Generate completion with optional MLflow span for tracing
                if _HAS_MLFLOW and hasattr(_mlflow, "start_span"):
                    with _mlflow.start_span(
                        name="intent_detection", span_type="LLM"
                    ) as intent_span:
                        if hasattr(intent_span, "set_inputs"):
                            intent_span.set_inputs(
                                {
                                    "model": model,
                                    "messages": messages,
                                    "temperature": 0.3,
                                }
                            )
                        content = await self._call_llm_with_retry(
                            messages=messages, model=model
                        )
                        if hasattr(intent_span, "set_outputs"):
                            intent_span.set_outputs(
                                {"response": content[:500] if content else ""}
                            )
                else:
                    content = await self._call_llm_with_retry(
                        messages=messages, model=model
                    )

            # Record success for circuit breaker
            self._record_success(model)

            # Check if content is empty
            if not content or not content.strip():
                logger.warning(
                    f"LLM returned empty response for intent detection with model {model}"
                )
                # Fall back to crew as default
                return {
                    "intent": "generate_crew",
                    "confidence": self.DEFAULT_FALLBACK_CONFIDENCE,
                    "extracted_info": {},
                    "source": "semantic_fallback",
                    "suggested_tools": [],
                }

            # Parse the response
            result = robust_json_parser(content)

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

            # Enhance extracted_info with semantic analysis
            result["extracted_info"]["semantic_analysis"] = semantic_analysis

            # Semantic override — only for non-crew intents.
            # The semantic layer can override the LLM for execute, configure,
            # and catalog intents, but it should NEVER downgrade crew to task
            # or agent, since crew is the safe default.
            semantic_confidence = (
                max(semantic_analysis["intent_scores"].values())
                / self.SEMANTIC_CONFIDENCE_NORMALIZER
            )
            semantic_suggested = semantic_analysis["suggested_intent"]

            if (
                semantic_confidence > self.SEMANTIC_OVERRIDE_THRESHOLD
                and result["confidence"] < self.LLM_CONFIDENCE_WEAK_THRESHOLD
                # Only override if semantic suggests a non-generation intent
                # (execute, configure, catalog) OR if LLM returned a weaker
                # generation intent and semantic suggests crew
                and semantic_suggested not in ("generate_task", "generate_agent")
            ):
                logger.info(
                    f"Using semantic analysis suggestion: {semantic_suggested} "
                    f"(confidence: {semantic_confidence:.2f}) over LLM result: "
                    f"{result['intent']} (confidence: {result['confidence']:.2f})"
                )
                result["intent"] = semantic_suggested
                result["confidence"] = min(1.0, max(result["confidence"], semantic_confidence))

            result["source"] = "llm"

            # Cache successful LLM results (never cache fallback/degraded)
            await intent_cache.set(group_id, cache_key, result)

            return result

        except Exception as e:
            logger.error(f"Error detecting intent: {str(e)}")
            self._record_failure(model)
            # Fall back to semantic analysis if LLM fails.
            # Default to generate_crew (the safe default) unless semantic
            # analysis strongly suggests a specific non-crew intent.
            semantic_confidence = (
                max(semantic_analysis["intent_scores"].values())
                / self.SEMANTIC_CONFIDENCE_NORMALIZER
            )
            semantic_suggested = semantic_analysis["suggested_intent"]

            # Only use non-crew semantic suggestions if they're very confident
            if (
                semantic_suggested in ("generate_task", "generate_agent")
                and semantic_confidence < 0.8
            ):
                semantic_suggested = "generate_crew"

            return {
                "intent": (
                    semantic_suggested
                    if semantic_confidence > self.SEMANTIC_FALLBACK_MIN_CONFIDENCE
                    else "generate_crew"
                ),
                "confidence": min(1.0, max(
                    self.DEFAULT_FALLBACK_CONFIDENCE, semantic_confidence
                )),
                "extracted_info": {"semantic_analysis": semantic_analysis},
                "suggested_prompt": message,
                "source": "semantic_fallback",
                "suggested_tools": [],
            }

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
                from src.services.mlflow_tracing_service import (
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

            # Detect intent
            intent_result = await self.detect_intent(
                request.message, model, group_context, available_tools
            )

            # Log the intent detection to DB (separate from MLflow)
            await self._log_llm_interaction(
                endpoint="detect-intent",
                prompt=request.message,
                response=str(intent_result),
                model=model,
                group_context=group_context,
            )

            # Create dispatcher response
            dispatcher_response = DispatcherResponse(
                intent=IntentType(intent_result["intent"]),
                confidence=max(0.0, min(1.0, float(intent_result["confidence"]))),
                extracted_info=intent_result["extracted_info"],
                suggested_prompt=intent_result["suggested_prompt"],
                source=intent_result.get("source"),
                suggested_tools=intent_result.get("suggested_tools", []),
            )

            # Resolve workspace tools: use user-selected tools if provided,
            # otherwise fetch all enabled workspace tools so the LLM can
            # make informed tool assignments based on actual availability.
            if request.tools:
                effective_tools = request.tools
            else:
                try:
                    from src.services.tool_service import ToolService
                    tool_svc = ToolService(self.session)
                    if group_context:
                        tools_resp = await tool_svc.get_enabled_tools_for_group(group_context)
                    else:
                        tools_resp = await tool_svc.get_enabled_tools()
                    effective_tools = [t.title for t in tools_resp.tools]
                except Exception as e:
                    logger.warning(f"Failed to fetch workspace tools, falling back to suggested: {e}")
                    effective_tools = intent_result.get("suggested_tools", [])

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
                        original_prompt=request.message,
                        model=request.model,
                        tools=effective_tools or [],
                    )
                    # Spawn progressive generation in background
                    asyncio.create_task(
                        self.crew_service.create_crew_progressive(
                            streaming_request, group_context, generation_id
                        )
                    )
                    generation_result = {
                        "generation_id": generation_id,
                        "type": "streaming",
                    }

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
                                    "planning": crew.planning,
                                    "planning_llm": crew.planning_llm,
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
                                        "planning": crew.planning,
                                        "planning_llm": crew.planning_llm,
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
                                    "planning": crew.planning,
                                    "planning_llm": crew.planning_llm,
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
                                        "planning": crew.planning,
                                        "planning_llm": crew.planning_llm,
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
                    command_help = dispatcher_response.extracted_info.get("command_help", "")
                    # Invalid/unrecognized command prefix
                    invalid = dispatcher_response.extracted_info.get("invalid_command", False)
                    invalid_cmd = dispatcher_response.extracted_info.get("command", "")
                    invalid_prefix = f"Unknown command `{invalid_cmd}`.\n\n" if invalid else ""

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
