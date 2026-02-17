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
from src.schemas.crew import CrewGenerationRequest, CrewGenerationResponse
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
    SEMANTIC_CONFIDENCE_NORMALIZER = 5.0
    SEMANTIC_FALLBACK_MIN_CONFIDENCE = 0.3
    SEMANTIC_OVERRIDE_THRESHOLD = 0.6
    LLM_CONFIDENCE_WEAK_THRESHOLD = 0.7
    DEFAULT_FALLBACK_CONFIDENCE = 0.3

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

    # Task-related action words that indicate the user wants to create a task
    TASK_ACTION_WORDS = {
        "find",
        "search",
        "locate",
        "discover",
        "identify",
        "get",
        "fetch",
        "retrieve",
        "analyze",
        "examine",
        "study",
        "investigate",
        "review",
        "assess",
        "evaluate",
        "create",
        "make",
        "build",
        "generate",
        "produce",
        "develop",
        "construct",
        "write",
        "compose",
        "draft",
        "prepare",
        "document",
        "record",
        "note",
        "calculate",
        "compute",
        "determine",
        "measure",
        "count",
        "sum",
        "total",
        "compare",
        "contrast",
        "match",
        "relate",
        "connect",
        "link",
        "associate",
        "organize",
        "sort",
        "arrange",
        "group",
        "categorize",
        "classify",
        "order",
        "summarize",
        "abstract",
        "condense",
        "outline",
        "highlight",
        "extract",
        "process",
        "handle",
        "manage",
        "coordinate",
        "execute",
        "perform",
        "run",
        "check",
        "verify",
        "validate",
        "confirm",
        "test",
        "inspect",
        "audit",
        "monitor",
        "track",
        "watch",
        "observe",
        "follow",
        "supervise",
        "oversee",
        "update",
        "modify",
        "change",
        "edit",
        "revise",
        "adjust",
        "alter",
        "send",
        "deliver",
        "transmit",
        "forward",
        "share",
        "distribute",
        "dispatch",
        "collect",
        "gather",
        "compile",
        "accumulate",
        "assemble",
        "combine",
        "convert",
        "transform",
        "translate",
        "adapt",
        "format",
        "parse",
        "decode",
    }

    # Agent-related keywords
    AGENT_KEYWORDS = {
        "agent",
        "assistant",
        "bot",
        "robot",
        "ai",
        "helper",
        "specialist",
        "expert",
        "analyst",
        "advisor",
        "consultant",
        "operator",
        "worker",
    }

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
        }
        if args.lower().startswith(("flow", "flows")) and intent in FLOW_INTENT_MAP:
            intent = FLOW_INTENT_MAP[intent]
            qualifier_found = True
            # Strip "flow" or "flows" prefix from args
            remaining = args.split(None, 1)
            args = remaining[1].strip() if len(remaining) > 1 else ""

        # Check for crew/crews qualifier (e.g. "/list crews", "/save crew My Crew")
        CREW_QUALIFIABLE = {"catalog_list", "catalog_load", "catalog_save", "catalog_schedule", "execute_crew"}
        if not qualifier_found and args.lower().startswith(("crew", "crews")) and intent in CREW_QUALIFIABLE:
            qualifier_found = True
            remaining = args.split(None, 1)
            args = remaining[1].strip() if len(remaining) > 1 else ""

        # Commands that require a crew/flow qualifier (bare /list, /load etc. show usage help)
        # /plans and /flows are aliases that already imply the qualifier, so they're excluded.
        QUALIFIER_REQUIRED = {"/list", "/load", "/save", "/run", "/exec", "/schedule"}
        if not qualifier_found and command in QUALIFIER_REQUIRED:
            COMMAND_USAGE = {
                "/list": "Usage: `/list crews` or `/list flows`",
                "/load": "Usage: `/load crew <name>` or `/load flow <name>`",
                "/save": "Usage: `/save crew [name]` or `/save flow [name]`",
                "/run": "Usage: `/run crew` or `/run flow`",
                "/exec": "Usage: `/run crew` or `/run flow`",
                "/schedule": "Usage: `/schedule crew`",
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

        Args:
            message: User's natural language message

        Returns:
            Dictionary containing semantic analysis results
        """
        # Normalize message for analysis
        words = re.findall(r"\b\w+\b", message.lower())
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

        # Detect command-like structures
        command_patterns = [
            r"^(find|get|create|make|build|search|analyze)",  # Starts with action
            r"^(i need|i want|help me|can you)",  # Request patterns
            r"^(an order|a task|a job)",  # Task-like prefixes
        ]

        # Detect configuration patterns
        configure_patterns = [
            r"(configure|config|setup|set up)",  # Configuration words
            r"(change|update|modify|adjust).*?(llm|model|tools|maxr|max|rpm)",  # Change configuration
            r"(select|choose|pick).*?(llm|model|tools)",  # Selection patterns
            r"(llm|model|tools|maxr).*?(setting|config)",  # Configuration contexts
        ]

        has_command_structure = any(
            re.search(pattern, message.lower()) for pattern in command_patterns
        )
        has_configure_structure = any(
            re.search(pattern, message.lower()) for pattern in configure_patterns
        )

        # Calculate intent suggestions based on semantic analysis
        # Check for complex multi-agent/multi-task workflows
        has_complex_task = len(task_actions) > 1 or bool(
            re.search(r"multiple|several|all|various|different", message.lower())
        )

        intent_scores = {
            "generate_task": len(task_actions) * 2
            + (1 if has_imperative else 0)
            + (1 if has_command_structure else 0),
            "generate_agent": len(agent_keywords) * 3,
            "generate_crew": len(crew_keywords) * 3
            + (2 if has_complex_task and crew_keywords else 0),
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

        if has_complex_task:
            semantic_hints.append("Complex multi-step task detected")
        if has_command_structure:
            semantic_hints.append("Command-like structure detected")
        if has_configure_structure:
            semantic_hints.append("Configuration structure detected")
        if has_imperative:
            semantic_hints.append("Imperative form detected")
        if has_question:
            semantic_hints.append("Question form detected")
        if has_greeting:
            semantic_hints.append("Conversational greeting detected")

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
            "has_command_structure": has_command_structure,
            "has_configure_structure": has_configure_structure,
            "has_complex_task": has_complex_task,
            "intent_scores": intent_scores,
            "semantic_hints": semantic_hints,
            "suggested_intent": (
                max(intent_scores, key=intent_scores.get)
                if max(intent_scores.values()) > 0
                else "unknown"
            ),
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
            # Use a default prompt if template not found
            system_prompt = """You are an intelligent intent detection system for a CrewAI workflow designer.

Analyze the user's message and determine their intent from these categories:

1. **generate_task**: User wants to create a single task or action. Look for:
   - Action words: find, search, analyze, create, write, calculate, etc.
   - Task descriptions: "find the best flight", "analyze this data", "write a report"
   - Instructions that could be automated: "get information about X", "compare Y and Z"
   - Casual requests that imply a task: "an order find...", "I need to...", "help me..."
   - Commands or directives: "find me", "get the", "calculate", "determine"

2. **generate_agent**: User wants to create a single agent with specific capabilities:
   - Explicit mentions of "agent", "assistant", "bot"
   - Role-based requests: "create a financial analyst", "I need a data scientist"
   - Capability-focused: "something that can analyze data and write reports"

3. **generate_crew**: User wants to create multiple agents and/or tasks working together:
   - Multiple roles mentioned: "team of agents", "research and writing team"
   - Complex workflows: "research then write then review"
   - Collaborative language: "agents working together", "workflow with multiple steps"
   - Planning language: "create a plan", "build a plan", "design a plan", "plan that", "plan to"
   - Strategic terms: "roadmap", "blueprint", "framework", "architecture", "strategy"
   - Complex multi-step operations: "get all news", "analyze multiple sources", "comprehensive collection"

4. **execute_crew**: User wants to execute/run an existing crew:
   - Execution commands: "execute crew", "run crew", "start crew", "ec"
   - Action words with crew context: "execute", "run", "start", "launch", "begin"
   - Short commands: "ec" (shorthand for execute crew)

6. **configure_crew**: User wants to configure workflow settings (LLM, max RPM, tools):
   - Configuration requests: "configure crew", "setup llm", "change model", "select tools"
   - Settings modifications: "update max rpm", "set llm model", "modify tools"
   - Preference adjustments: "choose different model", "adjust settings", "pick tools"
   - Direct mentions: "llm", "maxr", "max rpm", "tools", "config", "settings"

7. **catalog_list**: User wants to see available saved plans/crews:
   - Browse requests: "show my plans", "list saved crews", "what plans do I have"
   - Catalog browsing: "show catalog", "list available workflows"

8. **catalog_load**: User wants to load an existing plan onto the canvas:
   - Load requests: "load the research plan", "open my marketing crew"
   - Name references: includes the plan/crew name to load

9. **catalog_save**: User wants to save the current canvas as a plan:
   - Save requests: "save this plan", "save as my-research-crew"
   - Optionally includes a name for the plan

10. **catalog_schedule**: User wants to schedule a plan for automatic execution:
    - Schedule requests: "schedule this plan", "set up recurring execution"

11. **unknown**: Unclear or ambiguous messages that don't fit the above categories.

**CRITICAL RULES**:
1. Many task requests are phrased conversationally. Look for ACTION WORDS and GOALS rather than formal task language.
2. If the message describes multiple agents or complex workflows, it's generate_crew.

Return a JSON object with:
{
    "intent": "generate_task" | "generate_agent" | "generate_crew" | "execute_crew" | "configure_crew" | "catalog_list" | "catalog_load" | "catalog_save" | "catalog_schedule" | "unknown",
    "confidence": 0.0-1.0,
    "extracted_info": {
        "action_words": ["list", "of", "detected", "action", "words"],
        "entities": ["extracted", "entities", "or", "objects"],
        "goal": "what the user wants to accomplish",
        "config_type": "llm|maxr|tools|general" // Only for configure_crew intent
    },
    "suggested_prompt": "Enhanced version optimized for the specific service",
    "suggested_tools": ["ToolName1", "ToolName2"]  // Only from the available tools list, if provided
}

Examples:
- "Create an agent that can analyze data" -> generate_agent
- "I need a task to summarize documents" -> generate_task
- "an order find the best flight between zurich and montreal" -> generate_task
- "find me the cheapest hotel in paris" -> generate_task
- "get information about the weather tomorrow" -> generate_task
- "analyze this sales data and create a report" -> generate_task
- "Build a team of agents to handle customer support" -> generate_crew
- "Create a research agent and a writer agent with tasks for each" -> generate_crew
- "Create a plan for analyzing market data" -> generate_crew
- "Build a strategy with multiple agents" -> generate_crew
- "Design an approach using agents and tasks" -> generate_crew
- "Create a plan that will get all the news from switzerland" -> generate_crew
- "Plan to collect and analyze customer feedback" -> generate_crew- "execute crew" -> execute_crew
- "run crew" -> execute_crew
- "ec" -> execute_crew
- "start crew" -> execute_crew
- "launch crew" -> execute_crew
- "configure crew" -> configure_crew
- "setup llm" -> configure_crew
- "change model" -> configure_crew
- "select tools" -> configure_crew
- "update max rpm" -> configure_crew
- "adjust settings" -> configure_crew
- "list my plans" -> catalog_list
- "show saved crews" -> catalog_list
- "load the research plan" -> catalog_load
- "save this plan" -> catalog_save
- "schedule this crew" -> catalog_schedule

"""

        # Enhance the user message with semantic analysis
        enhanced_user_message = f"""Message: {message}

Semantic Analysis:
- Detected action words: {', '.join(semantic_analysis['task_actions']) if semantic_analysis['task_actions'] else 'None'}
- Agent keywords: {', '.join(semantic_analysis['agent_keywords']) if semantic_analysis['agent_keywords'] else 'None'}
- Crew keywords: {', '.join(semantic_analysis['crew_keywords']) if semantic_analysis['crew_keywords'] else 'None'}
- Execute keywords: {', '.join(semantic_analysis['execute_keywords']) if semantic_analysis['execute_keywords'] else 'None'}
- Configure keywords: {', '.join(semantic_analysis['configure_keywords']) if semantic_analysis['configure_keywords'] else 'None'}
- Catalog keywords: {', '.join(semantic_analysis['catalog_keywords']) if semantic_analysis['catalog_keywords'] else 'None'}
- Has imperative form: {semantic_analysis['has_imperative']}
- Has question form: {semantic_analysis['has_question']}
- Has command structure: {semantic_analysis['has_command_structure']}
- Has configure structure: {semantic_analysis['has_configure_structure']}
- Has complex multi-step task: {semantic_analysis.get('has_complex_task', False)}
- Semantic hints: {'; '.join(semantic_analysis['semantic_hints']) if semantic_analysis['semantic_hints'] else 'None'}
- Intent scores: {semantic_analysis.get('intent_scores', {})}
- Suggested intent from analysis: {semantic_analysis['suggested_intent']}


Please analyze this message and provide your intent classification, considering both the semantic analysis and the natural language content."""

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
            return {
                "intent": (
                    semantic_analysis["suggested_intent"]
                    if semantic_confidence > self.SEMANTIC_FALLBACK_MIN_CONFIDENCE
                    else "unknown"
                ),
                "confidence": max(
                    self.DEFAULT_FALLBACK_CONFIDENCE, semantic_confidence
                ),
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
                # Fall back to semantic analysis
                semantic_confidence = (
                    max(semantic_analysis["intent_scores"].values())
                    / self.SEMANTIC_CONFIDENCE_NORMALIZER
                )
                return {
                    "intent": (
                        semantic_analysis["suggested_intent"]
                        if semantic_confidence > self.SEMANTIC_FALLBACK_MIN_CONFIDENCE
                        else "unknown"
                    ),
                    "confidence": max(
                        self.DEFAULT_FALLBACK_CONFIDENCE, semantic_confidence
                    ),
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

            # If LLM result seems wrong and semantic analysis is confident, use semantic analysis
            semantic_confidence = (
                max(semantic_analysis["intent_scores"].values())
                / self.SEMANTIC_CONFIDENCE_NORMALIZER
            )

            if (
                semantic_confidence > self.SEMANTIC_OVERRIDE_THRESHOLD
                and result["confidence"] < self.LLM_CONFIDENCE_WEAK_THRESHOLD
            ):
                logger.info(
                    f"Using semantic analysis suggestion: {semantic_analysis['suggested_intent']} (confidence: {semantic_confidence:.2f}) over LLM result: {result['intent']} (confidence: {result['confidence']:.2f})"
                )
                result["intent"] = semantic_analysis["suggested_intent"]
                result["confidence"] = max(result["confidence"], semantic_confidence)

            result["source"] = "llm"

            # Cache successful LLM results (never cache fallback/degraded)
            await intent_cache.set(group_id, cache_key, result)

            return result

        except Exception as e:
            logger.error(f"Error detecting intent: {str(e)}")
            self._record_failure(model)
            # Fall back to semantic analysis if LLM fails
            semantic_confidence = (
                max(semantic_analysis["intent_scores"].values())
                / self.SEMANTIC_CONFIDENCE_NORMALIZER
            )
            return {
                "intent": (
                    semantic_analysis["suggested_intent"]
                    if semantic_confidence > self.SEMANTIC_FALLBACK_MIN_CONFIDENCE
                    else "unknown"
                ),
                "confidence": max(
                    self.DEFAULT_FALLBACK_CONFIDENCE, semantic_confidence
                ),
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
                confidence=intent_result["confidence"],
                extracted_info=intent_result["extracted_info"],
                suggested_prompt=intent_result["suggested_prompt"],
                source=intent_result.get("source"),
                suggested_tools=intent_result.get("suggested_tools", []),
            )

            # Use LLM-suggested tools when user didn't provide any
            effective_tools = (
                request.tools
                if request.tools
                else intent_result.get("suggested_tools", [])
            )

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
                    crew_request = CrewGenerationRequest(
                        prompt=dispatcher_response.suggested_prompt or request.message,
                        model=request.model,
                        tools=effective_tools,
                    )
                    generation_result = await self.crew_service.create_crew_complete(
                        crew_request, group_context, fast_planning=True
                    )

                elif dispatcher_response.intent == IntentType.EXECUTE_CREW:
                    generation_result = {
                        "type": "execute_crew",
                        "message": "Executing crew...",
                        "action": "execute_crew",
                        "extracted_info": dispatcher_response.extracted_info,
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
                        "- `/schedule crew` — Schedule the current crew for automatic execution\n"
                        "\n"
                        "**Flow Commands:**\n"
                        "- `/list flows` — List all saved flows\n"
                        "- `/load flow <name>` — Load a saved flow onto the canvas\n"
                        "- `/save flow [name]` — Save the current flow\n"
                        "- `/run flow` — Execute the current flow\n"
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
