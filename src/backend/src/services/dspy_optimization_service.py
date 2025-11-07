"""
DSPy Optimization Service for prompt optimization in dispatcher, agent, task, and crew generation.

This service integrates DSPy with MLflow 3.0 for automatic prompt optimization using
production traces, vector search for example retrieval, and continuous learning.
"""

import asyncio
import json
import logging
import os
import threading

from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Dict, Any, List, Optional, Tuple
from uuid import UUID
from dataclasses import dataclass, field
import hashlib

import dspy
from dspy import Signature, InputField, OutputField, Module, ChainOfThought
from dspy.teleprompt import BootstrapFewShot, BootstrapFewShotWithRandomSearch, MIPROv2
import mlflow
import mlflow.dspy
import numpy as np
from pydantic import BaseModel, Field

from src.services.mlflow_service import MLflowService
from src.services.databricks_service import DatabricksService
from src.repositories.databricks_vector_index_repository import DatabricksVectorIndexRepository
from src.schemas.databricks_vector_index import IndexType
from src.schemas.databricks_index_schemas import DatabricksIndexSchemas
from src.core.llm_manager import LLMManager
from src.utils.user_context import GroupContext
from src.repositories.dspy_config_repository import DSPyConfigRepository
from src.schemas.dspy_schemas import (
    OptimizationType,
    DeploymentStage,
    OptimizationStatus,
    ExampleSourceType,
)

# In-process coordination primitives
_GROUP_OPT_LOCKS: Dict[str, asyncio.Lock] = {}
_TRACE_HYDRATION_LOCK: Optional[asyncio.Lock] = None


def _get_group_lock(key: str) -> asyncio.Lock:
    lock = _GROUP_OPT_LOCKS.get(key)
    if lock is None:
        lock = asyncio.Lock()
        _GROUP_OPT_LOCKS[key] = lock
    return lock


def _get_trace_hydration_lock() -> asyncio.Lock:
    global _TRACE_HYDRATION_LOCK
    if _TRACE_HYDRATION_LOCK is None:
        _TRACE_HYDRATION_LOCK = asyncio.Lock()
    return _TRACE_HYDRATION_LOCK

logger = logging.getLogger(__name__)



class DSPyExample(BaseModel):
    """Training example for DSPy optimization."""
    input: Dict[str, Any]
    output: Dict[str, Any]
    score: float = Field(default=0.0, description="Quality score from 0-1")
    trace_id: Optional[str] = None
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = Field(default_factory=dict)


@dataclass
class OptimizationConfig:
    """Configuration for DSPy optimization."""
    max_bootstrapped_demos: int = 4
    max_labeled_demos: int = 4
    num_candidate_programs: int = 10
    num_threads: int = 4
    min_examples_for_optimization: int = 10
    optimization_interval_hours: int = 24
    vector_search_top_k: int = 10
    min_quality_score: float = 0.7
    use_random_search: bool = True
    use_mipro: bool = False  # For advanced optimization with 200+ examples


# DSPy Signatures for different generation types
class IntentDetectionSignature(Signature):
    """Detect user intent from natural language message."""
    message = dspy.InputField(desc="User's natural language message")
    semantic_hints = dspy.InputField(desc="Semantic analysis hints from keyword detection")

    intent = dspy.OutputField(desc="Detected intent: generate_task|generate_agent|generate_crew|execute_crew|configure_crew|unknown")
    confidence = dspy.OutputField(desc="Confidence score from 0.0 to 1.0")
    extracted_info = dspy.OutputField(desc="Extracted entities, action words, and goals as JSON")
    reasoning = dspy.OutputField(desc="Step-by-step reasoning for intent detection")


class AgentGenerationSignature(Signature):
    """Generate agent configuration from natural language description."""
    prompt = dspy.InputField(desc="Natural language description of desired agent")
    tools_available = dspy.InputField(desc="List of available tools with descriptions")

    agent_name = dspy.OutputField(desc="Generated agent name")
    role = dspy.OutputField(desc="Agent's role description")
    backstory = dspy.OutputField(desc="Agent's backstory and expertise")
    goal = dspy.OutputField(desc="Agent's primary goal")
    tools = dspy.OutputField(desc="List of tool names the agent should use")
    reasoning = dspy.OutputField(desc="Reasoning for agent configuration choices")


class TaskGenerationSignature(Signature):
    """Generate task configuration from natural language description."""
    prompt = dspy.InputField(desc="Natural language description of desired task")
    agent_context = dspy.InputField(desc="Optional context about available agents")

    task_name = dspy.OutputField(desc="Generated task name")
    description = dspy.OutputField(desc="Detailed task description")
    expected_output = dspy.OutputField(desc="Expected output format and content")
    tools = dspy.OutputField(desc="List of tools needed for the task")
    reasoning = dspy.OutputField(desc="Reasoning for task configuration")


class CrewGenerationSignature(Signature):
    """Generate complete crew configuration from natural language description."""
    prompt = dspy.InputField(desc="Natural language description of desired workflow")
    tools_available = dspy.InputField(desc="List of available tools with descriptions")

    crew_name = dspy.OutputField(desc="Generated crew name")
    agents = dspy.OutputField(desc="List of agent configurations as JSON")
    tasks = dspy.OutputField(desc="List of task configurations as JSON")
    workflow = dspy.OutputField(desc="Task dependencies and execution order as JSON")
    reasoning = dspy.OutputField(desc="Reasoning for crew architecture decisions")


# DSPy Modules using Chain of Thought
class IntentDetector(Module):
    """DSPy module for intent detection with Chain of Thought."""

    def __init__(self):
        super().__init__()
        self.detect = ChainOfThought(IntentDetectionSignature)

    def forward(self, message: str, semantic_hints: str) -> dspy.Prediction:
        return self.detect(message=message, semantic_hints=semantic_hints)


class AgentGenerator(Module):
    """DSPy module for agent generation with Chain of Thought."""

    def __init__(self):
        super().__init__()
        self.generate = ChainOfThought(AgentGenerationSignature)

    def forward(self, prompt: str, tools_available: str) -> dspy.Prediction:
        return self.generate(prompt=prompt, tools_available=tools_available)


class TaskGenerator(Module):
    """DSPy module for task generation with Chain of Thought."""

    def __init__(self):
        super().__init__()
        self.generate = ChainOfThought(TaskGenerationSignature)

    def forward(self, prompt: str, agent_context: str = "") -> dspy.Prediction:
        return self.generate(prompt=prompt, agent_context=agent_context)


class CrewGenerator(Module):
    """DSPy module for crew generation with Chain of Thought."""

    def __init__(self):
        super().__init__()
        self.generate = ChainOfThought(CrewGenerationSignature)

    def forward(self, prompt: str, tools_available: str) -> dspy.Prediction:
        return self.generate(prompt=prompt, tools_available=tools_available)


class RateLimitedLM:
    """Thin wrapper around a DSPy LM that limits concurrent calls via a threading.Semaphore.
    Works inside optimizer.compile(), which executes in a worker thread.
    """
    def __init__(self, base_lm, semaphore: threading.Semaphore):
        self._base = base_lm
        self._sem = semaphore

    def __getattr__(self, name):
        # Delegate attribute access to underlying LM
        return getattr(self._base, name)

    def __call__(self, *args, **kwargs):
        self._sem.acquire()
        try:
            return self._base(*args, **kwargs)
        finally:
            self._sem.release()





class DSPyOptimizationService:
    """Service for managing DSPy prompt optimization with MLflow and Vector Search."""

    def __init__(self, session: Any):
        """Initialize the optimization service with session."""
        self.session = session
        self.group_context = None  # Will be set per method call
        self.group_id = None  # Will be set when group_context is set

        # Initialize services - will be initialized when group context is set
        self.mlflow_service = None
        self.databricks_service = None

        # DSPy modules (will be loaded or initialized)
        self.modules: Dict[OptimizationType, Module] = {}
        self.optimized_modules: Dict[OptimizationType, Module] = {}

        # Configuration
        self.config = OptimizationConfig()

        # DSPy will be initialized when needed (async)
        self._dspy_initialized = False

        logger.info("DSPyOptimizationService initialized")

    def _ensure_services_initialized(self):
        """Ensure services are initialized with current group context."""
        if self.group_context and not self.mlflow_service:
            self.group_id = getattr(self.group_context, "primary_group_id", None)
            self.mlflow_service = MLflowService(self.session, group_id=self.group_id)

        if self.group_context and not self.databricks_service:
            self.databricks_service = DatabricksService(
                session=self.session,
                group_id=self.group_id  # Pass group_id, not group_context
            )

    async def _get_validated_group_id(self) -> Optional[str]:
        """Get validated group_id that exists in the database."""
        if not self.group_context or not self.group_context.primary_group_id:
            return None

        # Check if group exists
        from sqlalchemy import select
        from src.models.group import Group

        result = await self.session.execute(
            select(Group).where(Group.id == self.group_context.primary_group_id)
        )
        if result.scalar():
            return self.group_context.primary_group_id
        else:
            logger.warning(f"Group {self.group_context.primary_group_id} not found, using global context")
            return None

    async def _initialize_dspy(self):
        """Initialize DSPy with the configured LLM using LLMManager."""
        try:
            from src.core.llm_manager import LLMManager

            # Use the same model as dispatcher by default
            model_name = os.getenv("DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick")

            # Get LLM configuration from LLMManager
            model_params = await LLMManager.configure_litellm(model_name)

            # Create DSPy LM using the LLMManager configuration
            import dspy
            lm = dspy.LM(
                model=model_params["model"],
                api_key=model_params.get("api_key", ""),
                api_base=model_params.get("api_base"),
                temperature=0.3
            )

            # Wrap with a small concurrency limiter to avoid overwhelming the service
            try:
                max_cc = int(os.getenv("DSPY_MAX_CONCURRENT_LLM", "4"))
            except Exception:
                max_cc = 4
            if max_cc and max_cc > 0:
                if not hasattr(self, "_lm_semaphore") or getattr(self, "_lm_semaphore", None) is None or getattr(self, "_lm_semaphore", None)._value != max_cc:
                    try:
                        self._lm_semaphore = threading.BoundedSemaphore(max_cc)
                    except Exception:
                        self._lm_semaphore = threading.Semaphore(max_cc)
                lm = RateLimitedLM(lm, self._lm_semaphore)

            dspy.configure(lm=lm)
            logger.info(f"DSPy configured with model: {model_name} via LLMManager (max_concurrent_llm={max_cc})")

        except Exception as e:
            logger.error(f"Failed to initialize DSPy with LLMManager: {e}")
            # Fallback to direct configuration
            self._initialize_dspy_fallback()

    def _initialize_dspy_fallback(self):
        """Fallback DSPy initialization without LLMManager."""
        # Use the same model as dispatcher by default
        model_name = os.getenv("DEFAULT_DISPATCHER_MODEL", "databricks-llama-4-maverick")

        # Configure DSPy LM
        # For Databricks models, use the appropriate adapter
        if model_name.startswith("databricks-"):
            # Use OpenAI-compatible interface for Databricks
            import dspy
            # Use unified authentication
            from src.utils.databricks_auth import get_auth_context
            import asyncio
            auth = asyncio.run(get_auth_context())
            api_base = auth.workspace_url + "/serving-endpoints" if auth else "https://example.databricks.com/serving-endpoints"
            api_key = auth.token if auth else ""

            lm = dspy.LM(
                model=f"openai/{model_name}",
                api_base=api_base,
                api_key=api_key,
                temperature=0.3
            )
        else:
            # Use standard OpenAI or other providers
            lm = dspy.LM(model=model_name, temperature=0.3)

        # Wrap with a small concurrency limiter to avoid overwhelming the service
        try:
            max_cc = int(os.getenv("DSPY_MAX_CONCURRENT_LLM", "4"))
        except Exception:
            max_cc = 4
        if max_cc and max_cc > 0:
            if not hasattr(self, "_lm_semaphore") or getattr(self, "_lm_semaphore", None) is None or getattr(self, "_lm_semaphore", None)._value != max_cc:
                try:
                    self._lm_semaphore = threading.BoundedSemaphore(max_cc)
                except Exception:
                    self._lm_semaphore = threading.Semaphore(max_cc)
            lm = RateLimitedLM(lm, self._lm_semaphore)

        dspy.configure(lm=lm)
        logger.info(f"DSPy configured with fallback method: {model_name} (max_concurrent_llm={max_cc})")

    async def _enable_mlflow_tracing(self) -> bool:
        """Enable MLflow tracing for DSPy optimization."""
        try:
            self._ensure_services_initialized()
            if not self.mlflow_service:
                return False
            enabled = await self.mlflow_service.is_enabled()
            if not enabled:
                return False

            # Set up MLflow for DSPy
            mlflow.set_tracking_uri("databricks")

            # Use a dedicated experiment for DSPy optimization
            exp_name = os.getenv("MLFLOW_DSPY_EXPERIMENT", "/Shared/kasal-dspy-optimization")
            mlflow.set_experiment(exp_name)

            # Enable DSPy autologging
            mlflow.dspy.autolog(
                log_traces=True,
                log_models=True,
                log_input_examples=True
            )

            logger.info(f"MLflow DSPy tracking enabled in experiment: {exp_name}")
            return True

        except Exception as e:
            logger.warning(f"Failed to enable MLflow tracing for DSPy: {e}")
            return False

    async def _get_vector_repository(self, optimization_type: OptimizationType) -> Optional[Dict[str, Any]]:
        """Resolve document embeddings index settings for vector augmentation."""
        try:
            # Prefer the workspace's document embeddings index (not entity memory)
            from src.services.memory_config_service import MemoryConfigService
            from src.schemas.memory_backend import MemoryBackendType

            svc = MemoryConfigService(self.session)
            cfg = await svc.get_active_config(self.group_id)
            if not cfg or cfg.backend_type != MemoryBackendType.DATABRICKS or not cfg.databricks_config:
                return None

            db = cfg.databricks_config
            index_name = getattr(db, "document_index", None)
            endpoint_name = getattr(db, "document_endpoint_name", None) or db.endpoint_name
            workspace_url = db.workspace_url
            embedding_dimension = getattr(db, "embedding_dimension", 1024) or 1024

            if not index_name or not endpoint_name or not workspace_url:
                logger.info("Document embeddings index not configured; vector augmentation will be skipped.")
                return None

            return {
                "workspace_url": workspace_url,
                "index_name": index_name,
                "endpoint_name": endpoint_name,
                "embedding_dimension": embedding_dimension,
            }
        except Exception as e:
            logger.warning(f"Failed to resolve document embeddings index: {e}")
            return None

    def _generate_example_id(self, example: DSPyExample, optimization_type: OptimizationType) -> str:
        """Generate unique ID for an example."""
        content = f"{optimization_type.value}_{json.dumps(example.input)}_{json.dumps(example.output)}"
        return hashlib.md5(content.encode()).hexdigest()

    async def collect_examples_from_traces(self, optimization_type: OptimizationType, hours_back: int = 24) -> List[DSPyExample]:
        """Collect training examples from MLflow traces (MLflow 3.0 GenAI API).
        Uses mlflow.search_traces when available; otherwise returns an empty list.
        """
        examples: List[DSPyExample] = []
        # Single-flight hydration: ensure only one MLflow trace collection runs at a time
        _lock = _get_trace_hydration_lock()
        if _lock.locked():
            logger.info("Trace hydration already running; skipping concurrent collect_examples_from_traces call")
            return []
        await _lock.acquire()
        try:
            # Ensure we talk to Databricks tracking and locate the crew traces experiment
            mlflow.set_tracking_uri("databricks")
            exp_name = os.getenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/kasal-crew-execution-traces")
            experiment = mlflow.get_experiment_by_name(exp_name)
            if not experiment:
                logger.warning(f"MLflow experiment {exp_name} not found")
                return examples

            # MLflow 3.0+: prefer the new search_traces API
            search_traces = getattr(mlflow, "search_traces", None)
            if not callable(search_traces):
                logger.warning("mlflow.search_traces is not available in this MLflow version; skipping trace collection")
                return examples

            # Fetch traces associated with the experiment; filter in-Python by time window
            from datetime import datetime, timedelta, timezone
            cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_back)
            cutoff_ms = int(cutoff_time.timestamp() * 1000)

            try:
                df = search_traces(experiment_ids=[str(experiment.experiment_id)])
            except TypeError:
                # Some builds may expose a different signature; fall back to no-arg call
                df = search_traces()

            if df is None or len(df) == 0:
                logger.info(f"No traces found in experiment {exp_name}")
                return examples

            # Normalize and time-filter
            try:
                # Common columns: 'start_time', 'timestamp', 'trace_id', 'name', 'attributes', 'tags'
                import numpy as _np

                def _to_ms(series):
                    # Convert epoch series to milliseconds intelligently (ns/us/ms)
                    s = series.astype("int64").abs()
                    # Heuristic scaling based on magnitude
                    # ns ~ 1e18, us ~ 1e15, ms ~ 1e12, s ~ 1e9
                    if (s > 10**15).any():
                        return (series.astype("int64") // 10**6)
                    if (s > 10**12).any():
                        return (series.astype("int64") // 10**3)
                    # Already ms (or s); if seconds, multiply
                    if (s < 10**11).any():
                        # Treat values <1e11 as seconds, convert to ms
                        return (series.astype("int64") * 1000)
                    return series.astype("int64")

                if "start_time" in df.columns:
                    df_ts = _to_ms(df["start_time"])
                    df = df[df_ts >= cutoff_ms]
                elif "timestamp" in df.columns:
                    df_ts = _to_ms(df["timestamp"])
                    df = df[df_ts >= cutoff_ms]
            except Exception:
                pass

            # Limits: stop after N traces or once we have enough examples
            try:
                _max_traces = int(os.getenv("DSPY_MAX_TRACE_DOWNLOADS", "50"))
            except Exception:
                _max_traces = 50
            if _max_traces < 1:
                _max_traces = 1
            _min_needed = int(getattr(self.config, "min_examples_for_optimization", 10) or 10)
            _traces_seen = 0

            # Iterate traces and build DSPyExamples with best-effort extraction
            for _, row in df.iterrows():
                if _traces_seen >= _max_traces:
                    logger.info(f"Stopping trace hydration after {_traces_seen} traces (DSPY_MAX_TRACE_DOWNLOADS={_max_traces})")
                    break
                if len(examples) >= _min_needed:
                    logger.info(f"Collected required examples ({len(examples)}) >= {_min_needed}; stopping hydration early")
                    break
                _traces_seen += 1
                try:
                    trace_row = row.to_dict()
                    if optimization_type == OptimizationType.INTENT_DETECTION:
                        ex = self._extract_intent_example_from_trace(trace_row)
                    else:
                        ex = self._extract_crew_example_from_trace(trace_row)
                    if ex:
                        examples.append(ex)
                except Exception:
                    continue

            logger.info(f"Collected {len(examples)} examples from MLflow traces for {optimization_type.value} (traces_scanned={_traces_seen})")

        except Exception as e:
            logger.error(f"Failed to collect examples from traces: {e}")

        finally:
            _lock.release()
        return examples


    def _load_trace_spans(self, trace_row: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Best-effort loader for MLflow 3.x trace spans from artifacts.
        Expects a row from mlflow.search_traces(). Attempts to read tags.mlflow.artifactLocation
        and fetch <artifactLocation>/traces.json via mlflow.artifacts APIs.
        Returns a list of span dicts, or [] on failure.
        """
        try:
            tags = trace_row.get("tags") if isinstance(trace_row, dict) else None
            artifact_loc = None
            if isinstance(tags, dict):
                artifact_loc = tags.get("mlflow.artifactLocation") or tags.get("artifact_location")
            if not artifact_loc:
                return []

            artifact_uri = str(artifact_loc).rstrip("/") + "/traces.json"

            # Try load_text first (fast path)
            try:
                import mlflow
                art_ns = getattr(mlflow, "artifacts", None)
                load_text = getattr(art_ns, "load_text", None) if art_ns else None
                if callable(load_text):
                    content = load_text(artifact_uri=artifact_uri)
                    import json as _json
                    data = _json.loads(content)
                    if isinstance(data, dict) and isinstance(data.get("spans"), list):
                        return data.get("spans", [])
                    if isinstance(data, list):
                        return data
            except Exception:
                pass

            # Fallback: download to local path then read
            try:
                import mlflow
                art_ns = getattr(mlflow, "artifacts", None)
                download = getattr(art_ns, "download_artifacts", None) if art_ns else None
                if callable(download):
                    local_path = download(artifact_uri=artifact_uri)
                    import os as _os
                    if _os.path.isfile(local_path):
                        with open(local_path, "r", encoding="utf-8") as f:
                            import json as _json
                            data = _json.load(f)
                            if isinstance(data, dict) and isinstance(data.get("spans"), list):
                                return data.get("spans", [])
                            if isinstance(data, list):
                                return data
            except Exception:
                pass
        except Exception:
            pass
        return []


    def _extract_intent_example_from_trace(self, trace: Any) -> Optional[DSPyExample]:
        """Extract intent detection example from an MLflow 3.x trace row by reading spans.
        Accepts a DataFrame row converted to dict (from search_traces) or a minimal object.
        """
        try:
            row = trace if isinstance(trace, dict) else {}
            spans = self._load_trace_spans(row)

            # Find the most relevant span for intent detection
            intent_span = None
            for sp in spans:
                name = (sp.get("name") or sp.get("span_name") or "").lower() if isinstance(sp, dict) else ""
                if (
                    "detect_intent" in name
                    or "dispatcher.detect_intent" in name
                    or "intent" in name  # broader fallback
                    or "route" in name   # router-style naming
                ):
                    intent_span = sp
                    break
            if intent_span is None:
                try:
                    sample_names = [
                        (s.get("name") or s.get("span_name") or "")[:120]
                        for s in spans if isinstance(s, dict)
                    ][:10]
                    logger.debug(f"[TRACE_DEBUG] No intent span matched; sample span names: {sample_names}")
                except Exception:
                    pass

            input_data: Dict[str, Any] = {}
            output_data: Dict[str, Any] = {}

            # Heuristics to extract input/output from span attributes
            def _coerce_json(v):
                import json as _json
                if isinstance(v, (dict, list)):
                    return v
                if isinstance(v, str):
                    try:
                        return _json.loads(v)
                    except Exception:
                        return v
                return v

            if isinstance(intent_span, dict):
                attrs = intent_span.get("attributes") if isinstance(intent_span.get("attributes"), dict) else {}
                inputs = _coerce_json(
                    attrs.get("inputs")
                    or attrs.get("request")
                    or attrs.get("input")
                    or attrs.get("messages")
                    or attrs.get("prompt")
                    or attrs.get("question")
                )
                if isinstance(inputs, dict):
                    input_data.update(inputs)
                else:
                    if inputs:
                        input_data["message"] = inputs
                # Common/known fields
                for k in ("message", "semantic_hints", "question", "context", "tools"):
                    if k in attrs:
                        input_data.setdefault(k, _coerce_json(attrs.get(k)))
                outputs = _coerce_json(
                    attrs.get("outputs")
                    or attrs.get("response")
                    or attrs.get("output")
                    or attrs.get("prediction")
                    or attrs.get("answer")
                    or attrs.get("json")
                )
                if isinstance(outputs, dict):
                    output_data.update(outputs)
                else:
                    if outputs:
                        output_data["response"] = outputs
                # Intent-like fields
                for k in ("intent", "predicted_intent"):
                    if k in attrs:
                        output_data.setdefault("intent", attrs.get(k))
                if "confidence" in attrs:
                    output_data.setdefault("confidence", attrs.get("confidence"))

            # Fallbacks using trace row preview fields if spans didn't yield content
            if not input_data and isinstance(row.get("request_preview"), str) and row.get("request_preview").strip():
                input_data["message"] = row.get("request_preview")
            if not output_data and isinstance(row.get("response_preview"), str) and row.get("response_preview").strip():
                output_data["response"] = row.get("response_preview")

            if not input_data and not output_data:
                return None

            score = 0.8  # TODO: compute from downstream success when available
            trace_id = row.get("trace_id") or (intent_span.get("trace_id") if isinstance(intent_span, dict) else None)
            return DSPyExample(input=input_data, output=output_data, score=score, trace_id=str(trace_id) if trace_id else None)

        except Exception as e:
            logger.debug(f"Failed to extract intent example: {e}")
            return None

    def _extract_crew_example_from_trace(self, trace: Any) -> Optional[DSPyExample]:
        """Extract crew generation example from an MLflow 3.x trace row by reading spans.
        Accepts a DataFrame row converted to dict (from search_traces) or a minimal object.
        """
        try:
            row = trace if isinstance(trace, dict) else {}
            spans = self._load_trace_spans(row)

            crew_span = None
            for sp in spans:
                name = (sp.get("name") or sp.get("span_name") or "").lower() if isinstance(sp, dict) else ""
                if (
                    "generate_crew" in name
                    or "planner.generate_crew" in name
                    or "crew" in name
                    or "plan" in name
                ):
                    crew_span = sp
                    break
            if crew_span is None:
                try:
                    sample_names = [
                        (s.get("name") or s.get("span_name") or "")[:120]
                        for s in spans if isinstance(s, dict)
                    ][:10]
                    logger.debug(f"[TRACE_DEBUG] No crew span matched; sample span names: {sample_names}")
                except Exception:
                    pass

            input_data: Dict[str, Any] = {}
            output_data: Dict[str, Any] = {}

            def _coerce_json(v):
                import json as _json
                if isinstance(v, (dict, list)):
                    return v
                if isinstance(v, str):
                    try:
                        return _json.loads(v)
                    except Exception:
                        return v
                return v

            if isinstance(crew_span, dict):
                attrs = crew_span.get("attributes") if isinstance(crew_span.get("attributes"), dict) else {}
                inputs = _coerce_json(
                    attrs.get("inputs")
                    or attrs.get("request")
                    or attrs.get("input")
                    or attrs.get("requirements")
                    or attrs.get("prompt")
                    or attrs.get("context")
                )
                if isinstance(inputs, dict):
                    input_data.update(inputs)
                else:
                    if inputs:
                        input_data["request"] = inputs
                # Allow known input hints
                for k in ("goals", "constraints", "tools", "resources"):
                    if k in attrs:
                        input_data.setdefault(k, _coerce_json(attrs.get(k)))
                outputs = _coerce_json(
                    attrs.get("outputs")
                    or attrs.get("response")
                    or attrs.get("output")
                    or attrs.get("plan")
                    or attrs.get("json")
                )
                if isinstance(outputs, dict):
                    # We’re most interested in generated crew structure
                    for k in ("agents", "tasks", "crew", "plan"):
                        if k in outputs:
                            output_data[k] = outputs[k]
                    # Keep full outputs if nothing specific found
                    if not output_data:
                        output_data.update(outputs)
                elif outputs:
                    output_data["response"] = outputs

            if not output_data and isinstance(row.get("response_preview"), str) and row.get("response_preview").strip():
                output_data["response"] = row.get("response_preview")

            if not input_data and not output_data:
                return None

            score = 0.8
            trace_id = row.get("trace_id") or (crew_span.get("trace_id") if isinstance(crew_span, dict) else None)
            return DSPyExample(input=input_data, output=output_data, score=score, trace_id=str(trace_id) if trace_id else None)

        except Exception as e:
            logger.debug(f"Failed to extract crew example: {e}")
            return None

    async def store_examples_in_vector_search(self, examples: List[DSPyExample], optimization_type: OptimizationType):
        """Store examples in the document embeddings index (Databricks Vector Search).
        - Computes embeddings with LLMManager using databricks-gte-large-en (1024-d)
        - Upserts into databricks_config.document_index (DIRECT_ACCESS)
        """
        settings = await self._get_vector_repository(optimization_type)
        if not settings:
            logger.info("No document embeddings index configured; skipping DSPy example storage")
            return

        # Resolve group for tenant isolation (optional filter/metadata)
        group_id = await self._get_validated_group_id()
        group_id_str = str(group_id) if group_id else None

        # Prepare records with limited concurrency for embedding creation
        try:
            embed_cc = int(os.getenv("DSPY_EMBEDDING_CONCURRENCY", "4"))
        except Exception:
            embed_cc = 4
        if embed_cc < 1:
            embed_cc = 1
        sem = asyncio.Semaphore(embed_cc)
        records: List[Dict[str, Any]] = []

        async def build_record(ex: DSPyExample) -> Optional[Dict[str, Any]]:
            async with sem:
                try:
                    # Build canonical text for embedding
                    payload = {"optimization_type": optimization_type.value, "input": ex.input, "output": ex.output}
                    text = json.dumps(payload, ensure_ascii=False)

                    embedding = await LLMManager.get_embedding(text=text, model="databricks-gte-large-en")
                    if not embedding:
                        return None

                    # Map to Document schema
                    now_iso = (ex.timestamp if getattr(ex, "timestamp", None) else datetime.now(timezone.utc)).isoformat()
                    content_json = json.dumps({"input": ex.input, "output": ex.output}, ensure_ascii=False)
                    rec = {
                        "id": self._generate_example_id(ex, optimization_type),
                        "title": f"dspy_example:{optimization_type.value}",
                        "content": content_json,
                        "source": "dspy_examples",
                        "document_type": "json",
                        "section": "",
                        "chunk_index": 0,
                        "chunk_size": len(content_json),
                        "parent_document_id": "",
                        "agent_ids": "[]",
                        "created_at": now_iso,
                        "updated_at": now_iso,
                        "doc_metadata": json.dumps({
                            "optimization_type": optimization_type.value,
                            "quality_score": float(ex.score),
                            "trace_id": ex.trace_id
                        }, ensure_ascii=False),
                        "group_id": group_id_str,
                        "embedding": embedding,
                        "embedding_model": "databricks-gte-large-en",
                        "version": 1,
                    }
                    return rec
                except Exception as e:
                    logger.debug(f"Failed to build record for vector upsert: {e}")
                    return None

        tasks = [build_record(ex) for ex in examples if isinstance(ex.input, dict) and isinstance(ex.output, dict)]
        built = await asyncio.gather(*tasks, return_exceptions=False)
        records = [r for r in built if r]

        if not records:
            logger.info("No DSPy example records to upsert to document index (no embeddings or empty set)")
            return

        repo = DatabricksVectorIndexRepository(settings["workspace_url"])
        res = await repo.upsert(
            index_name=settings["index_name"],
            endpoint_name=settings["endpoint_name"],
            records=records,
        )
        if not res.get("success"):
            logger.warning(f"Upsert to document index failed: {res.get('message') or res.get('error')}")
        else:
            logger.info(f"Upserted {res.get('upserted_count', len(records))} DSPy example records to document index")

    async def retrieve_examples_from_vector_search(self, optimization_type: OptimizationType, query: str = "", top_k: int = 10) -> List[DSPyExample]:
        """Retrieve relevant examples from the document embeddings index using similarity search."""
        settings = await self._get_vector_repository(optimization_type)
        if not settings:
            return []

        # Build query text and embedding
        if not query:
            query = f"dspy:{optimization_type.value}"
        query_vec = await LLMManager.get_embedding(text=query, model="databricks-gte-large-en")
        if not query_vec:
            logger.debug("Query embedding creation failed; skipping vector retrieval")
            return []

        columns = DatabricksIndexSchemas.get_search_columns("document")

        # Optionally filter by group_id when available
        filters: Optional[Dict[str, Any]] = None
        group_id = await self._get_validated_group_id()
        if group_id:
            filters = {"group_id": str(group_id)}

        repo = DatabricksVectorIndexRepository(settings["workspace_url"])
        result = await repo.similarity_search(
            index_name=settings["index_name"],
            endpoint_name=settings["endpoint_name"],
            query_vector=query_vec,
            columns=columns,
            num_results=top_k,
            filters=filters,
        )
        if not result.get("success"):
            logger.debug(f"Vector search failed or returned no results: {result.get('message') or result.get('error')}")
            return []

        # Parse results into DSPyExample instances
        out: List[DSPyExample] = []
        data_array = (result.get("results", {}) or {}).get("result", {}).get("data_array", [])
        for row in data_array:
            try:
                parsed = DatabricksIndexSchemas.parse_search_result("document", row)
                content = parsed.get("content")
                input_data: Dict[str, Any] = {}
                output_data: Dict[str, Any] = {}
                if isinstance(content, str) and content:
                    try:
                        obj = json.loads(content)
                        input_data = obj.get("input", {}) if isinstance(obj, dict) else {}
                        output_data = obj.get("output", {}) if isinstance(obj, dict) else {}
                    except Exception:
                        # If content isn't JSON of our shape, place raw content under input.request
                        input_data = {"request": content}
                        output_data = {}
                # Extract metadata if available
                score = 0.8
                meta_raw = parsed.get("doc_metadata")
                if isinstance(meta_raw, str) and meta_raw:
                    try:
                        meta = json.loads(meta_raw)
                        score = float(meta.get("quality_score", score))
                        trace_id = meta.get("trace_id")
                    except Exception:
                        trace_id = None
                else:
                    trace_id = None

                # Timestamp
                ts = parsed.get("created_at") or parsed.get("updated_at")
                timestamp = datetime.now(timezone.utc)
                try:
                    if isinstance(ts, str) and ts:
                        timestamp = datetime.fromisoformat(ts)
                except Exception:
                    pass

                out.append(DSPyExample(input=input_data, output=output_data, score=score, trace_id=trace_id, timestamp=timestamp))
            except Exception as e:
                logger.debug(f"Failed to parse vector search result row: {e}")
                continue

        return out

    def _create_metric_function(self, optimization_type: OptimizationType):
        """Create metric function for optimization based on type."""

        def intent_metric(example, prediction, trace=None):
            """Metric for intent detection quality."""
            try:
                # Check if intent matches
                intent_match = example.output.get("intent") == prediction.intent

                # Check confidence threshold
                confidence = float(prediction.confidence)
                confidence_good = confidence > 0.7

                # Check if reasoning is provided
                has_reasoning = len(prediction.reasoning or "") > 10

                # Combined score
                score = (
                    0.5 * intent_match +
                    0.3 * confidence_good +
                    0.2 * has_reasoning
                )

                return score

            except Exception as e:
                logger.debug(f"Metric evaluation failed: {e}")
                return 0.0

        def crew_metric(example, prediction, trace=None):
            """Metric for crew generation quality."""
            try:
                # Check if crew structure is valid
                has_agents = len(json.loads(prediction.agents or "[]")) > 0
                has_tasks = len(json.loads(prediction.tasks or "[]")) > 0
                has_workflow = len(json.loads(prediction.workflow or "{}")) > 0

                # Check reasoning quality
                has_reasoning = len(prediction.reasoning or "") > 50

                # Combined score
                score = (
                    0.3 * has_agents +
                    0.3 * has_tasks +
                    0.2 * has_workflow +
                    0.2 * has_reasoning
                )

                return score

            except Exception as e:
                logger.debug(f"Metric evaluation failed: {e}")
                return 0.0

        # Return appropriate metric based on type
        metrics = {
            OptimizationType.INTENT_DETECTION: intent_metric,
            OptimizationType.AGENT_GENERATION: lambda e, p, t: 0.5,  # Placeholder
            OptimizationType.TASK_GENERATION: lambda e, p, t: 0.5,   # Placeholder
            OptimizationType.CREW_GENERATION: crew_metric
        }

        return metrics.get(optimization_type, lambda e, p, t: 0.5)

    async def optimize_module(self, optimization_type: OptimizationType, force: bool = False) -> Module:
        """Optimize a DSPy module using collected examples."""

        # Check if we should optimize
        if not force:
            # Check if enough time has passed since last optimization
            # This would check some persistent storage for last optimization time
            pass

        # Enable MLflow tracing
        await self._enable_mlflow_tracing()

        # Ensure DSPy LM is configured (defensive in case global state was reset)
        if not self._dspy_initialized:
            try:
                import dspy as _dspy
                _lm = getattr(getattr(_dspy, "settings", None), "lm", None)
                if _lm is None:
                    await self._initialize_dspy()
                    self._dspy_initialized = True
            except Exception:
                # On any error, force re-init
                await self._initialize_dspy()
                self._dspy_initialized = True

        # SQL-first: pull examples from DB within window and above quality
        from src.repositories.dspy_config_repository import DSPyConfigRepository
        dspy_repo = DSPyConfigRepository(self.session)
        group_id = await self._get_validated_group_id()
        sql_rows = await dspy_repo.get_training_examples(
            optimization_type=optimization_type,
            group_id=group_id,
            min_quality_score=self.config.min_quality_score,
            limit=2000,
            hours_back=self.config.optimization_interval_hours,
        )
        sql_examples: List[DSPyExample] = []
        try:
            from datetime import datetime as _dt, timezone as _tz
            for row in sql_rows:
                sql_examples.append(
                    DSPyExample(
                        input=row.input_data or {},
                        output=row.output_data or {},
                        score=float(row.quality_score or 0.0),
                        trace_id=row.trace_id,
                        timestamp=(row.created_at.replace(tzinfo=row.created_at.tzinfo or _tz.utc) if getattr(row.created_at, 'tzinfo', None) is not None else _dt.now(_tz.utc)),
                    )
                )
        except Exception:
            pass

        examples: List[DSPyExample] = list(sql_examples)

        # If still insufficient, backfill from traces and vector search
        if len(examples) < self.config.min_examples_for_optimization:
            trace_examples = await self.collect_examples_from_traces(
                optimization_type, hours_back=self.config.optimization_interval_hours
            )
            # Persist trace examples to SQL for reuse
            try:
                payload = [
                    {
                        "input_data": ex.input,
                        "output_data": ex.output,
                        "quality_score": float(ex.score),
                        "trace_id": ex.trace_id,
                    }
                    for ex in trace_examples
                    if isinstance(ex.input, dict) and isinstance(ex.output, dict)
                ]
                if payload:
                    await dspy_repo.create_training_examples(
                        examples=payload,
                        optimization_type=optimization_type,
                        group_id=group_id,
                    )
            except Exception as _e:
                logger.debug(f"Persist trace examples failed (non-fatal): {_e}")

            # Vector search can add more historical examples
            vector_examples = await self.retrieve_examples_from_vector_search(optimization_type)
            examples.extend(trace_examples)
            examples.extend(vector_examples)

        # Filter by quality score
        quality_examples = [ex for ex in examples if ex.score >= self.config.min_quality_score]

        # Cap training set to avoid overwhelming the service
        try:
            max_train = int(os.getenv("DSPY_MAX_TRAIN_EXAMPLES", "40"))
        except Exception:
            max_train = 40
        if len(quality_examples) > max_train:
            logger.info(f"Capping training examples from {len(quality_examples)} to {max_train} to reduce load")
            quality_examples = quality_examples[:max_train]

        if len(quality_examples) < self.config.min_examples_for_optimization:
            logger.warning(
                f"Not enough quality examples for optimization: {len(quality_examples)} < {self.config.min_examples_for_optimization}"
            )
            # Return unoptimized module
            return self._get_base_module(optimization_type)

        # Store newly collected trace examples in vector search for future use
        await self.store_examples_in_vector_search(
            [ex for ex in quality_examples if ex not in sql_examples], optimization_type
        )

        # Convert to DSPy format
        trainset = self._convert_to_dspy_examples(quality_examples, optimization_type)

        # Ensure DSPy is configured before optimization
        if not self._dspy_initialized:
            await self._initialize_dspy()
            self._dspy_initialized = True

        # Get base module
        base_module = self._get_base_module(optimization_type)

        # Create metric function
        metric = self._create_metric_function(optimization_type)

        # Choose optimizer based on number of examples and configuration
        if len(quality_examples) >= 200 and self.config.use_mipro:
            # Use MIPROv2 for large datasets
            optimizer = MIPROv2(
                metric=metric,
                num_candidates=20,
                init_temperature=0.7
            )
        elif self.config.use_random_search:
            # Use BootstrapFewShotWithRandomSearch
            optimizer = BootstrapFewShotWithRandomSearch(
                metric=metric,
                max_bootstrapped_demos=self.config.max_bootstrapped_demos,
                max_labeled_demos=self.config.max_labeled_demos,
                num_candidate_programs=self.config.num_candidate_programs,
                num_threads=self.config.num_threads
            )
        else:
            # Use basic BootstrapFewShot
            optimizer = BootstrapFewShot(
                metric=metric,
                max_bootstrapped_demos=self.config.max_bootstrapped_demos,
                max_labeled_demos=self.config.max_labeled_demos
            )

        # Run optimization with MLflow tracking
        timeout_s = int(os.getenv("DSPY_COMPILE_TIMEOUT_SECONDS", "300"))
        with mlflow.start_run(run_name=f"dspy_optimize_{optimization_type.value}"):
            mlflow.log_params({
                "optimization_type": optimization_type.value,
                "num_examples": len(quality_examples),
                "optimizer": optimizer.__class__.__name__,
                "config": self.config.__dict__
            })

            # Compile the program in a worker thread with timeout to avoid blocking the event loop
            def _compile():
                return optimizer.compile(base_module, trainset=trainset)

            try:
                optimized_module = await asyncio.wait_for(asyncio.to_thread(_compile), timeout=timeout_s)
            except asyncio.TimeoutError:
                logger.error(f"DSPy optimization compile timed out after {timeout_s}s")
                raise TimeoutError(f"DSPy optimization compile timed out after {timeout_s}s")

            # Log the optimized module
            mlflow.dspy.log_model(
                optimized_module,
                artifact_path=f"dspy_{optimization_type.value}",
                signature=mlflow.models.infer_signature(
                    model_input={"message": "example", "semantic_hints": "hints"},
                    model_output={"intent": "generate_task", "confidence": 0.9}
                )
            )

            # Evaluate on test set if available
            if len(quality_examples) > 20:
                test_examples = quality_examples[-10:]  # Last 10 as test
                test_score = self._evaluate_module(optimization_type, optimized_module, test_examples, metric)
                mlflow.log_metric("test_score", test_score)
                logger.info(f"Optimized module test score: {test_score}")

            # Store optimized module
            self.optimized_modules[optimization_type] = optimized_module

            logger.info(f"Successfully optimized {optimization_type.value} module")

        return optimized_module

    def _get_base_module(self, optimization_type: OptimizationType) -> Module:
        """Get base (unoptimized) module for a given type."""
        if optimization_type not in self.modules:
            if optimization_type == OptimizationType.INTENT_DETECTION:
                self.modules[optimization_type] = IntentDetector()
            elif optimization_type == OptimizationType.AGENT_GENERATION:
                self.modules[optimization_type] = AgentGenerator()
            elif optimization_type == OptimizationType.TASK_GENERATION:
                self.modules[optimization_type] = TaskGenerator()
            elif optimization_type == OptimizationType.CREW_GENERATION:
                self.modules[optimization_type] = CrewGenerator()
            else:
                raise ValueError(f"Unknown optimization type: {optimization_type}")

        return self.modules[optimization_type]

    def _convert_to_dspy_examples(self, examples: List[DSPyExample], optimization_type: OptimizationType) -> List[dspy.Example]:
        """Convert our examples to DSPy Example format."""
        dspy_examples = []

        for ex in examples:
            if optimization_type == OptimizationType.INTENT_DETECTION:
                dspy_ex = dspy.Example(
                    message=ex.input.get("message", ""),
                    semantic_hints=ex.input.get("semantic_hints", ""),
                    intent=ex.output.get("intent", "unknown"),
                    confidence=str(ex.output.get("confidence", 0.5)),
                    extracted_info=json.dumps(ex.output.get("extracted_info", {})),
                    reasoning=ex.output.get("reasoning", "")
                ).with_inputs("message", "semantic_hints")

            elif optimization_type == OptimizationType.AGENT_GENERATION:
                # inputs: prompt, tools_available; outputs: agent_name, role, backstory, goal, tools, reasoning
                tools_available = ex.input.get("tools_available", [])
                if not isinstance(tools_available, str):
                    try:
                        tools_available = json.dumps(tools_available)
                    except Exception:
                        tools_available = str(tools_available)
                tools_out = ex.output.get("tools", [])
                if not isinstance(tools_out, str):
                    try:
                        tools_out = json.dumps(tools_out)
                    except Exception:
                        tools_out = str(tools_out)
                dspy_ex = dspy.Example(
                    prompt=ex.input.get("prompt", ""),
                    tools_available=tools_available,
                    agent_name=ex.output.get("agent_name", ""),
                    role=ex.output.get("role", ""),
                    backstory=ex.output.get("backstory", ""),
                    goal=ex.output.get("goal", ""),
                    tools=tools_out,
                    reasoning=ex.output.get("reasoning", "")
                ).with_inputs("prompt", "tools_available")

            elif optimization_type == OptimizationType.TASK_GENERATION:
                # inputs: prompt, agent_context; outputs: task_name, description, expected_output, tools, reasoning
                tools_out = ex.output.get("tools", [])
                if not isinstance(tools_out, str):
                    try:
                        tools_out = json.dumps(tools_out)
                    except Exception:
                        tools_out = str(tools_out)
                dspy_ex = dspy.Example(
                    prompt=ex.input.get("prompt", ""),
                    agent_context=ex.input.get("agent_context", ""),
                    task_name=ex.output.get("task_name", ""),
                    description=ex.output.get("description", ""),
                    expected_output=ex.output.get("expected_output", ""),
                    tools=tools_out,
                    reasoning=ex.output.get("reasoning", "")
                ).with_inputs("prompt", "agent_context")

            elif optimization_type == OptimizationType.CREW_GENERATION:
                dspy_ex = dspy.Example(
                    prompt=ex.input.get("prompt", ""),
                    tools_available=(json.dumps(ex.input.get("tools_available", [])) if not isinstance(ex.input.get("tools_available", []), str) else ex.input.get("tools_available", "")),
                    crew_name=ex.output.get("crew_name", ""),
                    agents=json.dumps(ex.output.get("agents", [])),
                    tasks=json.dumps(ex.output.get("tasks", [])),
                    workflow=json.dumps(ex.output.get("workflow", {})),
                    reasoning=ex.output.get("reasoning", "")
                ).with_inputs("prompt", "tools_available")

            else:
                continue

            dspy_examples.append(dspy_ex)

        return dspy_examples

    def _evaluate_module(self, optimization_type: OptimizationType, module: Module, examples: List[DSPyExample], metric) -> float:
        """Evaluate a module on test examples."""
        scores = []

        for example in examples:
            try:
                # Convert to DSPy example using the correct type
                converted = self._convert_to_dspy_examples([example], optimization_type)
                if not converted:
                    continue
                dspy_ex = converted[0]

                # Run prediction
                prediction = module(**dspy_ex.inputs())

                # Calculate score
                score = metric(dspy_ex, prediction)
                scores.append(score)

            except Exception as e:
                logger.debug(f"Evaluation failed for example: {e}")
                scores.append(0.0)

        return np.mean(scores) if scores else 0.0

    async def get_optimized_module(self, optimization_type: OptimizationType) -> Module:
        """Get optimized module if available, otherwise return base module."""
        if optimization_type in self.optimized_modules:
            return self.optimized_modules[optimization_type]

        # Try to load from MLflow
        try:
            model_name = f"dspy_{optimization_type.value}"
            model = mlflow.dspy.load_model(model_name)
            self.optimized_modules[optimization_type] = model
            logger.info(f"Loaded optimized module from MLflow: {model_name}")
            return model
        except Exception as e:
            logger.debug(f"No optimized module found in MLflow: {e}")

        # Return base module
        return self._get_base_module(optimization_type)

    async def predict_intent(self, message: str, semantic_hints: str) -> Dict[str, Any]:
        """Predict intent using optimized DSPy module."""
        module = await self.get_optimized_module(OptimizationType.INTENT_DETECTION)

        try:
            prediction = module(message=message, semantic_hints=semantic_hints)

            return {
                "intent": prediction.intent,
                "confidence": float(prediction.confidence),
                "extracted_info": json.loads(prediction.extracted_info) if isinstance(prediction.extracted_info, str) else prediction.extracted_info,
                "reasoning": prediction.reasoning
            }

        except Exception as e:
            logger.error(f"DSPy intent prediction failed: {e}")
            # Fall back to default response
            return {
                "intent": "unknown",
                "confidence": 0.0,
                "extracted_info": {},
                "reasoning": f"Prediction failed: {str(e)}"
            }

    async def generate_crew(self, prompt: str, tools_available: str) -> Dict[str, Any]:
        """Generate crew using optimized DSPy module."""
        module = await self.get_optimized_module(OptimizationType.CREW_GENERATION)

        try:
            prediction = module(prompt=prompt, tools_available=tools_available)

            return {
                "crew_name": prediction.crew_name,
                "agents": json.loads(prediction.agents) if isinstance(prediction.agents, str) else prediction.agents,
                "tasks": json.loads(prediction.tasks) if isinstance(prediction.tasks, str) else prediction.tasks,
                "workflow": json.loads(prediction.workflow) if isinstance(prediction.workflow, str) else prediction.workflow,
                "reasoning": prediction.reasoning
            }

        except Exception as e:
            logger.error(f"DSPy crew generation failed: {e}")
            raise

    async def run_optimization_pipeline(self, optimization_types: Optional[List[OptimizationType]] = None):
        """Run the full optimization pipeline for specified types."""
        if optimization_types is None:
            optimization_types = list(OptimizationType)

        logger.info(f"Starting DSPy optimization pipeline for: {[t.value for t in optimization_types]}")

        results = {}
        for opt_type in optimization_types:
            try:
                logger.info(f"Optimizing {opt_type.value}...")
                optimized_module = await self.optimize_module(opt_type)
                results[opt_type] = {"status": "success", "module": optimized_module}

            except Exception as e:
                logger.error(f"Failed to optimize {opt_type.value}: {e}")
                results[opt_type] = {"status": "failed", "error": str(e)}

        return results

    async def clear_cache(self, optimization_type: OptimizationType):
        """Clear cached module for a specific optimization type."""
        if optimization_type in self.optimized_modules:
            del self.optimized_modules[optimization_type]
            logger.info(f"Cleared cache for {optimization_type.value}")

    async def clear_all_cache(self):
        """Clear all cached modules."""
        self.optimized_modules.clear()
        logger.info("Cleared all DSPy module caches")

    async def run_optimization(
        self,
        optimization_type: OptimizationType,
        run_id: UUID,
        force: bool = False
    ):
        """Run optimization for a specific type and update the run record."""
        from src.repositories.dspy_config_repository import DSPyConfigRepository
        from src.schemas.dspy_schemas import OptimizationStatus

        dspy_repo = DSPyConfigRepository(self.session)

        # Queue optimizations per workspace (group): only one runs at a time; others wait
        # Queue optimizations per workspace (group) using a simple in-process set
        if not hasattr(DSPyOptimizationService, "_opt_running"):
            DSPyOptimizationService._opt_running = set()  # type: ignore[attr-defined]
        _opt_group_id = await self._get_validated_group_id()
        _opt_key = f"opt:{str(_opt_group_id) if _opt_group_id else 'global'}"
        while _opt_key in DSPyOptimizationService._opt_running:  # type: ignore[attr-defined]
            await asyncio.sleep(0.2)
        DSPyOptimizationService._opt_running.add(_opt_key)  # type: ignore[attr-defined]

        try:
            # Update run status to running
            await dspy_repo.update_optimization_run(
                run_id=run_id,
                status=OptimizationStatus.RUNNING
            )

            # Check if we should run optimization
            if not force:
                # Check if we have a recent successful optimization
                group_id = await self._get_validated_group_id()
                recent_runs = await dspy_repo.get_recent_optimization_runs(
                    optimization_type=optimization_type,
                    group_id=group_id,
                    limit=1
                )

                if recent_runs and recent_runs[0].status == OptimizationStatus.COMPLETED.value:
                    from datetime import datetime as _dt, timezone as _tz
                    completed = recent_runs[0].completed_at
                    if getattr(completed, "tzinfo", None) is None:
                        completed = completed.replace(tzinfo=_tz.utc)
                    hours_since = (_dt.now(_tz.utc) - completed).total_seconds() / 3600
                    if hours_since < float(self.config.optimization_interval_hours):  # Respect configured interval
                        logger.info(f"Skipping optimization - recent run found {hours_since:.1f} hours ago (< {self.config.optimization_interval_hours}h)")
                        await dspy_repo.update_optimization_run(
                            run_id=run_id,
                            status=OptimizationStatus.COMPLETED,
                            error_message="Skipped - recent optimization exists"
                        )
                        return

            # SQL-first: read high-quality examples from DB, then backfill from traces if needed
            group_id = await self._get_validated_group_id()
            sql_rows = await dspy_repo.get_training_examples(
                optimization_type=optimization_type,
                group_id=group_id,
                min_quality_score=self.config.min_quality_score,
                limit=1000,
                hours_back=self.config.optimization_interval_hours,
            )
            examples: List[DSPyExample] = []
            try:
                from datetime import datetime as _dt, timezone as _tz
                for row in sql_rows:
                    ts = row.created_at
                    if ts is None:
                        ts = _dt.now(_tz.utc)
                    elif getattr(ts, "tzinfo", None) is None:
                        ts = ts.replace(tzinfo=_tz.utc)
                    examples.append(
                        DSPyExample(
                            input=row.input_data or {},
                            output=row.output_data or {},
                            score=float(row.quality_score or 0.0),
                            trace_id=row.trace_id,
                            timestamp=ts,
                        )
                    )
            except Exception:
                pass

            # If insufficient, collect from traces and persist
            min_needed = int(self.config.min_examples_for_optimization)
            if len(examples) < min_needed:
                needed = min_needed - len(examples)
                # Apply hydration timeout to avoid long-running downloads
                try:
                    _run_timeout = int(os.getenv("DSPY_OPTIMIZATION_RUN_TIMEOUT", "900") or 900)
                except Exception:
                    _run_timeout = 900
                try:
                    trace_examples = await asyncio.wait_for(
                        self.collect_examples_from_traces(
                            optimization_type,
                            hours_back=self.config.optimization_interval_hours,
                        ),
                        timeout=_run_timeout // 2 if _run_timeout > 60 else _run_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.error(f"Trace hydration timed out after {_run_timeout // 2 if _run_timeout > 60 else _run_timeout}s")
                    await dspy_repo.update_optimization_run(
                        run_id=run_id,
                        status=OptimizationStatus.FAILED,
                        error_message=f"Trace hydration timed out after {_run_timeout // 2 if _run_timeout > 60 else _run_timeout}s"
                    )
                    return
                # Persist collected examples to SQL for reusability
                try:
                    dspy_repo = DSPyConfigRepository(self.session)
                    payload = [
                        {
                            "input_data": ex.input,
                            "output_data": ex.output,
                            "quality_score": float(ex.score),
                            "trace_id": ex.trace_id,
                        }
                        for ex in trace_examples
                        if isinstance(ex.input, dict) and isinstance(ex.output, dict)
                    ]
                    if payload:
                        await dspy_repo.create_training_examples(
                            examples=payload,
                            optimization_type=optimization_type,
                            group_id=group_id,
                        )
                        logger.info(
                            f"Persisted {len(payload)} training examples to SQL for {optimization_type.value}"
                        )
                except Exception as e:
                    logger.warning(f"Failed to persist training examples to SQL: {e}")
                # Combine (keep SQL-first ordering)
                examples.extend(trace_examples)

            if len(examples) < min_needed:
                await dspy_repo.update_optimization_run(
                    run_id=run_id,
                    status=OptimizationStatus.FAILED,
                    error_message=f"Insufficient examples: {len(examples)} < {min_needed}"
                )
                return

            # Run optimization
            logger.info(f"Running optimization with {len(examples)} examples")
            # Apply global run timeout to the optimization step
            try:
                _run_timeout = int(os.getenv("DSPY_OPTIMIZATION_RUN_TIMEOUT", "900") or 900)
            except Exception:
                _run_timeout = 900
            try:
                optimized_module = await asyncio.wait_for(
                    self.optimize_module(
                        optimization_type=optimization_type
                    ),
                    timeout=_run_timeout,
                )
            except asyncio.TimeoutError:
                logger.error(f"Optimization timed out after {_run_timeout}s for {optimization_type.value}")
                await dspy_repo.update_optimization_run(
                    run_id=run_id,
                    status=OptimizationStatus.FAILED,
                    error_message=f"Optimization timed out after {_run_timeout}s"
                )
                return

            # Update run record with success
            await dspy_repo.update_optimization_run(
                run_id=run_id,
                status=OptimizationStatus.COMPLETED,
                num_training_examples=len(examples),
                best_score=0.85  # TODO: Get actual score from optimization
            )

            # Clear cache to use new optimized module
            await self.clear_cache(optimization_type)

        except Exception as e:
            logger.error(f"Optimization failed for {optimization_type.value}: {e}")
            await dspy_repo.update_optimization_run(
                run_id=run_id,
                status=OptimizationStatus.FAILED,
                error_message=str(e)
            )
        finally:
            try:
                DSPyOptimizationService._opt_running.discard(_opt_key)  # type: ignore[attr-defined]
            except Exception:
                pass

    async def get_all_configs(self) -> Dict[str, Any]:
        """Get all active DSPy configurations for each optimization type."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)
        configs = {}

        # Get validated group_id
        group_id = await self._get_validated_group_id()

        for opt_type in OptimizationType:
            config = await dspy_repo.get_active_config(
                optimization_type=opt_type,
                group_id=group_id
            )
            if config:
                configs[opt_type.value] = {
                    "id": str(config.id),
                    "version": config.version,
                    "is_active": config.is_active,
                    "deployment_stage": config.deployment_stage,
                    "mlflow_run_id": config.mlflow_run_id,
                    "test_score": config.test_score,
                    "created_at": config.created_at.isoformat() if config.created_at else None
                }
            else:
                configs[opt_type.value] = None

        return configs

    async def get_status(self) -> Dict[str, Any]:
        """Get DSPy optimization status for all types."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)
        status = {}

        # Get validated group_id
        group_id = await self._get_validated_group_id()

        for opt_type in OptimizationType:
            # Get active config
            config = await dspy_repo.get_active_config(
                optimization_type=opt_type,
                group_id=group_id
            )

            # Get recent runs
            recent_runs = await dspy_repo.get_recent_optimization_runs(
                optimization_type=opt_type,
                group_id=group_id,
                limit=1
            )

            # Get example count
            examples = await dspy_repo.get_training_examples(
                optimization_type=opt_type,
                group_id=group_id,
                limit=1  # Just to get count
            )

            status[opt_type.value] = {
                "has_active_config": config is not None,
                "config_version": config.version if config else 0,
                "last_optimization": recent_runs[0].completed_at.isoformat() if recent_runs and recent_runs[0].completed_at else None,
                "examples_available": len(examples),
                "is_optimized": config is not None and config.mlflow_run_id is not None
            }

        return status

    async def create_optimization_run(
        self,
        optimization_type: OptimizationType,
        optimizer_type: str,
        optimizer_params: Dict[str, Any]
    ):
        """Create a new optimization run record."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)

        # Get validated group_id
        group_id = await self._get_validated_group_id()

        return await dspy_repo.create_optimization_run(
            optimization_type=optimization_type,
            optimizer_type=optimizer_type,
            optimizer_params=optimizer_params,
            group_id=group_id,
            triggered_by="manual",
            triggered_by_user=None
        )

    async def get_optimization_runs(
        self,
        optimization_type: Optional[OptimizationType] = None,
        limit: int = 10
    ):
        """Get recent optimization runs."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)

        # Get validated group_id
        group_id = await self._get_validated_group_id()

        return await dspy_repo.get_recent_optimization_runs(
            optimization_type=optimization_type,
            group_id=group_id,
            limit=limit
        )

    async def get_training_examples(
        self,
        optimization_type: OptimizationType,
        min_quality_score: float = 0.0,
        limit: int = 100
    ):
        """Get training examples for an optimization type."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)

        # Get validated group_id
        group_id = await self._get_validated_group_id()

        return await dspy_repo.get_training_examples(
            optimization_type=optimization_type,
            group_id=group_id,
            min_quality_score=min_quality_score,
            limit=limit
        )


    async def upload_training_examples(self, batch):
        """Upload a batch of training examples (manual/synthetic/trace) to SQL.
        Expects DSPyExampleBatch; imported lazily to avoid circulars.
        """
        self._ensure_services_initialized()
        from src.repositories.dspy_config_repository import DSPyConfigRepository
        from src.schemas.dspy_schemas import DSPyExampleBatch as _Batch

        if not isinstance(batch, _Batch):
            # Allow dict-like input for flexibility
            try:
                from src.schemas.dspy_schemas import DSPyExampleCreate, ExampleSourceType
                examples_payload = []
                for ex in batch.get("examples", []):
                    # minimal validation
                    examples_payload.append({
                        "input_data": ex.get("input_data", {}),
                        "output_data": ex.get("output_data", {}),
                        "quality_score": float(ex.get("quality_score", 0.0)),
                        "trace_id": ex.get("trace_id"),
                    })
                optimization_type = batch.get("optimization_type")
            except Exception:
                raise ValueError("Invalid payload for training examples upload")
        else:
            examples_payload = [
                {
                    "input_data": ex.input_data,
                    "output_data": ex.output_data,
                    "quality_score": float(ex.quality_score or 0.0),
                    "trace_id": ex.trace_id,
                }
                for ex in batch.examples
            ]
            optimization_type = batch.optimization_type

        repo = DSPyConfigRepository(self.session)
        group_id = await self._get_validated_group_id()
        created = await repo.create_training_examples(
            examples=examples_payload,
            optimization_type=optimization_type,
            group_id=group_id,
        )
        return created

    async def cleanup_expired_cache(self) -> int:
        """Clean up expired database cache entries."""
        self._ensure_services_initialized()
        dspy_repo = DSPyConfigRepository(self.session)
        return await dspy_repo.cleanup_expired_cache()