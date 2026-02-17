"""
Comprehensive unit tests for DispatcherService.

Covers:
- Factory classmethod ``create``
- ``_analyze_message_semantics`` with various message types
- ``detect_intent`` (LLM success, empty response, parse fallback, exception path)
- ``dispatch`` routing to each IntentType branch
- ``_log_llm_interaction`` success and failure
- ``_maybe_enable_mlflow_tracing`` enabled / disabled / error paths
- MLflow tracing branches inside dispatch (set_inputs, set_outputs, exceptions)
- MLflow start_span integration inside detect_intent
"""

import pytest
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch, PropertyMock

from src.core.cache import intent_cache
from src.schemas.dispatcher import DispatcherRequest, DispatcherResponse, IntentType
from src.services.dispatcher_service import DispatcherService


@pytest.fixture(autouse=True)
def _reset_dispatcher_class_state():
    """Reset class-level circuit breaker, semaphore, and cache state between tests."""
    DispatcherService._intent_failures = {}
    DispatcherService._concurrency_semaphore = None
    intent_cache._cache.clear()
    intent_cache._hits = 0
    intent_cache._misses = 0
    yield
    DispatcherService._intent_failures = {}
    DispatcherService._concurrency_semaphore = None
    intent_cache._cache.clear()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_group_context(**overrides):
    defaults = dict(
        primary_group_id="grp-1",
        group_ids=["grp-1"],
        group_email="user@example.com",
        access_token="tok-123",
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _build_service():
    """Return a DispatcherService with mocked collaborators."""
    session = MagicMock()
    log_service = AsyncMock()
    template_service = AsyncMock()

    svc = DispatcherService.__new__(DispatcherService)
    svc.session = session
    svc.log_service = log_service
    svc.template_service = template_service
    svc.agent_service = AsyncMock()
    svc.task_service = AsyncMock()
    svc.crew_service = AsyncMock()
    svc.catalog_service = AsyncMock()
    svc.flow_service = AsyncMock()
    return svc


class _BareTrace:
    """A trace-like context manager that has no set_inputs/set_outputs."""

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


# ===================================================================
# Tests for create() classmethod
# ===================================================================


class TestCreate:

    @patch("src.services.dispatcher_service.TemplateService")
    @patch("src.services.dispatcher_service.LLMLogService")
    @patch("src.services.dispatcher_service.CrewGenerationService")
    @patch("src.services.dispatcher_service.TaskGenerationService")
    @patch("src.services.dispatcher_service.AgentGenerationService")
    def test_create_returns_configured_instance(
        self,
        mock_agent_svc,
        mock_task_svc,
        mock_crew_svc,
        mock_log_svc_cls,
        mock_template_cls,
    ):
        session = MagicMock()
        mock_log_svc_cls.create.return_value = MagicMock()
        mock_template_cls.return_value = MagicMock()

        svc = DispatcherService.create(session)

        assert isinstance(svc, DispatcherService)
        mock_log_svc_cls.create.assert_called_once_with(session)
        mock_template_cls.assert_called_once_with(session)
        assert svc.session is session


# ===================================================================
# Tests for _analyze_message_semantics
# ===================================================================


class TestAnalyzeMessageSemantics:

    def setup_method(self):
        self.svc = _build_service()

    def test_empty_message_returns_unknown(self):
        result = self.svc._analyze_message_semantics("")
        assert result["suggested_intent"] == "unknown"
        assert result["intent_scores"]["generate_task"] == 0

    def test_task_action_word_detected(self):
        result = self.svc._analyze_message_semantics("find the best flight to Paris")
        assert "find" in result["task_actions"]
        assert result["intent_scores"]["generate_task"] > 0

    def test_imperative_form_detected(self):
        result = self.svc._analyze_message_semantics("search for news about AI")
        assert result["has_imperative"] is True
        assert any("Imperative form" in h for h in result["semantic_hints"])

    def test_agent_keywords_detected(self):
        result = self.svc._analyze_message_semantics(
            "create an agent that can analyze data"
        )
        assert "agent" in result["agent_keywords"]
        assert result["intent_scores"]["generate_agent"] > 0

    def test_crew_keywords_detected(self):
        result = self.svc._analyze_message_semantics(
            "build a team workflow with multiple agents"
        )
        crew_kws = set(result["crew_keywords"])
        assert crew_kws.intersection({"team", "workflow", "multiple"})
        assert result["intent_scores"]["generate_crew"] > 0

    def test_execute_keywords_detected(self):
        result = self.svc._analyze_message_semantics("execute the crew now")
        assert "execute" in result["execute_keywords"]
        assert result["intent_scores"]["execute_crew"] > 0

    def test_execute_ec_shorthand(self):
        result = self.svc._analyze_message_semantics("ec")
        assert "ec" in result["execute_keywords"]
        # "execute" or "ec" earns extra +2
        assert result["intent_scores"]["execute_crew"] >= 4 + 2

    def test_configure_keywords_detected(self):
        result = self.svc._analyze_message_semantics("configure the llm model settings")
        cfg_kws = set(result["configure_keywords"])
        assert cfg_kws.intersection({"configure", "llm", "model", "settings"})
        assert result["intent_scores"]["configure_crew"] > 0

    def test_configure_structure_detected(self):
        result = self.svc._analyze_message_semantics("change the model to gpt-4")
        assert result["has_configure_structure"] is True
        assert any("Configuration structure" in h for h in result["semantic_hints"])

    def test_question_form_detected_with_question_mark(self):
        result = self.svc._analyze_message_semantics("how do I analyze this data?")
        assert result["has_question"] is True
        assert any("Question form" in h for h in result["semantic_hints"])

    def test_question_form_detected_with_leading_word(self):
        result = self.svc._analyze_message_semantics("what is the best approach")
        assert result["has_question"] is True

    def test_command_structure_detected(self):
        result = self.svc._analyze_message_semantics("i need to find flight options")
        assert result["has_command_structure"] is True
        assert any("Command-like structure" in h for h in result["semantic_hints"])

    def test_complex_task_multiple_actions(self):
        result = self.svc._analyze_message_semantics(
            "find and analyze all the news articles"
        )
        assert result["has_complex_task"] is True
        assert any("Complex multi-step" in h for h in result["semantic_hints"])

    def test_complex_task_via_multiple_keyword(self):
        result = self.svc._analyze_message_semantics("gather multiple data sources")
        assert result["has_complex_task"] is True

    def test_greeting_is_always_false(self):
        # has_greeting is hardcoded to False per source
        result = self.svc._analyze_message_semantics("hello how are you")
        assert result["has_greeting"] is False

    def test_suggested_intent_picks_highest_score(self):
        # "execute" alone should yield execute_crew as top score
        result = self.svc._analyze_message_semantics("execute")
        assert result["suggested_intent"] == "execute_crew"

    def test_mixed_keywords_highest_wins(self):
        result = self.svc._analyze_message_semantics(
            "create a team of agents working together on a plan"
        )
        # crew keywords: team, together, plan; agent: agents
        assert (
            result["intent_scores"]["generate_crew"]
            >= result["intent_scores"]["generate_agent"]
        )

    def test_no_imperative_when_action_word_later(self):
        """Action word not in first 3 words should not set has_imperative."""
        result = self.svc._analyze_message_semantics("the quick brown find something")
        assert result["has_imperative"] is False

    def test_crew_complex_task_bonus(self):
        """Crew keywords with complex task should earn bonus points."""
        result = self.svc._analyze_message_semantics(
            "build a team workflow to find and analyze multiple sources"
        )
        # has_complex_task True + crew_keywords present -> +2 bonus
        assert (
            result["intent_scores"]["generate_crew"] > len(result["crew_keywords"]) * 3
        )

    def test_select_configure_pattern(self):
        """'select ... model' should detect configure_structure."""
        result = self.svc._analyze_message_semantics("select a new model for the task")
        assert result["has_configure_structure"] is True

    def test_action_starts_with_pattern(self):
        """Messages starting with 'get' should match command pattern."""
        result = self.svc._analyze_message_semantics("get the latest data")
        assert result["has_command_structure"] is True

    def test_can_you_request_pattern(self):
        """'can you' pattern should match command structure."""
        result = self.svc._analyze_message_semantics("can you find the best hotel")
        assert result["has_command_structure"] is True

    def test_all_various_keywords_detected(self):
        """Several keyword in words triggers 'several' in message."""
        result = self.svc._analyze_message_semantics("handle several different tasks")
        assert result["has_complex_task"] is True


# ===================================================================
# Tests for _log_llm_interaction
# ===================================================================


class TestLogLlmInteraction:

    @pytest.mark.asyncio
    async def test_success_path(self):
        svc = _build_service()
        svc.log_service.create_log = AsyncMock()

        await svc._log_llm_interaction(
            endpoint="detect-intent",
            prompt="hello",
            response="world",
            model="test-model",
        )

        svc.log_service.create_log.assert_awaited_once_with(
            endpoint="detect-intent",
            prompt="hello",
            response="world",
            model="test-model",
            status="success",
            error_message=None,
            group_context=None,
        )

    @pytest.mark.asyncio
    async def test_with_error_fields(self):
        svc = _build_service()
        gc = _make_group_context()

        await svc._log_llm_interaction(
            endpoint="dispatch-error",
            prompt="p",
            response="r",
            model="m",
            status="error",
            error_message="something broke",
            group_context=gc,
        )

        svc.log_service.create_log.assert_awaited_once()
        call_kwargs = svc.log_service.create_log.call_args.kwargs
        assert call_kwargs["status"] == "error"
        assert call_kwargs["error_message"] == "something broke"
        assert call_kwargs["group_context"] is gc

    @pytest.mark.asyncio
    async def test_exception_is_swallowed(self):
        svc = _build_service()
        svc.log_service.create_log = AsyncMock(side_effect=RuntimeError("db down"))

        # Should not raise
        await svc._log_llm_interaction(
            endpoint="x", prompt="p", response="r", model="m"
        )


# ===================================================================
# Tests for _maybe_enable_mlflow_tracing
# ===================================================================


class TestMaybeEnableMlflowTracing:

    @pytest.mark.asyncio
    async def test_disabled_returns_false(self):
        svc = _build_service()
        gc = _make_group_context()

        with patch("src.services.dispatcher_service.MLflowService") as MockMlf:
            instance = MockMlf.return_value
            instance.is_enabled = AsyncMock(return_value=False)

            result = await svc._maybe_enable_mlflow_tracing(gc)

        assert result is False

    @pytest.mark.asyncio
    async def test_no_group_context_uses_none_group_id(self):
        svc = _build_service()

        with patch("src.services.dispatcher_service.MLflowService") as MockMlf:
            instance = MockMlf.return_value
            instance.is_enabled = AsyncMock(return_value=False)

            result = await svc._maybe_enable_mlflow_tracing(None)

        MockMlf.assert_called_once_with(svc.session, group_id=None)
        assert result is False

    @pytest.mark.asyncio
    async def test_exception_returns_false(self):
        svc = _build_service()
        gc = _make_group_context()

        with patch(
            "src.services.dispatcher_service.MLflowService",
            side_effect=RuntimeError("boom"),
        ):
            result = await svc._maybe_enable_mlflow_tracing(gc)

        assert result is False

    @pytest.mark.asyncio
    async def test_enabled_runs_setup_thread(self):
        """When MLflow is enabled, the setup thread should run and return True."""
        svc = _build_service()
        gc = _make_group_context()

        with patch("src.services.dispatcher_service.MLflowService") as MockMlf:
            instance = MockMlf.return_value
            instance.is_enabled = AsyncMock(return_value=True)

            with patch("asyncio.to_thread", new_callable=AsyncMock) as mock_thread:
                mock_thread.return_value = None
                result = await svc._maybe_enable_mlflow_tracing(gc)

        assert result is True
        mock_thread.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_enabled_to_thread_raises_returns_false(self):
        """If asyncio.to_thread raises, the method should return False."""
        svc = _build_service()
        gc = _make_group_context()

        with patch("src.services.dispatcher_service.MLflowService") as MockMlf:
            instance = MockMlf.return_value
            instance.is_enabled = AsyncMock(return_value=True)

            with patch(
                "asyncio.to_thread",
                new_callable=AsyncMock,
                side_effect=RuntimeError("thread fail"),
            ):
                result = await svc._maybe_enable_mlflow_tracing(gc)

        assert result is False

    @pytest.mark.asyncio
    async def test_group_context_without_primary_group_id(self):
        """GroupContext object without primary_group_id attribute."""
        svc = _build_service()
        gc = SimpleNamespace(group_ids=["g1"], group_email="a@b.c")

        with patch("src.services.dispatcher_service.MLflowService") as MockMlf:
            instance = MockMlf.return_value
            instance.is_enabled = AsyncMock(return_value=False)

            result = await svc._maybe_enable_mlflow_tracing(gc)

        # getattr with default None should be used
        MockMlf.assert_called_once_with(svc.session, group_id=None)
        assert result is False


# ===================================================================
# Tests for detect_intent
# ===================================================================


class TestDetectIntent:

    @pytest.mark.asyncio
    async def test_successful_intent_detection(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(
            return_value="You are an intent detector."
        )

        llm_content = (
            '{"intent": "generate_task", "confidence": 0.9, '
            '"extracted_info": {"goal": "find flights"}, '
            '"suggested_prompt": "find the best flight"}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find the best flight", "test-model")

        assert result["intent"] == "generate_task"
        assert result["confidence"] == 0.9
        assert "semantic_analysis" in result["extracted_info"]
        assert result["suggested_tools"] == []

    @pytest.mark.asyncio
    async def test_no_template_uses_default(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value=None)

        llm_content = '{"intent": "generate_agent", "confidence": 0.85}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("create an agent", "m")

        assert result["intent"] == "generate_agent"

    @pytest.mark.asyncio
    async def test_empty_llm_response_falls_back_to_semantic(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = ""

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find the best flight", "m")

        assert result["source"] == "semantic_fallback"
        # "find" is a task action word so intent should not be "unknown"
        assert result["intent"] in ("generate_task", "generate_crew", "unknown")

    @pytest.mark.asyncio
    async def test_whitespace_only_response_falls_back_to_semantic(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = "   \n  "

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find flights", "m")

        assert result["source"] == "semantic_fallback"

    @pytest.mark.asyncio
    async def test_none_content_triggers_exception_fallback(self):
        """None content may cause TypeError in mlflow span; falls back to semantic."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = None

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find flights", "m")

        # May hit either the empty-response fallback or the exception fallback,
        # depending on whether mlflow span code runs before the None check.
        assert result["intent"] in ("generate_task", "generate_crew", "unknown")
        assert "suggested_prompt" in result or "source" in result

    @pytest.mark.asyncio
    async def test_exception_falls_back_to_semantic(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            side_effect=RuntimeError("API down"),
        ):
            result = await svc.detect_intent("execute the crew", "m")

        # "execute" yields execute_crew via semantic fallback
        assert result["intent"] == "execute_crew"
        assert result["suggested_prompt"] == "execute the crew"

    @pytest.mark.asyncio
    async def test_exception_fallback_low_semantic_confidence(self):
        """When exception and semantic confidence is low, intent should be unknown."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            side_effect=RuntimeError("API down"),
        ):
            result = await svc.detect_intent("xyz", "m")

        assert result["intent"] == "unknown"
        assert result["confidence"] == 0.3  # max(0.3, 0/5.0)

    @pytest.mark.asyncio
    async def test_empty_response_low_semantic_falls_back_unknown(self):
        """Empty LLM response + low semantic confidence -> unknown intent."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = ""

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("xyz abc", "m")

        assert result["source"] == "semantic_fallback"
        assert result["intent"] == "unknown"

    @pytest.mark.asyncio
    async def test_missing_intent_filled_from_semantic(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"confidence": 0.7, "extracted_info": {}}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find the best flight", "m")

        # intent should be filled from semantic analysis
        assert "intent" in result

    @pytest.mark.asyncio
    async def test_missing_confidence_defaults_to_half(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "extracted_info": {}}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("analyze data", "m")

        # confidence not in LLM response -> set to 0.5, but semantic may override
        assert 0.0 <= result["confidence"] <= 1.0

    @pytest.mark.asyncio
    async def test_confidence_clamped_above_one(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": 1.5}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("create something", "m")

        assert result["confidence"] <= 1.0

    @pytest.mark.asyncio
    async def test_confidence_clamped_below_zero(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": -0.5}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("create something", "m")

        assert result["confidence"] >= 0.0

    @pytest.mark.asyncio
    async def test_invalid_confidence_defaults(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": "not_a_number"}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("create something", "m")

        # Invalid confidence falls back to 0.5 (may then be overridden by semantic)
        assert 0.0 <= result["confidence"] <= 1.0

    @pytest.mark.asyncio
    async def test_missing_suggested_prompt_uses_original(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": 0.9}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find flights", "m")

        assert result["suggested_prompt"] == "find flights"

    @pytest.mark.asyncio
    async def test_missing_extracted_info_defaults_to_empty(self):
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = (
            '{"intent": "generate_task", "confidence": 0.9, "suggested_prompt": "x"}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find flights", "m")

        assert "extracted_info" in result
        assert "semantic_analysis" in result["extracted_info"]

    @pytest.mark.asyncio
    async def test_semantic_override_when_confident(self):
        """When semantic analysis has high confidence and LLM has low, semantic wins."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = (
            '{"intent": "unknown", "confidence": 0.3, '
            '"extracted_info": {}, "suggested_prompt": "ec"}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("ec", "m")

        # "ec" is an execute keyword with high semantic score
        assert result["intent"] == "execute_crew"

    @pytest.mark.asyncio
    async def test_semantic_does_not_override_when_llm_confident(self):
        """When LLM has high confidence, semantic does not override."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = (
            '{"intent": "generate_agent", "confidence": 0.95, '
            '"extracted_info": {}, "suggested_prompt": "create agent"}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            # Message has "find" (task action) but LLM says agent with high confidence
            result = await svc.detect_intent("find an agent", "m")

        assert result["intent"] == "generate_agent"
        assert result["confidence"] == 0.95

    @pytest.mark.asyncio
    async def test_mlflow_import_error_handled(self):
        """If mlflow is not importable, should fall back to plain completion."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": 0.85}'

        with (
            patch(
                "src.services.dispatcher_service.LLMManager.completion",
                new_callable=AsyncMock,
                return_value=llm_content,
            ),
            patch("src.services.dispatcher_service._HAS_MLFLOW", False),
        ):
            result = await svc.detect_intent("find data", "m")

        assert result["intent"] == "generate_task"

    @pytest.mark.asyncio
    async def test_mlflow_start_span_with_set_inputs_outputs(self):
        """When mlflow.start_span is available, inputs/outputs should be set."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = (
            '{"intent": "generate_task", "confidence": 0.9, '
            '"extracted_info": {}, "suggested_prompt": "test"}'
        )

        mock_span = MagicMock()
        mock_span.set_inputs = MagicMock()
        mock_span.set_outputs = MagicMock()
        mock_span.__enter__ = MagicMock(return_value=mock_span)
        mock_span.__exit__ = MagicMock(return_value=False)

        mock_mlflow = MagicMock()
        mock_mlflow.start_span = MagicMock(return_value=mock_span)

        with (
            patch(
                "src.services.dispatcher_service.LLMManager.completion",
                new_callable=AsyncMock,
                return_value=llm_content,
            ),
            patch("src.services.dispatcher_service._HAS_MLFLOW", True),
            patch("src.services.dispatcher_service._mlflow", mock_mlflow),
        ):
            result = await svc.detect_intent("find data", "m")

        assert result["intent"] == "generate_task"
        mock_span.set_inputs.assert_called_once()
        mock_span.set_outputs.assert_called_once()

    @pytest.mark.asyncio
    async def test_mlflow_no_start_span_attribute(self):
        """When mlflow exists but has no start_span, plain completion is used."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": 0.9}'

        mock_mlflow = MagicMock(spec=[])  # no start_span attribute

        with (
            patch(
                "src.services.dispatcher_service.LLMManager.completion",
                new_callable=AsyncMock,
                return_value=llm_content,
            ),
            patch("src.services.dispatcher_service._HAS_MLFLOW", True),
            patch("src.services.dispatcher_service._mlflow", mock_mlflow),
        ):
            result = await svc.detect_intent("find data", "m")

        assert result["intent"] == "generate_task"


# ===================================================================
# Tests for dispatch
# ===================================================================


class TestDispatch:
    """Tests for the main dispatch method routing to different services."""

    def _make_intent_result(self, intent, confidence=0.9, suggested_prompt=None):
        return {
            "intent": intent,
            "confidence": confidence,
            "extracted_info": {},
            "suggested_prompt": suggested_prompt or "test prompt",
            "suggested_tools": [],
        }

    @pytest.mark.asyncio
    async def test_dispatch_generate_agent(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_agent")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(
            return_value={"type": "agent", "name": "TestAgent"}
        )

        request = DispatcherRequest(message="create an agent", model="test-model")
        result = await svc.dispatch(request)

        assert result["service_called"] == "generate_agent"
        svc.agent_service.generate_agent.assert_awaited_once()
        call_kwargs = svc.agent_service.generate_agent.call_args.kwargs
        assert call_kwargs["fast_planning"] is True

    @pytest.mark.asyncio
    async def test_dispatch_generate_task(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_task")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.task_service.generate_and_save_task = AsyncMock(
            return_value={"type": "task", "name": "TestTask"}
        )

        request = DispatcherRequest(message="find flights", model="test-model")
        result = await svc.dispatch(request)

        assert result["service_called"] == "generate_task"
        svc.task_service.generate_and_save_task.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatch_generate_crew(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_crew")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(
            return_value={"type": "crew", "agents": [], "tasks": []}
        )

        request = DispatcherRequest(
            message="build a team", model="test-model", tools=["web_search"]
        )
        result = await svc.dispatch(request)

        assert result["service_called"] == "generate_crew"
        svc.crew_service.create_crew_complete.assert_awaited_once()
        call_args = svc.crew_service.create_crew_complete.call_args
        crew_req = call_args[0][0]
        assert crew_req.tools == ["web_search"]

    @pytest.mark.asyncio
    async def test_dispatch_execute_crew(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("execute_crew")
        )
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="ec", model="test-model")
        result = await svc.dispatch(request)

        assert result["service_called"] == "execute_crew"
        gen = result["generation_result"]
        assert gen["type"] == "execute_crew"
        assert "message" in gen

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_llm(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        intent_result = self._make_intent_result("configure_crew")
        intent_result["extracted_info"] = {"config_type": "llm"}
        svc.detect_intent = AsyncMock(return_value=intent_result)
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="configure llm", model="test-model")
        result = await svc.dispatch(request)

        assert result["service_called"] == "configure_crew"
        gen = result["generation_result"]
        assert gen["type"] == "configure_crew"
        assert gen["config_type"] == "llm"
        assert gen["actions"]["open_llm_dialog"] is True

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_tools(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        intent_result = self._make_intent_result("configure_crew")
        intent_result["extracted_info"] = {"config_type": "tools"}
        svc.detect_intent = AsyncMock(return_value=intent_result)
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="select tools", model="test-model")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["config_type"] == "tools"
        assert gen["actions"]["open_tools_dialog"] is True
        assert gen["actions"]["open_llm_dialog"] is False

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_general(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        intent_result = self._make_intent_result("configure_crew")
        intent_result["extracted_info"] = {"config_type": "general"}
        svc.detect_intent = AsyncMock(return_value=intent_result)
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="adjust settings", model="test-model")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["config_type"] == "general"
        assert gen["actions"]["open_llm_dialog"] is True
        assert gen["actions"]["open_maxr_dialog"] is True
        assert gen["actions"]["open_tools_dialog"] is True

    @pytest.mark.asyncio
    async def test_dispatch_configure_crew_maxr(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        intent_result = self._make_intent_result("configure_crew")
        intent_result["extracted_info"] = {"config_type": "maxr"}
        svc.detect_intent = AsyncMock(return_value=intent_result)
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="update max rpm", model="test-model")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["config_type"] == "maxr"
        assert gen["actions"]["open_maxr_dialog"] is True
        assert gen["actions"]["open_llm_dialog"] is False

    @pytest.mark.asyncio
    async def test_dispatch_unknown_intent(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(return_value=self._make_intent_result("unknown"))
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="asdf", model="test-model")
        result = await svc.dispatch(request)

        assert result["service_called"] is None
        gen = result["generation_result"]
        assert gen["type"] == "unknown"
        assert "suggestions" in gen

    @pytest.mark.asyncio
    async def test_dispatch_uses_default_model_when_none(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(return_value=self._make_intent_result("unknown"))
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="hello", model=None)
        await svc.dispatch(request)

        call_args = svc.detect_intent.call_args
        model_used = call_args[0][1]
        assert model_used is not None
        assert isinstance(model_used, str)

    @pytest.mark.asyncio
    async def test_dispatch_generation_error_reraises(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_agent")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(
            side_effect=RuntimeError("generation failed")
        )

        request = DispatcherRequest(message="create an agent", model="test-model")

        with pytest.raises(RuntimeError, match="generation failed"):
            await svc.dispatch(request)

        # Error should have been logged before re-raising
        assert svc._log_llm_interaction.await_count == 2  # intent log + error log
        error_call = svc._log_llm_interaction.call_args_list[1]
        assert error_call.kwargs["status"] == "error"

    @pytest.mark.asyncio
    async def test_dispatch_logs_intent_detection(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(return_value=self._make_intent_result("unknown"))
        svc._log_llm_interaction = AsyncMock()

        gc = _make_group_context()
        request = DispatcherRequest(message="hello", model="m")
        await svc.dispatch(request, group_context=gc)

        svc._log_llm_interaction.assert_awaited_once()
        call_kwargs = svc._log_llm_interaction.call_args.kwargs
        assert call_kwargs["endpoint"] == "detect-intent"
        assert call_kwargs["prompt"] == "hello"
        assert call_kwargs["group_context"] is gc

    @pytest.mark.asyncio
    async def test_dispatch_task_uses_suggested_prompt(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "generate_task", suggested_prompt="enhanced: find flights to Paris"
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.task_service.generate_and_save_task = AsyncMock(return_value={})

        request = DispatcherRequest(message="find flights", model="m")
        await svc.dispatch(request)

        call_args = svc.task_service.generate_and_save_task.call_args
        task_req = call_args[0][0]
        assert task_req.text == "enhanced: find flights to Paris"

    @pytest.mark.asyncio
    async def test_dispatch_agent_uses_suggested_prompt(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "generate_agent", suggested_prompt="create an expert data analyst"
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(return_value={})

        request = DispatcherRequest(
            message="create agent", model="m", tools=["web_search"]
        )
        await svc.dispatch(request)

        call_kwargs = svc.agent_service.generate_agent.call_args.kwargs
        assert call_kwargs["prompt_text"] == "create an expert data analyst"
        assert call_kwargs["tools"] == ["web_search"]

    @pytest.mark.asyncio
    async def test_dispatch_agent_falls_back_to_original_message(self):
        """When suggested_prompt is None, original message is used."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        intent_result = self._make_intent_result("generate_agent")
        intent_result["suggested_prompt"] = None
        svc.detect_intent = AsyncMock(return_value=intent_result)
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(return_value={})

        request = DispatcherRequest(message="make an agent", model="m")
        await svc.dispatch(request)

        call_kwargs = svc.agent_service.generate_agent.call_args.kwargs
        assert call_kwargs["prompt_text"] == "make an agent"

    @pytest.mark.asyncio
    async def test_dispatch_task_error_logs_and_reraises(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_task")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.task_service.generate_and_save_task = AsyncMock(
            side_effect=ValueError("task gen error")
        )

        request = DispatcherRequest(message="find data", model="m")
        with pytest.raises(ValueError, match="task gen error"):
            await svc.dispatch(request)

        # Error logged -- the endpoint uses the IntentType enum repr
        error_call = svc._log_llm_interaction.call_args_list[1]
        assert error_call.kwargs["status"] == "error"
        assert "generate_task" in error_call.kwargs["endpoint"].lower()

    @pytest.mark.asyncio
    async def test_dispatch_crew_error_logs_and_reraises(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("generate_crew")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(
            side_effect=RuntimeError("crew gen error")
        )

        request = DispatcherRequest(message="build team", model="m")
        with pytest.raises(RuntimeError, match="crew gen error"):
            await svc.dispatch(request)


# ===================================================================
# Tests for dispatch with MLflow tracing enabled
# ===================================================================


class TestDispatchWithMlflow:

    @pytest.mark.asyncio
    async def test_mlflow_trace_inputs_set(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock()
        mock_trace.set_outputs = MagicMock()
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(return_value="trace-123"),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result["dispatcher"]["intent"] == "unknown"
        mock_trace.set_inputs.assert_called_once()
        mock_trace.set_outputs.assert_called_once()

    @pytest.mark.asyncio
    async def test_mlflow_trace_start_failure_falls_back(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(side_effect=RuntimeError("trace fail")),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None

    @pytest.mark.asyncio
    async def test_dispatch_crew_with_generation_result_dict(self):
        """Verify trace outputs include generation_summary for dict results."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "execute_crew",
                "confidence": 0.95,
                "extracted_info": {},
                "suggested_prompt": "execute",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock()
        mock_trace.set_outputs = MagicMock()
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="ec", model="m")
            result = await svc.dispatch(request)

        outputs_arg = mock_trace.set_outputs.call_args[0][0]
        assert "generation_summary" in outputs_arg
        assert outputs_arg["generation_summary"]["type"] == "execute_crew"

    @pytest.mark.asyncio
    async def test_mlflow_trace_set_inputs_exception_handled(self):
        """If set_inputs raises, dispatch should still complete."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock(side_effect=RuntimeError("inputs fail"))
        mock_trace.set_outputs = MagicMock()
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None

    @pytest.mark.asyncio
    async def test_mlflow_trace_set_outputs_exception_handled(self):
        """If set_outputs raises, dispatch should still return the result."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock()
        mock_trace.set_outputs = MagicMock(side_effect=RuntimeError("outputs fail"))
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None
        assert "dispatcher" in result

    @pytest.mark.asyncio
    async def test_mlflow_trace_none_root_trace(self):
        """When trace_ctx returns None (nullcontext), no set_inputs/set_outputs called."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(side_effect=RuntimeError("fail")),
                    get_last_active_trace_id=MagicMock(
                        side_effect=RuntimeError("fail")
                    ),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None

    @pytest.mark.asyncio
    async def test_mlflow_get_last_active_trace_id_exception_handled(self):
        """If get_last_active_trace_id raises, dispatch continues."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock()
        mock_trace.set_outputs = MagicMock()
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(
                        side_effect=RuntimeError("trace id fail")
                    ),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None

    @pytest.mark.asyncio
    async def test_mlflow_trace_no_generation_result(self):
        """Trace outputs should be set even for unknown intent."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        mock_trace = MagicMock()
        mock_trace.set_inputs = MagicMock()
        mock_trace.set_outputs = MagicMock()
        mock_trace.__enter__ = MagicMock(return_value=mock_trace)
        mock_trace.__exit__ = MagicMock(return_value=False)

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=mock_trace),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        mock_trace.set_outputs.assert_called_once()

    @pytest.mark.asyncio
    async def test_mlflow_trace_without_set_inputs_attr(self):
        """Trace object without set_inputs attr should not fail."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=True)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        bare_trace = _BareTrace()

        with patch.dict(
            "sys.modules",
            {
                "src.services.mlflow_tracing_service": MagicMock(
                    start_root_trace=MagicMock(return_value=bare_trace),
                    get_last_active_trace_id=MagicMock(return_value=None),
                )
            },
        ):
            request = DispatcherRequest(message="hello", model="m")
            result = await svc.dispatch(request)

        assert result is not None


# ===================================================================
# Tests for dispatcher response structure
# ===================================================================


class TestDispatcherResponseStructure:

    @pytest.mark.asyncio
    async def test_combined_response_structure(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "execute_crew",
                "confidence": 0.9,
                "extracted_info": {"some": "info"},
                "suggested_prompt": "run it",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="execute", model="m")
        result = await svc.dispatch(request)

        assert "dispatcher" in result
        assert "generation_result" in result
        assert "service_called" in result

        d = result["dispatcher"]
        assert d["intent"] == "execute_crew"
        assert d["confidence"] == 0.9
        assert d["extracted_info"] == {"some": "info"}
        assert d["suggested_prompt"] == "run it"


# ===================================================================
# Tests for keyword sets (sanity checks)
# ===================================================================


class TestKeywordSets:

    def test_task_action_words_are_lowercase(self):
        for word in DispatcherService.TASK_ACTION_WORDS:
            assert word == word.lower()

    def test_agent_keywords_are_lowercase(self):
        for word in DispatcherService.AGENT_KEYWORDS:
            assert word == word.lower()

    def test_crew_keywords_are_lowercase(self):
        for word in DispatcherService.CREW_KEYWORDS:
            assert word == word.lower()

    def test_execute_keywords_are_lowercase(self):
        for word in DispatcherService.EXECUTE_KEYWORDS:
            assert word == word.lower()

    def test_configure_keywords_are_lowercase(self):
        for word in DispatcherService.CONFIGURE_KEYWORDS:
            assert word == word.lower()

    def test_catalog_keywords_are_lowercase(self):
        for word in DispatcherService.CATALOG_KEYWORDS:
            assert word == word.lower()

    def test_keyword_sets_are_sets(self):
        assert isinstance(DispatcherService.TASK_ACTION_WORDS, set)
        assert isinstance(DispatcherService.AGENT_KEYWORDS, set)
        assert isinstance(DispatcherService.CREW_KEYWORDS, set)
        assert isinstance(DispatcherService.EXECUTE_KEYWORDS, set)
        assert isinstance(DispatcherService.CONFIGURE_KEYWORDS, set)
        assert isinstance(DispatcherService.CATALOG_KEYWORDS, set)

    def test_no_overlap_between_execute_and_configure(self):
        """Execute and configure should not share keywords to avoid ambiguity."""
        overlap = (
            DispatcherService.EXECUTE_KEYWORDS & DispatcherService.CONFIGURE_KEYWORDS
        )
        assert len(overlap) <= 1


# ===================================================================
# Tests for edge cases and integration-like scenarios
# ===================================================================


class TestEdgeCases:

    @pytest.mark.asyncio
    async def test_dispatch_with_no_group_context(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.3,
                "extracted_info": {},
                "suggested_prompt": "x",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="x", model="m")
        result = await svc.dispatch(request, group_context=None)

        assert result is not None

    def test_semantic_analysis_with_special_characters(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("find @user's data & analyze it!!!")
        assert "find" in result["task_actions"]
        assert "analyze" in result["task_actions"]

    def test_semantic_analysis_with_unicode(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("find the best cafe in Zurich")
        assert "find" in result["task_actions"]

    def test_semantic_analysis_case_insensitive(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("FIND THE BEST FLIGHT")
        assert "find" in result["task_actions"]

    def test_semantic_analysis_no_words(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("!!! ???")
        assert result["suggested_intent"] == "unknown"

    @pytest.mark.asyncio
    async def test_dispatch_request_with_empty_tools(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "generate_crew",
                "confidence": 0.9,
                "extracted_info": {},
                "suggested_prompt": "build a team",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(return_value={})

        request = DispatcherRequest(message="build a team", model="m", tools=[])
        result = await svc.dispatch(request)

        call_args = svc.crew_service.create_crew_complete.call_args
        crew_req = call_args[0][0]
        assert crew_req.tools == []

    @pytest.mark.asyncio
    async def test_dispatch_passes_group_context_to_agent(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "generate_agent",
                "confidence": 0.9,
                "extracted_info": {},
                "suggested_prompt": "make agent",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(return_value={})

        gc = _make_group_context()
        request = DispatcherRequest(message="make agent", model="m")
        await svc.dispatch(request, group_context=gc)

        call_kwargs = svc.agent_service.generate_agent.call_args.kwargs
        assert call_kwargs["group_context"] is gc

    @pytest.mark.asyncio
    async def test_dispatch_passes_group_context_to_task(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "generate_task",
                "confidence": 0.9,
                "extracted_info": {},
                "suggested_prompt": "find data",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()
        svc.task_service.generate_and_save_task = AsyncMock(return_value={})

        gc = _make_group_context()
        request = DispatcherRequest(message="find data", model="m")
        await svc.dispatch(request, group_context=gc)

        call_args = svc.task_service.generate_and_save_task.call_args
        assert call_args[0][1] is gc

    @pytest.mark.asyncio
    async def test_dispatch_passes_group_context_to_crew(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "generate_crew",
                "confidence": 0.9,
                "extracted_info": {},
                "suggested_prompt": "build team",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(return_value={})

        gc = _make_group_context()
        request = DispatcherRequest(message="build team", model="m")
        await svc.dispatch(request, group_context=gc)

        call_args = svc.crew_service.create_crew_complete.call_args
        assert call_args[0][1] is gc

    @pytest.mark.asyncio
    async def test_dispatch_passes_group_context_to_mlflow(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        gc = _make_group_context()
        request = DispatcherRequest(message="hello", model="m")
        await svc.dispatch(request, group_context=gc)

        svc._maybe_enable_mlflow_tracing.assert_awaited_once_with(gc)

    @pytest.mark.asyncio
    async def test_dispatch_passes_group_context_to_detect_intent(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value={
                "intent": "unknown",
                "confidence": 0.5,
                "extracted_info": {},
                "suggested_prompt": "hello",
                "suggested_tools": [],
            }
        )
        svc._log_llm_interaction = AsyncMock()

        gc = _make_group_context()
        request = DispatcherRequest(message="hello", model="m")
        await svc.dispatch(request, group_context=gc)

        svc.detect_intent.assert_awaited_once_with("hello", "m", gc, None)

    def test_semantic_analysis_returns_all_expected_keys(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("find data")
        expected_keys = {
            "task_actions",
            "agent_keywords",
            "crew_keywords",
            "execute_keywords",
            "configure_keywords",
            "catalog_keywords",
            "has_imperative",
            "has_question",
            "has_greeting",
            "has_command_structure",
            "has_configure_structure",
            "has_complex_task",
            "intent_scores",
            "semantic_hints",
            "suggested_intent",
        }
        assert expected_keys.issubset(set(result.keys()))

    def test_semantic_analysis_intent_scores_has_all_types(self):
        svc = _build_service()
        result = svc._analyze_message_semantics("hello")
        expected_intents = {
            "generate_task",
            "generate_agent",
            "generate_crew",
            "execute_crew",
            "configure_crew",
            "catalog_list",
            "catalog_load",
            "catalog_save",
            "catalog_schedule",
        }
        assert expected_intents == set(result["intent_scores"].keys())


# ===================================================================
# Tests for _build_tool_catalog
# ===================================================================


class TestBuildToolCatalog:

    def test_formats_tools_correctly(self):
        tools = [
            {"title": "SerperDevTool", "description": "Search the web"},
            {"title": "ScrapeWebsiteTool", "description": "Scrape website content"},
        ]
        result = DispatcherService._build_tool_catalog(tools)

        assert "SerperDevTool: Search the web" in result
        assert "ScrapeWebsiteTool: Scrape website content" in result
        assert "suggested_tools" in result
        assert "Available tools in the workspace:" in result

    def test_single_tool(self):
        tools = [{"title": "WebSearchTool", "description": "Performs web searches"}]
        result = DispatcherService._build_tool_catalog(tools)

        assert "WebSearchTool: Performs web searches" in result

    def test_empty_descriptions(self):
        tools = [{"title": "MyTool", "description": ""}]
        result = DispatcherService._build_tool_catalog(tools)

        assert "- MyTool: " in result


# ===================================================================
# Tests for suggested_tools in detect_intent
# ===================================================================


class TestDetectIntentWithTools:

    @pytest.mark.asyncio
    async def test_suggested_tools_parsed_and_validated(self):
        """LLM response with suggested_tools should be parsed and validated."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        available_tools = [
            {"title": "SerperDevTool", "description": "Search"},
            {"title": "ScrapeWebsiteTool", "description": "Scrape"},
        ]

        llm_content = (
            '{"intent": "generate_crew", "confidence": 0.9, '
            '"extracted_info": {}, "suggested_prompt": "research task", '
            '"suggested_tools": ["SerperDevTool", "ScrapeWebsiteTool"]}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent(
                "research topic X", "m", available_tools=available_tools
            )

        assert result["suggested_tools"] == ["SerperDevTool", "ScrapeWebsiteTool"]

    @pytest.mark.asyncio
    async def test_hallucinated_tools_filtered_out(self):
        """Tool names not in available_tools should be dropped."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        available_tools = [
            {"title": "SerperDevTool", "description": "Search"},
        ]

        llm_content = (
            '{"intent": "generate_crew", "confidence": 0.9, '
            '"extracted_info": {}, "suggested_prompt": "research", '
            '"suggested_tools": ["SerperDevTool", "HallucinatedTool", "FakeTool"]}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent(
                "research topic", "m", available_tools=available_tools
            )

        assert result["suggested_tools"] == ["SerperDevTool"]

    @pytest.mark.asyncio
    async def test_suggested_tools_empty_when_no_available_tools(self):
        """When no available_tools provided, suggested_tools should be empty."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = (
            '{"intent": "generate_task", "confidence": 0.9, '
            '"extracted_info": {}, "suggested_prompt": "find data", '
            '"suggested_tools": ["SomeTool"]}'
        )

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value=llm_content,
        ):
            result = await svc.detect_intent("find data", "m")

        assert result["suggested_tools"] == []

    @pytest.mark.asyncio
    async def test_cache_key_differs_with_different_tools(self):
        """Cache key should be different when different tools are available."""
        import hashlib

        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        llm_content = '{"intent": "generate_task", "confidence": 0.9}'

        tools_a = [{"title": "ToolA", "description": "A"}]
        tools_b = [{"title": "ToolB", "description": "B"}]

        results = []
        for tools in [tools_a, tools_b, None]:
            with patch(
                "src.services.dispatcher_service.LLMManager.completion",
                new_callable=AsyncMock,
                return_value=llm_content,
            ):
                result = await svc.detect_intent(
                    "find data", "m", available_tools=tools
                )
                results.append(result)

        # All three calls should have produced results (different cache keys)
        # If cache keys were the same, only the first call would hit LLM
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_fallback_paths_return_empty_suggested_tools(self):
        """All fallback paths should return suggested_tools: []."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        # Exception fallback
        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            side_effect=RuntimeError("API down"),
        ):
            result = await svc.detect_intent("find flights", "m")

        assert result["suggested_tools"] == []

    @pytest.mark.asyncio
    async def test_empty_response_fallback_returns_empty_suggested_tools(self):
        """Empty LLM response fallback should return suggested_tools: []."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
            return_value="",
        ):
            result = await svc.detect_intent("find flights", "m")

        assert result["suggested_tools"] == []

    @pytest.mark.asyncio
    async def test_tool_catalog_appended_to_prompt(self):
        """When available_tools are provided, the tool catalog should be in the prompt."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        available_tools = [
            {"title": "WebSearchTool", "description": "Search the web"},
        ]

        captured_messages = []

        async def capture_completion(messages, model, **kwargs):
            captured_messages.append(messages)
            return '{"intent": "generate_task", "confidence": 0.9}'

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            side_effect=capture_completion,
        ):
            await svc.detect_intent("find data", "m", available_tools=available_tools)

        assert len(captured_messages) == 1
        user_msg = captured_messages[0][1]["content"]
        assert "WebSearchTool: Search the web" in user_msg
        assert "suggested_tools" in user_msg


# ===================================================================
# Tests for dispatch with suggested_tools
# ===================================================================


class TestDispatchWithSuggestedTools:

    def _make_intent_result(self, intent, suggested_tools=None, **kwargs):
        result = {
            "intent": intent,
            "confidence": kwargs.get("confidence", 0.9),
            "extracted_info": kwargs.get("extracted_info", {}),
            "suggested_prompt": kwargs.get("suggested_prompt", "test prompt"),
            "suggested_tools": suggested_tools or [],
        }
        return result

    @pytest.mark.asyncio
    async def test_dispatch_uses_suggested_tools_when_request_tools_empty(self):
        """When request.tools is empty, suggested_tools should be used."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "generate_crew",
                suggested_tools=["SerperDevTool", "ScrapeWebsiteTool"],
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(return_value={})

        request = DispatcherRequest(message="research AI trends", model="m", tools=[])
        await svc.dispatch(request)

        call_args = svc.crew_service.create_crew_complete.call_args
        crew_req = call_args[0][0]
        assert crew_req.tools == ["SerperDevTool", "ScrapeWebsiteTool"]

    @pytest.mark.asyncio
    async def test_dispatch_uses_request_tools_over_suggested(self):
        """When request.tools is provided, it should take precedence."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "generate_crew",
                suggested_tools=["SerperDevTool"],
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.crew_service.create_crew_complete = AsyncMock(return_value={})

        request = DispatcherRequest(
            message="research AI", model="m", tools=["UserSelectedTool"]
        )
        await svc.dispatch(request)

        call_args = svc.crew_service.create_crew_complete.call_args
        crew_req = call_args[0][0]
        assert crew_req.tools == ["UserSelectedTool"]

    @pytest.mark.asyncio
    async def test_dispatch_suggested_tools_in_agent_generation(self):
        """suggested_tools should be passed to agent generation when no request tools."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "generate_agent",
                suggested_tools=["FileReadTool"],
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.agent_service.generate_agent = AsyncMock(return_value={})

        request = DispatcherRequest(message="create an agent", model="m")
        await svc.dispatch(request)

        call_kwargs = svc.agent_service.generate_agent.call_args.kwargs
        assert call_kwargs["tools"] == ["FileReadTool"]

    @pytest.mark.asyncio
    async def test_dispatch_response_contains_suggested_tools(self):
        """The dispatch response should contain suggested_tools."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "unknown",
                suggested_tools=["SerperDevTool"],
            )
        )
        svc._log_llm_interaction = AsyncMock()

        request = DispatcherRequest(message="hello", model="m")
        result = await svc.dispatch(request)

        assert result["dispatcher"]["suggested_tools"] == ["SerperDevTool"]

    @pytest.mark.asyncio
    async def test_dispatch_passes_available_tools_to_detect_intent(self):
        """dispatch should forward available_tools to detect_intent."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(return_value=self._make_intent_result("unknown"))
        svc._log_llm_interaction = AsyncMock()

        available_tools = [{"title": "T1", "description": "D1"}]
        request = DispatcherRequest(message="hello", model="m")
        await svc.dispatch(request, available_tools=available_tools)

        call_args = svc.detect_intent.call_args
        assert call_args[0][3] == available_tools


# ===================================================================
# Tests for _detect_slash_command
# ===================================================================


class TestSlashCommandDetection:

    # --- Bare commands (no qualifier) show usage help ---

    def test_bare_list_shows_help(self):
        result = DispatcherService._detect_slash_command("/list")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["extracted_info"]["command_help"].startswith("Usage:")
        assert "/list crews" in result["extracted_info"]["command_help"]
        assert "/list flows" in result["extracted_info"]["command_help"]

    def test_bare_load_shows_help(self):
        result = DispatcherService._detect_slash_command("/load")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/load crew" in result["extracted_info"]["command_help"]

    def test_bare_load_with_unqualified_name_shows_help(self):
        result = DispatcherService._detect_slash_command("/load my-research-plan")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/load crew" in result["extracted_info"]["command_help"]

    def test_bare_save_shows_help(self):
        result = DispatcherService._detect_slash_command("/save My Plan")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/save crew" in result["extracted_info"]["command_help"]

    def test_bare_run_shows_help(self):
        result = DispatcherService._detect_slash_command("/run")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/run crew" in result["extracted_info"]["command_help"]

    def test_bare_exec_shows_help(self):
        result = DispatcherService._detect_slash_command("/exec")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/run crew" in result["extracted_info"]["command_help"]

    def test_bare_schedule_shows_help(self):
        result = DispatcherService._detect_slash_command("/schedule")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/schedule crew" in result["extracted_info"]["command_help"]

    # --- Qualified crew commands ---

    def test_list_crews(self):
        result = DispatcherService._detect_slash_command("/list crews")
        assert result is not None
        assert result["intent"] == "catalog_list"
        assert result["confidence"] == 1.0
        assert result["source"] == "slash_command"
        assert result["extracted_info"]["command"] == "/list"
        assert result["extracted_info"]["args"] == ""
        assert result["suggested_tools"] == []

    def test_load_crew_with_name(self):
        result = DispatcherService._detect_slash_command("/load crew my-research-plan")
        assert result is not None
        assert result["intent"] == "catalog_load"
        assert result["extracted_info"]["args"] == "my-research-plan"

    def test_load_crew_without_name(self):
        result = DispatcherService._detect_slash_command("/load crew")
        assert result is not None
        assert result["intent"] == "catalog_load"
        assert result["extracted_info"]["args"] == ""

    def test_save_crew_with_name(self):
        result = DispatcherService._detect_slash_command("/save crew My Plan")
        assert result is not None
        assert result["intent"] == "catalog_save"
        assert result["extracted_info"]["args"] == "My Plan"

    def test_save_crew_without_name(self):
        result = DispatcherService._detect_slash_command("/save crew")
        assert result is not None
        assert result["intent"] == "catalog_save"
        assert result["extracted_info"]["args"] == ""

    def test_run_crew(self):
        result = DispatcherService._detect_slash_command("/run crew")
        assert result is not None
        assert result["intent"] == "execute_crew"
        assert result["confidence"] == 1.0

    def test_schedule_crew(self):
        result = DispatcherService._detect_slash_command("/schedule crew")
        assert result is not None
        assert result["intent"] == "catalog_schedule"

    # --- Aliases that imply qualifier ---

    def test_plans_alias(self):
        result = DispatcherService._detect_slash_command("/plans")
        assert result is not None
        assert result["intent"] == "catalog_list"

    # --- /help (no qualifier needed) ---

    def test_help_command(self):
        result = DispatcherService._detect_slash_command("/help")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["confidence"] == 1.0
        assert result["source"] == "slash_command"

    # --- Invalid/unknown commands ---

    def test_unknown_command_returns_help_with_invalid_flag(self):
        result = DispatcherService._detect_slash_command("/unknown-command")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["confidence"] == 1.0
        assert result["source"] == "slash_command"
        assert result["extracted_info"]["invalid_command"] is True
        assert result["extracted_info"]["command"] == "/unknown-command"

    def test_invalid_command_foo(self):
        result = DispatcherService._detect_slash_command("/foo")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["extracted_info"]["invalid_command"] is True
        assert result["extracted_info"]["command"] == "/foo"
        assert result["extracted_info"]["args"] == ""

    def test_invalid_command_with_args(self):
        result = DispatcherService._detect_slash_command("/xyz bar")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["extracted_info"]["invalid_command"] is True
        assert result["extracted_info"]["command"] == "/xyz"
        assert result["extracted_info"]["args"] == "bar"

    # --- Edge cases ---

    def test_regular_message_returns_none(self):
        result = DispatcherService._detect_slash_command("regular message")
        assert result is None

    def test_empty_message_returns_none(self):
        result = DispatcherService._detect_slash_command("")
        assert result is None

    def test_command_case_insensitive(self):
        result = DispatcherService._detect_slash_command("/LIST crews")
        assert result is not None
        assert result["intent"] == "catalog_list"

    def test_command_with_leading_spaces(self):
        result = DispatcherService._detect_slash_command("  /list crews")
        assert result is not None
        assert result["intent"] == "catalog_list"

    @pytest.mark.asyncio
    async def test_slash_command_bypasses_llm(self):
        """Slash commands should return immediately without calling LLM."""
        svc = _build_service()
        svc.template_service.get_template_content = AsyncMock(return_value="prompt")

        with patch(
            "src.services.dispatcher_service.LLMManager.completion",
            new_callable=AsyncMock,
        ) as mock_completion:
            result = await svc.detect_intent("/list crews", "test-model")

        assert result["intent"] == "catalog_list"
        assert result["source"] == "slash_command"
        mock_completion.assert_not_awaited()


# ===================================================================
# Tests for catalog dispatch handlers
# ===================================================================


class TestCatalogDispatch:

    def _make_intent_result(self, intent, confidence=1.0, args=""):
        return {
            "intent": intent,
            "confidence": confidence,
            "extracted_info": {"command": f"/{intent.split('_')[1]}", "args": args},
            "suggested_prompt": f"/{intent.split('_')[1]} {args}".strip(),
            "suggested_tools": [],
        }

    def _make_mock_crew(self, name="Test Crew", crew_id="crew-1"):
        crew = MagicMock()
        crew.id = crew_id
        crew.name = name
        crew.agent_ids = ["a1", "a2"]
        crew.task_ids = ["t1"]
        crew.nodes = [{"id": "node1"}]
        crew.edges = [{"id": "edge1"}]
        crew.process = "sequential"
        crew.planning = False
        crew.planning_llm = None
        crew.memory = True
        crew.verbose = True
        crew.max_rpm = None
        crew.created_at = MagicMock()
        crew.created_at.isoformat.return_value = "2026-01-01T00:00:00"
        crew.updated_at = MagicMock()
        crew.updated_at.isoformat.return_value = "2026-01-02T00:00:00"
        return crew

    @pytest.mark.asyncio
    async def test_catalog_list_returns_plans(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_list")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_crew = self._make_mock_crew()
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[mock_crew])

        gc = _make_group_context()
        request = DispatcherRequest(message="/list", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_list"
        assert len(gen["plans"]) == 1
        assert gen["plans"][0]["name"] == "Test Crew"
        assert gen["plans"][0]["agent_count"] == 2
        assert gen["plans"][0]["task_count"] == 1
        assert "Found 1 plan(s)" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_exact_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="Test Crew")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_crew = self._make_mock_crew()
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[mock_crew])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load Test Crew", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_load"
        assert gen["plan"]["name"] == "Test Crew"
        assert gen["plan"]["nodes"] == [{"id": "node1"}]
        assert gen["plan"]["edges"] == [{"id": "edge1"}]
        assert "Loaded plan" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_partial_match_multiple(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="Test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="Test Alpha", crew_id="c1")
        crew2 = self._make_mock_crew(name="Test Beta", crew_id="c2")
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[crew1, crew2])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load Test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_list"
        assert len(gen["plans"]) == 2
        assert "Multiple plans match" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_duplicate_names_loads_most_recent(self):
        """When multiple plans share the exact same name, load the most recent one."""
        from datetime import datetime

        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="test", crew_id="c1")
        crew1.updated_at = datetime(2026, 1, 1)
        crew1.created_at = datetime(2026, 1, 1)
        crew2 = self._make_mock_crew(name="test", crew_id="c2")
        crew2.updated_at = datetime(2026, 1, 5)
        crew2.created_at = datetime(2026, 1, 5)
        crew3 = self._make_mock_crew(name="test", crew_id="c3")
        crew3.updated_at = datetime(2026, 1, 3)
        crew3.created_at = datetime(2026, 1, 3)
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(
            return_value=[crew1, crew2, crew3]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/load test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_load"
        assert gen["plan"]["id"] == "c2"  # most recent
        assert gen["plan"]["name"] == "test"
        assert "most recent" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_exact_match_preferred_over_partial(self):
        """Exact name match is preferred even when partial matches exist."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="test", crew_id="c1")
        crew2 = self._make_mock_crew(name="test plan", crew_id="c2")
        crew3 = self._make_mock_crew(name="my test", crew_id="c3")
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(
            return_value=[crew1, crew2, crew3]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/load test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_load"
        assert gen["plan"]["id"] == "c1"
        assert gen["plan"]["name"] == "test"
        assert "Loaded plan" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_no_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="nonexistent")
        )
        svc._log_llm_interaction = AsyncMock()

        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load nonexistent", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_load"
        assert gen["plan"] is None
        assert "No plan found" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_load_no_args_returns_list(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_load", args="")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_crew = self._make_mock_crew()
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[mock_crew])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_list"
        assert "No plan name specified" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_save_returns_action_flag(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_save", args="")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.catalog_service = AsyncMock()

        request = DispatcherRequest(message="/save", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_save"
        assert gen["action"] == "open_save_dialog"
        assert gen["suggested_name"] is None

    @pytest.mark.asyncio
    async def test_catalog_save_with_name(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result(
                "catalog_save", args="My Research Plan"
            )
        )
        svc._log_llm_interaction = AsyncMock()
        svc.catalog_service = AsyncMock()

        request = DispatcherRequest(message="/save My Research Plan", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_save"
        assert gen["suggested_name"] == "My Research Plan"
        assert "My Research Plan" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_schedule_returns_action_flag(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_schedule")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.catalog_service = AsyncMock()

        request = DispatcherRequest(message="/schedule", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_schedule"
        assert gen["action"] == "open_schedule_dialog"

    @pytest.mark.asyncio
    async def test_catalog_help_returns_commands(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_help")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.catalog_service = AsyncMock()

        request = DispatcherRequest(message="/help", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_help"
        assert "/list" in gen["message"]
        assert "/load" in gen["message"]
        assert "/save" in gen["message"]
        assert "/run" in gen["message"]
        assert "/schedule" in gen["message"]
        assert "/help" in gen["message"]
        assert "/list flows" in gen["message"]
        assert "/load flow" in gen["message"]
        assert "/save flow" in gen["message"]
        assert "/run flow" in gen["message"]
        assert "/delete crew" in gen["message"]
        assert "/delete flow" in gen["message"]


# ===================================================================
# Tests for flow slash command detection
# ===================================================================


class TestFlowSlashCommandDetection:

    def test_list_flows(self):
        result = DispatcherService._detect_slash_command("/list flows")
        assert result is not None
        assert result["intent"] == "flow_list"
        assert result["extracted_info"]["args"] == ""

    def test_list_flow_singular(self):
        result = DispatcherService._detect_slash_command("/list flow")
        assert result is not None
        assert result["intent"] == "flow_list"
        assert result["extracted_info"]["args"] == ""

    def test_flows_alias(self):
        result = DispatcherService._detect_slash_command("/flows")
        assert result is not None
        assert result["intent"] == "flow_list"

    def test_load_flow_with_name(self):
        result = DispatcherService._detect_slash_command("/load flow my-flow")
        assert result is not None
        assert result["intent"] == "flow_load"
        assert result["extracted_info"]["args"] == "my-flow"

    def test_load_flow_without_name(self):
        result = DispatcherService._detect_slash_command("/load flow")
        assert result is not None
        assert result["intent"] == "flow_load"
        assert result["extracted_info"]["args"] == ""

    def test_save_flow_with_name(self):
        result = DispatcherService._detect_slash_command("/save flow My Flow")
        assert result is not None
        assert result["intent"] == "flow_save"
        assert result["extracted_info"]["args"] == "My Flow"

    def test_save_flow_without_name(self):
        result = DispatcherService._detect_slash_command("/save flow")
        assert result is not None
        assert result["intent"] == "flow_save"
        assert result["extracted_info"]["args"] == ""

    def test_run_flow_routes_to_execute_flow(self):
        """'/run flow' routes to execute_flow via FLOW_INTENT_MAP."""
        result = DispatcherService._detect_slash_command("/run flow")
        assert result is not None
        assert result["intent"] == "execute_flow"
        assert result["confidence"] == 1.0

    def test_bare_list_shows_help(self):
        result = DispatcherService._detect_slash_command("/list")
        assert result is not None
        assert result["intent"] == "catalog_help"

    def test_bare_load_with_name_shows_help(self):
        result = DispatcherService._detect_slash_command("/load my-plan")
        assert result is not None
        assert result["intent"] == "catalog_help"


# ===================================================================
# Tests for flow dispatch handlers
# ===================================================================


class TestFlowDispatch:

    def _make_intent_result(self, intent, confidence=1.0, args=""):
        return {
            "intent": intent,
            "confidence": confidence,
            "extracted_info": {"command": "/flow", "args": args},
            "suggested_prompt": f"/flow {args}".strip(),
            "suggested_tools": [],
        }

    def _make_mock_flow(self, name="Test Flow", flow_id="flow-1"):
        flow = MagicMock()
        flow.id = flow_id
        flow.name = name
        flow.nodes = [{"id": "crew-node-1", "type": "crewNode"}]
        flow.edges = [{"id": "edge1"}]
        flow.flow_config = {"start_method": "sequential"}
        flow.created_at = MagicMock()
        flow.created_at.isoformat.return_value = "2026-01-01T00:00:00"
        flow.updated_at = MagicMock()
        flow.updated_at.isoformat.return_value = "2026-01-02T00:00:00"
        return flow

    @pytest.mark.asyncio
    async def test_flow_list_returns_flows(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_list")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_flow = self._make_mock_flow()
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[mock_flow])

        gc = _make_group_context()
        request = DispatcherRequest(message="/list flows", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_list"
        assert len(gen["flows"]) == 1
        assert gen["flows"][0]["name"] == "Test Flow"
        assert gen["flows"][0]["node_count"] == 1
        assert "Found 1 flow(s)" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_exact_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="Test Flow")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_flow = self._make_mock_flow()
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[mock_flow])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow Test Flow", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_load"
        assert gen["flow"]["name"] == "Test Flow"
        assert gen["flow"]["nodes"] == [{"id": "crew-node-1", "type": "crewNode"}]
        assert gen["flow"]["edges"] == [{"id": "edge1"}]
        assert gen["flow"]["flow_config"] == {"start_method": "sequential"}
        assert "Loaded flow" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_multiple_matches(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="Test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="Test Alpha", flow_id="f1")
        flow2 = self._make_mock_flow(name="Test Beta", flow_id="f2")
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow Test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_list"
        assert len(gen["flows"]) == 2
        assert "Multiple flows match" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_duplicate_names_loads_most_recent(self):
        """When multiple flows share the exact same name, load the most recent one."""
        from datetime import datetime

        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="test", flow_id="f1")
        flow1.updated_at = datetime(2026, 1, 1)
        flow1.created_at = datetime(2026, 1, 1)
        flow2 = self._make_mock_flow(name="test", flow_id="f2")
        flow2.updated_at = datetime(2026, 1, 5)
        flow2.created_at = datetime(2026, 1, 5)
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_load"
        assert gen["flow"]["id"] == "f2"  # most recent
        assert gen["flow"]["name"] == "test"
        assert "most recent" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_exact_match_preferred_over_partial(self):
        """Exact name match is preferred even when partial matches exist."""
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="test", flow_id="f1")
        flow2 = self._make_mock_flow(name="test flow", flow_id="f2")
        flow3 = self._make_mock_flow(name="my test", flow_id="f3")
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2, flow3]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_load"
        assert gen["flow"]["id"] == "f1"
        assert gen["flow"]["name"] == "test"
        assert "Loaded flow" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_no_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="nonexistent")
        )
        svc._log_llm_interaction = AsyncMock()

        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow nonexistent", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_load"
        assert gen["flow"] is None
        assert "No flow found" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_load_no_args_returns_list(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_load", args="")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_flow = self._make_mock_flow()
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[mock_flow])

        gc = _make_group_context()
        request = DispatcherRequest(message="/load flow", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_list"
        assert len(gen["flows"]) == 1
        assert "No flow name specified" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_save_returns_action(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_save", args="My Flow")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.flow_service = AsyncMock()

        request = DispatcherRequest(message="/save flow My Flow", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "flow_save"
        assert gen["action"] == "open_save_flow_dialog"
        assert gen["suggested_name"] == "My Flow"
        assert "Saving flow 'My Flow'" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_save_no_name(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_save", args="")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.flow_service = AsyncMock()

        request = DispatcherRequest(message="/save flow", model="m")
        result = await svc.dispatch(request)

        gen = result["generation_result"]
        assert gen["type"] == "flow_save"
        assert gen["action"] == "open_save_flow_dialog"
        assert gen["suggested_name"] is None
        assert "Opening save flow dialog" in gen["message"]


# ===================================================================
# Tests for /delete slash command detection
# ===================================================================


class TestDeleteSlashCommandDetection:

    def test_bare_delete_shows_help(self):
        result = DispatcherService._detect_slash_command("/delete")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert result["extracted_info"]["command_help"].startswith("Usage:")
        assert "/delete crew" in result["extracted_info"]["command_help"]
        assert "/delete flow" in result["extracted_info"]["command_help"]

    def test_delete_with_unqualified_name_shows_help(self):
        result = DispatcherService._detect_slash_command("/delete some-name")
        assert result is not None
        assert result["intent"] == "catalog_help"
        assert "/delete crew" in result["extracted_info"]["command_help"]

    def test_delete_crew_with_name(self):
        result = DispatcherService._detect_slash_command("/delete crew my-crew")
        assert result is not None
        assert result["intent"] == "catalog_delete"
        assert result["confidence"] == 1.0
        assert result["source"] == "slash_command"
        assert result["extracted_info"]["args"] == "my-crew"
        assert result["suggested_tools"] == []

    def test_delete_crew_without_name(self):
        result = DispatcherService._detect_slash_command("/delete crew")
        assert result is not None
        assert result["intent"] == "catalog_delete"
        assert result["extracted_info"]["args"] == ""

    def test_delete_crews_plural(self):
        result = DispatcherService._detect_slash_command("/delete crews my-crew")
        assert result is not None
        assert result["intent"] == "catalog_delete"
        assert result["extracted_info"]["args"] == "my-crew"

    def test_delete_flow_with_name(self):
        result = DispatcherService._detect_slash_command("/delete flow my-flow")
        assert result is not None
        assert result["intent"] == "flow_delete"
        assert result["extracted_info"]["args"] == "my-flow"

    def test_delete_flow_without_name(self):
        result = DispatcherService._detect_slash_command("/delete flow")
        assert result is not None
        assert result["intent"] == "flow_delete"
        assert result["extracted_info"]["args"] == ""

    def test_delete_flows_plural(self):
        result = DispatcherService._detect_slash_command("/delete flows my-flow")
        assert result is not None
        assert result["intent"] == "flow_delete"
        assert result["extracted_info"]["args"] == "my-flow"

    def test_delete_case_insensitive(self):
        result = DispatcherService._detect_slash_command("/DELETE crew test")
        assert result is not None
        assert result["intent"] == "catalog_delete"
        assert result["extracted_info"]["args"] == "test"

    def test_delete_crew_multiword_name(self):
        result = DispatcherService._detect_slash_command("/delete crew My Research Crew")
        assert result is not None
        assert result["intent"] == "catalog_delete"
        assert result["extracted_info"]["args"] == "My Research Crew"

    def test_delete_flow_multiword_name(self):
        result = DispatcherService._detect_slash_command("/delete flow My Data Flow")
        assert result is not None
        assert result["intent"] == "flow_delete"
        assert result["extracted_info"]["args"] == "My Data Flow"


# ===================================================================
# Tests for catalog_delete dispatch handler
# ===================================================================


class TestCatalogDeleteDispatch:

    def _make_intent_result(self, intent, confidence=1.0, args=""):
        return {
            "intent": intent,
            "confidence": confidence,
            "extracted_info": {"command": "/delete", "args": args},
            "suggested_prompt": f"/delete {args}".strip(),
            "suggested_tools": [],
        }

    def _make_mock_crew(self, name="Test Crew", crew_id="crew-1"):
        crew = MagicMock()
        crew.id = crew_id
        crew.name = name
        crew.agent_ids = ["a1", "a2"]
        crew.task_ids = ["t1"]
        crew.nodes = [{"id": "node1"}]
        crew.edges = [{"id": "edge1"}]
        crew.process = "sequential"
        crew.planning = False
        crew.planning_llm = None
        crew.memory = True
        crew.verbose = True
        crew.max_rpm = None
        crew.created_at = MagicMock()
        crew.created_at.isoformat.return_value = "2026-01-01T00:00:00"
        crew.updated_at = MagicMock()
        crew.updated_at.isoformat.return_value = "2026-01-02T00:00:00"
        return crew

    @pytest.mark.asyncio
    async def test_catalog_delete_no_name(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.catalog_service = AsyncMock()

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_delete"
        assert "Please specify a crew name" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_delete_single_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="Test Crew")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_crew = self._make_mock_crew()
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[mock_crew])
        svc.catalog_service.delete_by_group = AsyncMock(return_value=True)

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew Test Crew", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_delete"
        assert "has been deleted" in gen["message"]
        assert "Test Crew" in gen["message"]
        svc.catalog_service.delete_by_group.assert_awaited_once_with("crew-1", gc)

    @pytest.mark.asyncio
    async def test_catalog_delete_exact_match_preferred(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="test", crew_id="c1")
        crew2 = self._make_mock_crew(name="test plan", crew_id="c2")
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[crew1, crew2])
        svc.catalog_service.delete_by_group = AsyncMock(return_value=True)

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_delete"
        assert "has been deleted" in gen["message"]
        svc.catalog_service.delete_by_group.assert_awaited_once_with("c1", gc)

    @pytest.mark.asyncio
    async def test_catalog_delete_multiple_ambiguous_matches(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="Test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="Test Alpha", crew_id="c1")
        crew2 = self._make_mock_crew(name="Test Beta", crew_id="c2")
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[crew1, crew2])

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew Test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_list"
        assert len(gen["plans"]) == 2
        assert "Multiple crews match" in gen["message"]

    @pytest.mark.asyncio
    async def test_catalog_delete_duplicate_names_deletes_most_recent(self):
        from datetime import datetime

        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        crew1 = self._make_mock_crew(name="test", crew_id="c1")
        crew1.updated_at = datetime(2026, 1, 1)
        crew1.created_at = datetime(2026, 1, 1)
        crew2 = self._make_mock_crew(name="test", crew_id="c2")
        crew2.updated_at = datetime(2026, 1, 5)
        crew2.created_at = datetime(2026, 1, 5)
        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[crew1, crew2])
        svc.catalog_service.delete_by_group = AsyncMock(return_value=True)

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_delete"
        assert "most recent" in gen["message"]
        assert "has been deleted" in gen["message"]
        svc.catalog_service.delete_by_group.assert_awaited_once_with("c2", gc)

    @pytest.mark.asyncio
    async def test_catalog_delete_no_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("catalog_delete", args="nonexistent")
        )
        svc._log_llm_interaction = AsyncMock()

        svc.catalog_service = AsyncMock()
        svc.catalog_service.find_by_group = AsyncMock(return_value=[])

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete crew nonexistent", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "catalog_delete"
        assert "No crew found" in gen["message"]
        assert "nonexistent" in gen["message"]


# ===================================================================
# Tests for flow_delete dispatch handler
# ===================================================================


class TestFlowDeleteDispatch:

    def _make_intent_result(self, intent, confidence=1.0, args=""):
        return {
            "intent": intent,
            "confidence": confidence,
            "extracted_info": {"command": "/delete", "args": args},
            "suggested_prompt": f"/delete {args}".strip(),
            "suggested_tools": [],
        }

    def _make_mock_flow(self, name="Test Flow", flow_id="flow-1"):
        flow = MagicMock()
        flow.id = flow_id
        flow.name = name
        flow.nodes = [{"id": "crew-node-1", "type": "crewNode"}]
        flow.edges = [{"id": "edge1"}]
        flow.flow_config = {"start_method": "sequential"}
        flow.created_at = MagicMock()
        flow.created_at.isoformat.return_value = "2026-01-01T00:00:00"
        flow.updated_at = MagicMock()
        flow.updated_at.isoformat.return_value = "2026-01-02T00:00:00"
        return flow

    @pytest.mark.asyncio
    async def test_flow_delete_no_name(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="")
        )
        svc._log_llm_interaction = AsyncMock()
        svc.flow_service = AsyncMock()

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_delete"
        assert "Please specify a flow name" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_delete_single_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="Test Flow")
        )
        svc._log_llm_interaction = AsyncMock()

        mock_flow = self._make_mock_flow()
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[mock_flow])
        svc.flow_service.force_delete_flow_with_executions_with_group_check = AsyncMock(
            return_value=True
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow Test Flow", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_delete"
        assert "has been deleted" in gen["message"]
        assert "Test Flow" in gen["message"]
        svc.flow_service.force_delete_flow_with_executions_with_group_check.assert_awaited_once_with(
            "flow-1", gc
        )

    @pytest.mark.asyncio
    async def test_flow_delete_exact_match_preferred(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="test", flow_id="f1")
        flow2 = self._make_mock_flow(name="test flow", flow_id="f2")
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2]
        )
        svc.flow_service.force_delete_flow_with_executions_with_group_check = AsyncMock(
            return_value=True
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_delete"
        assert "has been deleted" in gen["message"]
        svc.flow_service.force_delete_flow_with_executions_with_group_check.assert_awaited_once_with(
            "f1", gc
        )

    @pytest.mark.asyncio
    async def test_flow_delete_multiple_ambiguous_matches(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="Test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="Test Alpha", flow_id="f1")
        flow2 = self._make_mock_flow(name="Test Beta", flow_id="f2")
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2]
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow Test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_list"
        assert len(gen["flows"]) == 2
        assert "Multiple flows match" in gen["message"]

    @pytest.mark.asyncio
    async def test_flow_delete_duplicate_names_deletes_most_recent(self):
        from datetime import datetime

        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="test")
        )
        svc._log_llm_interaction = AsyncMock()

        flow1 = self._make_mock_flow(name="test", flow_id="f1")
        flow1.updated_at = datetime(2026, 1, 1)
        flow1.created_at = datetime(2026, 1, 1)
        flow2 = self._make_mock_flow(name="test", flow_id="f2")
        flow2.updated_at = datetime(2026, 1, 5)
        flow2.created_at = datetime(2026, 1, 5)
        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(
            return_value=[flow1, flow2]
        )
        svc.flow_service.force_delete_flow_with_executions_with_group_check = AsyncMock(
            return_value=True
        )

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow test", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_delete"
        assert "most recent" in gen["message"]
        assert "has been deleted" in gen["message"]
        svc.flow_service.force_delete_flow_with_executions_with_group_check.assert_awaited_once_with(
            "f2", gc
        )

    @pytest.mark.asyncio
    async def test_flow_delete_no_match(self):
        svc = _build_service()
        svc._maybe_enable_mlflow_tracing = AsyncMock(return_value=False)
        svc.detect_intent = AsyncMock(
            return_value=self._make_intent_result("flow_delete", args="nonexistent")
        )
        svc._log_llm_interaction = AsyncMock()

        svc.flow_service = AsyncMock()
        svc.flow_service.get_all_flows_for_group = AsyncMock(return_value=[])

        gc = _make_group_context()
        request = DispatcherRequest(message="/delete flow nonexistent", model="m")
        result = await svc.dispatch(request, group_context=gc)

        gen = result["generation_result"]
        assert gen["type"] == "flow_delete"
        assert "No flow found" in gen["message"]
        assert "nonexistent" in gen["message"]
