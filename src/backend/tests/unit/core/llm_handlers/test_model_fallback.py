"""Model-fallback policy: error classification + next-model selection.

Pins which failures warrant swapping models (and which must NOT), and how the
replacement is chosen — context-window failures need a larger window, others
just need a different healthy model.
"""

from types import SimpleNamespace

from src.core.llm_handlers.model_fallback import (
    CONTEXT_WINDOW,
    ENDPOINT_MISSING,
    FATAL_4XX,
    RATE_LIMIT,
    ModelCandidate,
    candidates_from_model_configs,
    classify_llm_error,
    select_fallback,
)


class NotFoundError(Exception):
    """Class-name match, like litellm.NotFoundError."""
    pass


class TestEndpointMissing:
    """ENDPOINT_NOT_FOUND (404): the model isn't deployed here → try another."""

    _MSG = ('litellm.NotFoundError: databricksException - '
            '{"error_code":"ENDPOINT_NOT_FOUND","message":"The given endpoint does '
            'not exist, please retry after checking the specified model and version '
            'deployment exists."}')

    def test_classify_endpoint_not_found_message(self):
        assert classify_llm_error(_Err(self._MSG, status=404)) == ENDPOINT_MISSING

    def test_classify_notfounderror_class(self):
        assert classify_llm_error(NotFoundError("nope")) == ENDPOINT_MISSING

    def test_selects_untried_different_family(self):
        cands = [
            ModelCandidate("databricks-gemini-2-5-flash", 1_000_000),
            ModelCandidate("databricks-claude-sonnet-4-5", 200_000),
        ]
        pick = select_fallback(
            cands, current_window=200_000, reason=ENDPOINT_MISSING,
            tried={"databricks-gemini-2-5-flash"},
            current_model="databricks-gemini-2-5-flash",
        )
        assert pick is not None and pick.name == "databricks-claude-sonnet-4-5"

    def test_returns_none_when_all_tried(self):
        cands = [ModelCandidate("a", 100), ModelCandidate("b", 200)]
        pick = select_fallback(
            cands, 200, ENDPOINT_MISSING, tried={"a", "b"}, current_model="a",
        )
        assert pick is None


class _Err(Exception):
    """Exception with an optional HTTP status, like litellm's errors."""

    def __init__(self, msg, status=None):
        super().__init__(msg)
        if status is not None:
            self.status_code = status


# Class-name-only matches (no message markers needed).
class ContextWindowExceededError(Exception):
    pass


class RateLimitError(Exception):
    pass


# --- classification ---------------------------------------------------------


class TestClassify:
    def test_context_window_from_message(self):
        # The exact Databricks error from the report.
        exc = _Err("prompt is too long: 212728 tokens > 200000 maximum", status=400)
        assert classify_llm_error(exc) == CONTEXT_WINDOW

    def test_context_window_from_class_name(self):
        assert classify_llm_error(ContextWindowExceededError("nope")) == CONTEXT_WINDOW

    def test_context_window_beats_generic_4xx(self):
        # Context errors arrive as 400 too; must classify as context, not fatal_4xx.
        exc = _Err("BadRequestError: maximum context length exceeded", status=400)
        assert classify_llm_error(exc) == CONTEXT_WINDOW

    def test_rate_limit_from_status(self):
        assert classify_llm_error(_Err("slow down", status=429)) == RATE_LIMIT

    def test_rate_limit_from_class_name(self):
        assert classify_llm_error(RateLimitError("429")) == RATE_LIMIT

    def test_fatal_4xx_gemini_thought_signature(self):
        exc = _Err(
            "litellm.BadRequestError: Function call is missing a thought_signature "
            "in functionCall parts ... INVALID_ARGUMENT",
            status=400,
        )
        assert classify_llm_error(exc) == FATAL_4XX

    def test_generic_400_is_not_fallback_worthy(self):
        # A plain bad request (no model-incompat marker) must NOT trigger fallback.
        assert (
            classify_llm_error(_Err("bad request: missing field", status=400)) is None
        )

    def test_auth_error_is_not_fallback_worthy(self):
        assert classify_llm_error(_Err("403 permission denied", status=403)) is None

    def test_none_and_unknown(self):
        assert classify_llm_error(None) is None
        assert classify_llm_error(_Err("connection reset")) is None

    def test_walks_exception_group(self):
        inner = _Err("prompt is too long: 1 tokens > 0 maximum", status=400)
        group = ExceptionGroup("task failed", [inner])
        assert classify_llm_error(group) == CONTEXT_WINDOW

    def test_walks_cause_chain(self):
        try:
            try:
                raise _Err("thought_signature missing", status=400)
            except Exception as inner:
                raise RuntimeError("wrapper") from inner
        except Exception as e:
            assert classify_llm_error(e) == FATAL_4XX


# --- selection --------------------------------------------------------------

A = ModelCandidate("a", context_window=8000)
B = ModelCandidate("b", context_window=128000)
C = ModelCandidate("c", context_window=1000000)


class TestSelect:
    def test_context_window_picks_largest_bigger_than_current(self):
        chosen = select_fallback(
            [A, B, C], current_window=128000, reason=CONTEXT_WINDOW, tried=set()
        )
        assert chosen == C  # only c is bigger than 128k

    def test_context_window_none_when_nothing_bigger(self):
        chosen = select_fallback(
            [A, B], current_window=128000, reason=CONTEXT_WINDOW, tried=set()
        )
        assert chosen is None  # caller should summarize instead

    def test_context_window_unknown_current_picks_roomiest(self):
        chosen = select_fallback(
            [A, B, C], current_window=0, reason=CONTEXT_WINDOW, tried=set()
        )
        assert chosen == C

    def test_fatal_4xx_picks_roomiest_untried(self):
        chosen = select_fallback(
            [A, B, C], current_window=0, reason=FATAL_4XX, tried={"c"}
        )
        assert chosen == B  # c already tried, b is roomiest remaining

    def test_rate_limit_picks_roomiest(self):
        chosen = select_fallback(
            [A, B], current_window=0, reason=RATE_LIMIT, tried=set()
        )
        assert chosen == B

    def test_excludes_tried_and_returns_none_when_exhausted(self):
        assert (
            select_fallback([A], current_window=0, reason=FATAL_4XX, tried={"a"})
            is None
        )

    def test_empty_candidates(self):
        assert (
            select_fallback([], current_window=0, reason=CONTEXT_WINDOW, tried=set())
            is None
        )


GEMINI = ModelCandidate("databricks-gemini-2-5-flash", 1_000_000)
CLAUDE = ModelCandidate("databricks-claude-sonnet-4-5", 200_000)
GPT = ModelCandidate("databricks-gpt-5", 400_000)


class TestFatal4xxFamilyAware:
    """fatal_4xx is usually family-wide (e.g. Gemini thought_signature), so the
    fallback should jump to a DIFFERENT family rather than bounce gemini->gemini."""

    def test_prefers_a_different_family(self):
        # gemini-2-5 is roomiest but same family as the failed gemini-3-5 -> skip it.
        chosen = select_fallback(
            [GEMINI, CLAUDE, GPT],
            current_window=0,
            reason=FATAL_4XX,
            tried=set(),
            current_model="databricks-gemini-3-5-flash",
        )
        assert chosen == GPT  # roomiest non-gemini

    def test_same_family_only_as_last_resort(self):
        chosen = select_fallback(
            [GEMINI],
            current_window=0,
            reason=FATAL_4XX,
            tried=set(),
            current_model="databricks-gemini-3-5-flash",
        )
        assert chosen == GEMINI  # nothing else left

    def test_no_current_model_falls_back_to_roomiest(self):
        chosen = select_fallback(
            [GEMINI, CLAUDE, GPT], current_window=0, reason=FATAL_4XX, tried=set()
        )
        assert chosen == GEMINI  # no family info -> roomiest


# --- candidate filtering ----------------------------------------------------


class TestCandidatesFromModelConfigs:
    def test_filters_current_nondatabricks_and_codex(self):
        models = [
            SimpleNamespace(
                key="databricks-gemini-3-5-flash",
                provider="databricks",
                context_window=1_000_000,
            ),  # current -> excluded
            SimpleNamespace(
                key="databricks-claude-sonnet-4-5",
                provider="databricks",
                context_window=200_000,
            ),
            SimpleNamespace(
                key="gemma4:e4b", provider="ollama", context_window=32_000
            ),  # non-databricks -> excluded
            SimpleNamespace(
                key="databricks-gpt-5-3-codex",
                provider="databricks",
                context_window=400_000,
            ),  # codex -> excluded (needs Responses API)
        ]
        out = candidates_from_model_configs(
            models, "databricks/databricks-gemini-3-5-flash"
        )
        assert [c.name for c in out] == ["databricks-claude-sonnet-4-5"]
        assert out[0].context_window == 200_000

    def test_safe_with_empty_none_and_missing_key(self):
        assert candidates_from_model_configs([], "x") == []
        assert candidates_from_model_configs(None, "x") == []
        bad = [SimpleNamespace(key=None, provider="databricks", context_window=1)]
        assert candidates_from_model_configs(bad, "x") == []
