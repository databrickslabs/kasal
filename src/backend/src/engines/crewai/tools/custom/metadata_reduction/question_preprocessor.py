"""
Question Preprocessor for Power BI Metadata Reduction.

Extracts structured intent from user questions before reduction:
- Time grain detection (year, quarter, month, week, day)
- Delta period detection (yoy, mom, wow, qoq)
- Output shape inference (top N, trend, comparison, single value)
- Keyword extraction (after stopword removal)
- Optional LLM layer for structured intent extraction

Author: Kasal Team
Date: 2026
"""

import json
import logging
import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set

import httpx

logger = logging.getLogger(__name__)


class TimeGrain(str, Enum):
    YEAR = "year"
    QUARTER = "quarter"
    MONTH = "month"
    WEEK = "week"
    DAY = "day"


class OutputShape(str, Enum):
    SINGLE_VALUE = "single_value"
    TOP_N = "top_n"
    TREND = "trend"
    COMPARISON = "comparison"
    CROSS_TAB = "cross_tab"
    LIST = "list"


@dataclass
class TimeIntelligence:
    """Time-related intent extracted from the question."""
    grain: Optional[TimeGrain] = None
    delta_periods: List[str] = field(default_factory=list)  # e.g., ["yoy", "mom"]
    has_ytd: bool = False
    has_mtd: bool = False
    has_qtd: bool = False


@dataclass
class QuestionIntent:
    """Structured intent extracted from a user question."""
    original_question: str
    keywords: List[str] = field(default_factory=list)
    measures: List[str] = field(default_factory=list)
    dimensions: List[str] = field(default_factory=list)
    time_intelligence: TimeIntelligence = field(default_factory=TimeIntelligence)
    output_shape: OutputShape = OutputShape.LIST
    top_n: Optional[int] = None
    needs_split: bool = False
    sub_questions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict for logging/output."""
        return {
            "original_question": self.original_question,
            "keywords": self.keywords,
            "measures": self.measures,
            "dimensions": self.dimensions,
            "time_intelligence": {
                "grain": self.time_intelligence.grain.value if self.time_intelligence.grain else None,
                "delta_periods": self.time_intelligence.delta_periods,
                "has_ytd": self.time_intelligence.has_ytd,
                "has_mtd": self.time_intelligence.has_mtd,
                "has_qtd": self.time_intelligence.has_qtd,
            },
            "output_shape": self.output_shape.value,
            "top_n": self.top_n,
            "needs_split": self.needs_split,
            "sub_questions": self.sub_questions,
        }


# ─── Token Lists ────────────────────────────────────────────────────────────

_TIME_GRAIN_TOKENS: Dict[TimeGrain, Set[str]] = {
    TimeGrain.YEAR: {"year", "yearly", "annual", "annually", "years", "yoy", "y/y"},
    TimeGrain.QUARTER: {"quarter", "quarterly", "quarters", "qoq", "q/q", "q1", "q2", "q3", "q4"},
    TimeGrain.MONTH: {"month", "monthly", "months", "mom", "m/m"},
    TimeGrain.WEEK: {"week", "weekly", "weeks", "wow", "w/w"},
    TimeGrain.DAY: {"day", "daily", "days"},
}

_DELTA_TOKENS: Dict[str, str] = {
    "yoy": "yoy", "y/y": "yoy", "year over year": "yoy", "year-over-year": "yoy",
    "mom": "mom", "m/m": "mom", "month over month": "mom", "month-over-month": "mom",
    "qoq": "qoq", "q/q": "qoq", "quarter over quarter": "qoq", "quarter-over-quarter": "qoq",
    "wow": "wow", "w/w": "wow", "week over week": "wow", "week-over-week": "wow",
    "delta": "delta", "change": "change", "diff": "diff", "difference": "diff",
    "growth": "growth", "decline": "decline", "increase": "increase", "decrease": "decrease",
    "variance": "variance", "vs": "comparison", "versus": "comparison", "compared to": "comparison",
}

_ACCUMULATION_TOKENS = {
    "ytd": "ytd", "year to date": "ytd", "year-to-date": "ytd",
    "mtd": "mtd", "month to date": "mtd", "month-to-date": "mtd",
    "qtd": "qtd", "quarter to date": "qtd", "quarter-to-date": "qtd",
}

_TOP_N_PATTERN = re.compile(
    r"\b(?:top|bottom|best|worst|highest|lowest|first|last)\s+(\d+)\b",
    re.IGNORECASE,
)

_TREND_TOKENS = {"trend", "over time", "evolution", "progression", "trajectory", "history", "historical"}

_COMPARISON_TOKENS = {"compare", "comparison", "vs", "versus", "against", "between", "relative"}

_SINGLE_VALUE_TOKENS = {"total", "sum", "count", "average", "avg", "overall", "grand total"}

# Stopwords for keyword extraction
_STOPWORDS = frozenset({
    "a", "an", "and", "are", "as", "at", "be", "by", "do", "for", "from",
    "get", "has", "have", "how", "i", "if", "in", "is", "it", "its", "me",
    "much", "my", "no", "not", "of", "on", "or", "our", "per", "show",
    "tell", "the", "to", "us", "was", "we", "what", "when", "where",
    "which", "who", "why", "will", "with", "would", "yes",
    "can", "could", "does", "each", "many", "should", "than", "that",
    "their", "them", "then", "there", "these", "this", "those", "very",
    "give", "list", "display", "report",
})


class QuestionPreprocessor:
    """Extract structured intent from user questions before metadata reduction."""

    def preprocess(
        self,
        question: str,
        known_measures: Optional[List[str]] = None,
        known_dimensions: Optional[List[str]] = None,
        llm_workspace_url: Optional[str] = None,
        llm_token: Optional[str] = None,
        llm_model: str = "databricks-claude-sonnet-4",
    ) -> QuestionIntent:
        """Extract structured intent from the user question.

        First runs deterministic extraction, then optionally refines with LLM.

        Args:
            question: The user's natural language question.
            known_measures: List of known measure names from the model.
            known_dimensions: List of known dimension column names.
            llm_workspace_url: Optional Databricks workspace URL for LLM.
            llm_token: Optional Databricks token for LLM.
            llm_model: LLM model name.

        Returns:
            QuestionIntent with extracted structure.
        """
        intent = self._deterministic_extract(question, known_measures, known_dimensions)

        # Optionally refine with LLM
        if llm_workspace_url and llm_token:
            try:
                intent = self._llm_extract(
                    intent, question, llm_workspace_url, llm_token, llm_model,
                    known_measures, known_dimensions,
                )
            except Exception as e:
                logger.warning(f"[QuestionPreprocessor] LLM extraction failed, using deterministic: {e}")

        logger.info(
            f"[QuestionPreprocessor] Intent: shape={intent.output_shape.value}, "
            f"time_grain={intent.time_intelligence.grain}, "
            f"deltas={intent.time_intelligence.delta_periods}, "
            f"measures={intent.measures[:5]}, dims={intent.dimensions[:5]}, "
            f"keywords={intent.keywords[:10]}, "
            f"needs_split={intent.needs_split}"
        )

        return intent

    def _deterministic_extract(
        self,
        question: str,
        known_measures: Optional[List[str]] = None,
        known_dimensions: Optional[List[str]] = None,
    ) -> QuestionIntent:
        """Pure deterministic extraction — no LLM needed."""
        q_lower = question.lower()

        # Time intelligence
        time_intel = self._detect_time_intelligence(q_lower)

        # Output shape
        output_shape, top_n = self._detect_output_shape(q_lower)

        # Keywords
        keywords = self._extract_keywords(question)

        # Match known measures/dimensions if provided
        matched_measures = self._match_known_names(q_lower, known_measures) if known_measures else []
        matched_dimensions = self._match_known_names(q_lower, known_dimensions) if known_dimensions else []

        return QuestionIntent(
            original_question=question,
            keywords=keywords,
            measures=matched_measures,
            dimensions=matched_dimensions,
            time_intelligence=time_intel,
            output_shape=output_shape,
            top_n=top_n,
        )

    def _detect_time_intelligence(self, q_lower: str) -> TimeIntelligence:
        """Detect time grain, delta periods, and accumulation from question."""
        time_intel = TimeIntelligence()

        # Detect grain (finest wins)
        for grain in [TimeGrain.DAY, TimeGrain.WEEK, TimeGrain.MONTH, TimeGrain.QUARTER, TimeGrain.YEAR]:
            tokens = _TIME_GRAIN_TOKENS[grain]
            for token in tokens:
                if token in q_lower.split() or token in q_lower:
                    time_intel.grain = grain
                    break
            if time_intel.grain:
                break

        # Detect delta periods
        for token, delta_type in _DELTA_TOKENS.items():
            if token in q_lower:
                if delta_type not in time_intel.delta_periods:
                    time_intel.delta_periods.append(delta_type)

        # Detect accumulation (YTD/MTD/QTD)
        for token, acc_type in _ACCUMULATION_TOKENS.items():
            if token in q_lower:
                if acc_type == "ytd":
                    time_intel.has_ytd = True
                elif acc_type == "mtd":
                    time_intel.has_mtd = True
                elif acc_type == "qtd":
                    time_intel.has_qtd = True

        return time_intel

    def _detect_output_shape(self, q_lower: str) -> tuple:
        """Detect the expected output shape and optional top-N value."""
        # Top N
        top_match = _TOP_N_PATTERN.search(q_lower)
        if top_match:
            return OutputShape.TOP_N, int(top_match.group(1))

        # Trend
        if any(tok in q_lower for tok in _TREND_TOKENS):
            return OutputShape.TREND, None

        # Comparison
        if any(tok in q_lower for tok in _COMPARISON_TOKENS):
            return OutputShape.COMPARISON, None

        # Single value (no grouping indicators)
        words = set(q_lower.split())
        if words & _SINGLE_VALUE_TOKENS and not any(
            w in q_lower for w in ["by", "per", "each", "group", "breakdown", "split"]
        ):
            return OutputShape.SINGLE_VALUE, None

        # Default: list
        return OutputShape.LIST, None

    def _extract_keywords(self, question: str) -> List[str]:
        """Extract meaningful keywords after stopword removal."""
        # Simple tokenization: lowercase, split on non-alphanumeric
        tokens = re.findall(r"[a-zA-Z0-9]+", question.lower())
        seen: set = set()
        keywords: List[str] = []
        for token in tokens:
            if token not in _STOPWORDS and len(token) >= 2 and token not in seen:
                seen.add(token)
                keywords.append(token)
        return keywords

    @staticmethod
    def _match_known_names(q_lower: str, known_names: List[str]) -> List[str]:
        """Match known measure/dimension names against the question text."""
        matched = []
        for name in known_names:
            # Normalize: try both exact and word-boundary match
            name_lower = name.lower()
            if name_lower in q_lower:
                matched.append(name)
        return matched

    def _llm_extract(
        self,
        deterministic_intent: QuestionIntent,
        question: str,
        llm_workspace_url: str,
        llm_token: str,
        llm_model: str,
        known_measures: Optional[List[str]] = None,
        known_dimensions: Optional[List[str]] = None,
    ) -> QuestionIntent:
        """Refine intent extraction using LLM. Synchronous (called from sync context)."""
        import asyncio

        async def _call():
            measure_hint = ""
            if known_measures:
                measure_hint = f"\nKnown measures: {', '.join(known_measures[:30])}"
            dim_hint = ""
            if known_dimensions:
                dim_hint = f"\nKnown dimensions: {', '.join(known_dimensions[:30])}"

            prompt = f"""Extract structured intent from this business question. Return ONLY valid JSON.

Question: "{question}"
{measure_hint}{dim_hint}

Return JSON:
{{"measures": ["measure names from question"], "dimensions": ["dimension/grouping columns"], "time_grain": "year|quarter|month|week|day|null", "output_shape": "single_value|top_n|trend|comparison|list"}}"""

            url = f"{llm_workspace_url.rstrip('/')}/serving-endpoints/{llm_model}/invocations"
            headers = {"Authorization": f"Bearer {llm_token}", "Content-Type": "application/json"}
            payload = {
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": 500,
                "temperature": 0.0,
            }

            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                content = resp.json().get("choices", [{}])[0].get("message", {}).get("content", "")

            # Parse LLM response
            try:
                parsed = json.loads(content.strip())
            except json.JSONDecodeError:
                json_match = re.search(r"\{.*\}", content, re.DOTALL)
                if json_match:
                    parsed = json.loads(json_match.group(0))
                else:
                    return deterministic_intent

            # Merge LLM results with deterministic results
            if parsed.get("measures"):
                deterministic_intent.measures = parsed["measures"]
            if parsed.get("dimensions"):
                deterministic_intent.dimensions = parsed["dimensions"]
            if parsed.get("time_grain") and parsed["time_grain"] != "null":
                try:
                    deterministic_intent.time_intelligence.grain = TimeGrain(parsed["time_grain"])
                except ValueError:
                    pass
            if parsed.get("output_shape"):
                try:
                    deterministic_intent.output_shape = OutputShape(parsed["output_shape"])
                except ValueError:
                    pass

            return deterministic_intent

        # Run async in sync context
        try:
            loop = asyncio.get_running_loop()
            import concurrent.futures
            import contextvars
            ctx = contextvars.copy_context()
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(ctx.run, asyncio.run, _call())
                return future.result(timeout=20)
        except RuntimeError:
            return asyncio.run(_call())

    # ─── Question Splitter ─────────────────────────────────────────────

    def detect_split(
        self,
        intent: QuestionIntent,
        known_measures: Optional[List[str]] = None,
    ) -> QuestionIntent:
        """Detect if the question should be split into sub-questions.

        Criteria for splitting:
        - Multiple measures detected AND output shape is not CROSS_TAB
        - Question contains "and" between measure-like phrases

        Updates intent.needs_split and intent.sub_questions in place.
        """
        measures = intent.measures
        if len(measures) <= 1:
            return intent

        # If multiple measures and not a cross-tab, suggest split
        if intent.output_shape != OutputShape.CROSS_TAB and len(measures) > 1:
            intent.needs_split = True
            q = intent.original_question

            # Build sub-questions: one per measure, keeping the rest of the question
            for measure in measures:
                # Simple heuristic: replace the original question's measure references
                sub_q = q
                for other in measures:
                    if other != measure:
                        # Remove "and <other>" or "<other> and" patterns
                        sub_q = re.sub(
                            rf"\s+and\s+{re.escape(other.lower())}",
                            "",
                            sub_q,
                            flags=re.IGNORECASE,
                        )
                        sub_q = re.sub(
                            rf"{re.escape(other.lower())}\s+and\s+",
                            "",
                            sub_q,
                            flags=re.IGNORECASE,
                        )
                        # Also try removing standalone mentions
                        sub_q = re.sub(
                            rf"\b{re.escape(other.lower())}\b",
                            "",
                            sub_q,
                            flags=re.IGNORECASE,
                        )
                # Clean up extra spaces
                sub_q = re.sub(r"\s+", " ", sub_q).strip()
                if sub_q:
                    intent.sub_questions.append(sub_q)

            logger.info(
                f"[QuestionPreprocessor] Split detected: {len(measures)} measures → "
                f"{len(intent.sub_questions)} sub-questions"
            )

        return intent
