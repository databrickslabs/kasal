"""LLM-powered fallback for DAX expressions that regex patterns can't translate.

Opt-in module: only invoked when `use_llm_fallback=True` in pipeline config.
Uses direct HTTP to Databricks serving endpoints (same pattern as mquery/llm_converter.py).
Fail-open: LLM errors never block measures — they stay untranslatable.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
from collections import OrderedDict
from typing import Any

from .data_classes import TranslationResult
from .utils import to_snake_case

logger = logging.getLogger(__name__)

_RUN_CACHE_MAX = 128

# ── Skill corpus (LLM-first) ────────────────────────────────────────────────
# The engineering DAX-translation + UC-metric-view skill files are vendored
# under skills/. Loaded once at module import and embedded as a STABLE system
# prefix marked with Anthropic cache_control:ephemeral so the serving endpoint
# caches it (full price once per run, ~10% per subsequent measure). The dax/
# files teach WHAT to translate; the uc-metric-views/ files teach HOW to write
# the target YAML — the LLM needs both to emit deployable, correct output.
_SKILLS_DIR = os.path.join(os.path.dirname(__file__), "skills")
_SKILL_FILES = (
    ("dax", "SKILL.md"), ("dax", "PATTERNS.md"),
    ("dax", "UNSUPPORTED.md"), ("dax", "EDGE_CASES.md"),
    ("uc-metric-views", "SYNTAX.md"), ("uc-metric-views", "WINDOW.md"),
    ("uc-metric-views", "LOD.md"), ("uc-metric-views", "JOINS.md"),
    ("uc-metric-views", "COMPOSABILITY.md"), ("uc-metric-views", "MATERIALIZATION.md"),
)


def _load_skill_corpus() -> str:
    """Concatenate the vendored skill .md files into one corpus string.

    Missing files are skipped with a warning (fail-open — a partial corpus still
    beats none). Returns '' if the skills dir is absent, in which case the
    translator falls back to the terse built-in guidance.
    """
    parts: list[str] = []
    for group, name in _SKILL_FILES:
        path = os.path.join(_SKILLS_DIR, group, name)
        try:
            with open(path, encoding="utf-8") as fh:
                parts.append(f"# === {group}/{name} ===\n\n{fh.read().strip()}")
        except FileNotFoundError:
            logger.warning("[DAX_LLM] skill file missing (skipped): %s/%s", group, name)
        except Exception as e:  # noqa: BLE001
            logger.warning("[DAX_LLM] skill file unreadable (%s/%s): %s", group, name, e)
    return "\n\n".join(parts)


# Loaded once at import. If empty (skills not vendored), the terse fallback
# system prompt below is used instead.
_SKILL_CORPUS = _load_skill_corpus()

# Terse built-in guidance used ONLY when the skill corpus is unavailable.
_FALLBACK_INSTRUCTIONS = """You are an expert DAX-to-Spark-SQL translator for Databricks Unity Catalog Metric Views.

Given a DAX measure expression, translate it to the equivalent Spark SQL expression.

Rules:
1. Use `source.column_name` for fact table columns
2. Use `alias.column_name` for joined dimension/fact columns
3. Use `MEASURE(measure_name)` to reference other translated measures
4. Use `SUM(source.col) FILTER (WHERE condition)` for filtered aggregations
5. Use `expr / NULLIF(expr, 0)` instead of DIVIDE()
6. Use bare column names in FILTER clauses (no table prefix)
7. DAX functions → SQL equivalents:
   - SUM(Table[col]) → SUM(source.col)
   - CALCULATE(expr, filter) → expr FILTER (WHERE filter)
   - DIVIDE(a, b) → a / NULLIF(b, 0)
   - DISTINCTCOUNTNOBLANK → COUNT(DISTINCT col)
   - SUMX(FILTER(T, cond), T[col]) → SUM(source.col) FILTER (WHERE cond)
   - COUNTX(FILTER(T, cond), T[col]) → COUNT(source.col) FILTER (WHERE cond)
   - AVERAGEX(FILTER(T, cond), T[col]) → AVG(source.col) FILTER (WHERE cond)
   - Table[col] = "val" → source.col = 'val'
   - Table[col] in {"a","b"} → source.col IN ('a', 'b')
   - && → AND, || → OR
8. If the expression CANNOT be translated to UC Metric View SQL, return success=false.
9. Use snake_case for measure names."""

# The JSON output contract (shared by corpus + fallback prompts). Adds the
# 7-category `dax_class` provenance label alongside the existing fields.
_OUTPUT_CONTRACT = """
Classify each measure into exactly one `dax_class`:
- "translatable_direct": a direct aggregation / simple expression
- "composed": references other measures via MEASURE()
- "filtered": CALCULATE/FILTER context that maps to a FILTER (WHERE ...) clause
- "architecture_change": needs a window/LOD/join restructure (still emittable)
- "display_layer": a formatting/UI artifact (FORMAT, color, label) — not a real metric
- "unsupported": PBI-specific semantics with no UCMV equivalent (ALLSELECTED, USERELATIONSHIP on inactive rels, visual-level filters, recursive iteration)
- "out_of_scope": not a measure translation task

ALWAYS respond with valid JSON (no markdown code blocks):
{
  "success": true/false,
  "sql_expr": "the SQL expression" or null,
  "dax_class": "one of the 7 categories above",
  "confidence": "high"/"medium"/"low",
  "explanation": "brief explanation of the translation",
  "error": "reason if success=false" or null
}"""

# Corpus-backed system prompt when skills are vendored; terse otherwise. The
# corpus is the STABLE prefix that gets cache_control:ephemeral at call time.
if _SKILL_CORPUS:
    _SYSTEM_PROMPT = (
        "You are an expert Power BI DAX → Databricks UC Metric View translator. "
        "Use the following skill corpus (engineering's DAX-translation decision "
        "framework + UC-metric-view target-language spec) as your authoritative "
        "guide for WHAT to translate and HOW to write the target YAML/SQL.\n\n"
        f"{_SKILL_CORPUS}\n\n"
        "----\n"
        "Translate the given DAX measure to a Spark SQL expression for a UC Metric "
        "View, following the corpus above." + _OUTPUT_CONTRACT
    )
else:
    _SYSTEM_PROMPT = _FALLBACK_INSTRUCTIONS + "\n" + _OUTPUT_CONTRACT


def _content_hash(text: str) -> str:
    """SHA-256 hash for cache key."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _build_user_prompt(
    measure_name: str,
    dax_expression: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    table_context: str = "",
) -> str:
    """Build the user prompt with context.

    ``table_context`` is a pre-formatted block describing the fact table this
    measure is allocated to (source table, available columns, joins, filters).
    It goes in the VARIABLE user message (not the cached skill prefix) so the
    LLM emits real column/join names instead of guessing — while the corpus
    stays cacheable. Stable within a table, so cheap relative to the corpus.
    """
    available_measures = ', '.join(sorted(base_names)[:50])  # cap at 50 for token efficiency
    ctx_block = f"\n## Fact table context\n{table_context}\n" if table_context else ""

    return f"""Translate this DAX measure to Spark SQL for a UC Metric View.
{ctx_block}
## Measure
Name: {measure_name}

## DAX Expression
{dax_expression}

## Available MEASURE() references (already translated)
{available_measures}

## Instructions
- Use ONLY the source columns / join aliases listed in the fact table context above; do not invent column names.
- If referencing another measure, use MEASURE(snake_case_name)
- Column references: source.column_name (fact) or alias.column_name (joined dimension)
- Return JSON with success, sql_expr, dax_class, confidence, explanation"""


def _parse_response(response_text: str) -> dict:
    """Parse and validate LLM response JSON."""
    try:
        # Strip markdown code blocks if present
        text = response_text.strip()
        if text.startswith('```json'):
            text = text.split('```json')[1].split('```')[0].strip()
        elif text.startswith('```'):
            text = text.split('```')[1].split('```')[0].strip()
        return json.loads(text)
    except json.JSONDecodeError:
        return {'success': False, 'error': 'Failed to parse LLM response'}


def _validate_sql(sql_expr: str) -> bool:
    """Check that the LLM output doesn't contain DAX-only constructs."""
    _DAX_ONLY = re.compile(
        r'\b(SELECTEDVALUE|ISFILTERED|HASONEVALUE|CONTAINSSTRING|'
        r'SWITCH\s*\(|ALLSELECTED|USERELATIONSHIP|EARLIER|'
        r'FIRSTDATE|LASTDATE|VALUES\s*\(|SUMMARIZE\s*\()\b',
        re.IGNORECASE,
    )
    return not _DAX_ONLY.search(sql_expr)


def _system_message(system_prompt: str) -> dict:
    """Build the system message.

    When the (large, stable) skill corpus is loaded, send the system prompt as a
    structured content block marked ``cache_control: ephemeral`` so the Databricks
    serving endpoint caches the prefix — full price on the first measure, ~10% on
    every subsequent one. Without the corpus the prompt is small, so a plain
    string message is fine (below the 1024-token cache minimum anyway).
    """
    if _SKILL_CORPUS:
        return {
            "role": "system",
            "content": [
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
        }
    return {"role": "system", "content": system_prompt}


async def _call_llm(
    prompt: str,
    system_prompt: str,
    model: str,
) -> dict:
    """Call the LLM, preferring the cache-aware usage-returning path.

    Uses ``LLMManager.completion_with_usage`` so the skill-corpus system prefix
    can carry ``cache_control`` and the ``usage`` block is returned (cache hits
    observable). Falls back to plain ``completion()`` if the cached path errors,
    so a transport hiccup never blocks translation (fail-open).
    """
    from src.core.llm_manager import LLMManager
    from src.utils.telemetry import get_user_agent_header, KasalProduct

    messages = [_system_message(system_prompt), {"role": "user", "content": prompt}]
    headers = get_user_agent_header(KasalProduct.POWERBI)
    try:
        result = await LLMManager.completion_with_usage(
            messages=messages,
            model=model,
            temperature=0.1,
            max_tokens=2000,
            extra_headers=headers,
        )
        return {"content": result.get("content"), "usage": result.get("usage", {})}
    except Exception as e:
        logger.warning(f"[DAX_LLM] cache-aware call failed ({e}); falling back to plain completion")
        try:
            content = await LLMManager.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                model=model,
                temperature=0.1,
                max_tokens=2000,
                extra_headers=headers,
            )
            return {"content": content, "usage": {}}
        except Exception as e2:
            logger.warning(f"[DAX_LLM] API call failed: {e2}")
            return {"content": None, "error": str(e2)}


async def translate_with_llm(
    measure: TranslationResult,
    table_key: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    model: str = 'databricks-claude-sonnet-4',
    cache: OrderedDict | None = None,
    table_context: str = "",
) -> TranslationResult:
    """Attempt LLM translation of a single untranslatable measure.

    Returns the measure with updated sql_expr/is_translatable if successful,
    or unchanged if LLM fails (fail-open).

    Args:
        cache: Run-scoped cache to prevent cross-tenant leakage.
               Falls back to a local (non-shared) OrderedDict if not provided.
        table_context: Pre-formatted fact-table schema/join context for this
               measure's table (goes in the user prompt, not the cached prefix).
    """
    _cache = cache if cache is not None else OrderedDict()

    # Cache key includes table_context: identical DAX on different tables (with
    # different source columns/joins) must translate independently.
    cache_key = _content_hash(measure.dax_expression + "\x00" + table_context)
    if cache_key in _cache:
        _cache.move_to_end(cache_key)
        cached = _cache[cache_key]
        if cached.get('success'):
            measure.sql_expr = cached['sql_expr']
            measure.is_translatable = True
            measure.confidence = cached.get('confidence', 'medium')
            measure.category = 'llm_translated'
            measure.dax_class = cached.get('dax_class')
            measure.skip_reason = ''
        elif cached.get('dax_class'):
            measure.dax_class = cached.get('dax_class')
        return measure

    # Build prompt
    user_prompt = _build_user_prompt(
        measure.original_name,
        measure.dax_expression,
        base_names,
        original_to_snake,
        table_context=table_context,
    )

    # Call LLM
    response = await _call_llm(user_prompt, _SYSTEM_PROMPT, model)

    if not response.get('content'):
        logger.warning(f"[DAX_LLM] No response for {measure.original_name}: {response.get('error')}")
        return measure

    # Parse response
    parsed = _parse_response(response['content'])

    # Cache result (run-scoped)
    if len(_cache) >= _RUN_CACHE_MAX:
        _cache.popitem(last=False)
    _cache[cache_key] = parsed

    if parsed.get('success') and parsed.get('sql_expr'):
        sql_expr = parsed['sql_expr']

        # Validate: no DAX-only constructs in output
        if not _validate_sql(sql_expr):
            logger.warning(f"[DAX_LLM] LLM output contains DAX constructs for {measure.original_name}")
            return measure

        measure.sql_expr = sql_expr
        measure.is_translatable = True
        measure.confidence = parsed.get('confidence', 'medium')
        measure.category = 'llm_translated'
        # dax_class = translation provenance/quality (7-cat); NOT emission routing.
        measure.dax_class = parsed.get('dax_class')
        measure.skip_reason = ''

        usage = response.get('usage', {}) or {}
        tokens = usage.get('total_tokens', 0)
        cache_read = usage.get('cache_read_input_tokens', 0)
        logger.info(
            f"[DAX_LLM] Translated {measure.original_name} → {sql_expr[:80]}... "
            f"(confidence={measure.confidence}, dax_class={measure.dax_class}, "
            f"tokens={tokens}, cache_read={cache_read})"
        )
    else:
        # Even on non-success, record the classification for reporting/telemetry.
        measure.dax_class = parsed.get('dax_class') or measure.dax_class
        reason = parsed.get('error', parsed.get('explanation', 'LLM could not translate'))
        logger.info(f"[DAX_LLM] Could not translate {measure.original_name} (dax_class={measure.dax_class}): {reason}")

    return measure


# Max concurrent LLM translations per chunk. Bounded so we get a large
# wall-clock speedup vs. the old one-at-a-time loop without hammering the
# serving endpoint into rate limits. Measures WITHIN a chunk run in parallel
# (so they can't see each other's freshly-translated MEASURE() refs), but
# base_names/original_to_snake are updated BETWEEN chunks so a later chunk still
# resolves references to measures translated in an earlier chunk — preserving
# most of the cross-measure reference benefit of the old sequential order.
_DAX_LLM_CONCURRENCY = 6


async def translate_batch_with_llm(
    measures: list[TranslationResult],
    table_key: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    model: str = 'databricks-claude-sonnet-4',
    topo_priority: dict[str, int] | None = None,
    table_context: str = "",
) -> list[TranslationResult]:
    """Attempt LLM translation of a batch of untranslatable measures.

    Processes in bounded-concurrency chunks (``_DAX_LLM_CONCURRENCY`` at a time)
    to cut wall-clock time for large models — the old fully-sequential loop could
    exceed the flow's crew timeout on models with hundreds of measures.
    Only attempts measures that aren't PBI artifacts. Authentication is handled
    internally by LLMManager (opt-in via use_llm_fallback upstream).

    ``topo_priority`` (optional): map of ``original_name -> topological rank``
    (dependencies first). When provided, candidates are sorted by it before
    chunking so a dependent measure lands in a LATER chunk than the measure it
    references — the earlier chunk's translation is merged into ``base_names``
    before the dependent runs, so its ``MEASURE()`` ref resolves. Without this,
    a parent+child in the SAME chunk (likely under llm_first, where most measures
    take this path) can't see each other's fresh translations.

    Returns the same list with updated measures where LLM succeeded.
    """
    import asyncio

    # Skip PBI artifacts — don't waste LLM tokens on FORMAT/Color/ISBLANK
    _ARTIFACT_KEYWORDS = (
        'FORMAT', 'Color', 'ISBLANK+BLANK', 'SELECTEDVALUE+SWITCH',
        'SELECTEDVALUE', 'DAX expression not available', 'ISFILTERED',
        'BLANK() placeholder',
    )

    candidates = [
        m for m in measures
        if not any(kw in m.skip_reason for kw in _ARTIFACT_KEYWORDS)
        and m.dax_expression.strip() not in ('', 'Not available')
    ]

    if not candidates:
        return measures

    # Order dependents after their dependencies so cross-chunk MEASURE() refs
    # resolve (Step 3.9). Stable sort keeps input order for equal ranks.
    if topo_priority:
        candidates.sort(key=lambda m: topo_priority.get(m.original_name, 0))

    logger.info(
        f"[DAX_LLM] Attempting LLM fallback for {len(candidates)} measures in "
        f"{table_key} (concurrency={_DAX_LLM_CONCURRENCY})"
    )

    # Run-scoped cache — prevents cross-tenant leakage between pipeline runs.
    # Shared across chunks so identical DAX only hits the LLM once.
    run_cache: OrderedDict[str, dict] = OrderedDict()

    translated_count = 0
    for start in range(0, len(candidates), _DAX_LLM_CONCURRENCY):
        chunk = candidates[start:start + _DAX_LLM_CONCURRENCY]
        # Snapshot the reference context so all measures in this chunk see the
        # same (already-translated) refs — matches deterministic behaviour and
        # avoids mutating shared dicts concurrently.
        snap_names = set(base_names)
        snap_map = dict(original_to_snake)
        await asyncio.gather(*(
            translate_with_llm(
                m, table_key, snap_names, snap_map,
                model=model, cache=run_cache, table_context=table_context,
            )
            for m in chunk
        ))
        # Merge this chunk's successes into the shared context for later chunks.
        for m in chunk:
            if m.is_translatable:
                translated_count += 1
                base_names.add(m.measure_name)
                original_to_snake[m.original_name] = m.measure_name

    logger.info(f"[DAX_LLM] {translated_count}/{len(candidates)} measures translated via LLM for {table_key}")
    return measures
