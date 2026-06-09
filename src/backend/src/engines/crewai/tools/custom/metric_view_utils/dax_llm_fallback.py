"""LLM-powered fallback for DAX expressions that regex patterns can't translate.

Opt-in module: only invoked when `use_llm_fallback=True` in pipeline config.
Uses direct HTTP to Databricks serving endpoints (same pattern as mquery/llm_converter.py).
Fail-open: LLM errors never block measures — they stay untranslatable.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import OrderedDict
from typing import Any

from .data_classes import TranslationResult
from .utils import to_snake_case

logger = logging.getLogger(__name__)

_RUN_CACHE_MAX = 128

_SYSTEM_PROMPT = """You are an expert DAX-to-Spark-SQL translator for Databricks Unity Catalog Metric Views.

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
8. If the expression CANNOT be translated to UC Metric View SQL (e.g., it requires
   row-level context, recursive iteration, or PBI-specific features like ALLSELECTED,
   USERELATIONSHIP with inactive relationships, or visual-level filters), return success=false.
9. Use snake_case for measure names.

ALWAYS respond with valid JSON (no markdown code blocks):
{
  "success": true/false,
  "sql_expr": "the SQL expression" or null,
  "confidence": "high"/"medium"/"low",
  "explanation": "brief explanation of the translation",
  "error": "reason if success=false" or null
}"""


def _content_hash(text: str) -> str:
    """SHA-256 hash for cache key."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _build_user_prompt(
    measure_name: str,
    dax_expression: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
) -> str:
    """Build the user prompt with context."""
    available_measures = ', '.join(sorted(base_names)[:50])  # cap at 50 for token efficiency

    return f"""Translate this DAX measure to Spark SQL for a UC Metric View.

## Measure
Name: {measure_name}

## DAX Expression
{dax_expression}

## Available MEASURE() references (already translated)
{available_measures}

## Instructions
- If referencing another measure, use MEASURE(snake_case_name)
- Column references: source.column_name
- Return JSON with success, sql_expr, confidence, explanation"""


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


async def _call_llm(
    prompt: str,
    system_prompt: str,
    model: str,
    workspace_url: str,
    token: str,
) -> dict:
    """Call Databricks serving endpoint directly via HTTP."""
    import httpx

    if not workspace_url or not token:
        return {'content': None, 'error': 'LLM not configured'}

    base_url = workspace_url.rstrip('/')
    from src.utils.databricks_url_utils import DatabricksURLUtils
    url, _gw_model = DatabricksURLUtils.construct_chat_completions_url(base_url, model)

    from src.utils.telemetry import get_user_agent_header, KasalProduct
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json',
        **get_user_agent_header(KasalProduct.POWERBI),
    }

    payload = {
        'messages': [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt},
        ],
        'max_tokens': 2000,
        'temperature': 0.1,
    }
    if _gw_model:
        payload["model"] = _gw_model

    try:
        async with httpx.AsyncClient(timeout=120.0) as client:
            response = await client.post(url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return {
                'content': result['choices'][0]['message']['content'],
                'usage': result.get('usage', {}),
            }
    except Exception as e:
        logger.warning(f"[DAX_LLM] API call failed: {e}")
        return {'content': None, 'error': str(e)}


async def translate_with_llm(
    measure: TranslationResult,
    table_key: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    model: str = 'databricks-claude-sonnet-4',
    workspace_url: str = '',
    token: str = '',
    cache: OrderedDict | None = None,
) -> TranslationResult:
    """Attempt LLM translation of a single untranslatable measure.

    Returns the measure with updated sql_expr/is_translatable if successful,
    or unchanged if LLM fails (fail-open).

    Args:
        cache: Run-scoped cache to prevent cross-tenant leakage.
               Falls back to a local (non-shared) OrderedDict if not provided.
    """
    _cache = cache if cache is not None else OrderedDict()

    # Check cache
    cache_key = _content_hash(measure.dax_expression)
    if cache_key in _cache:
        _cache.move_to_end(cache_key)
        cached = _cache[cache_key]
        if cached.get('success'):
            measure.sql_expr = cached['sql_expr']
            measure.is_translatable = True
            measure.confidence = cached.get('confidence', 'medium')
            measure.category = 'llm_translated'
            measure.skip_reason = ''
        return measure

    # Build prompt
    user_prompt = _build_user_prompt(
        measure.original_name,
        measure.dax_expression,
        base_names,
        original_to_snake,
    )

    # Call LLM
    response = await _call_llm(user_prompt, _SYSTEM_PROMPT, model, workspace_url, token)

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
        measure.skip_reason = ''

        tokens = response.get('usage', {}).get('total_tokens', 0)
        logger.info(
            f"[DAX_LLM] Translated {measure.original_name} → {sql_expr[:80]}... "
            f"(confidence={measure.confidence}, tokens={tokens})"
        )
    else:
        reason = parsed.get('error', parsed.get('explanation', 'LLM could not translate'))
        logger.info(f"[DAX_LLM] Could not translate {measure.original_name}: {reason}")

    return measure


async def translate_batch_with_llm(
    measures: list[TranslationResult],
    table_key: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    model: str = 'databricks-claude-sonnet-4',
    workspace_url: str = '',
    token: str = '',
) -> list[TranslationResult]:
    """Attempt LLM translation of a batch of untranslatable measures.

    Processes sequentially (not parallel) to avoid rate limiting.
    Only attempts measures that aren't PBI artifacts.

    Returns the same list with updated measures where LLM succeeded.
    """
    if not workspace_url or not token:
        logger.warning("[DAX_LLM] LLM not configured — skipping fallback")
        return measures

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

    logger.info(f"[DAX_LLM] Attempting LLM fallback for {len(candidates)} measures in {table_key}")

    # Run-scoped cache — prevents cross-tenant leakage between pipeline runs
    run_cache: OrderedDict[str, dict] = OrderedDict()

    translated_count = 0
    for m in candidates:
        await translate_with_llm(
            m, table_key, base_names, original_to_snake,
            model=model, workspace_url=workspace_url, token=token,
            cache=run_cache,
        )
        if m.is_translatable:
            translated_count += 1
            # Update tracking for subsequent measures
            base_names.add(m.measure_name)
            original_to_snake[m.original_name] = m.measure_name

    logger.info(f"[DAX_LLM] {translated_count}/{len(candidates)} measures translated via LLM for {table_key}")
    return measures
