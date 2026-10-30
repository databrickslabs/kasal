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

from src.services.settings.engine_settings import setting as engine_setting

from .data_classes import TranslationResult
from .dax_llm_prompts import _BATCH_OUTPUT_CONTRACT as _BATCH_OUTPUT_CONTRACT
from .dax_llm_prompts import _FALLBACK_INSTRUCTIONS as _FALLBACK_INSTRUCTIONS
from .dax_llm_prompts import _OUTPUT_CONTRACT as _OUTPUT_CONTRACT
from .dax_llm_prompts import _SKILL_CORPUS as _SKILL_CORPUS
from .dax_llm_prompts import _SKILL_FILES as _SKILL_FILES
from .dax_llm_prompts import _SKILLS_DIR as _SKILLS_DIR
from .dax_llm_prompts import _SQL_RULES as _SQL_RULES
from .dax_llm_prompts import _SYSTEM_PROMPT as _SYSTEM_PROMPT
from .dax_llm_prompts import _SYSTEM_PROMPT_BATCH as _SYSTEM_PROMPT_BATCH
from .dax_llm_prompts import _load_skill_corpus as _load_skill_corpus
from .function_ref_retriever import render_function_refs

logger = logging.getLogger(__name__)

_RUN_CACHE_MAX = 128


def _content_hash(text: str) -> str:
    """SHA-256 hash for cache key."""
    return hashlib.sha256(text.encode()).hexdigest()[:16]


def _domain_context_block(domain_context: str) -> str:
    """Render the optional customer-supplied domain-context block.

    Free text (vocabulary, business rules, fiscal-calendar quirks, cost-accounting
    terms). Goes in the VARIABLE user message like ``table_context``, so the
    cached skill-corpus prefix stays reusable. Empty string when none supplied.
    """
    if not domain_context:
        return ""
    return (
        "\n## Domain context (customer-supplied — authoritative business "
        f"semantics for this model)\n{domain_context}\n"
    )


def _time_intel_directive(enabled: bool) -> str:
    """Per-run time-intelligence gate.

    Time-intelligence (prior year/period, YTD/QTD) is expressible as a window
    function, but a window over the wrong period count is silently wrong — so we
    only let the model translate it when the reconciliation loop is ACTIVE to
    validate the numbers cell-by-cell. Off → decline it (an honest TODO beats an
    unvalidated prior-year figure).
    """
    if enabled:
        return (
            "\n## Time-intelligence: ENABLED this run (PBI reconciliation will "
            "validate the numbers)\nTranslate prior-year/period, YTD, QTD, MTD and "
            "SAMEPERIODLASTYEAR / DATEADD / PARALLELPERIOD / TOTALYTD via a WINDOW "
            "function over the period/fiscal column shown in the context (rule 5) — "
            "do NOT decline them.\n"
        )
    return (
        "\n## Time-intelligence: NOT validated this run\nIf a measure needs "
        "prior-year/period, YTD/QTD or period-shift semantics (SAMEPERIODLASTYEAR, "
        "DATEADD, PARALLELPERIOD, TOTALYTD, …), DECLINE it: success=false, "
        'dax_class="architecture_change". Do NOT emit unvalidated period math.\n'
    )


def _build_user_prompt(
    measure_name: str,
    dax_expression: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    table_context: str = "",
    domain_context: str = "",
    time_intel_enabled: bool = False,
) -> str:
    """Build the user prompt with context.

    ``table_context`` is a pre-formatted block describing the fact table this
    measure is allocated to (source table, available columns, joins, filters).
    It goes in the VARIABLE user message (not the cached skill prefix) so the
    LLM emits real column/join names instead of guessing — while the corpus
    stays cacheable. Stable within a table, so cheap relative to the corpus.

    ``domain_context`` is optional customer-supplied business context, appended
    the same way (see ``_domain_context_block``).
    """
    available_measures = ", ".join(
        sorted(base_names)[:50]
    )  # cap at 50 for token efficiency
    ctx_block = f"\n## Fact table context\n{table_context}\n" if table_context else ""
    dc_block = _domain_context_block(domain_context)
    ti_block = _time_intel_directive(time_intel_enabled)

    return f"""Translate this DAX measure to Spark SQL for a UC Metric View.
{ctx_block}{dc_block}{ti_block}
## Measure
Name: {measure_name}

## DAX Expression
{dax_expression}

## Available MEASURE() references (already translated)
{available_measures}

## Instructions
- Use ONLY the source columns / join aliases listed in the fact table context above; do not invent column names.
- Honour the domain context above (terminology, business rules, calendar) when interpreting the measure's intent.
- If referencing another measure, use MEASURE(snake_case_name)
- Column references: source.column_name (fact) or alias.column_name (joined dimension)
- Return JSON with success, sql_expr, dax_class, confidence, explanation"""


def _parse_response(response_text: str) -> dict:
    """Parse and validate LLM response JSON."""
    try:
        # Strip markdown code blocks if present
        text = response_text.strip()
        if text.startswith("```json"):
            text = text.split("```json")[1].split("```")[0].strip()
        elif text.startswith("```"):
            text = text.split("```")[1].split("```")[0].strip()
        parsed: dict = json.loads(text)
        return parsed
    except json.JSONDecodeError:
        return {"success": False, "error": "Failed to parse LLM response"}


def _strip_code_fences(text: str) -> str:
    """Strip a leading ```json / ``` markdown code fence if present."""
    text = text.strip()
    if text.startswith("```json"):
        return text.split("```json")[1].split("```")[0].strip()
    if text.startswith("```"):
        return text.split("```")[1].split("```")[0].strip()
    return text


def _build_batch_user_prompt(
    measures: list[TranslationResult],
    base_names: set[str],
    table_context: str = "",
    feedback: dict | None = None,
    domain_context: str = "",
    time_intel_enabled: bool = False,
) -> str:
    """Build ONE user prompt covering all measures in a batch.

    The fact-table context (source table, columns, joins, filters) is included
    ONCE for the whole batch — every measure in a ``translate_batch_with_llm``
    call belongs to the same table_key, so they share it. This is what makes
    batching cheap: the big shared context and the corpus system prefix are sent
    once, not once per measure.
    """
    available_measures = ", ".join(sorted(base_names)[:50])
    ctx_block = f"\n## Fact table context\n{table_context}\n" if table_context else ""
    dc_block = _domain_context_block(domain_context)
    ti_block = _time_intel_directive(time_intel_enabled)
    lines = []
    for i, m in enumerate(measures, start=1):
        entry = f"{i}. name: {m.original_name}\n   DAX: {m.dax_expression}"
        # Reconciliation feedback (iterative loop): a prior cycle's SQL for this
        # measure did NOT match Power BI — show the model where it diverged so it
        # corrects THIS measure rather than repeating the same translation.
        # Feedback may be keyed by the PBI measure name (original_name) or the
        # UCMV measure name (measure_name) — reconciliation reports the latter.
        _fb = feedback or {}
        hint = _fb.get(m.original_name) or _fb.get(m.measure_name)
        if hint:
            entry += f"\n   PREVIOUS ATTEMPT MISMATCHED POWER BI — FIX IT: {hint}"
        lines.append(entry)
    measures_block = "\n".join(lines)
    # Retrieval: inject deep UCMV-legal references for the long-tail DAX functions
    # this batch uses (skips functions already taught deeply in the cached prefix).
    # Goes in the VARIABLE user message so the corpus prefix stays cacheable.
    func_refs = render_function_refs(m.dax_expression for m in measures)
    return f"""Translate the following {len(measures)} DAX measures to Spark SQL for a UC Metric View, using the shared fact-table context below.
{ctx_block}{dc_block}{ti_block}
## Available MEASURE() references (already translated)
{available_measures}
{func_refs}
## Measures to translate
{measures_block}

## Instructions
- Use ONLY the source columns / join aliases listed in the fact table context above; do not invent column names.
- Honour the domain context above (terminology, business rules, calendar) when interpreting each measure's intent.
- If referencing another measure, use MEASURE(snake_case_name)
- Column references: source.column_name (fact) or alias.column_name (joined dimension)
- Return a JSON ARRAY with one object per measure, echoing "measure_name" exactly as given."""


def _parse_batch_response(response_text: str) -> list[dict] | None:
    """Parse a batch response into a list of per-measure result dicts.

    Accepts a bare JSON array, a ``{"measures": [...]}`` wrapper, or an object
    keyed by measure name. Returns ``None`` if nothing usable parses, which the
    caller treats as a signal to fall back to per-measure translation.
    """
    try:
        data = json.loads(_strip_code_fences(response_text))
    except (json.JSONDecodeError, IndexError):
        return None
    if isinstance(data, list):
        return [d for d in data if isinstance(d, dict)]
    if isinstance(data, dict):
        if isinstance(data.get("measures"), list):
            return [d for d in data["measures"] if isinstance(d, dict)]
        # Object keyed by measure name → normalise to a list.
        out = []
        for key, val in data.items():
            if isinstance(val, dict):
                val.setdefault("measure_name", key)
                out.append(val)
        return out or None
    return None


def _validate_sql(sql_expr: str) -> bool:
    """Check that the LLM output doesn't contain DAX-only constructs."""
    _DAX_ONLY = re.compile(
        r"\b(SELECTEDVALUE|ISFILTERED|HASONEVALUE|CONTAINSSTRING|"
        r"SWITCH\s*\(|ALLSELECTED|USERELATIONSHIP|EARLIER|"
        r"FIRSTDATE|LASTDATE|VALUES\s*\(|SUMMARIZE\s*\()\b",
        re.IGNORECASE,
    )
    return not _DAX_ONLY.search(sql_expr)


def _llm_declined_reason(dax_class: str | None, detail: str) -> str:
    """Human-readable terminal skip_reason for a measure the LLM declined.

    Names the LLM as the decider and why, so a reviewer can tell "the LLM judged
    this unsupported" apart from "nobody has looked at it yet".
    """
    label = {
        "unsupported": "no UC metric-view equivalent",
        "display_layer": "display/formatting only — not a data measure",
        "out_of_scope": "out of scope for a metric view",
        "architecture_change": "needs a source-model change (not expressible as-is)",
        "composed": "depends on other measures that must be defined first",
    }.get((dax_class or "").strip(), "")
    detail = (detail or "").strip().rstrip(".")
    parts = [p for p in (label, detail) if p]
    suffix = f" — {'; '.join(parts)}" if parts else ""
    cls = f" [{dax_class}]" if dax_class else ""
    return f"LLM declined{cls}{suffix}"


def _apply_parsed(
    measure: TranslationResult, parsed: dict, usage: dict | None = None
) -> None:
    """Apply a parsed LLM result dict to a measure in place (success or decline).

    Shared by the batch path so results are interpreted the same way the
    single-measure ``translate_with_llm`` interprets them. On success the measure
    is marked translatable; on decline (or SQL that still contains DAX-only
    constructs) the measure is left untranslated (fail-open) with a terminal
    ``skip_reason``.
    """
    if parsed.get("success") and parsed.get("sql_expr"):
        sql_expr = parsed["sql_expr"]
        # Validate: no DAX-only constructs leaked into the output.
        if not _validate_sql(sql_expr):
            logger.warning(
                f"[DAX_LLM] LLM output contains DAX constructs for {measure.original_name}"
            )
            return
        measure.sql_expr = sql_expr
        measure.is_translatable = True
        measure.confidence = parsed.get("confidence", "medium")
        measure.category = "llm_translated"
        # dax_class = translation provenance/quality (7-cat); NOT emission routing.
        measure.dax_class = parsed.get("dax_class")
        measure.explanation = parsed.get("explanation")
        measure.skip_reason = ""
        usage = usage or {}
        tokens = usage.get("total_tokens", 0)
        cache_read = usage.get("cache_read_input_tokens", 0)
        logger.info(
            f"[DAX_LLM] Translated {measure.original_name} → {sql_expr[:80]}... "
            f"(confidence={measure.confidence}, dax_class={measure.dax_class}, "
            f"tokens={tokens}, cache_read={cache_read})"
        )
    else:
        # Even on non-success, record the classification for reporting/telemetry.
        measure.dax_class = parsed.get("dax_class") or measure.dax_class
        measure.explanation = parsed.get("explanation") or measure.explanation
        reason = parsed.get(
            "error", parsed.get("explanation", "LLM could not translate")
        )
        measure.skip_reason = _llm_declined_reason(measure.dax_class, reason)
        logger.info(
            f"[DAX_LLM] Could not translate {measure.original_name} (dax_class={measure.dax_class}): {reason}"
        )


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
    max_tokens: int = 2000,
) -> dict:
    """Call the LLM, preferring the cache-aware usage-returning path.

    Uses ``LLMManager.completion_with_usage`` so the skill-corpus system prefix
    can carry ``cache_control`` and the ``usage`` block is returned (cache hits
    observable). Falls back to plain ``completion()`` if the cached path errors,
    so a transport hiccup never blocks translation (fail-open).

    ``max_tokens`` is raised by the batch path, which asks the model to translate
    many measures in one response (see ``translate_batch_with_llm``).
    """
    from src.services.llm.manager import LLMManager
    from src.utils.telemetry import KasalProduct, get_user_agent_header

    messages = [_system_message(system_prompt), {"role": "user", "content": prompt}]
    headers = get_user_agent_header(KasalProduct.POWERBI)
    try:
        result = await LLMManager.completion_with_usage(
            messages=messages,
            model=model,
            temperature=0.1,
            max_tokens=max_tokens,
            extra_headers=headers,
        )
        return {"content": result.get("content"), "usage": result.get("usage", {})}
    except Exception as e:
        logger.warning(
            f"[DAX_LLM] cache-aware call failed ({e}); falling back to plain completion"
        )
        try:
            content = await LLMManager.completion(
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt},
                ],
                model=model,
                temperature=0.1,
                max_tokens=max_tokens,
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
    model: str = "databricks-claude-sonnet-4-5",
    cache: OrderedDict | None = None,
    table_context: str = "",
    domain_context: str = "",
    time_intel_enabled: bool = False,
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
        if cached.get("success"):
            measure.sql_expr = cached["sql_expr"]
            measure.is_translatable = True
            measure.confidence = cached.get("confidence", "medium")
            measure.category = "llm_translated"
            measure.dax_class = cached.get("dax_class")
            measure.skip_reason = ""
        elif cached.get("dax_class"):
            measure.dax_class = cached.get("dax_class")
        return measure

    # Build prompt
    user_prompt = _build_user_prompt(
        measure.original_name,
        measure.dax_expression,
        base_names,
        original_to_snake,
        table_context=table_context,
        domain_context=domain_context,
        time_intel_enabled=time_intel_enabled,
    )

    # Inject a deep, UCMV-legal reference for the long-tail DAX functions THIS
    # measure uses (statistical aggregates, PATH, financial closed-forms, arg-order
    # traps…) — skipping functions the always-on corpus already teaches deeply.
    # Fail-open '' for simple measures, so it costs nothing on the common path.
    _fref = render_function_refs([measure.dax_expression])
    if _fref:
        user_prompt = f"{user_prompt}\n{_fref}"

    # Call LLM
    response = await _call_llm(user_prompt, _SYSTEM_PROMPT, model)

    if not response.get("content"):
        logger.warning(
            f"[DAX_LLM] No response for {measure.original_name}: {response.get('error')}"
        )
        return measure

    # Parse response
    parsed = _parse_response(response["content"])

    # Cache result (run-scoped)
    if len(_cache) >= _RUN_CACHE_MAX:
        _cache.popitem(last=False)
    _cache[cache_key] = parsed

    if parsed.get("success") and parsed.get("sql_expr"):
        sql_expr = parsed["sql_expr"]

        # Validate: no DAX-only constructs in output
        if not _validate_sql(sql_expr):
            logger.warning(
                f"[DAX_LLM] LLM output contains DAX constructs for {measure.original_name}"
            )
            return measure

        measure.sql_expr = sql_expr
        measure.is_translatable = True
        measure.confidence = parsed.get("confidence", "medium")
        measure.category = "llm_translated"
        # dax_class = translation provenance/quality (7-cat); NOT emission routing.
        measure.dax_class = parsed.get("dax_class")
        measure.skip_reason = ""

        usage = response.get("usage", {}) or {}
        tokens = usage.get("total_tokens", 0)
        cache_read = usage.get("cache_read_input_tokens", 0)
        logger.info(
            f"[DAX_LLM] Translated {measure.original_name} → {sql_expr[:80]}... "
            f"(confidence={measure.confidence}, dax_class={measure.dax_class}, "
            f"tokens={tokens}, cache_read={cache_read})"
        )
    else:
        # Even on non-success, record the classification for reporting/telemetry.
        measure.dax_class = parsed.get("dax_class") or measure.dax_class
        reason = parsed.get(
            "error", parsed.get("explanation", "LLM could not translate")
        )
        logger.info(
            f"[DAX_LLM] Could not translate {measure.original_name} (dax_class={measure.dax_class}): {reason}"
        )

    return measure


# Max concurrent LLM translations per chunk. Bounded so we get a large
# wall-clock speedup vs. the old one-at-a-time loop without hammering the
# serving endpoint into rate limits. Measures WITHIN a chunk run in parallel
# (so they can't see each other's freshly-translated MEASURE() refs), but
# base_names/original_to_snake are updated BETWEEN chunks so a later chunk still
# resolves references to measures translated in an earlier chunk — preserving
# most of the cross-measure reference benefit of the old sequential order.
_DAX_LLM_CONCURRENCY = 6


# Number of measures translated per LLM call. The skill-corpus system prefix
# (~14k tokens) and the shared fact-table context are sent ONCE per call, so a
# batch of N amortises them across N measures instead of paying them per measure
# — the single biggest lever on the workspace tokens-per-minute rate limit, since
# Databricks silently drops Anthropic prompt caching (every call is cache_read=0).
# Tunable in Tools → Advanced without a redeploy. Keep modest so one call's
# input+output stays well within context/output limits.
def _dax_llm_batch_size() -> int:
    return int(engine_setting("dax_llm_batch_size"))


async def translate_batch_with_llm(
    measures: list[TranslationResult],
    table_key: str,
    base_names: set[str],
    original_to_snake: dict[str, str],
    model: str = "databricks-claude-sonnet-4-5",
    topo_priority: dict[str, int] | None = None,
    table_context: str = "",
    feedback: dict | None = None,
    domain_context: str = "",
    time_intel_enabled: bool = False,
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

    # Skip genuine display-only PBI artifacts — no business logic to translate,
    # so they'd only waste LLM tokens (FORMAT/Color/ISBLANK guards, empty DAX,
    # BLANK placeholders).
    #
    # NOTE: SELECTEDVALUE / SELECTEDVALUE+SWITCH are deliberately NOT here. The
    # regex quick-reject correctly refuses to translate them (they can't be
    # expressed as a single regex pattern), but they are real, parameterized
    # business measures — exactly what the skill corpus (UNSUPPORTED.md's
    # "skip the SELECTEDVALUE guard, the measure is still valid" + WINDOW.md
    # time-intelligence → window-measure patterns) was vendored to handle. They
    # must reach the LLM. If the LLM genuinely can't express one (e.g. a slicer-
    # driven time-period selector), it returns success=false and the measure
    # stays a TODO — no worse than filtering it out here, and strictly better
    # for the salvageable ones.
    _ARTIFACT_KEYWORDS = (
        "FORMAT",
        "Color",
        "ISBLANK+BLANK",
        "DAX expression not available",
        "ISFILTERED",
        "BLANK() placeholder",
    )

    candidates = [
        m
        for m in measures
        if not any(kw in m.skip_reason for kw in _ARTIFACT_KEYWORDS)
        and m.dax_expression.strip() not in ("", "Not available")
    ]

    if not candidates:
        return measures

    # Order dependents after their dependencies so cross-chunk MEASURE() refs
    # resolve (Step 3.9). Stable sort keeps input order for equal ranks.
    if topo_priority:
        candidates.sort(key=lambda m: topo_priority.get(m.original_name, 0))

    batch_size = _dax_llm_batch_size()
    logger.info(
        f"[DAX_LLM] Attempting LLM fallback for {len(candidates)} measures in "
        f"{table_key} (batch_size={batch_size})"
    )

    # Run-scoped cache — prevents cross-tenant leakage between pipeline runs and
    # lets identical DAX (same table_context) reuse an earlier result in this run.
    run_cache: OrderedDict[str, dict] = OrderedDict()

    def _ck(m: TranslationResult) -> str:
        return _content_hash(m.dax_expression + "\x00" + table_context)

    translated_count = 0
    # Process in BATCHES: one LLM call per batch translates all its measures, so
    # the ~14k-token skill corpus + shared fact-table context are paid ONCE per
    # batch instead of per measure (~batch_size× fewer input tokens — what was
    # blowing the workspace tokens-per-minute limit). Batches run sequentially:
    # dependencies are ordered into earlier batches (topo_priority) and each
    # batch's successes merge into base_names before the next, so cross-measure
    # MEASURE() references still resolve, and the per-minute token burst stays low.
    for start in range(0, len(candidates), batch_size):
        batch = candidates[start : start + batch_size]
        snap_names = set(base_names)

        # Apply run-cache hits first; only the rest need an LLM call.
        need_llm: list[TranslationResult] = []
        for m in batch:
            cached = run_cache.get(_ck(m))
            if cached is not None:
                _apply_parsed(m, cached)
            else:
                need_llm.append(m)

        if need_llm:
            prompt = _build_batch_user_prompt(
                need_llm,
                snap_names,
                table_context,
                feedback,
                domain_context,
                time_intel_enabled,
            )
            # Budget output for the whole batch (each measure ~a few hundred tokens).
            batch_max_tokens = min(len(need_llm) * 400 + 800, 16000)
            response = await _call_llm(
                prompt, _SYSTEM_PROMPT_BATCH, model, max_tokens=batch_max_tokens
            )
            parsed_list = _parse_batch_response(response.get("content") or "")

            if parsed_list is None:
                # Whole batch unparseable → per-measure fallback so one malformed
                # response never loses a whole batch of measures.
                logger.warning(
                    f"[DAX_LLM] batch response unparseable for {table_key}; "
                    f"falling back to per-measure for {len(need_llm)} measure(s)"
                )
                await asyncio.gather(
                    *(
                        translate_with_llm(
                            m,
                            table_key,
                            snap_names,
                            dict(original_to_snake),
                            model=model,
                            cache=run_cache,
                            table_context=table_context,
                            domain_context=domain_context,
                            time_intel_enabled=time_intel_enabled,
                        )
                        for m in need_llm
                    )
                )
            else:
                by_name = {}
                for item in parsed_list:
                    nm = str(item.get("measure_name", "")).strip().lower()
                    if nm:
                        by_name[nm] = item
                missing: list[TranslationResult] = []
                for m in need_llm:
                    found = by_name.get(m.original_name.strip().lower())
                    if found is None:
                        missing.append(m)
                        continue
                    if len(run_cache) >= _RUN_CACHE_MAX:
                        run_cache.popitem(last=False)
                    run_cache[_ck(m)] = found
                    _apply_parsed(m, found, response.get("usage", {}) or {})
                # Any measure the model dropped from the array → per-measure retry.
                if missing:
                    logger.warning(
                        f"[DAX_LLM] {len(missing)} measure(s) missing from batch "
                        f"response in {table_key}; per-measure fallback"
                    )
                    await asyncio.gather(
                        *(
                            translate_with_llm(
                                m,
                                table_key,
                                snap_names,
                                dict(original_to_snake),
                                model=model,
                                cache=run_cache,
                                table_context=table_context,
                            )
                            for m in missing
                        )
                    )

        # Merge this batch's successes into the shared context for later batches.
        for m in batch:
            if m.is_translatable:
                translated_count += 1
                base_names.add(m.measure_name)
                original_to_snake[m.original_name] = m.measure_name

    logger.info(
        f"[DAX_LLM] {translated_count}/{len(candidates)} measures translated via LLM for {table_key}"
    )
    return measures
