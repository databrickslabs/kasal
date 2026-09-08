"""UCMV Re-evaluation Tool — find measures that TODAY's transpiler can recover.

WHY: Kasal's DAX→UC-Metric-View capability keeps improving (new translator patterns,
a better LLM-first path, an evolving skill corpus), but those gains only ever helped
NEW conversions. A model converted last month still shows the measures that failed
back then, even though several are translatable now.

This tool replays stored state — no PowerBI API round-trip — and re-tries ONLY the
previously-failed measures, reporting which are now recoverable so a human can choose
to re-transpile them.

Hard rules:
  * Only previously-FAILED measures are retried. A measure that already succeeded is
    never re-transpiled (that would risk silently changing a validated measure).
  * PROPOSE ONLY. This tool mutates nothing; applying a recovery goes through the
    normal UCMV route, human-triggered.

Cost controls (a scheduled sweep must not quietly burn tokens):
  * ``use_llm`` defaults to False → deterministic regex fast-path only, zero tokens.
  * Permanent-limitation categories (display artifacts, slicer scalars, prior-year…)
    are skipped unless ``include_impossible``.
  * Reviewer-dismissed measures (``wont_fix``/``hand_written``) are suppressed.
  * ``max_measures_per_dataset`` caps work per dataset, applied AFTER impact ordering.

Docs: src/docs/powerbi/ucmv-reevaluation-recoverable-measures.md
"""
import json
import logging
from typing import Any, Optional, Type

from crewai.tools import BaseTool
from pydantic import BaseModel, Field, PrivateAttr

from src.services.tools.metric_view_utils.utils import run_async

logger = logging.getLogger(__name__)


class UCMVReevaluationSchema(BaseModel):
    """Input schema for UCMVReevaluationTool."""

    dataset_ids: Optional[str] = Field(
        None,
        description=(
            "Optional JSON list or comma-separated PBI dataset ids to re-evaluate. "
            "Omit to sweep every dataset with a stored extraction for the group."
        ),
    )
    group_id: Optional[str] = Field(
        None, description="Group/tenant id to scope stored extractions (multi-tenant isolation).")
    include_impossible: Optional[bool] = Field(
        False,
        description=(
            "Also retry permanent-limitation categories (display artifacts, slicer "
            "scalars, prior-year time-intelligence). Usually wasted compute."
        ),
    )
    use_llm: Optional[bool] = Field(
        False,
        description=(
            "Allow the LLM-first translation path (COSTS TOKENS per measure). "
            "Default False = deterministic regex fast-path only."
        ),
    )
    max_measures_per_dataset: Optional[int] = Field(
        200, description="Cap retried measures per dataset (applied after impact ordering).")
    max_datasets: Optional[int] = Field(
        50, description="Cap how many datasets a single sweep scans.")
    force: Optional[bool] = Field(
        False,
        description=(
            "Re-try even when the capability fingerprint is UNCHANGED since the stored "
            "run. Normally such a retry cannot find anything new and is skipped; use "
            "this to verify the sweep works, or to re-check after a change the "
            "fingerprint didn't capture."
        ),
    )


class UCMVReevaluationTool(BaseTool):
    """Report previously-untranslatable measures that today's transpiler can recover."""

    name: str = "UCMV Re-evaluation"
    description: str = (
        "Scans stored PowerBI extractions and re-tries ONLY the measures that "
        "previously failed to transpile, using today's (improved) DAX→UC Metric View "
        "transpiler. Reports which measures are NOW recoverable, with their new SQL, "
        "the reason they failed before, and how many other measures depend on them. "
        "Read-only: it proposes re-transpilation candidates and never modifies "
        "metric views. Deterministic by default (no LLM tokens) unless use_llm=true."
    )
    args_schema: Type[BaseModel] = UCMVReevaluationSchema
    _default_config: dict = PrivateAttr(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True, "extra": "allow"}

    def __init__(self, **kwargs: Any) -> None:
        config_keys = (
            'dataset_ids', 'group_id', 'include_impossible', 'use_llm',
            'max_measures_per_dataset', 'max_datasets', 'force',
        )
        default_config: dict = {}
        for key in config_keys:
            if key in kwargs:
                default_config[key] = kwargs.pop(key)
        super().__init__(**kwargs)
        self._default_config = default_config

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _parse_dataset_ids(raw: Any) -> list[str]:
        """Accept a JSON list, a comma-separated string, or a real list."""
        if not raw:
            return []
        if isinstance(raw, list):
            return [str(x).strip() for x in raw if str(x).strip()]
        text = str(raw).strip()
        if text.startswith('['):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    return [str(x).strip() for x in parsed if str(x).strip()]
            except json.JSONDecodeError:
                pass
        return [p.strip() for p in text.split(',') if p.strip()]

    @staticmethod
    def _stored_untranslatable(record: Any) -> list[dict]:
        """Pull the stored ``untranslatable_items`` off a ConversionHistory row.

        The UCMV generator writes them into ``output_data.untranslatable_items``.
        Older rows predate that, so an empty list simply means "nothing recorded to
        re-evaluate for this run".
        """
        out = getattr(record, 'output_data', None)
        if isinstance(out, dict):
            val = out.get('untranslatable_items')
            if isinstance(val, list):
                return [i for i in val if isinstance(i, dict)]
        return []

    @staticmethod
    def _stored_review(record: Any) -> dict:
        """Reviewer annotations, when a saved (edited) result carried them forward."""
        out = getattr(record, 'output_data', None)
        if isinstance(out, dict):
            val = out.get('untranslatable_review')
            if isinstance(val, dict):
                return val
        return {}

    @staticmethod
    def _stored_config(record: Any) -> dict:
        """The pipeline config that produced this run (translator context)."""
        cfg = getattr(record, 'configuration', None)
        return cfg if isinstance(cfg, dict) else {}

    @staticmethod
    def _stored_dataset_id(record: Any) -> str:
        cfg = getattr(record, 'configuration', None)
        if isinstance(cfg, dict) and cfg.get('dataset_id'):
            return str(cfg['dataset_id'])
        inp = getattr(record, 'input_data', None)
        if isinstance(inp, dict) and inp.get('dataset_id'):
            return str(inp['dataset_id'])
        return ''

    @staticmethod
    def _harvest_untranslatable(obj: Any, out: list) -> None:
        """Walk a nested result blob collecting every ``untranslatable_items`` array.

        The UCMV generator's full output lives in the EXECUTION result, and tool
        outputs are frequently embedded as JSON *strings* inside it, so a plain
        ``.get()`` misses them. Recursing (and parsing string-encoded JSON) is what
        actually finds the measures.
        """
        if isinstance(obj, dict):
            val = obj.get('untranslatable_items')
            if isinstance(val, list):
                out.extend(i for i in val if isinstance(i, dict))
            for v in obj.values():
                if isinstance(v, str):
                    if 'untranslatable_items' in v:
                        try:
                            UCMVReevaluationTool._harvest_untranslatable(json.loads(v), out)
                        except (json.JSONDecodeError, ValueError):
                            pass
                else:
                    UCMVReevaluationTool._harvest_untranslatable(v, out)
        elif isinstance(obj, list):
            for v in obj:
                UCMVReevaluationTool._harvest_untranslatable(v, out)

    async def _load_executions(self, dataset_ids: list[str], group_id: Optional[str],
                               max_datasets: int) -> list[dict]:
        """Latest UCMV EXECUTION results carrying non-transpiled measures.

        The execution result blob is the authoritative record: it is the full tool
        output the UI renders (`untranslatable_items` included). ConversionHistory
        only ever gets a summary copy, so it is used as a fallback, not the source.

        Deliberately NOT filtered by group_id: historical execution rows frequently
        have it unset, and filtering would silently discard nearly everything.
        """
        from sqlalchemy import text as _text
        from src.services.tools.tool_session_provider import ToolSessionProvider

        out: list[dict] = []
        async with ToolSessionProvider.session() as session:
            rows = await session.execute(_text(
                "SELECT job_id, run_name, created_at, result::text AS blob "
                "FROM executionhistory "
                "WHERE result::text LIKE '%untranslatable_items%' "
                "ORDER BY created_at DESC "
                "LIMIT :lim"
            ), {"lim": max(max_datasets * 2, 20)})
            for job_id, run_name, created_at, blob in rows.all():
                if not blob:
                    continue
                try:
                    parsed = json.loads(blob)
                except (json.JSONDecodeError, ValueError):
                    continue
                items: list = []
                self._harvest_untranslatable(parsed, items)
                if not items:
                    continue
                # The execution blob carries the measures but NOT the capability
                # fingerprint or the pipeline config — those are written to
                # conversion_history by the generator. Pull them from the matching
                # conversion row so (a) "nothing to gain" is detected correctly and
                # (b) the retry sees the same translator context (filter_sets,
                # fact_join_map, …) the original run had. Falls back to whatever the
                # blob happens to contain.
                fp = self._find_fingerprint(parsed)
                cfg = self._find_config(parsed)
                if not fp or not cfg:
                    conv = await session.execute(_text(
                        "SELECT configuration::text FROM conversion_history "
                        "WHERE execution_id = :jid AND source_format = 'powerbi_dax' "
                        "ORDER BY created_at DESC LIMIT 1"
                    ), {"jid": str(job_id)})
                    row = conv.first()
                    if row and row[0]:
                        try:
                            conv_cfg = json.loads(row[0])
                            if isinstance(conv_cfg, dict):
                                fp = fp or conv_cfg.get('capability_fingerprint')
                                cfg = cfg or conv_cfg
                        except (json.JSONDecodeError, ValueError):
                            pass

                out.append({
                    'source': 'execution',
                    'key': str(job_id),
                    'label': run_name or str(job_id),
                    'created_at': str(created_at or ''),
                    'items': items,
                    # None = unknown (run pre-dates fingerprint recording), which
                    # has_capability_changed treats as "may have gained" — the honest
                    # default rather than silently skipping.
                    'fingerprint': fp,
                    'config': cfg or {},
                })
                if len(out) >= max_datasets:
                    break
        return out

    @staticmethod
    def _find_fingerprint(obj: Any) -> Optional[str]:
        """Pull a recorded capability_fingerprint out of a nested blob, if present."""
        if isinstance(obj, dict):
            fp = obj.get('capability_fingerprint')
            if isinstance(fp, str) and fp:
                return fp
            for v in obj.values():
                got = UCMVReevaluationTool._find_fingerprint(v)
                if got:
                    return got
        elif isinstance(obj, list):
            for v in obj:
                got = UCMVReevaluationTool._find_fingerprint(v)
                if got:
                    return got
        return None

    @staticmethod
    def _find_config(obj: Any) -> dict:
        """Best-effort pipeline config (translator context) from a nested blob."""
        if isinstance(obj, dict):
            for key in ('proposed_config', 'config', 'configuration'):
                v = obj.get(key)
                if isinstance(v, dict) and v:
                    return v
            for v in obj.values():
                got = UCMVReevaluationTool._find_config(v)
                if got:
                    return got
        elif isinstance(obj, list):
            for v in obj:
                got = UCMVReevaluationTool._find_config(v)
                if got:
                    return got
        return {}

    async def _load_records(self, dataset_ids: list[str], group_id: Optional[str],
                            max_datasets: int) -> list[Any]:
        """Fallback source: ConversionHistory rows (summary copies).

        Kept for runs whose execution row has aged out. NOT group-filtered (most
        historical rows have no group_id) and a row with no dataset_id falls back to
        its conversion id rather than being dropped.
        """
        from src.services.tools.tool_session_provider import ToolSessionProvider

        records: list[Any] = []
        wanted = set(dataset_ids)
        async with ToolSessionProvider.conversion_repo() as repo:
            rows = await repo.find_by_formats(
                source_format='powerbi_dax',
                target_format='uc_metrics',
                group_id=None,
                limit=max(max_datasets * 10, 50),
            )
            seen: set[str] = set()
            for row in rows:
                # Fall back to the conversion id so a run without dataset_id is still
                # re-evaluable instead of silently skipped.
                ds = self._stored_dataset_id(row) or f"conversion-{getattr(row, 'id', '?')}"
                if ds in seen:
                    continue
                if wanted and ds not in wanted:
                    continue
                seen.add(ds)
                records.append(row)
                if len(records) >= max_datasets:
                    break
        return records

    # ------------------------------------------------------------------
    # Run
    # ------------------------------------------------------------------
    def _run(self, **kwargs: Any) -> str:
        from src.services.tools.metric_view_utils.capability_version import (
            capability_summary, has_capability_changed,
        )
        from src.services.tools.metric_view_utils.reevaluate import (
            reevaluate_measures,
        )

        def _get(key: str, default: Any = None) -> Any:
            if key in kwargs and kwargs[key] not in (None, ''):
                return kwargs[key]
            if key in self._default_config and self._default_config[key] not in (None, ''):
                return self._default_config[key]
            return default

        dataset_ids = self._parse_dataset_ids(_get('dataset_ids'))
        group_id = _get('group_id')
        include_impossible = bool(_get('include_impossible', False))
        use_llm = bool(_get('use_llm', False))
        max_per_ds = int(_get('max_measures_per_dataset', 200) or 200)
        max_datasets = int(_get('max_datasets', 50) or 50)
        force = bool(_get('force', False))

        capability = capability_summary()
        logger.info(
            "[UCMVReeval] capability=%s patterns=%s datasets=%s group=%s use_llm=%s",
            capability.get('fingerprint'), capability.get('pattern_count'),
            dataset_ids or 'ALL', group_id, use_llm,
        )

        # PRIMARY source: execution results (the full tool output the UI renders —
        # this is where untranslatable_items actually lands). ConversionHistory only
        # receives a summary copy, so it is the fallback.
        executions: list[dict] = []
        try:
            executions = run_async(self._load_executions(dataset_ids, group_id, max_datasets))
        except Exception as exc:
            logger.warning("[UCMVReeval] execution scan failed (%s) — trying conversions", exc)

        records: list[Any] = []
        if not executions:
            try:
                records = run_async(self._load_records(dataset_ids, group_id, max_datasets))
            except Exception as exc:
                logger.error("[UCMVReeval] could not load stored runs: %s", exc)
                return json.dumps({
                    'error': f'could not load stored runs: {exc}',
                    'capability': capability,
                    'datasets': [],
                    'summary': {'datasets_scanned': 0, 'measures_recovered': 0},
                }, indent=2)

        datasets_report: list[dict] = []
        total_recovered = 0
        total_retried = 0

        # ── Execution-sourced runs (the rich path) ──────────────────────────────
        for ex in executions:
            items = ex['items']
            fingerprint_changed = has_capability_changed(ex.get('fingerprint')) or force
            base = {
                'dataset_id': ex['label'],
                'workspace_id': None,
                'converted_at': ex['created_at'],
                'conversion_id': ex['key'],
                'source': 'execution',
            }
            if not fingerprint_changed:
                datasets_report.append({
                    **base, 'fingerprint_changed': False,
                    'note': 'transpiler unchanged since this run — nothing to gain',
                    'newly_translatable': [],
                    'still_failing_count': len(items), 'skipped_count': 0,
                })
                continue

            result = reevaluate_measures(
                items, ex.get('config') or {},
                review=None,
                include_impossible=include_impossible,
                use_llm=use_llm,
                limit=max_per_ds,
            )
            counts = result.get('counts', {})
            total_recovered += counts.get('recovered', 0)
            total_retried += counts.get('retried', 0)
            entry = {
                **base, 'fingerprint_changed': True,
                'newly_translatable': result.get('newly_translatable', []),
                'still_failing_count': counts.get('still_failing', 0),
                'skipped_count': counts.get('skipped', 0),
                'counts': counts,
            }
            # Be explicit about WHY we retried, so "fingerprint_changed: true" is not
            # read as "the transpiler improved" when it really means "unknown".
            if not ex.get('fingerprint'):
                entry['fingerprint_note'] = (
                    'run pre-dates capability-fingerprint recording — retried because a '
                    'gain cannot be ruled out, not because the transpiler is known to '
                    'have changed'
                )
            if not counts.get('recovered') and not use_llm:
                entry['note'] = (
                    'deterministic fast-path only (use_llm=false). Measures previously '
                    'routed to the LLM cannot be recovered without use_llm=true.'
                )
            datasets_report.append(entry)

        for record in records:
            cfg = self._stored_config(record)
            ds_id = self._stored_dataset_id(record)
            items = self._stored_untranslatable(record)
            stored_fp = cfg.get('capability_fingerprint')
            base = {
                'dataset_id': ds_id,
                'workspace_id': cfg.get('workspace_id'),
                'converted_at': str(getattr(record, 'created_at', '') or ''),
                'conversion_id': getattr(record, 'id', None),
            }

            fingerprint_changed = has_capability_changed(stored_fp) or force
            if not items:
                datasets_report.append({
                    **base,
                    'fingerprint_changed': fingerprint_changed,
                    'note': (
                        'no stored non-transpiled measures for this run '
                        '(pre-dates re-evaluation recording, or everything translated)'
                    ),
                    'newly_translatable': [],
                    'still_failing_count': 0,
                    'skipped_count': 0,
                })
                continue

            if not fingerprint_changed:
                # Nothing changed since this run produced its result → a retry cannot
                # find anything new. Report it explicitly instead of silently skipping.
                datasets_report.append({
                    **base,
                    'fingerprint_changed': False,
                    'note': 'transpiler unchanged since this run — nothing to gain',
                    'newly_translatable': [],
                    'still_failing_count': len(items),
                    'skipped_count': 0,
                })
                continue

            result = reevaluate_measures(
                items,
                cfg,
                review=self._stored_review(record),
                include_impossible=include_impossible,
                use_llm=use_llm,
                limit=max_per_ds,
            )
            counts = result.get('counts', {})
            total_recovered += counts.get('recovered', 0)
            total_retried += counts.get('retried', 0)

            datasets_report.append({
                **base,
                'fingerprint_changed': True,
                'newly_translatable': result.get('newly_translatable', []),
                'still_failing_count': counts.get('still_failing', 0),
                'skipped_count': counts.get('skipped', 0),
                'counts': counts,
            })

        output = {
            'capability': capability,
            'settings': {
                'include_impossible': include_impossible,
                'use_llm': use_llm,
                'max_measures_per_dataset': max_per_ds,
                'max_datasets': max_datasets,
                'force': force,
            },
            'datasets': datasets_report,
            'summary': {
                'datasets_scanned': len(datasets_report),
                'measures_retried': total_retried,
                'measures_recovered': total_recovered,
                'datasets_with_recoveries': sum(
                    1 for d in datasets_report if d.get('newly_translatable')),
            },
        }
        logger.info(
            "[UCMVReeval] done: %s dataset(s), %s retried, %s recoverable",
            len(datasets_report), total_retried, total_recovered,
        )
        return json.dumps(output, indent=2, default=str)
