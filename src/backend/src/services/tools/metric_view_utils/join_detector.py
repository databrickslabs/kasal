"""Join Detector — auto-detect joins from DAX dimension references."""

from __future__ import annotations

import logging
import re

from .constants import RE_DAX_DIM_REF
from .data_classes import TableInfo
from .period_folding import (
    PeriodConfig,
    detect_period_columns,
    fold_period_dimensions,
    has_closed_period_logic,
)
from .utils import col_to_readable

logger = logging.getLogger(__name__)

_RE_SAFE_ALIAS = re.compile(r"^[a-zA-Z_]\w*$")
# A dedup ORDER BY from config: columns, each optionally ASC / DESC.
_RE_SAFE_ORDER_BY = re.compile(
    r"^[A-Za-z_][\w.]*(?:\s+(?:ASC|DESC))?(?:\s*,\s*[A-Za-z_][\w.]*(?:\s+(?:ASC|DESC))?)*$",
    re.IGNORECASE,
)
# A KBI code becomes both a string literal and part of a column alias.
_RE_SAFE_KBI_CODE = re.compile(r"^[A-Za-z0-9_]+$")


def _is_placeholder_source(source: str) -> bool:
    """True when ``source`` is a stub rather than a resolved physical table.

    The generation-time M parser can fail to resolve a *parametric* source
    (``Databricks.Catalogs(ServerHostName, HTTP_Path, …)``, where the catalog is
    a model parameter) and leave a bare table name, which the target-catalog
    default then prefixes into ``main.default.<name>``. A real UC source is a
    3-level ``catalog.schema.table``. ``join_key_map[dim].source_table``, filled
    during config-gen enrichment (which HAS the model parameters), is the
    authoritative value to prefer over such a stub — so a dimension join never
    ships pointing at ``main.default.*``.
    """
    if not source or not source.strip():
        return True
    t = source.strip()
    low = t.lower()
    if low.startswith("todo") or low.startswith("main.default."):
        return True
    if "(" in t:  # an inline subquery / pivot source is real, not a stub
        return False
    return t.count(".") < 2  # not a 3-level catalog.schema.table


def _sanitize_alias(alias: str) -> str:
    """Sanitize a SQL alias to prevent injection. Only allow alphanumeric + underscore."""
    if not _RE_SAFE_ALIAS.match(alias):
        raise ValueError(f"Invalid SQL alias: {alias}")
    return alias


def _safe_identifiers(*names: object) -> bool:
    """Whether every config name is a plain identifier — they reach the
    executed pivot SQL unquoted."""
    return all(_RE_SAFE_ALIAS.match(str(n)) for n in names)


def _pivot_kbi_codes(
    fact_name: str, pivot_col: str, measures: list[dict], fj: dict
) -> set[str]:
    """KBI codes the measures' DAX filters ``fact[pivot_col]`` on, else the
    fact join's configured ``kbi_codes``. Each becomes a string literal AND part
    of a column alias, so config codes are kept only when ``[A-Za-z0-9_]+`` —
    the same shape the DAX extraction can produce."""
    kbi_codes: set[str] = set()
    kbi_pattern = re.compile(
        rf"\b{re.escape(fact_name)}\[{re.escape(pivot_col)}\]\s*"
        rf'(?:=\s*"([A-Z0-9]+)"|in\s*\{{([^}}]+)\}})',
        re.IGNORECASE,
    )
    for m_entry in measures:
        for hit in kbi_pattern.finditer(m_entry.get("dax_expression", "")):
            if hit.group(1):
                kbi_codes.add(hit.group(1))
            elif hit.group(2):
                kbi_codes.update(re.findall(r"[A-Z0-9]+", hit.group(2)))
    if not kbi_codes:
        config_codes = [
            c for c in fj.get("kbi_codes", []) if _RE_SAFE_KBI_CODE.match(str(c))
        ]
        if config_codes:
            kbi_codes.update(config_codes)
            logger.info(
                "Using %d kbi_codes from fact_join_map config for %s",
                len(config_codes),
                fact_name,
            )
    return kbi_codes


class JoinDetector:
    """Auto-detect joins from DAX measure references to dimension tables."""

    def __init__(self, mquery_tables: dict[str, TableInfo], config: dict | None = None):
        self.mquery_tables = mquery_tables
        cfg = config or {}
        # Per-model period-folding overrides (crew input under config.naming_config.period).
        self._period_config = PeriodConfig.from_dict(
            (cfg.get("naming_config") or {}).get("period")
        )
        self._join_key_map = cfg.get("join_key_map", {})
        self._fact_join_map = cfg.get("fact_join_map", {})
        # P4a: global opt-in to dedup ALL dim joins with a QUALIFY subquery
        # (per-dim `dedup_dim` in join_key_map takes precedence / adds to this).
        self._dedup_all_dim_joins = bool(cfg.get("dedup_dim_joins", False))
        # Dimensions a measure references but no join could be built for;
        # reported in the migration report (the view would otherwise just
        # lack them).
        self.dropped_dims: list[dict] = []

    def _record_dropped(self, fact_table_key: str, dim_name: str) -> None:
        if any(
            d["fact"] == fact_table_key and d["dimension"] == dim_name
            for d in self.dropped_dims
        ):
            return
        logger.warning(
            "[JoinDetector] %s: join to %s dropped, no physical source table",
            fact_table_key,
            dim_name,
        )
        self.dropped_dims.append(
            {
                "fact": fact_table_key,
                "dimension": dim_name,
                "reason": (
                    "no physical source table: its Power Query M keeps only some "
                    "rows (or could not be resolved), and join_key_map has no "
                    "source_table; supply a source view"
                ),
            }
        )

    def detect(
        self,
        fact_table_key: str,
        measures: list[dict],
        fact_info: TableInfo,
        inner_dim_joins: bool = False,
    ) -> list[dict]:
        """Detect required dimension joins for a fact table based on DAX references."""
        referenced_dims: set[str] = set()
        for m in measures:
            dax = m.get("dax_expression", "")
            for match in RE_DAX_DIM_REF.finditer(dax):
                table_ref = match.group(1)
                if table_ref in self._join_key_map:
                    referenced_dims.add(table_ref)

        # Also add dims where the join_key matches a GROUP BY column
        # (these are natural joins even without DAX references)
        for dim_name, jk_check in self._join_key_map.items():
            if dim_name in referenced_dims:
                continue
            jk_key = jk_check.get("join_key", "")
            if jk_key and jk_key in fact_info.group_by_columns:
                referenced_dims.add(dim_name)
            else:
                for alt_k in jk_check.get("alt_join_keys", []):
                    if alt_k in fact_info.group_by_columns:
                        referenced_dims.add(dim_name)
                        break

        joins = []
        for dim_name in sorted(referenced_dims):
            jk = self._join_key_map[dim_name]
            dim_table_info = self.mquery_tables.get(dim_name)
            source = ""
            if dim_table_info and dim_table_info.source_table:
                source = dim_table_info.source_table
            else:
                # Fallback: check fact table's dim_source_tables (from MQuery LEFT JOINs)
                alias = jk.get("alias", dim_name.lower())
                for fact_alias, fact_src in fact_info.dim_source_tables.items():
                    if (
                        fact_alias.lower() == alias
                        or dim_name.lower() in fact_alias.lower()
                    ):
                        source = fact_src
                        break
            # join_key_map[dim].source_table is filled during config-gen
            # enrichment, which resolves parametric sources using the model's
            # expressions the generation-time parser may lack. Prefer it whenever
            # what we resolved above is empty or a `main.default.*` placeholder,
            # so the join points at the real table instead of a stub.
            jk_source = jk.get("source_table", "")
            if _is_placeholder_source(source) and not _is_placeholder_source(jk_source):
                source = jk_source
            elif not source:
                source = jk_source
            if not source:
                self._record_dropped(fact_table_key, dim_name)
                continue

            alias = jk["alias"]
            join_key = jk["join_key"]
            dim_key = jk.get("dim_key", join_key)

            has_key = join_key in fact_info.group_by_columns or (
                not fact_info.group_by_columns and join_key in fact_info.full_sql
            )

            # P4b: zero-pad the fact-side key when the dim key is a fixed-width
            # code and the fact carries the unpadded form (classic SAP
            # co_code_bw(3) → comp_code(4)). Config-driven — the pad length is a
            # data fact the tool can't infer, but once given it generates exactly
            # the LPAD the customer otherwise hand-writes.
            fact_key_sql = self._padded_key_sql("source", join_key, jk)
            join_on = f"{fact_key_sql} = {alias}.{dim_key}"

            if not has_key:
                alt_keys = jk.get("alt_join_keys", [])
                alt_join_on = jk.get("alt_join_on")
                if (
                    alt_keys
                    and alt_join_on
                    and all(k in fact_info.group_by_columns for k in alt_keys)
                ):
                    has_key = True
                    join_on = alt_join_on

            if has_key:
                # P4a: dedup a non-unique dim on its key via a QUALIFY subquery
                # source, so the join can't fan out rows. Opt-in (per-dim
                # `dedup_dim` flag or global `dedup_dim_joins`) because whether a
                # dim is non-unique is a data fact; when flagged we generate the
                # SELECT DISTINCT … QUALIFY ROW_NUMBER() = 1 the GT hand-wrote.
                join_source = self._maybe_dedup_source(source, dim_key, jk)
                join_entry = {
                    "name": alias,
                    "source": join_source,
                    "join_on": join_on,
                }
                if inner_dim_joins:
                    join_entry["join_type"] = "inner"
                joins.append(join_entry)

        return joins

    def _padded_key_sql(self, prefix: str, join_key: str, jk: dict) -> str:
        """Build the fact-side join-key expression, applying LPAD when the dim
        config declares a fixed-width padded key (P4b). ``jk['pad_key']`` is
        ``{'len': int, 'char': str}`` (char defaults to '0'); absent → no padding.
        """
        pad = jk.get("pad_key")
        base = f"{prefix}.{join_key}"
        if isinstance(pad, dict) and pad.get("len"):
            char = str(pad.get("char", "0"))[:1] or "0"
            return f"LPAD({base}, {int(pad['len'])}, '{char}')"
        return base

    def _maybe_dedup_source(self, source: str, dim_key: str, jk: dict) -> str:
        """Wrap a dim source in a QUALIFY-dedup subquery when the join is flagged
        as pointing at a non-unique dim (P4a). Returns ``source`` unchanged when
        not flagged, or when ``source`` is already a subquery. ``order_by``
        defaults to the dim key (deterministic pick of the first row per key)."""
        if not (jk.get("dedup_dim") or self._dedup_all_dim_joins):
            return source
        s = source.strip()
        if s.startswith("("):
            return source  # already a subquery — don't double-wrap
        order_by = jk.get("dedup_order_by") or dim_key
        # Both come from config and reach the executed view source unquoted.
        if not (_RE_SAFE_ALIAS.match(dim_key) and _RE_SAFE_ORDER_BY.match(order_by)):
            logger.warning("[JOIN] unsafe dedup key/order for %s — not deduped", source)
            return source
        return (
            f"(SELECT * FROM {source} "  # identifiers validated above; source from the join map — nosemgrep: tmp.databricks.sql-injection.fstring-select
            f"QUALIFY ROW_NUMBER() OVER "
            f"(PARTITION BY {dim_key} ORDER BY {order_by}) = 1)"
        )

    def detect_fact_joins(
        self, fact_table_key: str, measures: list[dict], fact_info: TableInfo
    ) -> list[dict]:
        """Detect required fact-to-fact joins for cross-table measures."""
        referenced_facts = self._referenced_fact_names(fact_table_key, measures)
        joins: list[dict] = []
        for fact_name in sorted(referenced_facts):
            fj = self._fact_join_map[fact_name]
            # S7: a table already used as a DIMENSION (present in join_key_map) must
            # not also be emitted as a plain fact join — that produced OTC's
            # duplicate calendar445 join (once as the real dim, once as a fact on a
            # non-existent `.Date` key). Special fact-join modes (pivot / union /
            # source_embed) are genuinely fact-shaped and are left alone.
            is_plain_join = not (
                fj.get("pivot_col") or fj.get("source_embed") or fj.get("union_mode")
            )
            if is_plain_join and fact_name in self._join_key_map:
                logger.info(
                    "[%s] Skipping fact join '%s' — already joined as a dimension "
                    "(join_key_map); a dim must not double as a fact join (S7)",
                    fact_table_key,
                    fact_name,
                )
                continue
            fact_table_info = self.mquery_tables.get(fact_name)
            source_table = self._fact_join_source_table(
                fact_table_key, fact_name, fj, fact_table_info
            )
            if not source_table:
                logger.warning(
                    f"[{fact_table_key}] Cannot add fact join '{fact_name}' — "
                    f"no source table. Add 'source_table' to fact_join_map['{fact_name}']."
                )
                continue
            alias = _sanitize_alias(fj["alias"])

            join_on = self._fact_join_on_clause(
                fact_table_key, fact_name, fj, alias, fact_info
            )
            if join_on is None:
                continue

            # Pivot narrow/vertical KBI tables: None = not a pivot join (fall
            # through), [] = skip this fact, [join] = the pivoted join.
            pivot_joins = self._pivot_fact_joins(
                fact_name, fj, alias, source_table, join_on, measures
            )
            if pivot_joins is not None:
                joins.extend(pivot_joins)
                continue

            # Source-embed mode
            if fj.get("source_embed"):
                joins.append(
                    {
                        "name": alias,
                        "_source_embed": True,
                        "_embed_inline_sql": fj.get("inline_source", ""),
                        "_embed_join_key": fj.get("embed_join_key", []),
                        "_embed_columns": fj.get("embed_columns", {}),
                        "_fact_join_config": fj,
                        "_pbi_name": fact_name,
                    }
                )
                continue

            join_source = fj.get("inline_source") or source_table
            join_on = self._qualify_static_filters(fj, alias, join_on, fact_table_info)
            joins.append(
                {
                    "name": alias,
                    "source": join_source,
                    "join_on": join_on,
                    "_fact_join_config": fj,
                    "_pbi_name": fact_name,
                }
            )
        return self._dedup_joins(joins)

    def _referenced_fact_names(
        self, fact_table_key: str, measures: list[dict]
    ) -> set[str]:
        """Facts a cross-table measure references, plus facts whose config targets
        this fact (union_mode / source_embed) even without an explicit DAX ref."""
        referenced_facts: set[str] = set()
        for m in measures:
            for ref in m.get("direct_fact_refs", []):
                if ref != fact_table_key and ref in self._fact_join_map:
                    referenced_facts.add(ref)

        # Also add fact joins that target this fact table (union_mode, source_embed)
        # even without explicit DAX references — config declares the relationship
        logger.info(
            f"[{fact_table_key}] detect_fact_joins: {len(self._fact_join_map)} entries in fact_join_map, "
            f"{len(referenced_facts)} from DAX refs"
        )
        for fj_name, fj_cfg in self._fact_join_map.items():
            if fj_name in referenced_facts or fj_name == fact_table_key:
                continue
            target = fj_cfg.get("target_fact", "")
            if target == fact_table_key:
                referenced_facts.add(fj_name)
                logger.info(
                    f"[{fact_table_key}] Added fact join '{fj_name}' via target_fact config"
                )

        logger.info(
            f"[{fact_table_key}] Total referenced_facts after config scan: {referenced_facts}"
        )
        return referenced_facts

    @staticmethod
    def _fact_join_source_table(
        fact_table_key: str,
        fact_name: str,
        fj: dict,
        fact_table_info: TableInfo | None,
    ) -> str:
        """Physical source of a fact join: the M-derived table, else the config's."""
        if fact_table_info and fact_table_info.source_table:
            logger.info(
                f"[{fact_table_key}] Fact join '{fact_name}': source from mquery_tables = {fact_table_info.source_table}"
            )
            return fact_table_info.source_table
        # Fallback: use source_table from fact_join_map config
        source_table: str = fj.get("source_table", "")
        logger.info(
            f"[{fact_table_key}] Fact join '{fact_name}': source from config = {source_table}"
        )
        return source_table

    def _fact_join_on_clause(
        self,
        fact_table_key: str,
        fact_name: str,
        fj: dict,
        alias: str,
        fact_info: TableInfo,
    ) -> str | None:
        """ON clause for a fact join, or ``None`` when the join must be dropped."""
        if "join_on_expr" in fj:
            join_on: str = fj["join_on_expr"].format(alias=alias)
            for src_ref in re.findall(r"\bsource\.(\w+)", join_on):
                if src_ref in fact_info.group_by_columns:
                    continue
                calc = next(
                    (c for c in fact_info.calculated_columns if c["name"] == src_ref),
                    None,
                )
                if calc is not None:
                    calc_expr = calc["expr"]
                    for gb in fact_info.group_by_columns:
                        calc_expr = re.sub(
                            rf"(?<![.\w])\b{re.escape(gb)}\b(?!\s*\()",
                            f"source.{gb}",
                            calc_expr,
                        )
                    join_on = join_on.replace(f"source.{src_ref}", calc_expr)
                    continue
                # S7: the ON clause names a fact-side column that the
                # fact does not have (OTC's calendar445 join on
                # `source.date` when the raw column is `date_id`). Only
                # trust this when we actually know the fact's columns
                # (non-empty group_by and not present in the raw SQL) —
                # otherwise keep the join rather than risk a regression.
                known_cols = bool(fact_info.group_by_columns)
                in_sql = src_ref in (fact_info.full_sql or "")
                if known_cols and not in_sql:
                    logger.info(
                        "[%s] Dropping fact join '%s' — join key "
                        "source.%s does not exist on the fact (S7)",
                        fact_table_key,
                        fact_name,
                        src_ref,
                    )
                    return None
                logger.warning(
                    "[JOIN] source.%s referenced in join_on_expr but "
                    "not found in group_by_columns or calculated_columns",
                    src_ref,
                )
            return join_on
        if "join_key" in fj:
            join_keys = (
                fj["join_key"] if isinstance(fj["join_key"], list) else [fj["join_key"]]
            )
            if not all(k in fact_info.group_by_columns for k in join_keys):
                return None
            return " AND ".join(f"source.{k} = {alias}.{k}" for k in join_keys)
        return None

    def _pivot_fact_joins(
        self,
        fact_name: str,
        fj: dict,
        alias: str,
        source_table: str,
        join_on: str,
        measures: list[dict],
    ) -> list[dict] | None:
        """Pivoted narrow/vertical KBI join(s) for a fact.

        Returns ``None`` when the fact is not a pivot join (or has no KBI codes
        for the measures, so it falls through to the plain join), ``[]`` when it
        must be skipped, and the one join to add otherwise.
        """
        pivot_col = fj.get("pivot_col")
        if not pivot_col:
            return None
        value_col = fj.get("value_col", "val")
        _col_map = fj.get("column_map", {})
        phys_value_col = _col_map.get(value_col, value_col)
        grain_cols = fj.get("grain", [])
        if not _safe_identifiers(pivot_col, phys_value_col, *grain_cols):
            logger.warning("[JOIN] unsafe pivot identifiers for %s", fact_name)
            return []
        kbi_codes = _pivot_kbi_codes(fact_name, pivot_col, measures, fj)
        if not kbi_codes:
            return None

        pivot_kbi_map = {code: f"sc_{code.lower()}" for code in kbi_codes}
        implicit = [
            f.format(alias="").lstrip(".") for f in fj.get("implicit_filters", [])
        ]
        where_clause = ("WHERE " + " AND ".join(implicit)) if implicit else ""
        pivot_select = ",\n    ".join(
            f"SUM(CASE WHEN {pivot_col} = '{code}' THEN {phys_value_col} END)"
            f" AS sc_{code.lower()}"
            for code in sorted(kbi_codes)
        )
        grain_str = ", ".join(grain_cols)

        if fj.get("union_mode"):
            key_expr = fj.get("union_key_expr", "")
            if not key_expr:
                logger.warning(
                    "[JOIN] union_key_expr not configured for %s — skipping union join",
                    fact_name,
                )
                return []
            null_pivot_cols = ", ".join(
                f"CAST(NULL AS DOUBLE) AS sc_{code.lower()}"
                for code in sorted(kbi_codes)
            )
            union_arm_sql = (
                f"SELECT {key_expr}, {grain_str},\n    {pivot_select}\n"  # identifiers/codes validated; key_expr is pipeline-config SQL — nosemgrep: tmp.databricks.sql-injection.fstring-select
                f"FROM {source_table}\n"
                f"{where_clause}\n"
                f"GROUP BY {grain_str}"
            )
            return [
                {
                    "name": alias,
                    "_union_mode": True,
                    "_union_arm_sql": union_arm_sql,
                    "_null_pivot_cols": null_pivot_cols,
                    "_primary_exclude_filter": fj.get("primary_exclude_filter", ""),
                    "_pivot_kbi_map": pivot_kbi_map,
                    "_fact_join_config": fj,
                    "_pbi_name": fact_name,
                }
            ]

        join_source = (
            f"SELECT {grain_str},\n    {pivot_select}\n"  # identifiers and KBI codes validated above — nosemgrep: tmp.databricks.sql-injection.fstring-select
            f"FROM {source_table}\n"
            f"{where_clause}\n"
            f"GROUP BY {grain_str}"
        )
        return [
            {
                "name": alias,
                "source": join_source,
                "join_on": join_on,
                "_fact_join_config": fj,
                "_pbi_name": fact_name,
                "_pivot_kbi_map": pivot_kbi_map,
            }
        ]

    @staticmethod
    def _qualify_static_filters(
        fj: dict, alias: str, join_on: str, fact_table_info: TableInfo | None
    ) -> str:
        """Append the fact's static source filters to the ON clause, alias-qualified."""
        if (
            fj.get("inline_source")
            or not fact_table_info
            or not fact_table_info.static_filters
        ):
            return join_on
        qualified = []
        for f in fact_table_info.static_filters:
            f = f.replace("source.", f"{alias}.")
            f = re.sub(
                r"(?<![.\w])(\w+)\s*(=|<>|!=|IN\b|NOT\s+IN\b|LIKE\b|<|>|<=|>=)",
                rf"{alias}.\1 \2",
                f,
            )
            qualified.append(f)
        return join_on + " AND " + " AND ".join(qualified)

    @staticmethod
    def _dedup_joins(joins: list[dict]) -> list[dict]:
        """Drop duplicate joins (same alias, or same source + ON clause).

        S7: keeps the first occurrence so a fact is never joined to the same table
        twice (which would fan out rows or raise a duplicate-alias error). Special
        join modes without a plain ``source``/``join_on`` (pivot/union/embed) are
        keyed on their alias alone."""
        seen: set[tuple] = set()
        out: list[dict] = []
        for j in joins:
            key = (j.get("name"), j.get("source", ""), j.get("join_on", ""))
            if key in seen or (j.get("name"),) in seen:
                logger.info("Dropping duplicate join '%s' (S7 dedup)", j.get("name"))
                continue
            seen.add(key)
            seen.add((j.get("name"),))
            out.append(j)
        return out

    def get_dim_dimensions(self, joins: list[dict], fact_info: TableInfo) -> list[dict]:
        """Get extra dimensions from joined dimension tables."""
        dims = []
        for j in joins:
            alias = j["name"]
            for dim_name, jk in self._join_key_map.items():
                if jk["alias"] == alias:
                    for col_spec in jk["dim_columns"]:
                        if isinstance(col_spec, dict):
                            dim_name_str = col_spec["name"]
                            dim_expr = f"{alias}.{col_spec['expr']}"
                        else:
                            dim_name_str = col_spec
                            dim_expr = f"{alias}.{col_spec}"
                        dims.append(
                            {
                                "name": dim_name_str,
                                "expr": dim_expr,
                                "comment": f"{col_to_readable(dim_name_str)} from {dim_name}",
                            }
                        )
                    break
        return self._fold_calendar_period_dims(dims)

    def _fold_calendar_period_dims(self, dims: list[dict]) -> list[dict]:
        """Fold the report's period logic into this view's dimensions.

        If a joined dimension carries the report's closed-period columns
        (``Past_flag`` / ``Latest_Month_Label`` / ``Latest_Year_Label``), replace the
        generic per-column dims with canonical, richly-documented period dimensions
        (``past_flag``, ``latest_month_label``, ``latest_year_label``, ``fiscper``,
        ``fiscal_year``, ``fiscal_month``). This runs for EVERY view of the report, so
        each one answers "this month" / "this year" as a plain filter — the period
        logic lives in the view, not in a shared cross-report calendar object.

        A no-op unless a joined dimension actually exposes closed-period logic, so
        plain date dimensions are untouched."""
        # Group emitted dims by join alias, recording each dim's source column
        # (the part after "alias.").
        by_alias: dict = {}
        for d in dims:
            expr = d.get("expr", "")
            if "." in expr:
                alias, col = expr.split(".", 1)
                by_alias.setdefault(alias, []).append(col)

        cfg = self._period_config
        cal_alias = next(
            (a for a, cols in by_alias.items() if has_closed_period_logic(cols, cfg)),
            None,
        )
        if cal_alias is None:
            return dims

        cal_cols = by_alias[cal_alias]
        period_src = set(detect_period_columns(cal_cols, cfg).values())
        # Drop the generic dims for the calendar's period columns; keep everything
        # else (including the calendar's non-period columns).
        kept = [
            d
            for d in dims
            if not (
                d.get("expr", "").split(".", 1)[0] == cal_alias
                and d.get("expr", "").split(".", 1)[-1] in period_src
            )
        ]
        kept_names = {d.get("name", "").lower() for d in kept}
        folded = [
            f
            for f in fold_period_dimensions(cal_alias, cal_cols, config=cfg)
            if str(f["name"]).lower() not in kept_names
        ]
        return kept + folded
