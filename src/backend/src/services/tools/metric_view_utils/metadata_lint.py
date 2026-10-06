"""Metadata Lint — enforce naming/metadata rules on a generated UC Metric View.

Pure, synchronous inspection pass. Given a generated metric-view spec (its
dimensions and measures, each carrying ``name`` / ``expr`` / ``display_name`` /
``synonyms`` / ``comment``) it returns a flat list of :class:`LintFinding`
violations against the customer's validated PBI-mirroring playbook — the naming
and metadata failures Genie actually hit (see
``PBI_MIRRORING_PLAYBOOK.md`` § "Naming convention for metric views").

No I/O, no DB, no LLM: this is a lint util, not a service, so it stays sync and
side-effect-free. Wire it into the pipeline elsewhere — this module only decides
what is wrong, never what to do about it.

Input contract
--------------
``lint_metric_view`` accepts EITHER:

* a ``dict`` shaped like the emitted UC-MV YAML (parsed) — the richest input,
  because ``display_name`` / ``synonyms`` / ``comment`` only exist post-emit::

      {
          "dimensions": [
              {"name": "region", "expr": "dim_region.region",
               "display_name": "Region", "synonyms": ["area", "territory"],
               "comment": "Region of the company ..."},
              ...
          ],
          "measures": [
              {"name": "total_sle_actual", "expr": "SUM(...)/SUM(...)",
               "display_name": "Total SLE (Actual)", "synonyms": [...],
               "comment": "...", "format": {"type": "percentage"}},
              ...
          ],
      }

* OR any object exposing ``.dimensions`` and ``.measures`` (e.g.
  :class:`data_classes.MetricViewSpec`). Dimension entries are read as dicts;
  measure entries may be dicts OR :class:`data_classes.TranslationResult`
  objects (``measure_name`` / ``sql_expr`` / ``skip_reason`` are mapped onto
  ``name`` / ``expr`` / ``comment``). Note a bare ``MetricViewSpec`` carries no
  ``display_name`` / ``synonyms`` on its measures, so the synonym/display rules
  simply find nothing to flag on that shape — feed the emitted-YAML dict when
  those rules matter.

Rules implemented (rule_id — severity)
--------------------------------------
* ``MV001_SNAKE_CASE``            — error   : name is not snake_case
* ``MV002_KPI_SCENARIO_SUFFIX``   — warning : KPI-looking measure lacks a scenario suffix
* ``MV003_ABSOLUTE_UNIT_SUFFIX``  — warning : absolute building block lacks a unit suffix
* ``MV004_DUPLICATE_DIMENSION``   — warning : two dimensions are the same concept
* ``MV005_SYNONYM_CLASH``         — error   : one synonym string on two different fields
* ``MV006_SYNONYM_LIMIT``         — error   : more than 10 synonyms on a field
* ``MV007_MISSING_DISPLAY_NAME``  — warning : field has no display_name
* ``MV008_WEAK_DESCRIPTION``      — warning : measure comment empty or trivially short
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from dataclasses import field as dataclass_field

# ─── Severities ──────────────────────────────────────────────────────────────

SEVERITY_ERROR = "error"
SEVERITY_WARNING = "warning"

# ─── Rule ids ────────────────────────────────────────────────────────────────

RULE_SNAKE_CASE = "MV001_SNAKE_CASE"
RULE_KPI_SCENARIO_SUFFIX = "MV002_KPI_SCENARIO_SUFFIX"
RULE_ABSOLUTE_UNIT_SUFFIX = "MV003_ABSOLUTE_UNIT_SUFFIX"
RULE_DUPLICATE_DIMENSION = "MV004_DUPLICATE_DIMENSION"
RULE_SYNONYM_CLASH = "MV005_SYNONYM_CLASH"
RULE_SYNONYM_LIMIT = "MV006_SYNONYM_LIMIT"
RULE_MISSING_DISPLAY_NAME = "MV007_MISSING_DISPLAY_NAME"
RULE_WEAK_DESCRIPTION = "MV008_WEAK_DESCRIPTION"

# Unity Catalog hard limit on synonyms per field.
MAX_SYNONYMS_PER_FIELD = 10

# A comment shorter than this (after stripping) is treated as no real
# description. Deliberately generous — rule 8 is a soft nudge, not a content
# judge (the playbook wants default/unit/sign stated, but we do not parse for
# that; an empty or one-word comment is the only thing we flag).
_MIN_DESCRIPTION_LEN = 15

# Scenario suffix an explicit KPI measure should end with, optionally followed
# by a per-unit / percentage qualifier (playbook:
# ``<kpi>_<actual|bp|re|py>[_per_uc|_pct_nsr|_pct]``).
#
# These are the CCH/Total-SC vocabulary and are DEFAULTS ONLY — a different model
# uses different scenario words (budget/forecast/ly/plan). They are overridable
# per run via LintConfig (crew input), so the rule stays general.
_DEFAULT_SCENARIO_SUFFIXES = ("actual", "bp", "re", "py")
_DEFAULT_SCENARIO_QUALIFIERS = ("per_uc", "pct_nsr", "pct")

# Strict snake_case: lowercase alnum words joined by single underscores, no
# leading/trailing/double underscore, no uppercase, spaces or special chars.
_SNAKE_CASE_RE = re.compile(r"^[a-z0-9]+(?:_[a-z0-9]+)*$")

# Unit tokens a building-block (absolute) measure should carry in its name so it
# is never mistaken for the bare KPI (playbook: Genie picked ``epl`` (hours)
# over the ratio ``epl_actual`` because the hours column was not named
# ``epl_hours``). Matched as a trailing ``_<unit>`` on the name.
#
# DEFAULTS ONLY — units are domain-specific (hl, pallets, fte, …); overridable
# per run via LintConfig (crew input).
_DEFAULT_UNIT_SUFFIXES = (
    "hours",
    "hrs",
    "hour",
    "uc",
    "phc",
    "count",
    "cnt",
    "qty",
    "quantity",
    "amount",
    "amt",
    "days",
    "day",
    "units",
    "cases",
    "kg",
    "tonnes",
    "tons",
    "liters",
    "litres",
    "min",
    "mins",
    "sec",
    "secs",
    "eur",
    "usd",
    "value",
    "val",
    "pct",
    "ratio",
    "rate",
    "share",
    "percent",
    "index",
    "score",
    "flag",
)

# Known dimension alias groups: each set holds names that mean the SAME concept.
# MV004's PRIMARY detection is structural (two dimensions sharing a source
# expression), which needs no vocabulary. These alias groups are a DEFAULT
# supplement for same-concept-different-column cases (``company_code`` vs
# ``comp_code``); they are the CCH/SAP vocabulary and overridable per run.
_DEFAULT_DIMENSION_ALIAS_GROUPS: tuple[frozenset[str], ...] = (
    frozenset({"company_code", "comp_code", "company", "company_id", "cocd"}),
    frozenset({"fiscal_period", "fiscper", "fiscal_year_period", "period"}),
    frozenset({"fiscal_year", "fisc_year", "fiscyear", "fyear"}),
    frozenset({"fiscal_month", "fisc_month", "fiscmonth"}),
    frozenset({"plant_code", "plant", "plant_id", "werks"}),
    frozenset(
        {
            "work_center",
            "workcenter",
            "wkctr",
            "work_center_code",
            "workcenter_code",
        }
    ),
    frozenset({"material", "material_code", "matnr", "material_id"}),
    frozenset({"customer", "customer_code", "customer_id", "kunnr"}),
    frozenset({"currency", "currency_code", "currency_type", "waers"}),
    frozenset({"country", "country_code", "country_id", "land1"}),
)


def _build_scenario_re(suffixes, qualifiers) -> "re.Pattern":
    """Compile the scenario-suffix regex from token lists (empty → matches none)."""
    if not suffixes:
        return re.compile(r"(?!)")  # never matches
    quals = "|".join(re.escape(q) for q in qualifiers) if qualifiers else ""
    tail = rf"(?:_(?:{quals}))?" if quals else ""
    body = "|".join(re.escape(s) for s in suffixes)
    return re.compile(rf"_(?:{body}){tail}$", re.IGNORECASE)


def _build_unit_re(units) -> "re.Pattern":
    """Compile the unit-suffix regex from a token list (empty → matches none)."""
    if not units:
        return re.compile(r"(?!)")
    body = "|".join(re.escape(u) for u in units)
    return re.compile(rf"_(?:{body})$", re.IGNORECASE)


@dataclass
class LintConfig:
    """Per-run, model-specific lint vocabulary. Every field DEFAULTS to the
    CCH/Total-SC values, so an unconfigured run behaves exactly as before; a crew
    supplies its own model's words via :meth:`from_dict`.

    Only genuinely model-specific knobs live here — the structural rules (MV001
    snake_case, MV004 shared-expr, MV005 synonym clash, MV006 UC synonym limit,
    MV007 display_name) need no vocabulary and are not configurable."""

    scenario_suffixes: tuple = _DEFAULT_SCENARIO_SUFFIXES
    scenario_qualifiers: tuple = _DEFAULT_SCENARIO_QUALIFIERS
    unit_suffixes: tuple = _DEFAULT_UNIT_SUFFIXES
    dimension_alias_groups: tuple = _DEFAULT_DIMENSION_ALIAS_GROUPS
    min_description_len: int = _MIN_DESCRIPTION_LEN

    def __post_init__(self):
        self._scenario_re = _build_scenario_re(
            self.scenario_suffixes, self.scenario_qualifiers
        )
        self._unit_re = _build_unit_re(self.unit_suffixes)

    @property
    def scenario_re(self) -> "re.Pattern":
        return self._scenario_re

    @property
    def unit_re(self) -> "re.Pattern":
        return self._unit_re

    @classmethod
    def from_dict(cls, data) -> "LintConfig":
        """Build from a crew-supplied dict, ignoring junk and falling back to
        defaults for any missing/empty key. Keys (all optional):
        ``scenario_suffixes``, ``scenario_qualifiers``, ``unit_suffixes`` (lists
        of strings), ``dimension_alias_groups`` (list of lists of strings),
        ``min_description_len`` (int)."""
        if not isinstance(data, dict) or not data:
            return cls()

        def _tokens(key, default):
            v = data.get(key)
            if isinstance(v, (list, tuple)):
                toks = tuple(str(t).strip().lower() for t in v if str(t).strip())
                return toks or default
            return default

        groups = data.get("dimension_alias_groups")
        if isinstance(groups, (list, tuple)) and groups:
            parsed = tuple(
                frozenset(str(m).strip().lower() for m in g if str(m).strip())
                for g in groups
                if isinstance(g, (list, tuple)) and len(g) > 1
            )
            alias_groups = parsed or _DEFAULT_DIMENSION_ALIAS_GROUPS
        else:
            alias_groups = _DEFAULT_DIMENSION_ALIAS_GROUPS

        min_len = data.get("min_description_len")
        if not isinstance(min_len, int) or min_len < 0:
            min_len = _MIN_DESCRIPTION_LEN

        return cls(
            scenario_suffixes=_tokens("scenario_suffixes", _DEFAULT_SCENARIO_SUFFIXES),
            scenario_qualifiers=_tokens(
                "scenario_qualifiers", _DEFAULT_SCENARIO_QUALIFIERS
            ),
            unit_suffixes=_tokens("unit_suffixes", _DEFAULT_UNIT_SUFFIXES),
            dimension_alias_groups=alias_groups,
            min_description_len=min_len,
        )


# ─── Finding ─────────────────────────────────────────────────────────────────


@dataclass
class LintFinding:
    """A single metadata-lint violation.

    Attributes
    ----------
    rule_id : str
        Stable id of the rule that fired (one of the ``RULE_*`` constants).
    severity : str
        ``"error"`` or ``"warning"``.
    field : str
        Name of the offending measure/dimension (``""`` for view-wide findings).
    message : str
        Human-readable explanation, actionable on its own.
    """

    rule_id: str
    severity: str
    field: str
    message: str


# ─── Internal normalized field ───────────────────────────────────────────────


@dataclass
class _Field:
    """Normalized view of a measure or dimension for the rule passes."""

    name: str
    expr: str
    display_name: str
    synonyms: list
    comment: str
    kind: str  # "measure" | "dimension"


def _as_list(value) -> list:
    """Coerce a synonyms value into a clean list of non-empty strings."""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v is not None and str(v).strip()]
    # A single scalar synonym.
    return [str(value)] if str(value).strip() else []


def _field_from_dict(entry: dict, kind: str) -> _Field:
    """Build a :class:`_Field` from an emitted-YAML dict entry."""
    return _Field(
        name=str(entry.get("name") or "").strip(),
        expr=str(entry.get("expr") or "").strip(),
        display_name=str(entry.get("display_name") or "").strip(),
        synonyms=_as_list(entry.get("synonyms")),
        comment=str(entry.get("comment") or "").strip(),
        kind=kind,
    )


def _field_from_object(obj, kind: str) -> _Field:
    """Build a :class:`_Field` from an object (e.g. ``TranslationResult``).

    Maps the engine dataclass field names onto the lint contract:
    ``measure_name`` -> name, ``sql_expr`` -> expr, ``skip_reason`` -> comment
    (only when no explicit ``comment`` attribute is present).
    """
    name = getattr(obj, "measure_name", None) or getattr(obj, "name", None) or ""
    expr = getattr(obj, "sql_expr", None) or getattr(obj, "expr", None) or ""
    comment = getattr(obj, "comment", None) or getattr(obj, "skip_reason", None) or ""
    return _Field(
        name=str(name).strip(),
        expr=str(expr or "").strip(),
        display_name=str(getattr(obj, "display_name", "") or "").strip(),
        synonyms=_as_list(getattr(obj, "synonyms", None)),
        comment=str(comment).strip(),
        kind=kind,
    )


def _coerce_field(entry, kind: str) -> _Field:
    """Normalize one dimension/measure entry regardless of dict-vs-object."""
    if isinstance(entry, dict):
        return _field_from_dict(entry, kind)
    return _field_from_object(entry, kind)


def _extract_fields(spec) -> tuple[list[_Field], list[_Field]]:
    """Return ``(dimensions, measures)`` as normalized :class:`_Field` lists.

    Accepts a dict (emitted-YAML shape) or any object exposing
    ``.dimensions`` / ``.measures``.
    """
    if isinstance(spec, dict):
        raw_dims = spec.get("dimensions") or []
        raw_meas = spec.get("measures") or []
    else:
        raw_dims = getattr(spec, "dimensions", None) or []
        raw_meas = getattr(spec, "measures", None) or []
    dimensions = [_coerce_field(d, "dimension") for d in raw_dims]
    measures = [_coerce_field(m, "measure") for m in raw_meas]
    return dimensions, measures


# ─── Expression analysis helpers ─────────────────────────────────────────────


def _strip_string_literals(expr: str) -> str:
    """Remove single/double-quoted string literals so operator checks are safe.

    Prevents a ``/`` or other operator inside a literal (e.g. ``'JUICE-BRICK'``,
    a path) from being read as a division/operator.
    """
    without_single = re.sub(r"'[^']*'", " ", expr)
    return re.sub(r'"[^"]*"', " ", without_single)


_AGG_RE = re.compile(
    r"\b(?:SUM|COUNT|COUNT_IF|COUNTIF|AVG|MIN|MAX|SUM0)\s*\(",
    re.IGNORECASE,
)


def _has_division(expr: str) -> bool:
    """True if the expression divides (a ratio), ignoring string literals."""
    return "/" in _strip_string_literals(expr)


def _is_aggregate(expr: str) -> bool:
    """True if the expression is an aggregate (SUM/COUNT/AVG/...)."""
    return bool(_AGG_RE.search(expr))


def _looks_like_kpi(f: _Field, has_percentage_format: bool) -> bool:
    """Heuristic: a KPI/ratio measure.

    A ratio (division present) or an explicitly percentage-formatted measure
    behaves as a KPI that should carry a scenario suffix. Kept deliberately
    narrow so plain summed building blocks are NOT dragged in.
    """
    return _has_division(f.expr) or has_percentage_format


def _is_absolute_building_block(f: _Field) -> bool:
    """Heuristic: an ABSOLUTE building block — a summed/counted raw column.

    Aggregate present, no division. These carry a unit in the name; a bare name
    here is the ``epl``-over-``epl_actual`` failure.
    """
    return _is_aggregate(f.expr) and not _has_division(f.expr)


def _has_scenario_suffix(name: str, scenario_re: "re.Pattern") -> bool:
    return bool(scenario_re.search(name))


def _has_unit_suffix(name: str, unit_re: "re.Pattern") -> bool:
    return bool(unit_re.search(name))


def _has_percentage_format(entry) -> bool:
    """True if the raw entry declares a percentage format block."""
    if isinstance(entry, dict):
        fmt = entry.get("format") or {}
    else:
        fmt = getattr(entry, "format", None) or {}
    if isinstance(fmt, dict):
        return str(fmt.get("type", "")).lower() == "percentage"
    return False


def _suggest_snake_case(name: str) -> str:
    """Best-effort snake_case suggestion for a non-conforming name."""
    suggestion = name.strip()
    suggestion = suggestion.replace("%", "pct").replace("+", "plus")
    suggestion = suggestion.replace("&", "and")
    # Replace any remaining run of non-alnum with a single underscore.
    suggestion = re.sub(r"[^0-9A-Za-z]+", "_", suggestion)
    suggestion = suggestion.strip("_").lower()
    return re.sub(r"_+", "_", suggestion)


def _canonical_dim_group(name: str, alias_groups) -> frozenset[str] | None:
    """Return the alias group a dimension name belongs to, or None."""
    low = name.lower()
    for group in alias_groups:
        if low in group:
            return group
    return None


def _normalize_expr(expr: str) -> str:
    """Normalise a dimension expression for structural equality (whitespace-
    and case-insensitive), so two dims pointing at the same source column
    compare equal regardless of formatting."""
    return re.sub(r"\s+", "", expr or "").lower()


# ─── Individual rule passes ──────────────────────────────────────────────────


def _check_snake_case(fields: list[_Field]) -> list[LintFinding]:
    """MV001: every measure/dimension name must be snake_case."""
    findings: list[LintFinding] = []
    for f in fields:
        if not f.name:
            findings.append(
                LintFinding(
                    RULE_SNAKE_CASE,
                    SEVERITY_ERROR,
                    "",
                    f"A {f.kind} has an empty name.",
                )
            )
            continue
        if not _SNAKE_CASE_RE.match(f.name):
            suggestion = _suggest_snake_case(f.name)
            hint = f" (suggest '{suggestion}')" if suggestion else ""
            findings.append(
                LintFinding(
                    RULE_SNAKE_CASE,
                    SEVERITY_ERROR,
                    f.name,
                    f"{f.kind.capitalize()} name '{f.name}' is not snake_case; "
                    f"use lowercase words joined by single underscores, no "
                    f"spaces or special chars ('%'->'pct', '+'->'plus'){hint}.",
                )
            )
    return findings


def _check_kpi_scenario_suffix(
    measures: list[_Field], pct_format_names: set[str], scenario_re: "re.Pattern"
) -> list[LintFinding]:
    """MV002: a KPI-looking measure should end with a scenario suffix."""
    findings: list[LintFinding] = []
    for f in measures:
        if not f.name:
            continue
        is_kpi = _looks_like_kpi(f, f.name in pct_format_names)
        if is_kpi and not _has_scenario_suffix(f.name, scenario_re):
            findings.append(
                LintFinding(
                    RULE_KPI_SCENARIO_SUFFIX,
                    SEVERITY_WARNING,
                    f.name,
                    f"Measure '{f.name}' looks like a KPI (ratio/percentage) "
                    f"but carries no explicit scenario suffix "
                    f"(_actual|_bp|_re|_py, optionally +_per_uc|_pct_nsr|_pct). "
                    f"Genie guesses scenarios by analogy when they are implicit.",
                )
            )
    return findings


def _check_absolute_unit_suffix(
    measures: list[_Field], unit_re: "re.Pattern", scenario_re: "re.Pattern"
) -> list[LintFinding]:
    """MV003: an absolute building block must carry its unit in the name."""
    findings: list[LintFinding] = []
    for f in measures:
        if not f.name:
            continue
        if not _is_absolute_building_block(f):
            continue
        # A scenario-suffixed name is a KPI variant, not a bare building block;
        # a unit-suffixed name already states its unit — both are fine.
        if _has_unit_suffix(f.name, unit_re) or _has_scenario_suffix(
            f.name, scenario_re
        ):
            continue
        findings.append(
            LintFinding(
                RULE_ABSOLUTE_UNIT_SUFFIX,
                SEVERITY_WARNING,
                f.name,
                f"Measure '{f.name}' is an absolute building block (summed raw "
                f"column, not a ratio) but its name carries no unit "
                f"(e.g. '{f.name}_hours'). A bare KPI-looking name lets Genie "
                f"pick the raw column over the ratio KPI.",
            )
        )
    return findings


def _check_duplicate_dimensions(
    dimensions: list[_Field], alias_groups
) -> list[LintFinding]:
    """MV004: two dimensions that are the same concept under different names.

    Primary detection is STRUCTURAL and vocabulary-free (two dimensions resolving
    to the same source expression); the alias-group and synonym-overlap passes
    catch same-concept-different-column cases the structural pass cannot see."""
    findings: list[LintFinding] = []
    reported: set[frozenset[str]] = set()

    # (a0) STRUCTURAL: two dimensions sharing the same source expression. No
    # vocabulary needed — this is the general, model-agnostic rule.
    expr_members: dict[str, list[str]] = {}
    for d in dimensions:
        if d.name and d.expr:
            expr_members.setdefault(_normalize_expr(d.expr), []).append(d.name)
    for members in expr_members.values():
        uniq = sorted(set(members))
        if len(uniq) > 1:
            pair_key = frozenset(uniq)
            if pair_key in reported:
                continue
            reported.add(pair_key)
            findings.append(
                LintFinding(
                    RULE_DUPLICATE_DIMENSION,
                    SEVERITY_WARNING,
                    ", ".join(uniq),
                    f"Dimensions {uniq} resolve to the same source expression; "
                    f"they are one concept — keep a single name per view.",
                )
            )

    # (a) known alias groups (configurable vocabulary; default = CCH/SAP names).
    group_members: dict[frozenset[str], list[str]] = {}
    for d in dimensions:
        group = _canonical_dim_group(d.name, alias_groups)
        if group is not None:
            group_members.setdefault(group, []).append(d.name)
    for group, members in group_members.items():
        if len(members) > 1:
            pair_key = frozenset(members)
            if pair_key in reported:
                continue
            reported.add(pair_key)
            findings.append(
                LintFinding(
                    RULE_DUPLICATE_DIMENSION,
                    SEVERITY_WARNING,
                    ", ".join(sorted(members)),
                    f"Dimensions {sorted(members)} are the same concept under "
                    f"different names; keep one name per concept per view.",
                )
            )

    # (b) one dimension's name appears in another's synonyms (same concept).
    names_lower = {d.name.lower(): d.name for d in dimensions if d.name}
    for d in dimensions:
        for syn in d.synonyms:
            other = names_lower.get(syn.strip().lower())
            if other and other != d.name:
                pair_key = frozenset({d.name, other})
                if pair_key in reported:
                    continue
                reported.add(pair_key)
                findings.append(
                    LintFinding(
                        RULE_DUPLICATE_DIMENSION,
                        SEVERITY_WARNING,
                        f"{d.name}, {other}",
                        f"Dimension '{other}' also appears as a synonym of "
                        f"'{d.name}'; these are the same concept — keep one.",
                    )
                )
    return findings


def _check_synonym_clash(fields: list[_Field]) -> list[LintFinding]:
    """MV005: the same synonym string must not appear on two fields."""
    findings: list[LintFinding] = []
    synonym_to_fields: dict[str, list[str]] = {}
    synonym_display: dict[str, str] = {}
    for f in fields:
        seen_here: set[str] = set()
        for syn in f.synonyms:
            key = syn.strip().lower()
            if not key or key in seen_here:
                continue
            seen_here.add(key)
            synonym_to_fields.setdefault(key, []).append(f.name)
            synonym_display.setdefault(key, syn.strip())
    for key, owners in synonym_to_fields.items():
        if len(owners) > 1:
            findings.append(
                LintFinding(
                    RULE_SYNONYM_CLASH,
                    SEVERITY_ERROR,
                    ", ".join(owners),
                    f"Synonym '{synonym_display[key]}' is attached to "
                    f"{len(owners)} fields ({owners}); a synonym must belong to "
                    f"exactly one field per view or Genie cannot disambiguate.",
                )
            )
    return findings


def _check_synonym_limit(fields: list[_Field]) -> list[LintFinding]:
    """MV006: at most 10 synonyms per field (Unity Catalog limit)."""
    findings: list[LintFinding] = []
    for f in fields:
        count = len(f.synonyms)
        if count > MAX_SYNONYMS_PER_FIELD:
            findings.append(
                LintFinding(
                    RULE_SYNONYM_LIMIT,
                    SEVERITY_ERROR,
                    f.name,
                    f"{f.kind.capitalize()} '{f.name}' has {count} synonyms; "
                    f"Unity Catalog allows at most {MAX_SYNONYMS_PER_FIELD}.",
                )
            )
    return findings


def _check_display_name(fields: list[_Field]) -> list[LintFinding]:
    """MV007: every measure/dimension should carry a display_name."""
    findings: list[LintFinding] = []
    for f in fields:
        if not f.display_name:
            findings.append(
                LintFinding(
                    RULE_MISSING_DISPLAY_NAME,
                    SEVERITY_WARNING,
                    f.name,
                    f"{f.kind.capitalize()} '{f.name}' has no display_name; "
                    f"carry the Power BI label as display_name.",
                )
            )
    return findings


def _check_description_quality(
    measures: list[_Field], min_len: int
) -> list[LintFinding]:
    """MV008: warn on empty/trivially short measure descriptions."""
    findings: list[LintFinding] = []
    for f in measures:
        comment = f.comment.strip()
        if not comment:
            findings.append(
                LintFinding(
                    RULE_WEAK_DESCRIPTION,
                    SEVERITY_WARNING,
                    f.name,
                    f"Measure '{f.name}' has no description; state the default, "
                    f"unit and sign so Genie and reviewers know how to read it.",
                )
            )
        elif len(comment) < min_len:
            findings.append(
                LintFinding(
                    RULE_WEAK_DESCRIPTION,
                    SEVERITY_WARNING,
                    f.name,
                    f"Measure '{f.name}' description is trivially short "
                    f"('{comment}'); state the default, unit and sign.",
                )
            )
    return findings


# ─── Entrypoint ──────────────────────────────────────────────────────────────


def lint_metric_view(
    spec_or_dict, config: "LintConfig | None" = None
) -> list[LintFinding]:
    """Lint a generated UC metric-view spec against the naming/metadata rules.

    Parameters
    ----------
    spec_or_dict
        A dict shaped like the emitted UC-MV YAML, or an object exposing
        ``.dimensions`` and ``.measures`` (e.g. ``MetricViewSpec``). See the
        module docstring for the exact contract.
    config
        Model-specific vocabulary (scenario words, unit list, alias groups,
        description length). Defaults to the CCH/Total-SC values, so an
        unconfigured call is unchanged; a crew passes its own via
        ``LintConfig.from_dict``.

    Returns
    -------
    list[LintFinding]
        Every violation found, in a stable order (rule-by-rule). Empty when the
        spec is clean. Never raises on a well-formed spec.
    """
    cfg = config or LintConfig()
    dimensions, measures = _extract_fields(spec_or_dict)
    all_fields = dimensions + measures

    # Which measures declared a percentage format (feeds the KPI heuristic).
    pct_format_names: set[str] = set()
    if isinstance(spec_or_dict, dict):
        raw_measures = spec_or_dict.get("measures") or []
    else:
        raw_measures = getattr(spec_or_dict, "measures", None) or []
    for raw, normalized in zip(raw_measures, measures):
        if normalized.name and _has_percentage_format(raw):
            pct_format_names.add(normalized.name)

    findings: list[LintFinding] = []
    findings.extend(_check_snake_case(all_fields))
    findings.extend(
        _check_kpi_scenario_suffix(measures, pct_format_names, cfg.scenario_re)
    )
    findings.extend(_check_absolute_unit_suffix(measures, cfg.unit_re, cfg.scenario_re))
    findings.extend(_check_duplicate_dimensions(dimensions, cfg.dimension_alias_groups))
    findings.extend(_check_synonym_clash(all_fields))
    findings.extend(_check_synonym_limit(all_fields))
    findings.extend(_check_display_name(all_fields))
    findings.extend(_check_description_quality(measures, cfg.min_description_len))
    return findings


# ─── Summary helper ──────────────────────────────────────────────────────────


@dataclass
class LintSummary:
    """Aggregate view of a lint run — for logging / gating decisions."""

    total: int = 0
    error_count: int = 0
    warning_count: int = 0
    by_rule: dict = dataclass_field(default_factory=dict)

    @property
    def has_errors(self) -> bool:
        return self.error_count > 0


def summarize_findings(findings: list[LintFinding]) -> LintSummary:
    """Roll findings up into counts by severity and by rule.

    Pure convenience: callers gate on ``summary.has_errors`` or log
    ``summary.by_rule`` without re-walking the list.
    """
    summary = LintSummary()
    summary.total = len(findings)
    for finding in findings:
        if finding.severity == SEVERITY_ERROR:
            summary.error_count += 1
        else:
            summary.warning_count += 1
        summary.by_rule[finding.rule_id] = summary.by_rule.get(finding.rule_id, 0) + 1
    return summary


def format_findings(findings: list[LintFinding]) -> str:
    """Render findings as a compact multi-line report (one per line).

    Sorted errors-first so the blocking problems lead. Returns a single 'clean'
    line when there is nothing to report.
    """
    if not findings:
        return "metadata-lint: no findings"
    order = {SEVERITY_ERROR: 0, SEVERITY_WARNING: 1}
    ordered = sorted(findings, key=lambda f: (order.get(f.severity, 2), f.rule_id))
    lines = [
        f"[{f.severity.upper()}] {f.rule_id} ({f.field or '-'}): {f.message}"
        for f in ordered
    ]
    return "\n".join(lines)
