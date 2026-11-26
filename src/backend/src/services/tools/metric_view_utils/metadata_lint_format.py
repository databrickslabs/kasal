"""Display-format lint rules for metric-view measures (MV009, MV010).

Kept apart from ``metadata_lint`` to hold that module under its size target.

* ``MV009_MISSING_FORMAT`` (warning): a measure has no ``format``. Genie then
  prints the raw float. For a dict entry (emitted YAML) the explicit ``format``
  is read. For an object entry (``TranslationResult``) the format the emitter
  would derive from the measure name and expression is used, so the rule is
  about what would actually be emitted.
* ``MV010_PERCENT_DOUBLE_SCALED`` (error): a percentage-formatted measure whose
  expression also multiplies by 100. ``percentage`` multiplies on display, so the
  value would show 100 times too large. Convert with ``/ 100`` instead.
"""

from __future__ import annotations

from .measure_format import derive_measure_format, expr_scales_by_100
from .metadata_lint_fields import SEVERITY_ERROR, SEVERITY_WARNING, LintFinding, _Field

RULE_MISSING_FORMAT = "MV009_MISSING_FORMAT"
RULE_PERCENT_DOUBLE_SCALED = "MV010_PERCENT_DOUBLE_SCALED"


def _explicit_format(raw: object) -> dict | None:
    """The format declared on the raw entry, or ``None`` if absent."""
    if isinstance(raw, dict):
        fmt = raw.get("format")
    else:
        fmt = getattr(raw, "format", None)
    return fmt if isinstance(fmt, dict) and fmt.get("type") else None


def _effective_format(field: _Field, raw: object) -> dict | None:
    """The format the measure carries, or would carry when emitted."""
    if isinstance(raw, dict):
        return _explicit_format(raw)
    return _explicit_format(raw) or derive_measure_format(field.name, field.expr)


def check_measure_formats(
    measures: list[tuple[_Field, object]],
) -> list[LintFinding]:
    """Run MV009 and MV010 over ``(normalized field, raw entry)`` pairs."""
    findings: list[LintFinding] = []
    for field, raw in measures:
        if not field.name:
            continue
        fmt = _effective_format(field, raw)
        if fmt is None:
            findings.append(
                LintFinding(
                    RULE_MISSING_FORMAT,
                    SEVERITY_WARNING,
                    field.name,
                    f"Measure '{field.name}' has no format; Genie prints the raw "
                    f"value (e.g. 0.636... instead of 63.6%). Take the format from "
                    f"the Power BI formatString: a percentage uses type percentage "
                    f"with 2 places, an amount or count uses type number.",
                )
            )
            continue
        if fmt.get("type") == "percentage" and expr_scales_by_100(field.expr):
            findings.append(
                LintFinding(
                    RULE_PERCENT_DOUBLE_SCALED,
                    SEVERITY_ERROR,
                    field.name,
                    f"Measure '{field.name}' is percentage-formatted but its "
                    f"expression multiplies by 100; percentage already scales for "
                    f"display, so the value would show 100 times too large. Return "
                    f"a fraction (divide by 100 on a 0-100 source) and keep the "
                    f"percentage format.",
                )
            )
    return findings
