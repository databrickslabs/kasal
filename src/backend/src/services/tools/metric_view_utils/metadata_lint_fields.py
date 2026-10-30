"""Normalized field view for the metadata-lint rule passes.

Turns an emitted metric-view dict entry, or any object exposing the
``TranslationResult`` / dimension attributes, into one :class:`_Field` shape so
the rules in ``metadata_lint`` never branch on the input form.
"""

from __future__ import annotations

from dataclasses import dataclass

# ─── Internal normalized field ───────────────────────────────────────────────


@dataclass
class _Field:
    """Normalized view of a measure or dimension for the rule passes."""

    name: str
    expr: str
    display_name: str
    synonyms: list[str]
    comment: str
    kind: str  # "measure" | "dimension"


def _as_list(value: object) -> list[str]:
    """Coerce a synonyms value into a clean list of non-empty strings."""
    if not value:
        return []
    if isinstance(value, (list, tuple)):
        return [str(v) for v in value if v is not None and str(v).strip()]
    # A single scalar synonym.
    return [str(value)] if str(value).strip() else []


def _field_from_dict(entry: dict[str, object], kind: str) -> _Field:
    """Build a :class:`_Field` from an emitted-YAML dict entry."""
    return _Field(
        name=str(entry.get("name") or "").strip(),
        expr=str(entry.get("expr") or "").strip(),
        display_name=str(entry.get("display_name") or "").strip(),
        synonyms=_as_list(entry.get("synonyms")),
        comment=str(entry.get("comment") or "").strip(),
        kind=kind,
    )


def _field_from_object(obj: object, kind: str) -> _Field:
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


def _coerce_field(entry: object, kind: str) -> _Field:
    """Normalize one dimension/measure entry regardless of dict-vs-object."""
    if isinstance(entry, dict):
        return _field_from_dict(entry, kind)
    return _field_from_object(entry, kind)


def _extract_fields(spec: object) -> tuple[list[_Field], list[_Field]]:
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
