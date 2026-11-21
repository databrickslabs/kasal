"""Shorthand row filters that expand to the shared access functions.

``{"value_access": {"field": "company_code", "column": "comp_code"}}`` ->
``<access_schema>.value_access('company_code', comp_code)``
``{"object_access": "grp_a;grp_b"}`` -> ``<access_schema>.object_access('grp_a;grp_b')``

The function names and argument order are the ones of the reference access
notebook, so a customer who repoints the functions at their own rules keeps
these configs unchanged. Explicit ``function`` / ``columns`` / ``constants``
entries stay valid and are not touched here.
"""

from __future__ import annotations

from collections.abc import Mapping

from src.services.tools.metric_view_utils.source_layer_emitter import RowFilter
from src.services.tools.ucmv_source_layer_utils.names import is_uc_schema

VALUE_ACCESS = "value_access"
OBJECT_ACCESS = "object_access"
SHORTHAND_KEYS = (VALUE_ACCESS, OBJECT_ACCESS)
_EXPLICIT_KEYS = ("function", "columns", "constants")


def is_shorthand(spec: Mapping[str, object]) -> bool:
    return any(key in spec for key in SHORTHAND_KEYS)


def shorthand_row_filter(
    spec: Mapping[str, object], access_schema: str | None
) -> RowFilter:
    """The RowFilter of a shorthand entry; raises ValueError with the fix to make."""
    kinds = [key for key in SHORTHAND_KEYS if key in spec]
    if len(kinds) != 1:
        raise ValueError("use exactly one of value_access or object_access")
    if any(key in spec for key in _EXPLICIT_KEYS):
        raise ValueError(
            "value_access / object_access cannot be combined with function, columns or constants"
        )
    if not access_schema or not is_uc_schema(access_schema):
        raise ValueError(
            "access_schema must be catalog.schema to expand value_access / object_access"
        )
    if kinds[0] == VALUE_ACCESS:
        return _value_access(spec[VALUE_ACCESS], access_schema)
    return _object_access(spec[OBJECT_ACCESS], access_schema)


def _value_access(value: object, access_schema: str) -> RowFilter:
    if not isinstance(value, dict):
        raise ValueError("value_access must be an object {field, column}")
    field = str(value.get("field") or "").strip()
    column = str(value.get("column") or "").strip()
    if not field or not column:
        raise ValueError("value_access needs both field and column")
    return RowFilter(
        function=f"{access_schema}.{VALUE_ACCESS}", columns=[column], constants=[field]
    )


def _object_access(groups: object, access_schema: str) -> RowFilter:
    if isinstance(groups, list):
        text = ";".join(str(g).strip() for g in groups if str(g).strip())
    elif isinstance(groups, str):
        text = groups.strip()
    else:
        raise ValueError(
            "object_access must be a ;-separated string or a list of groups"
        )
    return RowFilter(function=f"{access_schema}.{OBJECT_ACCESS}", constants=[text])
