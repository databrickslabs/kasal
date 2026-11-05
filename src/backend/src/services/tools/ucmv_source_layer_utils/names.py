"""Identifier quoting, snake_case names, SQL literals and UTC timestamps."""

from __future__ import annotations

import math
import re
from datetime import datetime, timezone

from src.services.tools.metric_view_utils.dax_condition_sql import _sql_str_literal

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_UC_PART = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_-]*$")
_NON_ALNUM = re.compile(r"[^0-9a-z]+")


def snake_case(name: str) -> str:
    """'Calendar Day' -> 'calendar_day' (the name a spaced PBI name gets in UC)."""
    return _NON_ALNUM.sub("_", name.lower()).strip("_")


def is_identifier(name: str) -> bool:
    return bool(_IDENTIFIER.match(name or ""))


def is_uc_fqn(name: str) -> bool:
    """``catalog.schema.object`` with optional back-quotes around each part."""
    parts = [p.strip("`") for p in (name or "").strip().split(".")]
    return len(parts) == 3 and all(_UC_PART.match(p) for p in parts)


def quote_part(part: str) -> str:
    return "`" + part.replace("`", "") + "`"


def quote_fqn(name: str) -> str:
    """``cat.sch.obj`` -> ``\\`cat\\`.\\`sch\\`.\\`obj\\``` (each part quoted)."""
    parts = [p.strip("`") for p in name.strip().split(".") if p.strip("`")]
    return ".".join(quote_part(p) for p in parts)


_FRACTION = re.compile(r"\.(\d+)")


def parse_utc(text: str) -> datetime:
    """An ISO-8601 time from Power BI or Databricks, as an aware UTC datetime.

    Accepts a trailing ``Z``, fractions of any length (cut to microseconds)
    and a missing offset (read as UTC).
    """
    value = text.strip().replace("Z", "+00:00")
    value = _FRACTION.sub(lambda m: "." + m.group(1)[:6].ljust(6, "0"), value, count=1)
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_stamp(moment: datetime) -> str:
    """``YYYY-MM-DD HH:MM:SS`` in UTC, the form the snapshot SQL uses."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def sql_literal(value: object) -> str:
    """A Python value as a Databricks SQL literal (strings escaped)."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return repr(value)
    if isinstance(value, float):
        return repr(value) if math.isfinite(value) else "NULL"
    return _sql_str_literal(value)
