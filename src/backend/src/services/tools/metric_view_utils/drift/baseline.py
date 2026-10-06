"""Read the DEPLOYED (verified) metric-view YAML back out of Unity Catalog.

The deployed definition is the baseline of a drift check — it is what a human
reviewed and deployed, so it is treated as read-only truth.

``DESCRIBE TABLE EXTENDED <view> AS JSON`` returns it in ``view_text``. Note that UC
stores a RE-SERIALISED copy of the YAML (keys like ``on`` come back quoted, long
lines re-wrapped, ``#`` comments gone) — so the baseline is UC's text, not the file
that was originally deployed. The patcher only ever inserts into this text, so every
line UC returned survives unchanged.

SQL execution is injected (``sql_fn(statement) -> {"success", "data", "error"}``) —
the shape ``MetricViewDeployerTool._execute_sql_sync`` returns — so this module
holds no auth or HTTP code and is unit-testable.
"""

import json
import re
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

import yaml

SqlFn = Callable[[str], dict]

# catalog.schema.view, each part a plain identifier or `back-quoted`.
_PART = r"(?:`[^`]+`|[A-Za-z_][\w\-]*)"
_FQN_RE = re.compile(rf"^\s*({_PART})\.({_PART})\.({_PART})\s*$")


@dataclass
class BaselineView:
    """One deployed metric view, as UC returns it."""

    full_name: str
    catalog: str
    schema: str
    name: str
    yaml_text: str = ""
    spec: dict = field(default_factory=dict)
    error: Optional[str] = None

    @property
    def ok(self) -> bool:
        return self.error is None and bool(self.spec)


def parse_view_names(raw: Any) -> list[str]:
    """Accept a JSON list, newline/comma-separated text, or a list of names.

    Databricks Catalog Explorer URLs (``…/explore/data/<cat>/<sch>/<view>``) are
    accepted too, since that is what users copy from the browser.
    """
    if not raw:
        return []
    items: list[str]
    if isinstance(raw, list):
        items = [str(x) for x in raw]
    else:
        text = str(raw).strip()
        items = []
        if text.startswith("["):
            try:
                parsed = json.loads(text)
                if isinstance(parsed, list):
                    items = [str(x) for x in parsed]
            except json.JSONDecodeError:
                items = []
        if not items:
            items = re.split(r"[\n,;]+", text)
    out: list[str] = []
    for item in items:
        name = _url_to_fqn(item.strip())
        if name and name not in out:
            out.append(name)
    return out


def _url_to_fqn(value: str) -> str:
    m = re.search(r"/explore/data/([^/?#]+)/([^/?#]+)/([^/?#]+)", value)
    if m:
        return ".".join(m.groups())
    return value


def split_full_name(full_name: str) -> Optional[tuple[str, str, str]]:
    """``catalog.schema.view`` → parts (back-quotes stripped), or None if malformed."""
    m = _FQN_RE.match(full_name or "")
    if not m:
        return None
    return tuple(p.strip("`") for p in m.groups())  # type: ignore[return-value]


def _quoted(parts: tuple[str, str, str]) -> str:
    return ".".join(f"`{p.replace('`', '``')}`" for p in parts)


def parse_describe_view_text(data: dict) -> Optional[str]:
    """Pull ``view_text`` (the metric-view YAML) out of a DESCRIBE … AS JSON result."""
    try:
        rows = (data or {}).get("result", {}).get("data_array") or []
        doc = json.loads(rows[0][0])
    except (IndexError, TypeError, ValueError, AttributeError):
        return None
    if not isinstance(doc, dict):
        return None
    if str(doc.get("type", "")).upper() not in ("METRIC_VIEW", ""):
        return None
    text = doc.get("view_text")
    return text if isinstance(text, str) and text.strip() else None


def parse_show_create(data: dict) -> Optional[str]:
    """Fallback: the YAML between ``$$ … $$`` in a SHOW CREATE TABLE result."""
    try:
        rows = (data or {}).get("result", {}).get("data_array") or []
        ddl = rows[0][0]
    except (IndexError, TypeError, AttributeError):
        return None
    m = re.search(r"\$\$\s*\n?(.*?)\n?\s*\$\$", ddl or "", re.DOTALL)
    return m.group(1) if m else None


def fetch_baseline(full_name: str, sql_fn: SqlFn) -> BaselineView:
    """Read one deployed metric view. Never raises — failures land in ``.error``."""
    parts = split_full_name(full_name)
    if not parts:
        return BaselineView(
            full_name=full_name,
            catalog="",
            schema="",
            name=full_name,
            error="not a catalog.schema.view name",
        )
    view = BaselineView(
        full_name=".".join(parts), catalog=parts[0], schema=parts[1], name=parts[2]
    )
    quoted = _quoted(parts)

    res = sql_fn(f"DESCRIBE TABLE EXTENDED {quoted} AS JSON")
    text = parse_describe_view_text(res.get("data", {})) if res.get("success") else None
    if text is None:
        alt = sql_fn(f"SHOW CREATE TABLE {quoted}")
        text = parse_show_create(alt.get("data", {})) if alt.get("success") else None
        if text is None:
            view.error = (
                res.get("error")
                or alt.get("error")
                or "view exists but is not a metric view (no YAML definition)"
            )
            return view

    view.yaml_text = text
    try:
        spec = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        view.error = f"deployed YAML does not parse: {exc}"
        return view
    if not isinstance(spec, dict):
        view.error = "deployed YAML is not a mapping"
        return view
    view.spec = spec
    return view
