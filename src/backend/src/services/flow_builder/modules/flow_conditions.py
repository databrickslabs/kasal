"""Path-aware state lookup for flow router conditions.

The router UI does not let anyone type an expression. ``ConditionBuilder``
emits exactly one addressing form — ``state.get("<field>", "")`` — so the only
place a fix can live is behind that call: in what *looking up a field* means.

Before this module the lookup was a flat ``dict.get`` against the top level of
flow state. Two shapes were therefore unreachable:

* a value nested inside an object — ``{"classification": {"category": ...}}``
* a value repeated across a list — 29 articles each carrying ``category``

Both are what a language model actually returns, so the condition read False and
the route silently never fired.

``ConditionState.get`` resolves in four ordered steps: the exact top-level key
(unchanged behaviour, so saved flows keep their answer), then an explicit path
(``classification.category``, ``articles[].category``), then a bounded search
for a uniquely-named leaf, then the caller's default. It never raises — a
router that cannot answer must not take a branch by accident.

Values gathered across a list become a :class:`MatchList`, which compares with
*any element matches* semantics. That is the piece that makes the batch case
answerable through the UI's ordinary operators, with the string it already
emits unchanged.
"""

from __future__ import annotations

import json
import logging
import re
from collections.abc import Mapping
from typing import Any, Dict, Final, Iterable, List, Tuple

logger = logging.getLogger(__name__)


class _Missing:
    """Distinct from ``None``, which is a legitimate resolved value."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Final = _Missing()

# Bookkeeping the flow engine writes into state. A user's field must never bind
# to one of these by accident during the leaf search.
_SKIP_KEYS: Final = frozenset(
    {
        "kasal_crew_identities",
        "previous_output",
        "messages",
        "id",
    }
)

# Bounds. Resolution is pure data traversal, but flow state holds whole crew
# outputs, so every walk is capped rather than trusted to terminate on shape.
_MAX_DEPTH: Final = 6
_MAX_NODES: Final = 4000
_MAX_SEGMENTS: Final = 32

# `[*]` is accepted alongside `[]`. Detection-rule guardrails spell the same
# idea `findings[*].source`, and someone who learned it there and typed
# `articles[*].category` here got a silently unresolved lookup — exactly the
# failure this module exists to remove.
_SEGMENT_RE: Final = re.compile(r"^([^\[\]]*)((?:\[\d*\*?\])*)$")
_BRACKET_RE: Final = re.compile(r"\[(\d*|\*)\]")


def _is_empty(value: Any) -> bool:
    """Whether a present value should still fall through to path resolution.

    A declared-but-unfilled field arrives as ``None`` or ``""`` — that is the
    observed shape when a crew's structured output failed to parse. Treating it
    as an answer is what made the top-level key win and the real value, sitting
    one level down, stay invisible.
    """
    return value is None or value == "" or value == [] or value == {}


def state_snapshot(state: Any) -> Dict[str, Any]:
    """A plain-dict view of flow state, defensively.

    Deliberately does NOT call ``state.items()``. ``merge_parsed_json`` writes
    ``state["items"] = [...]`` whenever a crew emits a top-level JSON array; on
    a typed state that is a ``setattr`` which shadows the ``items`` method, and
    the next call raises ``TypeError: 'list' object is not callable``. That
    error escaped the router and stopped the flow.

    Each source is tried independently so one failure cannot lose the rest.
    """
    out: Dict[str, Any] = {}

    if isinstance(state, dict):
        try:
            out.update(state)
        except Exception:  # noqa: BLE001 - a broken mapping yields nothing
            pass
        return out

    try:
        dumped = type(state).model_dump(state)  # type: ignore[attr-defined]
        if isinstance(dumped, dict):
            out.update(dumped)
    except Exception:  # noqa: BLE001 - not a pydantic model, or a bad field
        pass

    try:
        extra = getattr(state, "__pydantic_extra__", None)
        if isinstance(extra, dict):
            out.update(extra)
    except Exception:  # noqa: BLE001
        pass

    try:
        for key, value in vars(state).items():
            if not key.startswith("_"):
                out.setdefault(key, value)
    except Exception:  # noqa: BLE001 - no __dict__ (slots)
        pass

    return out


class MatchStr(str):
    """A string value from flow state, compared without regard to case.

    A router routes on a label the MODEL chose, and models are not consistent
    about capitalisation: the same classify step emitted ``politics`` one run
    and ``Politics`` the next. A condition the user assembled from a dropdown
    should not silently stop matching because of that — observed on a real run,
    where a correct classification of ``Politics`` failed ``== "politics"`` and
    the branch never ran.

    Deliberately narrow: only values RESOLVED FOR A CONDITION are wrapped, so
    nothing else in the flow sees different equality. ``__hash__`` folds too, so
    the hash/eq contract holds if one of these is ever used as a key.
    """

    __slots__ = ()

    def __eq__(self, other: Any) -> Any:
        if isinstance(other, str):
            return self.casefold() == other.casefold()
        return NotImplemented

    def __ne__(self, other: Any) -> Any:
        equal = self.__eq__(other)
        return equal if equal is NotImplemented else not equal

    def __hash__(self) -> int:
        return hash(self.casefold())

    def __contains__(self, needle: Any) -> bool:
        if isinstance(needle, str):
            return needle.casefold() in self.casefold()
        return False

    def startswith(self, prefix: Any, *args: Any) -> bool:
        if isinstance(prefix, str):
            return self.casefold().startswith(prefix.casefold())
        return super().startswith(prefix, *args)

    def endswith(self, suffix: Any, *args: Any) -> bool:
        if isinstance(suffix, str):
            return self.casefold().endswith(suffix.casefold())
        return super().endswith(suffix, *args)


def _fold(value: Any) -> Any:
    """Wrap strings so a condition compares them case-insensitively.

    Plain lists are left alone: only a PROJECTION (already a MatchList) carries
    any-element semantics, and quietly promoting an ordinary list value would
    change what an exact-key lookup means.
    """
    if isinstance(value, MatchList):
        return MatchList(_fold(element) for element in value)
    if type(value) is str:
        return MatchStr(value)
    return value


class MatchList(list):
    """Values gathered across a list, compared as *any element matches*.

    ``articles[].category`` over 29 articles yields the 29 category strings.
    ``== "politics"`` then means "at least one article is politics", which is
    the question a router is actually asking and the one the UI's ordinary
    operators can express.

    Every comparison is total. A naive ``any(...)`` over a mixed list raises
    ``TypeError`` on the first incomparable element; that propagates out of
    ``safe_eval`` and the router catches it and skips the route — reinstating
    the silent miss this module exists to remove. Elements that cannot be
    compared are skipped instead.
    """

    __hash__ = None  # type: ignore[assignment]

    def _any(self, op, other: Any) -> bool:
        for element in self:
            # A projection through two lists — orders[].lines[].sku — gathers a
            # list of lists. Recurse so "any" keeps meaning "any, at any level"
            # rather than comparing an inner list against a scalar.
            if isinstance(element, (list, tuple)):
                if MatchList(element)._any(op, other):
                    return True
                continue
            try:
                if op(element, other):
                    return True
            except TypeError:
                continue
        return False

    def __eq__(self, other: Any) -> bool:  # type: ignore[override]
        return self._any(lambda a, b: a == b, other)

    def __ne__(self, other: Any) -> bool:  # type: ignore[override]
        # MUST be explicit. A list subclass inherits list.__ne__, which compares
        # the list itself against the operand and is therefore always True for a
        # scalar — an always-firing route. "not equals" on a projection means
        # NO element matches.
        return not self.__eq__(other)

    def __gt__(self, other: Any) -> bool:  # type: ignore[override]
        return self._any(lambda a, b: a > b, other)

    def __ge__(self, other: Any) -> bool:  # type: ignore[override]
        return self._any(lambda a, b: a >= b, other)

    def __lt__(self, other: Any) -> bool:  # type: ignore[override]
        return self._any(lambda a, b: a < b, other)

    def __le__(self, other: Any) -> bool:  # type: ignore[override]
        return self._any(lambda a, b: a <= b, other)

    def __contains__(self, needle: Any) -> bool:
        # The UI's `contains` operator emits `value in field`. On a plain string
        # that is a substring test; on a projection the natural reading is "any
        # element is (or contains) this". Support both, at any depth.
        return self._any(
            lambda a, b: (
                a == b or (isinstance(a, str) and isinstance(b, str) and b in a)
            ),
            needle,
        )

    def startswith(self, prefix: Any) -> bool:
        return self._any(lambda a, b: isinstance(a, str) and a.startswith(b), prefix)

    def endswith(self, suffix: Any) -> bool:
        return self._any(lambda a, b: isinstance(a, str) and a.endswith(b), suffix)

    def _map(self, transform) -> "MatchList":
        return MatchList(
            (
                MatchList(e)._map(transform)
                if isinstance(e, (list, tuple))
                else (transform(e) if isinstance(e, str) else e)
            )
            for e in self
        )

    def lower(self) -> "MatchList":
        return self._map(str.lower)

    def upper(self) -> "MatchList":
        return self._map(str.upper)

    def strip(self) -> "MatchList":
        return self._map(str.strip)


def _child(container: Any, name: str) -> Any:
    """One step down. A list projects the step across its elements."""
    if isinstance(container, Mapping):
        return container.get(name, MISSING)

    if isinstance(container, (list, tuple)):
        gathered = MatchList()
        for element in container:
            value = _child(element, name)
            if value is not MISSING:
                gathered.append(value)
        return gathered if gathered else MISSING

    try:
        return getattr(container, name)
    except Exception:  # noqa: BLE001 - not addressable
        return MISSING


def resolve_path(root: Any, path: str) -> Any:
    """Resolve ``a.b``, ``a[].b`` or ``a[0].b`` against *root*.

    Returns :data:`MISSING` rather than raising when any step does not exist.
    """
    segments = path.split(".")
    if len(segments) > _MAX_SEGMENTS:
        return MISSING

    value: Any = root
    for segment in segments:
        matched = _SEGMENT_RE.match(segment)
        if not matched:
            return MISSING
        name, brackets = matched.group(1), matched.group(2)

        if name:
            value = _child(value, name)
            if value is MISSING:
                return MISSING

        for index_text in _BRACKET_RE.findall(brackets):
            if index_text in ("", "*"):
                if isinstance(value, (list, tuple)):
                    value = MatchList(value)
                else:
                    return MISSING
            else:
                try:
                    value = value[int(index_text)]
                except Exception:  # noqa: BLE001 - out of range / not indexable
                    return MISSING

    return value


def _walk(snapshot: Mapping) -> Iterable[Tuple[str, Any, int]]:
    """Breadth-first over nested mappings and lists of mappings.

    Yields ``(path, value, depth)``. Cycle-safe by object identity, and bounded
    on both depth and node count.
    """
    queue: List[Tuple[str, Any, int]] = [("", snapshot, 0)]
    seen = {id(snapshot)}
    visited = 0

    while queue and visited < _MAX_NODES:
        prefix, container, depth = queue.pop(0)
        if depth > _MAX_DEPTH:
            continue

        if isinstance(container, Mapping):
            items = container.items()
        elif isinstance(container, (list, tuple)):
            # A list is transparent: its element fields are reachable at the
            # same logical depth as the list's own name.
            for element in container:
                if isinstance(element, Mapping) and id(element) not in seen:
                    seen.add(id(element))
                    queue.append((prefix, element, depth))
            continue
        else:
            continue

        for key, value in items:
            if not isinstance(key, str) or key.startswith("_") or key in _SKIP_KEYS:
                continue
            visited += 1
            path = f"{prefix}.{key}" if prefix else key
            yield path, value, depth

            if isinstance(value, (Mapping, list, tuple)) and id(value) not in seen:
                seen.add(id(value))
                queue.append((path, value, depth + 1))


def find_leaf(snapshot: Mapping, name: str) -> Tuple[Any, str]:
    """Find a uniquely-named leaf anywhere in *snapshot*.

    Shallowest wins. Two distinct paths at the winning depth is an ambiguity and
    resolves to :data:`MISSING` — guessing which one the author meant is how a
    router takes a plausible wrong branch. The note explains it for the log.
    """
    candidates: List[Tuple[int, str, Any]] = [
        (depth, path, value)
        for path, value, depth in _walk(snapshot)
        if path.rsplit(".", 1)[-1] == name
    ]

    # Only candidates that actually hold something. Without this the search
    # re-finds the very top-level key ``get`` just rejected as empty — the
    # declared-but-unfilled field shadows the real value one level down, which
    # is the exact shape a failed structured-output parse leaves behind.
    candidates = [c for c in candidates if not _is_empty(c[2])]
    if not candidates:
        return MISSING, ""

    best_depth = min(depth for depth, _, _ in candidates)
    by_path: Dict[str, List[Any]] = {}
    for depth, path, value in candidates:
        if depth == best_depth:
            by_path.setdefault(path, []).append(value)

    # Several DISTINCT paths is a genuine ambiguity. Several values under ONE
    # path is not — that is every element of a list carrying the same field,
    # which is exactly the case this whole module exists to answer.
    if len(by_path) > 1:
        return MISSING, "ambiguous: " + ", ".join(sorted(by_path))

    path, values = next(iter(by_path.items()))
    if len(values) > 1:
        return MatchList(values), f"{path}[]"

    value = values[0]
    if isinstance(value, (list, tuple)) and not isinstance(value, MatchList):
        return MatchList(value), f"{path}[]"
    return value, path


class ConditionState:
    """The object a router condition sees as ``state``.

    A transparent proxy, NOT a ``Mapping``. Attribute access must keep working:
    ``state.has_results == True`` is a supported, tested condition form, and a
    ``Mapping`` wrapper silently breaks it. Writes pass through to the wrapped
    state so existing merge behaviour is unchanged.
    """

    __slots__ = ("_base", "_snapshot", "_misses")

    def __init__(self, base: Any) -> None:
        object.__setattr__(self, "_base", base)
        object.__setattr__(self, "_snapshot", None)
        object.__setattr__(self, "_misses", {})

    # -- lookup ---------------------------------------------------------

    def _snap(self) -> Dict[str, Any]:
        cached = object.__getattribute__(self, "_snapshot")
        if cached is None:
            cached = state_snapshot(object.__getattribute__(self, "_base"))
            object.__setattr__(self, "_snapshot", cached)
        return cached

    def _invalidate(self) -> None:
        object.__setattr__(self, "_snapshot", None)

    def get(self, key: Any, default: Any = None) -> Any:
        return _fold(self._resolve(key, default))

    def _resolve(self, key: Any, default: Any = None) -> Any:
        if not isinstance(key, str):
            return default

        snapshot = self._snap()

        # 1. exact top-level key, if it actually holds something
        if key in snapshot and not _is_empty(snapshot[key]):
            return snapshot[key]

        # 2. explicit path
        if "." in key or "[" in key:
            value = resolve_path(snapshot, key)
            if value is not MISSING:
                logger.info("router condition resolved path %r", key)
                return value
            object.__getattribute__(self, "_misses")[key] = "no such path"
            return default

        # 3. uniquely-named leaf, nested or across a list
        value, note = find_leaf(snapshot, key)
        if value is not MISSING:
            logger.info("router condition resolved %r via %s", key, note)
            return value
        if note:
            object.__getattribute__(self, "_misses")[key] = note
            logger.warning("router condition could not resolve %r: %s", key, note)

        # 4. the empty top-level value, if there was one, else the default
        if key in snapshot:
            return snapshot[key]
        object.__getattribute__(self, "_misses").setdefault(key, "not found")
        return default

    def describe(self, limit: int = 40) -> str:
        """Addressable paths and their values, for the no-route-matched log.

        Whoever authored the condition cannot see state while authoring it. This
        is the one moment they can, so it names the paths that would have worked.
        """
        lines: List[str] = []
        for path, value, _ in _walk(self._snap()):
            if isinstance(value, Mapping):
                continue
            if isinstance(value, (list, tuple)):
                lines.append(f"  {path}[] ({len(value)} items)")
            else:
                text = repr(value)
                lines.append(f"  {path} = {text[:80]}")
            if len(lines) >= limit:
                lines.append("  ...")
                break
        return "\n".join(lines) or "  (state is empty)"

    @property
    def misses(self) -> Dict[str, str]:
        return dict(object.__getattribute__(self, "_misses"))

    # -- transparent proxy ----------------------------------------------

    def __getattr__(self, name: str) -> Any:
        return getattr(object.__getattribute__(self, "_base"), name)

    def __setattr__(self, name: str, value: Any) -> None:
        setattr(object.__getattribute__(self, "_base"), name, value)
        self._invalidate()

    def __getitem__(self, key: Any) -> Any:
        base = object.__getattribute__(self, "_base")
        try:
            return base[key]
        except (KeyError, TypeError, AttributeError):
            value = self.get(key, MISSING)
            if value is MISSING:
                raise KeyError(key) from None
            return value

    def __setitem__(self, key: Any, value: Any) -> None:
        base = object.__getattribute__(self, "_base")
        try:
            base[key] = value
        except Exception as exc:  # noqa: BLE001
            # A typed state rejects a field it does not declare. That must not
            # abort condition evaluation — the router still has to answer from
            # whatever else it holds.
            logger.debug("state rejected key %r: %s", key, exc)
        self._invalidate()

    def update(self, other: Mapping) -> None:
        """Tolerant merge.

        ``DictLikeState.update`` raises on any key the typed state does not
        declare. That raise used to escape the router and stop the flow. Here an
        undeclared key is skipped and recorded — the crew output is still fully
        readable through the snapshot, so conditions can address it either way.
        """
        base = object.__getattribute__(self, "_base")
        try:
            base.update(other)
        except Exception:  # noqa: BLE001 - fall back to per-key
            for key, value in other.items():
                try:
                    base[key] = value
                except Exception:  # noqa: BLE001
                    logger.debug("state has no field %r; readable via path", key)
        self._invalidate()

    def __contains__(self, key: Any) -> bool:
        return key in self._snap()

    def __iter__(self):
        return iter(self._snap())

    def __len__(self) -> int:
        return len(self._snap())

    def keys(self):  # noqa: D102 - dict surface
        return self._snap().keys()

    def values(self):  # noqa: D102 - dict surface
        return self._snap().values()

    def items(self):  # noqa: D102 - dict surface
        return self._snap().items()

    def __repr__(self) -> str:
        # The router logs "State contents: {...}" unconditionally; without this
        # that diagnostic degrades to <ConditionState object at 0x...>.
        return repr(self._snap())


#: Suffixes a `where(...)` term may carry, and what each means. A bare field name
#: is equality. Chosen over free-text sub-expressions because the UI GENERATES
#: these strings — keyword arguments need no nested quoting and round-trip
#: through the condition builder unchanged.
_WHERE_OPS: Final = {
    "": lambda a, b: _casefold(a) == _casefold(b) if isinstance(a, str) else a == b,
    "ne": lambda a, b: (
        not (_casefold(a) == _casefold(b) if isinstance(a, str) else a == b)
    ),
    "gt": lambda a, b: a > b,
    "gte": lambda a, b: a >= b,
    "lt": lambda a, b: a < b,
    "lte": lambda a, b: a <= b,
    "contains": lambda a, b: (
        _casefold(b) in _casefold(a) if isinstance(a, str) else b in a
    ),
    "startswith": lambda a, b: (
        isinstance(a, str) and _casefold(a).startswith(_casefold(b))
    ),
    "endswith": lambda a, b: isinstance(a, str) and _casefold(a).endswith(_casefold(b)),
}


def _term(name: str) -> Tuple[str, Any]:
    """Split ``score__gt`` into the field and its comparison."""
    field, _, suffix = name.rpartition("__")
    if field and suffix in _WHERE_OPS:
        return field, _WHERE_OPS[suffix]
    return name, _WHERE_OPS[""]


def make_where(state: "ConditionState"):
    """Build the ``where`` a router condition may call.

    Answers the one question a projection cannot: "is there a SINGLE item that
    satisfies all of these at once". ``articles[].category`` and
    ``articles[].score`` are gathered independently, so ``A and B`` over them is
    satisfied by *some* article being politics and *some other* article scoring
    over 5 — element identity is lost. This keeps it.

    Returns the matching items, so the call is the condition: a non-empty list
    is truthy, and ``not where(...)`` reads as "no item matches".

    Bounded by the list it is given and by the number of terms; there is no
    nesting construct, so nothing compounds.
    """

    def where(path: str, **terms: Any) -> MatchList:
        items = state.get(path, None)
        if isinstance(items, str) or not isinstance(items, (list, tuple)):
            # A single object is a list of one — asking "any item where…" of a
            # lone mapping should answer about that mapping, not nothing.
            items = [items] if isinstance(items, Mapping) else []

        matched = MatchList()
        for item in items:
            if not isinstance(item, Mapping):
                continue
            if all(_matches(item, name, expected) for name, expected in terms.items()):
                matched.append(item)
        return matched

    return where


def _matches(item: Mapping, name: str, expected: Any) -> bool:
    field, compare = _term(name)
    if field not in item:
        return False
    try:
        return bool(compare(item[field], expected))
    except TypeError:
        # Total, like MatchList: a raising comparison would propagate out of
        # safe_eval and be swallowed into a silent skipped route.
        return False


def _casefold(value: Any) -> Any:
    """Case-folded when it is a string, so `where` matches what `==` matches."""
    return value.casefold() if isinstance(value, str) else value


def report_no_route(
    router_name: str,
    outcomes: Iterable[Tuple[str, str, Any]],
    state: Any,
    *,
    has_default: bool,
) -> None:
    """Explain, once, why a router took no route.

    Both arms of "nothing matched" used to log at INFO — including the one that
    silently falls through to ``default``. A router that takes a plausible wrong
    branch and a router that never fired looked identical in the log, which is
    how a flow reported success while running the wrong half of itself.
    """
    lines = [
        f"Router {router_name} matched no route "
        f"({'taking default' if has_default else 'flow stops here'})."
    ]
    for route_name, condition, result in outcomes:
        lines.append(f"  {route_name}: {condition}  ->  {result}")

    if isinstance(state, ConditionState):
        misses = state.misses
        if misses:
            lines.append("Unresolved lookups:")
            for key, why in misses.items():
                lines.append(f"  {key} -> {why}")
        lines.append("Addressable state paths:")
        lines.append(state.describe())

    logger.error("\n".join(lines))
