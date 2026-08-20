"""Incremental (streaming) A2UI composition — parse a surface as it is generated.

The composer's LLM emits ONE large JSON object. Waiting for it means the reader
sees nothing for the whole call: measured 25-45s for a deck's outline pass plus
~140s for the deck itself. This module turns that single blob into a sequence of
small A2UI messages that can be shipped the moment each piece is complete, so a
presentation's slides appear one at a time instead of all at once at the end.

The message set mirrors the A2UI protocol's streaming model (v0.9.1 / v1.0):
JSONL, one top-level key per message, ``createSurface`` then ``updateComponents``
then ``updateDataModel``. That shape is not an accident of ours — A2UI is built
for exactly this, because its components are an ADJACENCY LIST (a flat array
whose parents name their children by id). A renderer holds them in a map and
rebuilds the tree at render time, so components may arrive in any order and a
child id that has not arrived yet simply renders as nothing and fills in later.
``A2UIRenderer.tsx`` already behaves that way, which is what makes progressive
rendering nearly free on the render side.

Two properties this module guarantees, because composition sits on the answer
path of every chat turn and must never be able to break a run:

* **It never raises.** Every entry point swallows its own errors and degrades to
  "no deltas" — the caller then ships the final surface exactly as it does today.
* **Replaying every emitted message reconstructs the final surface.** The stream
  is an optimisation, never a second source of truth; the validated surface at
  the end of ``compose_a2ui`` stays authoritative and supersedes the stream.
"""

from __future__ import annotations

import json
import re
from typing import Any, Callable, Dict, List, Optional, Tuple

#: The A2UI dialect version stamped on every streamed message.
A2UI_VERSION = "v1.0"

#: The surface id every message in a run carries. A chat turn renders exactly one
#: composed surface, and the client routes by the SSE envelope's job id, so a
#: per-run id would buy nothing and risk the instant shell, the outline skeleton
#: and the streamed deck being read as three different surfaces.
SURFACE_ID = "a2ui"

# Components that make a surface a genuine DELIVERABLE (a real graph/table/number,
# a diagram, a map, a board, an image gallery). A dashboard/document surface with
# none of these is just prose wrapped in Text/Markdown and would render the answer
# twice (see the double-render rule in the A2UI CLAUDE.md). The data-viz + diagram
# + gallery components must be listed here or their surfaces get dropped back to
# plain text. Lives here rather than in ``runner`` because the STREAM gate needs it
# too — a dashboard cannot start streaming until it has proven it carries data.
DATA_COMPONENTS = frozenset(
    {
        "Chart",
        "Table",
        "Stat",
        "KeyValue",
        "Grid",
        "Forecast",
        "Graph",
        "Sequence",
        "Album",
        "Map",
        "Diagram",
        # Region shading and flow ribbons are genuine deliverables: omitted here,
        # their whole surface is dropped as "prose-only" and the answer silently
        # falls back to markdown (the Album bug in the A2UI checklist).
        "RegionHeatmap",
        "Sankey",
        # A board is a deliverable for the same reason a Table is: it carries
        # structured items, not prose. Omitting it would drop every Kanban
        # dashboard back to markdown.
        "Kanban",
    }
)

#: Surface kinds a plain-prose answer degrades into. These must prove they carry a
#: data component before anything is streamed for them — see ``SurfaceStreamer``.
GATED_SURFACE_KINDS = frozenset({"dashboard", "document"})


# ── Forgiving parse of a truncated surface ──────────────────────────────────
# A recursive-descent reader that raises `_Incomplete` the moment it runs out of
# input. Truncation is the NORMAL case here (we are parsing mid-generation), so
# it is a control-flow signal rather than an error: whatever was fully read before
# it fires is complete and safe to emit.


class _Incomplete(Exception):
    """The buffer ends mid-value. Everything read before this point is valid."""


class _Reader:
    __slots__ = ("s", "i")

    def __init__(self, s: str, i: int = 0) -> None:
        self.s = s
        self.i = i

    def ws(self) -> str:
        s, n = self.s, len(self.s)
        while self.i < n and s[self.i] in " \t\r\n":
            self.i += 1
        if self.i >= n:
            raise _Incomplete
        return s[self.i]

    def take(self, ch: str) -> None:
        if self.ws() != ch:
            raise _Incomplete
        self.i += 1

    def string(self) -> str:
        """Read a complete JSON string, or raise if it is still open."""
        if self.ws() != '"':
            raise _Incomplete
        s, n = self.s, len(self.s)
        j = self.i + 1
        while j < n:
            c = s[j]
            if c == "\\":
                j += 2
                continue
            if c == '"':
                raw = s[self.i : j + 1]
                self.i = j + 1
                try:
                    return json.loads(raw)
                except Exception:  # noqa: BLE001 — an unparseable string is not done
                    raise _Incomplete
            j += 1
        raise _Incomplete

    def value(self) -> Any:
        """Read one complete JSON value. Raises `_Incomplete` if truncated."""
        c = self.ws()
        if c == '"':
            return self.string()
        if c == "{" or c == "[":
            return self._container()
        # number / true / false / null — complete only once a delimiter follows,
        # otherwise a buffer ending in "12" might really be "1234".
        s, n = self.s, len(self.s)
        j = self.i
        while j < n and s[j] not in ",}] \t\r\n":
            j += 1
        if j >= n:
            raise _Incomplete
        try:
            out = json.loads(s[self.i : j])
        except Exception:  # noqa: BLE001
            raise _Incomplete
        self.i = j
        return out

    def _container(self) -> Any:
        """Slice a balanced {...} / [...] and hand it to json.loads."""
        s, n = self.s, len(self.s)
        start = self.i
        depth = 0
        in_str = False
        esc = False
        j = self.i
        while j < n:
            c = s[j]
            if in_str:
                if esc:
                    esc = False
                elif c == "\\":
                    esc = True
                elif c == '"':
                    in_str = False
            elif c == '"':
                in_str = True
            elif c in "{[":
                depth += 1
            elif c in "}]":
                depth -= 1
                if depth == 0:
                    try:
                        out = json.loads(s[start : j + 1])
                    except Exception:  # noqa: BLE001
                        raise _Incomplete
                    self.i = j + 1
                    return out
            j += 1
        raise _Incomplete


class PartialSurface:
    """Whatever of a surface has been fully generated so far."""

    __slots__ = ("surface_kind", "root", "components", "data_model", "complete")

    def __init__(self) -> None:
        self.surface_kind: Optional[str] = None
        self.root: Optional[str] = None
        self.components: List[Dict[str, Any]] = []
        self.data_model: Dict[str, Any] = {}
        self.complete: bool = False


def _strip_preamble(text: str) -> str:
    """Drop a ``` fence or any prose before the surface object."""
    s = text.lstrip()
    if s.startswith("```"):
        nl = s.find("\n")
        s = s[nl + 1 :] if nl != -1 else ""
    start = s.find("{")
    return s[start:] if start != -1 else ""


def scan_partial(text: str) -> PartialSurface:
    """Read as much of a (possibly still-generating) surface as is complete.

    Never raises: a buffer that is not yet parseable simply yields an empty
    ``PartialSurface``, and the caller emits nothing this round.
    """
    out = PartialSurface()
    try:
        body = _strip_preamble(text)
        if not body:
            return out
        r = _Reader(body)
        r.take("{")
        while True:
            c = r.ws()
            if c == "}":
                out.complete = True
                return out
            if c == ",":
                r.i += 1
                continue
            key = r.string()
            r.take(":")
            if key == "components":
                _scan_array_items(r, out.components)
            elif key == "dataModel":
                _scan_object_entries(r, out.data_model)
            else:
                value = r.value()
                if key == "surfaceKind" and isinstance(value, str):
                    out.surface_kind = value
                elif key == "root" and isinstance(value, str):
                    out.root = value
    except _Incomplete:
        return out
    except Exception:  # noqa: BLE001 — a malformed stream is simply "nothing yet"
        return out


def _scan_array_items(r: _Reader, sink: List[Dict[str, Any]]) -> None:
    """Append every COMPLETE element of an array, then re-raise on truncation."""
    r.take("[")
    while True:
        c = r.ws()
        if c == "]":
            r.i += 1
            return
        if c == ",":
            r.i += 1
            continue
        item = r.value()  # raises _Incomplete on a half-written element
        if isinstance(item, dict):
            sink.append(item)


def _scan_object_entries(r: _Reader, sink: Dict[str, Any]) -> None:
    """Record every COMPLETE key/value of an object, then re-raise on truncation."""
    r.take("{")
    while True:
        c = r.ws()
        if c == "}":
            r.i += 1
            return
        if c == ",":
            r.i += 1
            continue
        key = r.string()
        r.take(":")
        sink[key] = r.value()  # raises _Incomplete on a half-written value


# ── Message construction ────────────────────────────────────────────────────


def create_surface_msg(
    surface_id: str,
    *,
    surface_kind: str,
    root: str,
    components: Optional[List[Dict[str, Any]]] = None,
    data_model: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A2UI ``createSurface``. ``surfaceKind``/``root`` are our dialect's fields —
    the protocol implies a root id of "root" and carries the container kind out of
    band, but our renderer reads both off the surface, so they ride along here."""
    payload: Dict[str, Any] = {
        "surfaceId": surface_id,
        "surfaceKind": surface_kind,
        "root": root,
    }
    if components:
        payload["components"] = components
    if data_model:
        payload["dataModel"] = data_model
    return {"version": A2UI_VERSION, "createSurface": payload}


def update_components_msg(
    surface_id: str, components: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """A2UI ``updateComponents`` — a batch added to the renderer's component map."""
    return {
        "version": A2UI_VERSION,
        "updateComponents": {"surfaceId": surface_id, "components": components},
    }


def update_data_model_msg(surface_id: str, path: str, value: Any) -> Dict[str, Any]:
    """A2UI ``updateDataModel`` — a JSON Pointer path and its new value."""
    return {
        "version": A2UI_VERSION,
        "updateDataModel": {"surfaceId": surface_id, "path": path, "value": value},
    }


def delete_surface_msg(surface_id: str) -> Dict[str, Any]:
    """A2UI ``deleteSurface`` — retract a surface the reader should not keep.

    Needed because streaming commits early: if composition then falls back to
    prose, or a late gate drops the surface, the half-drawn thing on screen has
    to go rather than sit there as a permanent artefact of an answer that was
    never delivered.
    """
    return {"version": A2UI_VERSION, "deleteSurface": {"surfaceId": surface_id}}


#: Message types that carry a WHOLE surface rather than a change to one.
_SNAPSHOT_KEYS = ("createSurface", "deleteSurface")


def is_snapshot(msg: Dict[str, Any]) -> bool:
    """Does this message stand on its own, without the ones before it?

    The distinction decides whether a message may be REPLAYED to a client that
    connects late, and it is not cosmetic: the instant shell is emitted at the
    very start of a run, but the browser cannot open its event stream until the
    POST that starts the run has returned the job id. The shell therefore always
    races the subscriber and, as a non-replayable message, always lost — the
    server shipped a deck frame at t=0 that nobody could receive.

    Snapshots replay, increments do not. A late joiner then gets the shell (or,
    later in the run, the committed final surface) and simply misses the
    intermediate batches it no longer needs.
    """
    return isinstance(msg, dict) and any(k in msg for k in _SNAPSHOT_KEYS)


def _pointer(key: str) -> str:
    """Escape a dataModel key into a JSON Pointer segment (RFC 6901)."""
    return "/" + key.replace("~", "~0").replace("/", "~1")


def apply_messages(messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Replay a message list into a surface dict — the Python twin of the
    frontend reducer, and the basis of the round-trip test that keeps the two
    honest. Returns None if the stream never created a surface."""
    surface: Optional[Dict[str, Any]] = None
    for msg in messages:
        if not isinstance(msg, dict):
            continue
        if "createSurface" in msg:
            m = msg["createSurface"]
            surface = {
                "surfaceKind": m.get("surfaceKind"),
                "root": m.get("root"),
                "components": list(m.get("components") or []),
                "dataModel": dict(m.get("dataModel") or {}),
            }
        elif surface is None:
            continue  # a stray update before createSurface — the spec forbids it
        elif "updateComponents" in msg:
            by_id = {c.get("id"): i for i, c in enumerate(surface["components"])}
            for comp in msg["updateComponents"].get("components") or []:
                idx = by_id.get(comp.get("id"))
                if idx is None:
                    surface["components"].append(comp)
                else:
                    surface["components"][idx] = comp
        elif "deleteSurface" in msg:
            surface = None
        elif "updateDataModel" in msg:
            m = msg["updateDataModel"]
            path = m.get("path") or "/"
            value = m.get("value")
            if path == "/":
                surface["dataModel"] = value if isinstance(value, dict) else {}
            else:
                key = path.lstrip("/").replace("~1", "/").replace("~0", "~")
                if value is None:
                    surface["dataModel"].pop(key, None)
                else:
                    surface["dataModel"][key] = value
    return surface


# ── The streamer ────────────────────────────────────────────────────────────

#: A sink for one A2UI message. Must never raise — the streamer calls it from the
#: composer's worker thread and a throw there would fail the run.
MessageSink = Callable[[Dict[str, Any]], None]


class SurfaceStreamer:
    """Turns a growing composer buffer into A2UI messages, emitting each piece once.

    Call ``feed(buffer)`` with the WHOLE text generated so far (not a delta) — the
    scan is a cheap forward pass and re-reading is what keeps the parser stateless
    and testable. Anything newly complete since the last call is emitted.
    """

    #: Emit a full snapshot every N components.
    #:
    #: A run's event stream belongs to whichever session the reader is viewing,
    #: so switching away closes it and the increments generated meanwhile are
    #: never delivered. Only SNAPSHOTS are replayed on reconnect, so without
    #: these the reader would come back to the shell and wait for the final
    #: commit to repair it. A periodic checkpoint bounds that loss: a
    #: reconnecting client catches up to within this many components.
    CHECKPOINT_EVERY = 12

    def __init__(
        self,
        surface_id: str,
        sink: MessageSink,
        *,
        revision: int = 0,
        checkpoint_every: Optional[int] = None,
    ) -> None:
        self.surface_id = surface_id
        self.revision = revision
        self._sink = sink
        self._created = False
        self._sent_components = 0
        self._sent_keys: set = set()
        self._held: List[Dict[str, Any]] = []
        self._gate_open = False
        self._checkpoint_every = (
            self.CHECKPOINT_EVERY if checkpoint_every is None else checkpoint_every
        )
        self._last_checkpoint = 0
        self.messages: List[Dict[str, Any]] = []  # everything emitted, for tests

    # -- gating -------------------------------------------------------------
    def _gate_allows(self, part: PartialSurface) -> bool:
        """Dashboard/document surfaces must prove they carry a data component
        before ANY of their messages ship.

        Without this, streaming would contradict the prose gate: we would paint a
        dashboard live and then discover at the end that it holds nothing but
        Text, which ``compose_surface`` drops as prose-only. The reader would have
        watched a surface build and then seen it retracted — strictly worse than
        the wait. So those two kinds buffer until a real deliverable shows up, and
        the explicitly-requested kinds (presentation/quiz/mindmap/flashcards/map)
        stream from the first component, which is where the latency actually hurts.
        """
        if self._gate_open:
            return True
        if part.surface_kind is None:
            # Not known yet. Hold — an unknown kind must not be treated as an
            # ungated one, or every surface streams its first components before
            # we have any idea whether it is allowed to.
            return False
        if part.surface_kind not in GATED_SURFACE_KINDS:
            self._gate_open = True
            return True
        for comp in part.components:
            if (comp.get("component") or comp.get("type")) in DATA_COMPONENTS:
                self._gate_open = True
                return True
        return False

    def _emit(self, msg: Dict[str, Any]) -> None:
        self.messages.append(msg)
        if not self._gate_open:
            self._held.append(msg)
            return
        if self._held:
            held, self._held = self._held, []
            for m in held:
                self._deliver(m)
        self._deliver(msg)

    def _deliver(self, msg: Dict[str, Any]) -> None:
        try:
            self._sink(msg)
        except Exception:  # noqa: BLE001 — a broken sink must not fail the run
            pass

    # -- the pass ------------------------------------------------------------
    def feed(self, buffer: str) -> int:
        """Emit whatever became complete. Returns the number of messages emitted."""
        before = len(self.messages)
        try:
            part = scan_partial(buffer)
            # Check the gate FIRST so a surface that is allowed to stream never
            # spends a round in the hold buffer.
            self._gate_allows(part)
            if not self._created:
                # Nothing can ship before surfaceKind and root are known: the
                # renderer needs a container and an entry point, and the protocol
                # requires createSurface to precede every other message.
                if not (part.surface_kind and part.root):
                    return 0
                self._created = True
                self._emit(
                    create_surface_msg(
                        self.surface_id,
                        surface_kind=part.surface_kind,
                        root=part.root,
                    )
                )
            new = part.components[self._sent_components :]
            if new:
                self._sent_components = len(part.components)
                self._emit(update_components_msg(self.surface_id, new))
            for key, value in part.data_model.items():
                if key not in self._sent_keys:
                    self._sent_keys.add(key)
                    self._emit(update_data_model_msg(self.surface_id, _pointer(key), value))
            # Re-check: the components just parsed may be what opens the gate, and
            # everything held so far should ship the moment it does.
            if self._gate_allows(part) and self._held:
                self._emit_held()
            self._maybe_checkpoint(part)
        except Exception:  # noqa: BLE001 — streaming is best-effort, always
            return 0
        return len(self.messages) - before

    def _maybe_checkpoint(self, part: PartialSurface) -> None:
        """Leave a catch-up point a reconnecting client can land on."""
        if not self._checkpoint_every or not part.surface_kind or not part.root:
            return
        if self._sent_components - self._last_checkpoint < self._checkpoint_every:
            return
        self._last_checkpoint = self._sent_components
        self._emit(
            create_surface_msg(
                self.surface_id,
                surface_kind=part.surface_kind,
                root=part.root,
                components=list(part.components),
                data_model=dict(part.data_model),
            )
        )

    def _emit_held(self) -> None:
        held, self._held = self._held, []
        for m in held:
            self._deliver(m)

    def abandon(self) -> None:
        """Drop anything still held. Used when the gate never opened — a
        dashboard that turned out to be prose-only never reaches the reader."""
        self._held = []


# ── Presentation skeleton ───────────────────────────────────────────────────


def skeleton_from_outline(
    outline: List[Dict[str, str]], *, deck_id: str = "deck"
) -> Optional[Dict[str, Any]]:
    """Build a placeholder deck from the outline pre-pass.

    This is the cheapest large win available. ``plan_presentation_outline``
    already knows every slide's title and layout variant BEFORE the expensive
    compose call, so the deck's shape can be on screen while the slides are still
    being written — the reader sees the real structure and real titles in seconds
    instead of a spinner for a couple of minutes.

    Slide ids are pinned to ``slide_1..slide_N`` so the real slides, which the
    composer is told to number the same way, replace these by id rather than
    piling up beside them.
    """
    try:
        slides = [s for s in (outline or []) if isinstance(s, dict) and s.get("title")]
        if len(slides) < 2:
            return None
        components: List[Dict[str, Any]] = [
            {
                "id": deck_id,
                "component": "SlideDeck",
                "children": [f"slide_{i + 1}" for i in range(len(slides))],
            }
        ]
        for i, s in enumerate(slides):
            components.append(
                {
                    "id": f"slide_{i + 1}",
                    "component": "Slide",
                    "variant": s.get("variant") or "content",
                    "title": s.get("title"),
                    # Marks the slide as not-yet-written so the renderer can show a
                    # placeholder rather than an empty stage. Harmless to a renderer
                    # that does not know the flag — it is just an unused prop.
                    "pending": True,
                }
            )
        return {
            "surfaceKind": "presentation",
            "root": deck_id,
            "components": components,
            "dataModel": {},
        }
    except Exception:  # noqa: BLE001
        return None


# ── Instant shell ───────────────────────────────────────────────────────────
# The skeleton above is derived from the outline pre-pass, which cannot run until
# the agent has written its answer — measured 13.4s for the prose plus 8.7s for
# the outline, so the deck's real titles are 22 seconds away no matter how well
# the composer streams. The shell below needs NO model at all: the moment the
# request is recognised as a presentation we already know a deck is coming, and
# roughly how big, so the frame can be on screen immediately and be replaced by
# the real thing when it arrives.

#: Leading imperatives a deck request opens with, stripped to leave the subject.
_REQUEST_PREFIX = re.compile(
    r"^\s*(?:please\s+)?"
    r"(?:can\s+you\s+|could\s+you\s+|i\s+(?:want|need)\s+(?:you\s+to\s+)?)?"
    r"(?:create|make|build|generate|draft|prepare|design|draw|show|write"
    r"|produce|compose|put\s+together|give\s+me|do)\s+"
    r"(?:me\s+)?(?:a|an|the)?\s*"
    r"(?:\w+[- ])?"  # an adjective: "short deck", "10-slide deck"
    # Every deliverable that gets a shell, not just decks — a quiz request whose
    # noun is not stripped titles itself "Make Me a Quiz About SQL Joins".
    r"(?:presentation|deck|slide\s*deck|slides|slideshow|powerpoint|pitch"
    r"|quiz|quizzes|flash\s*cards?|flashcards|mind\s*map|mindmap|map"
    r"|kanban(?:\s+board)?|sprint\s+board|task\s+board|project\s+board"
    r"|album|gallery|dashboard|report|forecast|network\s+graph|graph"
    r"|diagram|flow\s*chart|timeline|roadmap)\s*"
    r"(?:on|about|for|covering|regarding|explaining|of)?\s*",
    re.IGNORECASE,
)

#: Words a title keeps lowercase unless they lead it.
_TITLE_MINOR = frozenset(
    {"a", "an", "the", "and", "or", "but", "for", "of", "on", "in", "to", "with",
     "at", "by", "from", "as", "vs", "via"}
)

#: Tokens that are acronyms, not words — "how llm works" should not title-case
#: into "How Llm Works".
_TITLE_ACRONYMS = frozenset(
    {"llm", "llms", "ai", "ml", "api", "apis", "kpi", "kpis", "roi", "sql", "etl",
     "ui", "ux", "gpu", "cpu", "saas", "b2b", "b2c", "crm", "erp", "rag", "mcp",
     "aws", "gcp", "hr", "it", "seo", "faq", "pdf", "csv", "json", "sdk"}
)


#: The chat path hands the composer a TASK DESCRIPTION, not the bare message —
#: "Respond directly and helpfully…\n\nUSER REQUEST …:\n<the actual request>\n\n
#: Expected output: …". A title derived from that envelope is the boilerplate,
#: not the subject, which is how the instant shell shipped with a blank title.
_USER_REQUEST_BLOCK = re.compile(
    r"USER REQUEST[^\n:]*:\s*\n(.+?)(?:\n\s*\n|\Z)", re.IGNORECASE | re.DOTALL
)


def unwrap_request(query: str) -> str:
    """The user's own words, from whatever envelope the caller happens to hold."""
    if not query:
        return ""
    found = _USER_REQUEST_BLOCK.search(query)
    return (found.group(1) if found else query).strip()


def title_from_request(query: str) -> str:
    """Best-effort deck title from the user's words alone — no model involved.

    Provisional by design: it is replaced by the outline's real title a few
    seconds later. It exists so the instant shell shows something true rather
    than a blank stage.
    """
    try:
        subject = _REQUEST_PREFIX.sub("", unwrap_request(query), count=1)
        subject = subject.strip().strip(".!?,;:").strip()
        if not subject or len(subject) > 120:
            return ""
        words = subject.split()
        out = []
        for i, w in enumerate(words):
            low = w.lower()
            if low in _TITLE_ACRONYMS:
                out.append(low.upper())
            elif i and low in _TITLE_MINOR:
                out.append(low)
            elif w.isupper() and len(w) > 1:
                out.append(w)  # already an acronym the user typed
            else:
                out.append(low[:1].upper() + low[1:])
        return " ".join(out)
    except Exception:  # noqa: BLE001
        return ""


#: Deliverable -> the surfaceKind it renders on, for the kinds that OWN a canvas.
#:
#: These are exactly the kinds the prose gate never drops (see
#: ``GATED_SURFACE_KINDS``), and that is the whole criterion. Asking for a quiz
#: or a deck or a mindmap tells us a surface of that shape is coming, so a frame
#: for it is a promise we can keep. A dashboard or a report is the opposite: it
#: renders on ``dashboard``/``document``, which get dropped back to plain text
#: when the answer turns out to carry no data — so a frame there would be shown
#: and then taken away, which is worse than never showing one.
SHELLABLE_KINDS = {
    # Own a canvas. The request alone settles the shape, and the prose gate can
    # never drop them, so their frame is a promise that is always kept.
    "presentation": "presentation",
    "quiz": "quiz",
    "flashcards": "flashcards",
    "mindmap": "mindmap",
    "map": "map",
    # Components that fill a dashboard/document canvas. Asking for a kanban board
    # or a gallery is just as explicit, so these get a frame too — but their
    # surface CAN be dropped as prose-only, so the frame is provisional. The
    # client keeps streaming the answer's text underneath these (see
    # ``RETRACTABLE_SHELL_KINDS``), which is what makes a retraction cost nothing.
    "kanban": "dashboard",
    "album": "dashboard",
    "graph": "dashboard",
    "sequence": "dashboard",
    "diagram": "dashboard",
    "dashboard": "dashboard",
    "forecast": "document",
    "report": "document",
    "genie": "document",
}

#: Shells that may still be taken back. Identical to the gated kinds — named
#: separately because the client reads it as "keep the prose flowing", which is a
#: different question from "may this surface be dropped".
RETRACTABLE_SHELL_KINDS = GATED_SURFACE_KINDS


def shell_from_request(
    query: str,
    *,
    kind: str = "presentation",
    variant: str = "",
    slides: int = 8,
    deck_id: str = "deck",
) -> Optional[Dict[str, Any]]:
    """A deck frame with no content, buildable the instant a request arrives.

    Costs nothing — no LLM call, no answer, no outline — so it can ship before
    the agent has written a word. Every slide is ``pending``; the outline's
    skeleton replaces them by id as soon as it lands, and the composed slides
    replace those in turn.
    """
    try:
        title = title_from_request(query)
        if kind != "presentation":
            # Everything that is not a deck is ONE component filling its canvas —
            # a quiz, a card stack, a mindmap, a map — so its frame is a single
            # placeholder shaped like that component rather than a list of slides.
            return {
                "surfaceKind": kind,
                "root": "shell",
                "components": [
                    {
                        "id": "shell",
                        "component": "Skeleton",
                        # The DELIVERABLE, not the canvas: "kanban" and "album"
                        # both live on a dashboard, and a frame shaped like a
                        # generic dashboard tells the reader nothing about which
                        # of the two is coming.
                        "variant": variant or kind,
                        "title": title or None,
                        "pending": True,
                    }
                ],
                "dataModel": {},
            }
        count = max(3, min(int(slides or 8), 24))
        components: List[Dict[str, Any]] = [
            {
                "id": deck_id,
                "component": "SlideDeck",
                "children": [f"slide_{i + 1}" for i in range(count)],
            },
            # The opener carries the derived title so the frame reads as THIS
            # deck rather than as a generic placeholder.
            {
                "id": "slide_1",
                "component": "Slide",
                "variant": "title",
                "title": title or None,
                "pending": True,
            },
        ]
        for i in range(1, count):
            components.append(
                {
                    "id": f"slide_{i + 1}",
                    "component": "Slide",
                    "variant": "content",
                    "pending": True,
                }
            )
        return {
            "surfaceKind": "presentation",
            "root": deck_id,
            "components": components,
            "dataModel": {},
        }
    except Exception:  # noqa: BLE001
        return None


def surface_to_messages(
    surface: Dict[str, Any], surface_id: str
) -> List[Dict[str, Any]]:
    """Express a whole surface as a single ``createSurface`` — used to ship the
    skeleton, and to replace a streamed revision with the authoritative one."""
    return [
        create_surface_msg(
            surface_id,
            surface_kind=str(surface.get("surfaceKind") or "document"),
            root=str(surface.get("root") or ""),
            components=list(surface.get("components") or []),
            data_model=dict(surface.get("dataModel") or {}),
        )
    ]
