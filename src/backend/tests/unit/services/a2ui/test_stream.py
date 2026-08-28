"""Streaming composition: the incremental parser, the gate, and the skeleton.

The property that matters most is the round trip — replaying every message the
streamer emitted must rebuild EXACTLY the surface the composer produced. The
stream is an optimisation, never a second source of truth, and this is what stops
it from drifting into one.
"""

import json

import pytest

from src.services.a2ui.stream import (
    DATA_COMPONENTS,
    GATED_SURFACE_KINDS,
    RETRACTABLE_SHELL_KINDS,
    SHELLABLE_KINDS,
    SurfaceStreamer,
    apply_messages,
    create_surface_msg,
    delete_surface_msg,
    is_snapshot,
    scan_partial,
    shell_from_request,
    surface_to_messages,
    title_from_request,
    update_components_msg,
    update_data_model_msg,
)

DECK = {
    "surfaceKind": "presentation",
    "root": "deck",
    "components": [
        {"id": "deck", "component": "SlideDeck", "children": ["slide_1", "slide_2"]},
        {"id": "slide_1", "component": "Slide", "variant": "title", "title": "Q3"},
        {
            "id": "slide_2",
            "component": "Slide",
            "variant": "visual",
            "title": "Revenue",
            "children": ["chart_1"],
        },
        {
            "id": "chart_1",
            "component": "Chart",
            "chartType": "bar",
            "data": {"path": "/rows"},
        },
    ],
    "dataModel": {"rows": [{"m": "Jul", "v": 12}], "note": "in $m"},
}

DASHBOARD_PROSE = {
    "surfaceKind": "dashboard",
    "root": "col",
    "components": [
        {"id": "col", "component": "Column", "children": ["t1"]},
        {"id": "t1", "component": "Text", "text": "Nothing but words."},
    ],
    "dataModel": {},
}

DASHBOARD_DATA = {
    "surfaceKind": "dashboard",
    "root": "col",
    "components": [
        {"id": "col", "component": "Column", "children": ["t1", "tbl"]},
        {"id": "t1", "component": "Text", "text": "Here are the numbers."},
        {"id": "tbl", "component": "Table", "columns": ["a"], "rows": {"path": "/r"}},
    ],
    "dataModel": {"r": [[1]]},
}


def _stream(surface, *, chunk=7):
    """Feed a surface's JSON through the streamer in fixed-size slices."""
    sent = []
    s = SurfaceStreamer("sid", sent.append)
    raw = json.dumps(surface)
    for i in range(0, len(raw), chunk):
        s.feed(raw[: i + chunk])
    return s, sent


# ── the round trip ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("chunk", [1, 3, 17, 64, 100_000])
def test_replaying_the_stream_rebuilds_the_surface(chunk):
    """Whatever the chunk boundaries, the messages reconstruct the deck exactly."""
    _, sent = _stream(DECK, chunk=chunk)
    assert apply_messages(sent) == DECK


def test_the_stream_opens_with_a_created_surface():
    """createSurface must come first (the protocol requires it). LATER ones are
    allowed and expected — they are the periodic catch-up checkpoints."""
    _, sent = _stream(DECK)
    assert "createSurface" in sent[0]
    assert all(len(m) == 2 and m["version"] == "v1.0" for m in sent)


def test_components_arrive_before_the_surface_is_finished():
    """The point of the exercise: slides ship while the JSON is still being written."""
    raw = json.dumps(DECK)
    # Mid-deck: slide_2 is complete, the chart it points at is not yet written.
    cut = raw.index('"id": "chart_1"')
    sent = []
    s = SurfaceStreamer("sid", sent.append)
    s.feed(raw[:cut])
    streamed = [
        c
        for m in sent
        if "updateComponents" in m
        for c in m["updateComponents"]["components"]
    ]
    assert [c["id"] for c in streamed] == ["deck", "slide_1", "slide_2"]
    assert not s._held


def test_no_component_is_emitted_twice():
    _, sent = _stream(DECK, chunk=2)
    ids = [
        c["id"]
        for m in sent
        if "updateComponents" in m
        for c in m["updateComponents"]["components"]
    ]
    assert ids == sorted(set(ids), key=ids.index)
    assert len(ids) == len(DECK["components"])


def test_a_half_written_component_is_never_emitted():
    """Truncation mid-object must yield nothing for that object, not a partial."""
    raw = json.dumps(DECK)
    cut = raw.index('"variant": "visual"') + 10
    part = scan_partial(raw[:cut])
    assert [c["id"] for c in part.components] == ["deck", "slide_1"]
    assert part.complete is False


def test_data_model_keys_stream_as_json_pointers():
    _, sent = _stream(DECK)
    updates = [m["updateDataModel"] for m in sent if "updateDataModel" in m]
    assert [(u["path"], u["value"]) for u in updates] == [
        ("/rows", DECK["dataModel"]["rows"]),
        ("/note", "in $m"),
    ]


# ── the prose gate ──────────────────────────────────────────────────────────


def test_a_prose_only_dashboard_streams_nothing():
    """It would be retracted at the end, so it must never reach the reader."""
    s, sent = _stream(DASHBOARD_PROSE)
    assert sent == []
    assert s.messages, "the streamer still tracked them internally"


def test_a_dashboard_streams_once_it_proves_it_has_data():
    s, sent = _stream(DASHBOARD_DATA)
    assert apply_messages(sent) == DASHBOARD_DATA
    assert "createSurface" in sent[0], "held messages flush in order"


def test_kanban_counts_as_a_deliverable():
    """It was in the renderer and catalog but not the gate — the Album bug."""
    assert "Kanban" in DATA_COMPONENTS


# ── robustness: this sits on the answer path of every chat turn ─────────────


@pytest.mark.parametrize(
    "junk",
    [
        "",
        "   ",
        "not json at all",
        "{",
        '{"surfaceKind":',
        '{"surfaceKind": "presentation", "root": "deck", "components": [{"id":',
        '```json\n{"surfaceKind": "presentation"',
        '{"components": [1, 2, 3]}',
        "}{[]",
    ],
)
def test_garbage_never_raises(junk):
    sent = []
    s = SurfaceStreamer("sid", sent.append)
    assert s.feed(junk) >= 0
    assert scan_partial(junk) is not None


def test_a_throwing_sink_cannot_break_the_run():
    def boom(_msg):
        raise RuntimeError("the SSE broadcast died")

    s = SurfaceStreamer("sid", boom)
    for i in range(0, len(json.dumps(DECK)), 40):
        s.feed(json.dumps(DECK)[: i + 40])
    assert s.messages  # it kept going


def test_a_fenced_response_is_handled():
    raw = "```json\n" + json.dumps(DECK) + "\n```"
    sent = []
    SurfaceStreamer("sid", sent.append).feed(raw)
    assert apply_messages(sent) == DECK


def test_prose_before_the_object_is_skipped():
    raw = "Here is the surface you asked for:\n" + json.dumps(DECK)
    sent = []
    SurfaceStreamer("sid", sent.append).feed(raw)
    assert apply_messages(sent) == DECK


def test_a_number_is_not_emitted_until_a_delimiter_follows():
    """A buffer ending in '12' might really be '1234' — never guess."""
    assert scan_partial('{"dataModel": {"n": 12').data_model == {}
    assert scan_partial('{"dataModel": {"n": 12,').data_model == {"n": 12}


# ── the skeleton ────────────────────────────────────────────────────────────


OUTLINE = [
    {"title": "Why now", "variant": "title", "visual": "none", "focus": ""},
    {"title": "The market", "variant": "content", "visual": "chart:bar", "focus": ""},
    {"title": "Takeaways", "variant": "content", "visual": "none", "focus": ""},
]


@pytest.mark.parametrize(
    "query,expected",
    [
        ("make me a quiz on the future of AI", "The Future of AI"),
        ("build a mind map covering RAG pipelines", "RAG Pipelines"),
        ("please generate a short quiz on SQL joins", "SQL Joins"),
        ("can you put together a quiz about b2b saas pricing", "B2B SAAS Pricing"),
    ],
)
def test_a_title_is_derived_from_the_request_alone(query, expected):
    assert title_from_request(query) == expected


@pytest.mark.parametrize(
    "query",
    ["", "   ", "create a presentation", "make a deck about " + "x" * 200],
)
def test_an_underivable_title_is_empty_rather_than_wrong(query):
    assert title_from_request(query) == ""


def test_the_shell_never_raises():
    assert shell_from_request(None, kind="quiz") is not None


# ── replay policy ───────────────────────────────────────────────────────────
# Which messages survive to a client that connects late. The instant shell is
# broadcast before the browser can open its event stream at all, so getting this
# wrong means shipping a deck frame nobody can receive.


def test_a_created_surface_is_a_snapshot():
    assert is_snapshot(create_surface_msg("s", surface_kind="presentation", root="d"))


def test_a_retraction_is_a_snapshot():
    """It must reach a late joiner too, or they keep a surface that was taken back."""
    assert is_snapshot(delete_surface_msg("s"))


def test_component_and_data_batches_are_increments():
    assert not is_snapshot(update_components_msg("s", [{"id": "a", "component": "Text"}]))
    assert not is_snapshot(update_data_model_msg("s", "/k", 1))


@pytest.mark.parametrize("junk", [None, {}, "createSurface", 42, []])
def test_nonsense_is_not_a_snapshot(junk):
    assert is_snapshot(junk) is False


def test_replaying_only_the_snapshots_still_yields_the_final_surface():
    """What a late joiner actually receives: they miss the batches and land on
    the committed surface, which is the whole point of committing one."""
    _, sent = _stream(DECK)
    snapshots = [m for m in sent if is_snapshot(m)]
    final = surface_to_messages(DECK, "sid")
    assert apply_messages(snapshots + final) == DECK


# ── shells for the other canvases ───────────────────────────────────────────
# A shell is a PROMISE that a surface of this shape is coming. It is offered for
# the kinds that own a canvas and never get dropped, and withheld from the ones
# the prose gate can retract.


@pytest.mark.parametrize(
    "kind", ["quiz", "flashcards", "mindmap", "map"]
)
def test_a_non_deck_shell_is_one_placeholder_on_its_own_canvas(kind):
    shell = shell_from_request(f"make me a {kind} about python", kind=kind)
    assert shell["surfaceKind"] == kind
    assert shell["root"] == "shell"
    assert len(shell["components"]) == 1
    node = shell["components"][0]
    assert node["component"] == "Skeleton"
    assert node["variant"] == kind
    assert node["pending"] is True


def test_shellable_kinds_split_into_certain_and_retractable():
    """Both halves exist on purpose, and the split is not free-form.

    A kind that owns its own canvas can never be dropped, so its frame is a
    promise always kept and the client may silence the prose beneath it. A kind
    living on dashboard/document CAN be dropped as prose-only, so its frame is
    provisional and the text keeps flowing underneath. `RETRACTABLE_SHELL_KINDS`
    must be exactly the second half — the client keys its behaviour off it.
    """
    kinds = set(SHELLABLE_KINDS.values())
    certain = kinds - GATED_SURFACE_KINDS
    retractable = kinds & GATED_SURFACE_KINDS

    assert certain == {"quiz", "flashcards", "mindmap", "map"}
    assert retractable == RETRACTABLE_SHELL_KINDS
    assert retractable == {"dashboard", "document"}


def test_the_quiz_shell_carries_the_derived_title():
    shell = shell_from_request("make me a quiz about SQL joins", kind="quiz")
    assert shell["components"][0]["title"] == "SQL Joins"


# ── catch-up checkpoints ────────────────────────────────────────────────────
# A run's event stream belongs to whichever session the reader is viewing, so
# switching away closes it and the increments produced meanwhile are never
# delivered. Only snapshots replay on reconnect, so periodic ones bound how far
# behind a returning reader can be.

BIG_DECK = {
    "surfaceKind": "presentation",
    "root": "deck",
    "components": (
        [{"id": "deck", "component": "SlideDeck", "children": [f"s{i}" for i in range(30)]}]
        + [
            {"id": f"s{i}", "component": "Slide", "variant": "content", "title": f"Slide {i}"}
            for i in range(30)
        ]
    ),
    "dataModel": {},
}


def test_a_long_surface_leaves_catch_up_points():
    _, sent = _stream(BIG_DECK, chunk=40)
    snapshots = [m for m in sent if "createSurface" in m]
    assert len(snapshots) > 1, "no checkpoint was ever emitted"


def test_a_checkpoint_carries_everything_so_far():
    """It has to stand alone — that is the entire point of replaying it."""
    _, sent = _stream(BIG_DECK, chunk=40)
    snapshots = [m for m in sent if "createSurface" in m]
    last = snapshots[-1]["createSurface"]
    assert last["root"] == "deck"
    assert len(last["components"]) >= SurfaceStreamer.CHECKPOINT_EVERY


def test_a_reader_who_sees_only_snapshots_still_converges():
    """Exactly what a client that switched away and came back receives: it
    missed the increments, and lands on the newest snapshot instead."""
    _, sent = _stream(BIG_DECK, chunk=40)
    snapshots = [m for m in sent if "createSurface" in m]
    rebuilt = apply_messages(snapshots + surface_to_messages(BIG_DECK, "sid"))
    assert rebuilt == BIG_DECK


def test_checkpoints_can_be_switched_off():
    sent = []
    st = SurfaceStreamer("sid", sent.append, checkpoint_every=0)
    raw = json.dumps(BIG_DECK)
    for i in range(0, len(raw), 40):
        st.feed(raw[: i + 40])
    assert len([m for m in sent if "createSurface" in m]) == 1
    assert apply_messages(sent) == BIG_DECK


def test_checkpoints_do_not_change_the_result():
    """They are redundancy, not content — the round trip must be unaffected."""
    _, sent = _stream(BIG_DECK, chunk=40)
    assert apply_messages(sent) == BIG_DECK
