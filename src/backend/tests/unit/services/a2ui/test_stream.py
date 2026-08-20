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
    skeleton_from_outline,
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


def test_presentations_are_not_gated():
    """Only the kinds prose degrades into are gated; a deck streams immediately."""
    assert "presentation" not in GATED_SURFACE_KINDS
    raw = json.dumps(DECK)
    sent = []
    SurfaceStreamer("sid", sent.append).feed(raw[: raw.index('"slide_1"')])
    assert sent and "createSurface" in sent[0]


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


def test_skeleton_carries_the_real_titles_and_ids():
    sk = skeleton_from_outline(OUTLINE)
    assert sk["surfaceKind"] == "presentation"
    assert sk["components"][0]["children"] == ["slide_1", "slide_2", "slide_3"]
    assert [c.get("title") for c in sk["components"][1:]] == [
        "Why now",
        "The market",
        "Takeaways",
    ]
    assert all(c["pending"] for c in sk["components"][1:])


def test_skeleton_ids_match_what_the_real_slides_replace():
    """The skeleton is only useful if the real deck overwrites it by id."""
    sk = skeleton_from_outline(OUTLINE)
    msgs = surface_to_messages(sk, "sid")
    real = {
        "id": "slide_2",
        "component": "Slide",
        "variant": "visual",
        "title": "The market",
    }
    msgs.append(
        {
            "version": "v1.0",
            "updateComponents": {"surfaceId": "sid", "components": [real]},
        }
    )
    out = apply_messages(msgs)
    assert len(out["components"]) == 4, "replaced in place, not appended"
    assert out["components"][2] == real


@pytest.mark.parametrize("bad", [None, [], [{"title": "only one"}], "nonsense"])
def test_a_weak_outline_yields_no_skeleton(bad):
    assert skeleton_from_outline(bad) is None


# ── the instant shell ───────────────────────────────────────────────────────
# It ships before any model runs, so it is the only thing the reader can see in
# the first ~20 seconds of a deck request. It has to be right without help.


@pytest.mark.parametrize(
    "query,expected",
    [
        ("create a presentation on how llm works", "How LLM Works"),
        ("Create a presentation about our Q3 results", "Our Q3 Results"),
        ("make me a deck on the future of AI", "The Future of AI"),
        ("build a slide deck covering RAG pipelines", "RAG Pipelines"),
        ("please generate a short presentation on SQL joins", "SQL Joins"),
        ("can you put together a pitch for b2b saas pricing", "B2B SAAS Pricing"),
        ("i want you to create a powerpoint explaining ETL", "ETL"),
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


def test_the_shell_is_a_renderable_deck_immediately():
    shell = shell_from_request("create a presentation on how llm works", slides=6)
    assert shell["surfaceKind"] == "presentation"
    assert shell["root"] == "deck"
    assert len(shell["components"]) == 7  # the deck + 6 slides
    assert shell["components"][0]["children"] == [f"slide_{i}" for i in range(1, 7)]
    assert shell["components"][1]["title"] == "How LLM Works"
    assert all(c["pending"] for c in shell["components"][1:])


def test_the_shell_ids_are_the_ones_the_skeleton_overwrites():
    """Shell -> outline skeleton -> real slides, all replacing in place."""
    shell = shell_from_request("create a presentation on X", slides=3)
    skeleton = skeleton_from_outline(OUTLINE)
    out = apply_messages(
        surface_to_messages(shell, "sid") + surface_to_messages(skeleton, "sid")
    )
    assert [c["id"] for c in out["components"]] == [
        "deck",
        "slide_1",
        "slide_2",
        "slide_3",
    ]
    assert out["components"][1]["title"] == "Why now"  # the outline's real title


# 0/None read as "unspecified" and take the default; a real number is clamped.
@pytest.mark.parametrize("n,expected", [(0, 8), (None, 8), (1, 3), (8, 8), (99, 24), (-4, 3)])
def test_the_shell_slide_count_is_bounded(n, expected):
    assert len(shell_from_request("a presentation on X", slides=n)["components"]) == expected + 1


def test_the_shell_never_raises():
    assert shell_from_request(None) is not None


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

    assert certain == {"presentation", "quiz", "flashcards", "mindmap", "map"}
    assert retractable == RETRACTABLE_SHELL_KINDS
    assert retractable == {"dashboard", "document"}


def test_a_deck_shell_is_still_slides_not_a_skeleton():
    """A deck's frame carries REAL titles from the outline, so it uses Slide."""
    shell = shell_from_request("create a presentation on X", kind="presentation")
    assert {c["component"] for c in shell["components"]} == {"SlideDeck", "Slide"}


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
