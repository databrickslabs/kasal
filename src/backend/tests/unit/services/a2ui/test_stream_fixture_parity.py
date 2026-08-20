"""The streamed wire format is a contract between two languages.

``services/a2ui/stream.py`` produces the messages; the frontend's
``shared/a2ui/stream.ts`` consumes them. Neither one's tests can catch a change
that breaks the other, so a committed fixture sits between them: this test
regenerates it from the live streamer, and the frontend test replays exactly
these bytes through the TypeScript reducer.

If this fails, the Python side changed. Regenerate the fixture AND check the
reducer still handles it — do not just paste the new bytes in.
"""

import json
import pathlib

from src.services.a2ui.stream import SurfaceStreamer, apply_messages

FIXTURE = (
    pathlib.Path(__file__).parents[4]
    / ".."
    / "frontend"
    / "src"
    / "shared"
    / "a2ui"
    / "stream.test.fixture.json"
).resolve()

#: The surface the fixture is generated from. Kept here rather than in the
#: fixture so a hand-edit of the JSON cannot quietly redefine the expectation.
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

CHUNK = 9  # the slice size the fixture was generated with


def _regenerate():
    sent = []
    s = SurfaceStreamer("fixture", sent.append)
    raw = json.dumps(DECK)
    for i in range(0, len(raw), CHUNK):
        s.feed(raw[: i + CHUNK])
    return sent


def test_the_fixture_exists_where_the_frontend_test_reads_it():
    assert FIXTURE.exists(), f"missing {FIXTURE}"


def test_the_fixture_is_what_the_streamer_currently_emits():
    """A drifted fixture means the frontend is being tested against a wire format
    the backend no longer speaks."""
    committed = json.loads(FIXTURE.read_text())

    assert committed["messages"] == _regenerate(), (
        "stream.py's output changed. Regenerate stream.test.fixture.json and confirm "
        "shared/a2ui/stream.ts still applies it correctly."
    )
    assert committed["expected"] == DECK


def test_the_fixture_round_trips_on_this_side_too():
    assert apply_messages(json.loads(FIXTURE.read_text())["messages"]) == DECK
