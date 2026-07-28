"""Shaping a run's output for both protocols.

The shaping decision is made once here so an MCP tool result and an A2A
Artifact carry the same content — only the field names differ.
"""

from src.services.external.artifacts import build


class TestTextResults:
    def test_prose_becomes_a_text_part(self):
        artifact = build("the answer")
        assert artifact.text == "the answer"
        assert artifact.as_dict() == {"parts": [{"kind": "text", "text": "the answer"}]}

    def test_none_produces_no_parts(self):
        assert build(None).parts == []
        assert build(None).text is None


class TestStructuredResults:
    def test_a_json_string_is_offered_as_BOTH_text_and_data(self):
        """Not instead. The caller may want either, and discarding the readable
        form to 'helpfully' return structure is how a human-facing client ends
        up showing nothing."""
        artifact = build('{"score": 9}')
        kinds = [p.kind for p in artifact.parts]
        assert kinds == ["text", "data"]
        assert artifact.text == '{"score": 9}'
        assert artifact.parts[1].content == {"score": 9}

    def test_text_that_only_looks_like_json_stays_text(self):
        artifact = build("{not actually json")
        assert [p.kind for p in artifact.parts] == ["text"]

    def test_an_engine_wrapper_yields_prose_and_the_full_structure(self):
        artifact = build({"raw": "the summary", "usage": {"tokens": 10}})
        assert artifact.text == "the summary"
        data = [p for p in artifact.parts if p.kind == "data"][0]
        assert data.content["usage"] == {"tokens": 10}

    def test_a_dict_with_no_prose_is_data_only(self):
        artifact = build({"rows": [1, 2, 3]})
        assert [p.kind for p in artifact.parts] == ["data"]
        assert artifact.text is None

    def test_a_list_is_data(self):
        artifact = build([1, 2, 3])
        assert [p.kind for p in artifact.parts] == ["data"]
        assert artifact.parts[0].content == [1, 2, 3]


class TestWireShape:
    def test_part_kind_follows_the_a2a_vocabulary(self):
        """text / data / url — the published standard. MCP has no equivalent to
        borrow, so A2A's names are used on both surfaces."""
        assert build("x").as_dict()["parts"][0] == {"kind": "text", "text": "x"}
        assert build({"a": 1}).as_dict()["parts"][0] == {
            "kind": "data",
            "data": {"a": 1},
        }
