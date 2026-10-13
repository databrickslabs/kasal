"""Graph memory's pure parts: keys, keyword tokens, rank fusion, and the
tolerant reading of an extraction reply."""

from src.services.memory.engine.extract import (
    MAX_FACTS_PER_WRITE,
    extract_facts,
    parse_facts,
)
from src.services.memory.engine.graph_types import (
    entity_key,
    fuse,
    keyword_tokens,
    rank_hits,
    relation_key,
)

from .graph_fakes import ScriptedLLM, fact


class TestKeys:
    def test_spellings_of_one_entity_share_a_key(self):
        assert entity_key("Zürich") == entity_key("  zurich ") == "zurich"
        assert entity_key("Project-Atlas") == entity_key("project atlas")

    def test_relation_is_bounded_snake_case(self):
        assert relation_key("Preferred Output-Format") == "preferred_output_format"
        assert len(relation_key("x" * 200)) <= 40

    def test_keyword_tokens_drop_stopwords_and_punctuation(self):
        assert keyword_tokens("What is the RTX 3090 Ti's VRAM?") == [
            "rtx",
            "3090",
            "vram",
        ]


class TestRanking:
    def test_fuse_prefers_items_ranked_by_both(self):
        assert fuse(["a", "b", "c"], ["c", "a"])[:2] == ["a", "c"]

    def test_floor_is_the_only_gate_and_keywords_only_rerank(self):
        seeds = rank_hits([("a", 0.9), ("b", 0.6), ("c", 0.2)], ["c", "b"], 5, 0.5)
        # c matched a keyword but is below the floor: never a seed.
        assert [fid for fid, _ in seeds] == ["b", "a"]
        assert dict(seeds) == {"a": 0.9, "b": 0.6}

    def test_nothing_above_the_floor_returns_nothing(self):
        assert rank_hits([("a", 0.3)], ["a"], 5, 0.5) == []


class TestParseFacts:
    def test_seed_relation_flag_overrides_the_model(self):
        facts = parse_facts(
            {
                "facts": [
                    fact(
                        "user",
                        "works_at",
                        "Acme",
                        "The user works at Acme.",
                        single=False,
                    ),
                    fact(
                        "user",
                        "prefers",
                        "tables",
                        "The user prefers tables.",
                        single=True,
                    ),
                ]
            }
        )
        assert [(f.relation, f.single) for f in facts] == [
            ("works_at", True),
            ("prefers", False),
        ]

    def test_coined_relation_keeps_the_models_flag(self):
        (parsed,) = parse_facts(
            {
                "facts": [
                    fact("user", "Favourite Colour", "blue", "It is blue.", single=True)
                ]
            }
        )
        assert (parsed.relation, parsed.single) == ("favourite_colour", True)

    def test_malformed_entries_are_dropped_not_raised(self):
        payload = {
            "facts": [
                "nonsense",
                {"subject": "user", "relation": "prefers"},  # no object/statement
                fact("user", "prefers", "tables", "The user prefers tables."),
                fact("USER", "prefers", "Tables", "Same fact again."),  # repeat
            ]
        }
        assert len(parse_facts(payload)) == 1
        assert parse_facts(None) == [] and parse_facts({"facts": "x"}) == []

    def test_unknown_types_collapse_and_count_is_capped(self):
        many = [
            fact("user", "uses", f"tool {i}", f"Uses tool {i}.", kind="gadget")
            for i in range(20)
        ]
        parsed = parse_facts({"facts": many})
        assert len(parsed) == MAX_FACTS_PER_WRITE
        assert parsed[0].subject_type == "concept" and parsed[0].object_type is None


class TestEntityNames:
    """An entity is named by its bare name. "AWS (Solutions Architect,
    2023-2024)" as an entity makes a different AWS node for every job."""

    def _job(self, obj, **extra):
        item = fact(
            "Dana", "worked_at", obj, "Dana worked there.", object_type="organization"
        )
        (parsed,) = parse_facts({"facts": [{**item, **extra}]})
        return parsed

    def test_qualifiers_stay_off_the_entity_name(self):
        parsed = self._job(
            "Acme", qualifiers={"Role": "Engineer", "from": "2016", "to": 2023}
        )
        assert parsed.object == "Acme"
        assert parsed.qualifiers == {"role": "Engineer", "from": "2016", "to": "2023"}

    def test_a_parenthesis_on_an_entity_becomes_a_qualifier(self):
        parsed = self._job("AWS (Solutions Architect, ~2023 – ~2024)")
        assert parsed.object == "AWS"
        assert parsed.qualifiers == {"detail": "Solutions Architect, ~2023 – ~2024"}

    def test_two_jobs_at_one_company_name_the_same_entity(self):
        first, second = self._job("AWS (engineer, 2019)"), self._job(
            "AWS (architect, 2023)"
        )
        assert first.object == second.object == "AWS"

    def test_a_plain_value_keeps_its_parenthesis(self):
        (parsed,) = parse_facts(
            {
                "facts": [
                    fact("Acme", "revenue_fy2025", "USD 4.2 bn (reported)", "It was.")
                ]
            }
        )
        assert parsed.object == "USD 4.2 bn (reported)" and parsed.qualifiers == {}

    def test_malformed_qualifiers_are_dropped(self):
        assert self._job("Acme", qualifiers="since 2016").qualifiers == {}


class TestExtractFacts:
    def test_no_llm_or_short_text_extracts_nothing(self):
        assert extract_facts(None, "The user works at Acme, remember that.") == []
        llm = ScriptedLLM([fact("user", "uses", "x", "Uses x.")])
        assert extract_facts(llm, "hi") == [] and llm.calls == 0

    def test_failed_call_extracts_nothing(self):
        class Broken:
            def call(self, messages):
                raise RuntimeError("endpoint down")

        assert extract_facts(Broken(), "The user works at Acme, remember that.") == []
