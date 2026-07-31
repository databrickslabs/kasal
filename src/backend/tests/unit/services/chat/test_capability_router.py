"""The extraction contract: nothing is bound that the user did not say.

The single highest-risk behaviour in capability routing. A MISSING value is
handled — the user is asked. An INVENTED value is not: nothing looks wrong, no
card renders, the run completes cleanly, and the answer is for the wrong quarter.

So the bulk of this file is prompts that OMIT a required field, asserting the
field comes back absent rather than plausibly filled. The span check is what
makes that mechanical instead of a request the prompt makes politely.
"""

import pytest

from src.schemas.crew_publication import PublishedCapability
from src.seeds.prompt_templates import ROUTE_CAPABILITY_TEMPLATE
from src.services.chat.capability_router import (
    ROUTE_CONFIDENCE_THRESHOLD,
    build_route_messages,
    declared_fields,
    parse_route_response,
    render_route_catalog,
)

RISK_REVIEW = PublishedCapability(
    entity_type="crew",
    entity_id="11111111-1111-1111-1111-111111111111",
    name="quarterly_risk_review",
    description="Runs the quarterly risk review for one region.",
    input_schema={
        "type": "object",
        "properties": {
            "region": {"type": "string", "description": "The market region"},
            "quarter": {"type": "string"},
            "format": {"type": "string"},
        },
        "required": ["region", "quarter"],
    },
)

NEWS_DIGEST = PublishedCapability(
    entity_type="flow",
    entity_id="22222222-2222-2222-2222-222222222222",
    name="news_digest",
    description="Summarises today's news for a topic.",
    input_schema=None,
)

CAPABILITIES = [RISK_REVIEW, NEWS_DIGEST]


def _response(**overrides):
    payload = {
        "capability": "quarterly_risk_review",
        "confidence": 0.9,
        "inputs": {},
        "reason": "matches the risk review",
    }
    payload.update(overrides)
    return payload


class TestDeclaredFields:
    def test_reads_properties_and_required(self):
        names, required = declared_fields(RISK_REVIEW)
        assert names == ["region", "quarter", "format"]
        assert required == ["region", "quarter"]

    def test_no_schema_declares_nothing(self):
        # Every publication created before the publish dialog wrote schemas is
        # in this state. It routes; it just extracts nothing.
        assert declared_fields(NEWS_DIGEST) == ([], [])

    def test_absent_required_is_not_empty_required(self):
        # Absent means nobody has said, so everything counts as required.
        # Empty means the publisher said nothing is. They must not collapse.
        absent = PublishedCapability(
            entity_id="x",
            name="a",
            description="d",
            input_schema={"type": "object", "properties": {"a": {"type": "string"}}},
        )
        empty = PublishedCapability(
            entity_id="y",
            name="b",
            description="d",
            input_schema={
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": [],
            },
        )
        assert declared_fields(absent) == (["a"], ["a"])
        assert declared_fields(empty) == (["a"], [])


class TestRouteCatalog:
    """The catalog is per-workspace DATA, so it rides in the user message.

    Not in the system prompt: that is the ``route_capability`` template row, and
    an optimisation run against a prompt with one workspace's crews baked into
    it would be tuning on that workspace.
    """

    def test_lists_every_capability_with_its_description(self):
        catalog = render_route_catalog(CAPABILITIES)
        for cap in CAPABILITIES:
            assert cap.name in catalog
            assert cap.description in catalog

    def test_marks_required_and_optional_inputs(self):
        catalog = render_route_catalog(CAPABILITIES)
        assert "region (required)" in catalog
        assert "format (optional)" in catalog
        assert "The market region" in catalog

    def test_says_so_when_a_capability_declares_nothing(self):
        assert "inputs: none declared" in render_route_catalog([NEWS_DIGEST])

    def test_catalog_is_not_in_the_system_prompt(self):
        messages = build_route_messages("anything", CAPABILITIES)
        catalog = render_route_catalog(CAPABILITIES)
        assert catalog in messages[1]["content"]
        assert catalog not in messages[0]["content"]
        # The template's own worked examples happen to mention a risk review,
        # so match on a capability the examples cannot know about.
        assert NEWS_DIGEST.description not in messages[0]["content"]

    def test_uses_the_supplied_template_when_there_is_one(self):
        messages = build_route_messages("hi", CAPABILITIES, "GROUP OVERRIDE")
        assert messages[0]["content"] == "GROUP OVERRIDE"

    def test_falls_back_to_the_seed_the_row_comes_from(self):
        # Not to a second copy of the prompt. One source, so it cannot drift.
        system = build_route_messages("hi", CAPABILITIES)[0]["content"]
        assert system == ROUTE_CAPABILITY_TEMPLATE
        assert "DO NOT INFER IT" in system
        assert "DO NOT SUPPLY A DEFAULT" in system

    def test_carries_the_user_message_verbatim(self):
        content = build_route_messages("Kick off the Q3 review", CAPABILITIES)[1][
            "content"
        ]
        assert content.endswith("USER MESSAGE\nKick off the Q3 review")
        assert "quarterly_risk_review" in content


class TestPicking:
    def test_binds_values_the_user_actually_stated(self):
        decision = parse_route_response(
            _response(
                inputs={
                    "region": {"value": "DACH", "source_span": "DACH"},
                    "quarter": {"value": "Q3", "source_span": "Q3"},
                }
            ),
            "Kick off the Q3 risk review for DACH",
            CAPABILITIES,
        )
        assert decision.capability == "quarterly_risk_review"
        assert decision.inputs == {"region": "DACH", "quarter": "Q3"}
        assert decision.is_confident

    def test_null_capability_is_a_clean_decline(self):
        decision = parse_route_response(
            _response(capability=None), "something else entirely", CAPABILITIES
        )
        assert decision.capability is None
        assert not decision.is_confident

    def test_a_name_not_in_the_catalog_is_refused(self):
        # Our prompt bug, not the user's. It must not resolve to anything.
        decision = parse_route_response(
            _response(capability="some_other_crew"), "run it", CAPABILITIES
        )
        assert decision.capability is None

    def test_low_confidence_is_not_confident(self):
        decision = parse_route_response(
            _response(confidence=ROUTE_CONFIDENCE_THRESHOLD - 0.01),
            "run the risk review",
            CAPABILITIES,
        )
        assert decision.capability == "quarterly_risk_review"
        assert not decision.is_confident

    def test_confidence_out_of_range_is_clamped(self):
        assert (
            parse_route_response(
                _response(confidence=1.4), "run it", CAPABILITIES
            ).confidence
            == 1.0
        )

    def test_unusable_output_returns_none(self):
        assert parse_route_response("not json at all", "x", CAPABILITIES) is None
        assert parse_route_response(["a", "list"], "x", CAPABILITIES) is None


class TestDoesNotInvent:
    """Five prompts that omit a required field, and one fabricated span."""

    @pytest.mark.parametrize(
        "message,fabricated,span",
        [
            # Region stated, quarter is not. Q3 is plausible; the user never
            # said it, and the whole point is that this cannot slip through.
            ("Kick off the risk review for DACH", "quarter", "Q3"),
            ("Run the risk review for DACH please", "quarter", "this quarter"),
            # Quarter stated, region is not.
            ("Run the Q3 risk review", "region", "EMEA"),
            ("Do the Q3 risk review now", "region", "the usual region"),
            # Neither stated.
            ("Run the risk review", "quarter", "Q4"),
        ],
    )
    def test_a_value_with_no_quote_in_the_message_is_dropped(
        self, message, fabricated, span
    ):
        decision = parse_route_response(
            _response(inputs={fabricated: {"value": "made up", "source_span": span}}),
            message,
            CAPABILITIES,
        )
        assert fabricated not in decision.inputs
        assert decision.dropped[fabricated] == (
            "source_span is not in the user's message"
        )

    def test_an_explicit_null_is_the_correct_answer(self):
        decision = parse_route_response(
            _response(
                inputs={
                    "region": {"value": "DACH", "source_span": "DACH"},
                    "quarter": None,
                }
            ),
            "Kick off the risk review for DACH",
            CAPABILITIES,
        )
        assert decision.inputs == {"region": "DACH"}
        # A null is expected, not an error — nothing to report.
        assert decision.dropped == {}

    def test_a_field_the_capability_never_declared_is_dropped(self):
        decision = parse_route_response(
            _response(inputs={"urgency": {"value": "high", "source_span": "urgently"}}),
            "Run the risk review urgently",
            CAPABILITIES,
        )
        assert decision.inputs == {}
        assert "urgency" in decision.dropped

    def test_nothing_binds_for_a_capability_with_no_schema(self):
        decision = parse_route_response(
            _response(
                capability="news_digest",
                inputs={"topic": {"value": "AI", "source_span": "AI"}},
            ),
            "Summarise the AI news",
            CAPABILITIES,
        )
        assert decision.inputs == {}

    def test_a_quote_survives_case_and_whitespace_differences(self):
        # A model that quotes "DACH" where the user typed "dach" has still
        # quoted. Failing that would drop good values and teach nobody anything.
        decision = parse_route_response(
            _response(
                inputs={"region": {"value": "DACH", "source_span": "DACH  region"}}
            ),
            "run the risk review for the dach region",
            CAPABILITIES,
        )
        assert decision.inputs == {"region": "DACH"}

    def test_a_missing_span_is_not_a_quote(self):
        decision = parse_route_response(
            _response(inputs={"region": {"value": "DACH"}}),
            "run the risk review for DACH",
            CAPABILITIES,
        )
        assert decision.inputs == {}

    def test_an_empty_value_binds_nothing(self):
        decision = parse_route_response(
            _response(inputs={"region": {"value": "   ", "source_span": "DACH"}}),
            "run the risk review for DACH",
            CAPABILITIES,
        )
        assert decision.inputs == {}
