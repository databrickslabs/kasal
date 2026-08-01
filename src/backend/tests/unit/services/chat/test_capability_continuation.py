"""Staying with a capability that is holding the conversation.

Turn 1 routes "swiss news please" to a flow. Turn 2 is "and Germany?" — a
fragment that matches nothing in the catalogue on its own words. Without this,
the router declines, the chat answers instead, and the flow never learns the
turn happened: its state silently stops tracking the conversation the user is
having.

Two halves, tested here:

* the router SEES which capability produced each answer, so it can pick the same
  one deliberately;
* when it declines anyway, a capability that is mid-conversation gets the turn
  rather than the chat.
"""

from types import SimpleNamespace

from src.schemas.crew_publication import PublishedCapability
from src.services.chat.capability_router import (
    CONTINUATION_CONFIDENCE,
    continue_decision,
    held_conversation,
    render_route_catalog,
)
from src.services.chat.conversation_context import Turn, render_turns

NEWS_FLOW = PublishedCapability(
    entity_type="flow",
    entity_id="flow-1",
    name="swiss_news_flow",
    description="Researches news and produces a briefing.",
    conversational=True,
)
ONE_SHOT = PublishedCapability(
    entity_type="crew",
    entity_id="crew-1",
    name="risk_review",
    description="Runs the quarterly risk review.",
)


def answer(index, capability=None, preview="a briefing"):
    return Turn(
        index=index,
        role="assistant",
        preview=preview,
        content=preview,
        capability=capability,
    )


def question(index, text="and Germany?"):
    return Turn(index=index, role="user", preview=text, content=text)


class TestTheRouterCanSeeWhoAnswered:
    def test_an_answer_names_the_capability_that_produced_it(self):
        rendered = render_turns(
            [question(1, "swiss news"), answer(2, "swiss_news_flow")]
        )

        assert "[answer 2, from swiss_news_flow]" in rendered

    def test_an_answer_from_the_chat_itself_names_nothing(self):
        rendered = render_turns([answer(1)])

        assert "[answer 1]" in rendered
        assert "from" not in rendered

    def test_the_catalogue_says_which_capabilities_hold_a_conversation(self):
        rendered = render_route_catalog([NEWS_FLOW, ONE_SHOT])

        assert "holds a conversation" in rendered.split("risk_review")[0]
        assert "holds a conversation" not in rendered.split("risk_review")[1]


class TestHeldConversation:
    def test_the_last_answer_decides(self):
        turns = [question(1), answer(2, "swiss_news_flow"), question(3)]

        assert held_conversation(turns, [NEWS_FLOW, ONE_SHOT]) is NEWS_FLOW

    def test_a_one_shot_capability_never_holds_one(self):
        # A crew that answers once has no state to continue, and re-running it
        # on "why?" would bill a full run to answer a question about its output.
        turns = [answer(1, "risk_review")]

        assert held_conversation(turns, [NEWS_FLOW, ONE_SHOT]) is None

    def test_a_more_recent_answer_from_elsewhere_ends_it(self):
        # Once the user has been answered by something else, the conversation
        # has moved; dragging it back would be worse than declining.
        turns = [answer(1, "swiss_news_flow"), answer(2, "risk_review")]

        assert held_conversation(turns, [NEWS_FLOW, ONE_SHOT]) is None

    def test_a_capability_no_longer_in_the_catalogue_does_not_hold_one(self):
        # Unpublishing it, or losing visibility to this group, takes effect on
        # the next turn rather than being remembered from history.
        turns = [answer(1, "swiss_news_flow")]

        assert held_conversation(turns, [ONE_SHOT]) is None

    def test_a_capability_that_stopped_being_conversational_does_not_hold_one(self):
        turns = [answer(1, "swiss_news_flow")]
        no_longer = NEWS_FLOW.model_copy(update={"conversational": False})

        assert held_conversation(turns, [no_longer]) is None

    def test_no_conversation_holds_nothing(self):
        assert held_conversation([], [NEWS_FLOW]) is None
        assert held_conversation([question(1)], [NEWS_FLOW]) is None


class TestContinuationDecision:
    def test_it_routes_to_the_capability_holding_the_conversation(self):
        decision = continue_decision(NEWS_FLOW, "and Germany?")

        assert decision.capability == "swiss_news_flow"
        assert decision.is_confident

    def test_it_extracts_no_inputs(self):
        # The turn's text reaches the flow as its user message, which is what a
        # conversational flow reads. Inventing input values from a fragment is
        # exactly the guessing the extraction rules forbid.
        assert continue_decision(NEWS_FLOW, "and Germany?").inputs == {}

    def test_its_confidence_is_distinguishable_from_a_model_match(self):
        # This is a structural inference, not a semantic match the model made,
        # and a trials table should be able to tell the two apart.
        assert continue_decision(NEWS_FLOW, "x").confidence == CONTINUATION_CONFIDENCE

    def test_it_says_why(self):
        assert "conversation" in continue_decision(NEWS_FLOW, "x").reason


class TestCapabilityIsReadFromTheStoredTurn:
    def test_a_routed_answer_carries_its_capability(self):
        from src.services.chat.conversation_context import _capability_of

        row = SimpleNamespace(
            generation_result={"__chatmode": {"capability": "swiss_news_flow"}}
        )

        assert _capability_of(row) == "swiss_news_flow"

    def test_an_older_row_simply_has_none(self):
        from src.services.chat.conversation_context import _capability_of

        assert _capability_of(SimpleNamespace(generation_result=None)) is None
        assert _capability_of(SimpleNamespace(generation_result={})) is None
        assert (
            _capability_of(SimpleNamespace(generation_result={"__chatmode": {}}))
            is None
        )
