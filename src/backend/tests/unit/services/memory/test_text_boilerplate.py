"""What memory keeps of a task must not be the run scaffold."""

from src.services.memory.text import strip_run_boilerplate


def test_the_restate_brief_is_stripped_like_the_research_one():
    text = (
        "ANSWER FROM THE CONVERSATION — the transcript above already holds what "
        "this turn needs:\nprovide the features in a short compact table\n\n"
        "Restate or reshape that answer as asked. Use a tool only for something "
        "the transcript does not contain; do not research the question again.\n\n"
        "Expected output: A helpful, complete answer to the user's request."
    )
    assert (
        strip_run_boilerplate(text).strip()
        == "provide the features in a short compact table"
    )


def test_the_research_grounding_is_still_stripped():
    text = (
        "USER REQUEST — this run exists to answer it:\nprovide me swiss news\n\n"
        "MCP data sources attached — query them for data questions: browser\n\n"
        "Expected output: A helpful, complete answer to the user's request."
    )
    assert strip_run_boilerplate(text).strip() == "provide me swiss news"
