"""A restate/reshape turn is grounded on the transcript, not sent to research.

With "USER REQUEST — this run exists to answer it: gather …" plus "MCP data
sources attached — query them", the light agent researched a report it could
already see in full. A turn the dispatcher marks ``answer_from_conversation``
gets a different brief; the MCP servers stay attached as tools for the gaps.
"""

from src.schemas.crew import CrewStreamingRequest
from src.services.generation.crews import (
    ANSWER_FROM_CONVERSATION_RULE,
    CrewGenerationService,
)


def _task(**kw):
    req = CrewStreamingRequest(
        prompt="p",
        original_prompt="gather crewAI agentic features",
        mcp_servers=["browser"],
        **kw,
    )
    agents = [
        {"id": "a1", "role": "Assistant", "goal": "g", "backstory": "b", "tools": []}
    ]
    tasks = [
        {
            "id": "t1",
            "description": "Respond directly and helpfully to the user's request.",
            "expected_output": "out",
            "agent_id": "a1",
            "context": [],
            "tools": [],
        }
    ]
    cfg = CrewGenerationService.build_crew_config_from_generated(req, agents, tasks)
    return cfg["tasks_yaml"]["task_t1"]


class TestAnswerFromConversationGrounding:
    def test_a_fresh_request_keeps_the_research_grounding(self):
        desc = _task()["description"]
        assert (
            "USER REQUEST — this run exists to answer it:\ngather crewAI agentic features"
            in desc
        )
        assert (
            "MCP data sources attached — query them for data questions: browser" in desc
        )

    def test_a_restate_turn_is_grounded_on_the_transcript(self):
        task = _task(answer_from_conversation=True)
        desc = task["description"]
        assert "ANSWER FROM THE CONVERSATION" in desc
        assert "gather crewAI agentic features" in desc
        assert ANSWER_FROM_CONVERSATION_RULE in desc
        assert "this run exists to answer it" not in desc
        assert "query them for data questions" not in desc
        # The servers stay attached: a gap in the transcript may still need them.
        assert task["tool_configs"]["MCP_SERVERS"] == {"servers": ["browser"]}
