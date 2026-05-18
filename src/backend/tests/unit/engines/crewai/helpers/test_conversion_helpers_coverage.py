"""
Coverage tests for engines/crewai/helpers/conversion_helpers.py
Covers missing lines: 41-44 (knowledge_sources branch), 63-70 (tool_configs branch)
"""
from src.engines.crewai.helpers.conversion_helpers import extract_crew_yaml_data


def test_extract_agent_with_knowledge_sources():
    """Test agent with knowledge_sources field (lines 41-44)."""
    agents_yaml = {
        "researcher": {
            "role": "Researcher",
            "knowledge_sources": ["doc1.pdf", "doc2.pdf", "doc3.pdf"],
        }
    }
    tasks_yaml = {}

    agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
    assert len(agents_data) == 1
    assert agents_data[0]["knowledge_sources"] == ["doc1.pdf", "doc2.pdf", "doc3.pdf"]
    assert agents_data[0]["id"] == "researcher"


def test_extract_agent_without_knowledge_sources():
    """Test agent without knowledge_sources (line 46)."""
    agents_yaml = {
        "researcher": {
            "role": "Researcher",
            # No knowledge_sources
        }
    }
    tasks_yaml = {}

    agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
    assert len(agents_data) == 1
    assert "knowledge_sources" not in agents_data[0]


def test_extract_task_with_tool_configs():
    """Test task with tool_configs field (lines 63-70)."""
    tasks_yaml = {
        "search_task": {
            "description": "Search the web",
            "tool_configs": {
                "search_tool": {
                    "api_key": "short_key",
                    "endpoint": "https://api.example.com/search",
                    "long_value": "x" * 50,  # > 30 chars, should be truncated in log
                },
                "another_tool": "not_a_dict",  # non-dict tool config
            }
        }
    }
    agents_yaml = {}

    agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
    assert len(tasks_data) == 1
    assert tasks_data[0]["tool_configs"]["search_tool"]["api_key"] == "short_key"


def test_extract_task_without_tool_configs():
    """Test task without tool_configs (line 72)."""
    tasks_yaml = {
        "simple_task": {
            "description": "Do something",
            # No tool_configs
        }
    }
    agents_yaml = {}

    agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
    assert len(tasks_data) == 1
    assert "tool_configs" not in tasks_data[0]


def test_extract_with_both_knowledge_sources_and_tool_configs():
    """Test full scenario with both fields."""
    agents_yaml = {
        "smart_agent": {
            "role": "Smart Agent",
            "knowledge_sources": ["manual.pdf"],
        }
    }
    tasks_yaml = {
        "smart_task": {
            "description": "Smart task",
            "tool_configs": {
                "web_tool": {"base_url": "https://web.example.com"},
            }
        }
    }

    agents_data, tasks_data = extract_crew_yaml_data(agents_yaml, tasks_yaml)
    assert agents_data[0]["knowledge_sources"] == ["manual.pdf"]
    assert tasks_data[0]["tool_configs"]["web_tool"]["base_url"] == "https://web.example.com"
