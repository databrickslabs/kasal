"""Guard: the flow listener's runtime Task rebuild must preserve execution config.

flow_methods rebuilds each listener task at runtime to inject previous-step
context. The original code constructed `Task(description=, agent=, expected_output=)`
only — silently dropping tools, output_pydantic, output_json and converter_cls.
That made @listen crews lose their tools (no MCP/tool calls) and their structured
schema (no .pydantic → flaky routing). This guards against regressing to the
field-dropping reconstruction.

Asserted via source because the rebuild lives inside a runtime closure that needs
a fully-built flow to exercise.
"""

from pathlib import Path


def _flow_methods_source() -> str:
    p = (
        Path(__file__).resolve().parents[6]
        / "src" / "engines" / "crewai" / "paths" / "flow" / "modules" / "flow_methods.py"
    )
    return p.read_text()


def test_listener_runtime_task_carries_tools_and_structured_output():
    src = _flow_methods_source()
    # The runtime Task() reconstruction must forward these fields from the
    # original task; otherwise listener crews lose tools + structured output.
    for field in (
        "tools=getattr(task, 'tools'",
        "output_pydantic=getattr(task, 'output_pydantic'",
        "output_json=getattr(task, 'output_json'",
        "converter_cls=getattr(task, 'converter_cls'",
    ):
        assert field in src, f"listener Task rebuild dropped: {field}"
