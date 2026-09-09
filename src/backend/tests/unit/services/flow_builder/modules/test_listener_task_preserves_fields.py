"""Context injection must preserve the task policies on both execution engines."""

import pytest
from pydantic import BaseModel

from src.services.flow_builder.modules.task_context import task_with_context


@pytest.mark.parametrize("engine", ["kasal", "crewai"])
def test_context_preserves_human_review_schema_and_identity(engine):
    if engine == "kasal":
        from src.services.execution.runtime import Task
    else:
        from crewai import Task

    class Output(BaseModel):
        value: str

    def review(output):
        return True, output

    task = Task(
        description="Original",
        expected_output="JSON",
        guardrail=review,
        output_pydantic=Output,
        guardrail_max_retries=4,
    )
    task._kasal_task_id = "catalog-task"
    updated = task_with_context(task, "\nSource output")
    assert updated.description == "Original\nSource output"
    assert task.description == "Original"
    assert updated is not task
    assert updated.guardrail is task.guardrail
    assert updated.output_pydantic is Output
    assert updated.guardrail_max_retries == 4
    assert updated._kasal_task_id == "catalog-task"
    assert updated.id == task.id


def test_retry_feedback_only_changes_the_reviewed_crew():
    from types import SimpleNamespace

    from src.services.flow_builder.modules.task_context import tasks_for_review

    task = SimpleNamespace(description="Research", output=None)
    callbacks = {
        "review_feedback": {"method": "research", "reason": "Use newer sources"}
    }
    assert tasks_for_review([task], "other", callbacks)[0] is task
    reviewed = tasks_for_review([task], "research", callbacks)[0]
    assert "Use newer sources" in reviewed.description
    assert task.description == "Research"
