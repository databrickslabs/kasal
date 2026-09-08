"""Add context without losing guardrails, callbacks or private task identity."""

from copy import copy


def task_with_context(task, context):
    # Both harness Task types implement shallow copying. Keep tool/agent and
    # guardrail identities, without rerunning validators or mutating the catalog.
    result = copy(task)
    result.description = f"{task.description}{context}"
    result.output = None
    return result


def tasks_for_review(tasks, method_name, callbacks):
    """Only the rejected crew receives the reviewer's correction instructions."""
    feedback = (callbacks or {}).get("review_feedback") or {}
    reason = feedback.get("reason")
    if (
        feedback.get("method") != method_name
        or not isinstance(reason, str)
        or not reason.strip()
    ):
        return tasks
    return [
        task_with_context(
            task, f"\n\nHuman review — correct the previous attempt:\n{reason.strip()}"
        )
        for task in tasks
    ]
