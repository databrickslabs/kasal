"""The agent's plan for the task it is doing.

The behaviours that matter here are the ones a long task depends on: the plan
survives across tool rounds, it can be REWRITTEN when evidence contradicts it,
it does not leak between tasks, and — the one the hierarchical process gets
wrong without help — a delegated task does not clobber its manager's plan.
"""

import pytest

from src.services.execution.runtime.plan import (
    MAX_CONTENT_CHARS,
    MAX_ITEMS,
    build_plan_tool,
    plan,
    plan_counts,
    plan_scope,
    plan_summary,
    render_plan,
    reset_plan,
    unfinished_plan_items,
    write_plan,
)


@pytest.fixture(autouse=True)
def _fresh_plan():
    reset_plan()
    yield
    reset_plan()


def _items(*specs):
    return [{"id": i, "content": c, "status": s} for i, c, s in specs]


class TestWritingAPlan:
    def test_a_written_plan_reads_back(self):
        write_plan(_items(("1", "read the schema", "in_progress")))
        assert [(i.id, i.status) for i in plan()] == [("1", "in_progress")]

    def test_order_is_priority_and_is_preserved(self):
        write_plan(_items(("a", "first", "pending"), ("b", "second", "pending")))
        assert [i.id for i in plan()] == ["a", "b"]

    def test_a_replan_replaces_the_whole_list(self):
        """The point of the tool: evidence made the old decomposition wrong."""
        write_plan(_items(("1", "regional breakdown", "pending")))
        write_plan(_items(("2", "segment breakdown", "pending")))
        assert [i.id for i in plan()] == ["2"]

    def test_merge_updates_by_id_and_appends_new(self):
        write_plan(
            _items(("1", "read schema", "in_progress"), ("2", "build view", "pending"))
        )
        write_plan(
            _items(("1", "read schema", "completed"), ("3", "publish", "pending")),
            merge=True,
        )
        assert [(i.id, i.status) for i in plan()] == [
            ("1", "completed"),
            ("2", "pending"),
            ("3", "pending"),
        ]

    def test_an_unknown_status_falls_back_to_pending(self):
        write_plan([{"id": "1", "content": "x", "status": "almost-done"}])
        assert plan()[0].status == "pending"

    def test_items_without_id_or_content_are_dropped(self):
        write_plan(
            [
                {"id": "", "content": "x"},
                {"id": "1", "content": ""},
                {"id": "2", "content": "ok"},
            ]
        )
        assert [i.id for i in plan()] == ["2"]

    def test_content_and_item_count_are_bounded(self):
        write_plan(
            [{"id": str(n), "content": "x" * 5000} for n in range(MAX_ITEMS + 40)]
        )
        assert len(plan()) == MAX_ITEMS
        assert all(len(i.content) <= MAX_CONTENT_CHARS for i in plan())


class TestReadingProgress:
    def test_counts_by_status(self):
        write_plan(
            _items(
                ("1", "a", "completed"),
                ("2", "b", "in_progress"),
                ("3", "c", "pending"),
                ("4", "d", "cancelled"),
            )
        )
        counts = plan_counts()
        assert (
            counts["completed"],
            counts["in_progress"],
            counts["pending"],
            counts["cancelled"],
        ) == (1, 1, 1, 1)

    def test_cancelled_is_a_decision_not_an_omission(self):
        """A guardrail asking "what is left" must not be told about work the
        agent deliberately abandoned."""
        write_plan(
            _items(("1", "wrong approach", "cancelled"), ("2", "real work", "pending"))
        )
        assert [i.id for i in unfinished_plan_items()] == ["2"]

    def test_summary_names_what_is_still_open(self):
        write_plan(
            _items(("1", "a", "completed"), ("2", "publish the dashboard", "pending"))
        )
        summary = plan_summary()
        assert "1/2" in summary
        assert "publish the dashboard" in summary

    def test_summary_is_empty_without_a_plan(self):
        assert plan_summary() == ""

    def test_render_marks_each_status(self):
        write_plan(
            _items(
                ("1", "a", "completed"),
                ("2", "b", "in_progress"),
                ("3", "c", "pending"),
                ("4", "d", "cancelled"),
            )
        )
        rendered = render_plan()
        assert "[x] 1." in rendered and "[>] 2." in rendered
        assert "[ ] 3." in rendered and "[~] 4." in rendered


class TestScoping:
    def test_reset_clears_the_previous_task(self):
        """Per task, not per run — the next task must not report work this one
        did."""
        write_plan(_items(("1", "a", "completed")))
        reset_plan()
        assert plan() == []

    def test_a_delegated_task_does_not_clobber_the_managers_plan(self):
        """The hierarchical case. A coworker's task runs on the SAME thread and
        context, so without a scope its reset destroys the manager's plan."""
        write_plan(_items(("m1", "manager step", "in_progress")))

        with plan_scope():
            reset_plan()  # what the delegated task's execute_sync does
            write_plan(_items(("w1", "worker step", "completed")))
            assert [i.id for i in plan()] == ["w1"]

        assert [i.id for i in plan()] == ["m1"]

    def test_the_worker_does_not_start_with_the_managers_plan(self):
        write_plan(_items(("m1", "manager step", "in_progress")))
        with plan_scope():
            assert plan() == []

    def test_scope_restores_even_when_the_body_raises(self):
        write_plan(_items(("m1", "manager step", "pending")))
        with pytest.raises(ValueError):
            with plan_scope():
                write_plan(_items(("w1", "worker", "pending")))
                raise ValueError("delegation blew up")
        assert [i.id for i in plan()] == ["m1"]

    def test_scopes_nest(self):
        write_plan(_items(("a", "outer", "pending")))
        with plan_scope():
            write_plan(_items(("b", "middle", "pending")))
            with plan_scope():
                write_plan(_items(("c", "inner", "pending")))
                assert [i.id for i in plan()] == ["c"]
            assert [i.id for i in plan()] == ["b"]
        assert [i.id for i in plan()] == ["a"]


class TestTheToolTheModelCalls:
    def test_it_is_engine_machinery_not_a_selection(self):
        """A task that picks its own tools replaces the agent's; this must
        survive that, or the model is told to keep a plan it cannot write."""
        assert getattr(build_plan_tool(), "_kasal_always_available", False) is True

    def test_reading_an_empty_plan_says_so(self):
        assert "empty" in build_plan_tool()._run().lower()

    def test_writing_then_reading(self):
        tool = build_plan_tool()
        tool._run(
            todos=_items(
                ("1", "read the schema", "completed"), ("2", "build it", "pending")
            )
        )
        out = tool._run()
        assert "1/2 completed" in out
        assert "read the schema" in out

    def test_a_json_string_payload_is_accepted(self):
        """Models intermittently send the array as a string."""
        out = build_plan_tool()._run(
            todos='[{"id":"1","content":"x","status":"pending"}]'
        )
        assert "Error" not in out
        assert len(plan()) == 1

    def test_an_unparseable_string_is_reported_not_raised(self):
        out = build_plan_tool()._run(todos="not json at all")
        assert out.startswith("Error")

    def test_a_non_list_payload_is_reported(self):
        out = build_plan_tool()._run(todos={"id": "1"})
        assert out.startswith("Error")

    def test_merge_through_the_tool(self):
        tool = build_plan_tool()
        tool._run(todos=_items(("1", "a", "pending"), ("2", "b", "pending")))
        tool._run(todos=_items(("1", "a", "completed")), merge=True)
        assert [(i.id, i.status) for i in plan()] == [
            ("1", "completed"),
            ("2", "pending"),
        ]

    def test_the_description_tells_the_model_how_to_replan(self):
        """The guidance is the tool's whole interface — it lives in the schema
        so it stays cacheable rather than shifting every turn."""
        description = build_plan_tool().description
        assert "replan" in description.lower()
        assert "one item in_progress" in description.lower()
        assert "cancel" in description.lower()


class TestThePromptAsksForIt:
    """The tool alone was not enough.

    Measured on a real run: the plan tool was equipped and present in the
    model's tool list, the agent made six tool calls, and none was ``todo``. A
    tool description is read once the model is already considering that tool; it
    is weak at prompting the model to reach for it unprompted.
    """

    def test_guidance_lands_in_backstory_by_default(self):
        from src.services.execution.kernel.agent_plan import inject_plan_guidance

        kwargs = {"backstory": "You are an analyst."}
        field = inject_plan_guidance(kwargs)
        assert field == "backstory"
        assert "You are an analyst." in kwargs["backstory"]
        assert "`todo`" in kwargs["backstory"]

    def test_guidance_follows_a_custom_system_template(self):
        from src.services.execution.kernel.agent_plan import inject_plan_guidance

        kwargs = {"system_template": "TEMPLATE", "backstory": "B"}
        assert inject_plan_guidance(kwargs) == "system_template"
        assert "`todo`" in kwargs["system_template"]
        assert "`todo`" not in kwargs["backstory"]

    def test_guidance_is_appended_so_security_stays_first(self):
        """The security preamble is the highest-priority instruction and must
        not be displaced by operational guidance."""
        from src.services.execution.kernel.agent_plan import inject_plan_guidance
        from src.services.execution.kernel.agent_security import (
            inject_security_preamble,
        )

        kwargs = {"backstory": "B"}
        inject_security_preamble(kwargs)
        inject_plan_guidance(kwargs)
        text = kwargs["backstory"]
        assert text.index("SECURITY INSTRUCTION") < text.index("PLANNING:")

    def test_equipping_attaches_the_tool_and_the_guidance_together(self):
        from src.services.execution.kernel.agent_plan import add_plan_tool

        kwargs = {"backstory": "B"}
        assert add_plan_tool(kwargs, label="a") is True
        assert [t.name for t in kwargs["tools"]] == ["todo"]
        assert "`todo`" in kwargs["backstory"]

    def test_equipping_twice_does_not_duplicate_either(self):
        from src.services.execution.kernel.agent_plan import add_plan_tool

        kwargs = {"backstory": "B"}
        add_plan_tool(kwargs, label="a")
        add_plan_tool(kwargs, label="a")
        assert [t.name for t in kwargs["tools"]] == ["todo"]
        assert kwargs["backstory"].count("PLANNING:") == 1
