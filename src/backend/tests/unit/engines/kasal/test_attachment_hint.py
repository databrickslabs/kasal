"""The chat agent must know its conversation has files attached.

The failure: attaching a file in ChatMode bound DatabricksKnowledgeSearchTool and
scoped it to that file, but no part of the prompt mentioned the file. Asked "what
is existing in this report", the agent replied "The report content isn't provided
here … please share or upload the report" — holding the tool that would have read
it. The trace shows no tool call at all: it did not decline to search, it had no
reason to think there was anything to search.
"""

from src.engines.kasal.paths.light_agent.attachment_hint import (
    MAX_LISTED,
    attached_file_names,
    build_attachment_hint,
)


def _spec(*paths):
    return {
        "role": "Assistant",
        "tools": ["DatabricksKnowledgeSearchTool"],
        "tool_configs": {"DatabricksKnowledgeSearchTool": {"file_paths": list(paths)}},
    }


class TestAttachedFileNames:
    def test_reads_the_names_out_of_tool_configs(self):
        names = attached_file_names(
            _spec(
                "uploads/user_dev_localhost/chat-ms3vrecg-3ljbyq/full-report.md",
                "uploads/user_dev_localhost/d6bbd3eb-ad57/recipe-report.md",
            )
        )
        assert names == ["full-report.md", "recipe-report.md"]

    def test_the_internal_path_is_not_exposed(self):
        """The stored path is a workspace location, not something to hand a
        model; the name is what the user typed about."""
        (name,) = attached_file_names(_spec("uploads/g/chat-abc/quarterly.pdf"))
        assert name == "quarterly.pdf"
        assert "uploads/" not in name

    def test_the_same_file_attached_twice_is_named_once(self):
        assert attached_file_names(
            _spec("uploads/g/session-a/report.md", "uploads/g/session-b/report.md")
        ) == ["report.md"]

    def test_collects_across_every_tool_that_carries_paths(self):
        spec = {
            "tool_configs": {
                "DatabricksKnowledgeSearchTool": {"file_paths": ["a/one.md"]},
                "SomeOtherTool": {"file_paths": ["b/two.md"], "other": 1},
                "ToolWithNoFiles": {"limit": 5},
            }
        }
        assert attached_file_names(spec) == ["one.md", "two.md"]

    def test_a_single_path_given_as_a_string(self):
        spec = {"tool_configs": {"T": {"file_paths": "uploads/g/s/report.md"}}}
        assert attached_file_names(spec) == ["report.md"]

    def test_nothing_attached(self):
        assert attached_file_names({}) == []
        assert attached_file_names({"tool_configs": {}}) == []
        assert attached_file_names({"tool_configs": {"T": {"file_paths": []}}}) == []


class TestBuildAttachmentHint:
    def test_names_what_is_attached_and_how_to_read_it(self):
        hint = build_attachment_hint(_spec("uploads/g/s/full-report.md"))

        assert "full-report.md" in hint
        assert "DatabricksKnowledgeSearchTool" in hint

    def test_it_hints_rather_than_dictating(self):
        """A hint states what is available. It does not order the agent to
        search, prescribe queries, or claim what the files contain."""
        hint = build_attachment_hint(_spec("uploads/g/s/report.md")).lower()

        for imperative in ("you must", "always search", "first search", "before answering"):
            assert imperative not in hint

    def test_no_attachments_costs_the_prompt_nothing(self):
        assert build_attachment_hint({}) == ""
        assert build_attachment_hint({"tool_configs": {}}) == ""

    def test_a_bulk_upload_cannot_crowd_out_the_prompt(self):
        spec = _spec(*[f"uploads/g/s/file-{i}.md" for i in range(MAX_LISTED + 5)])

        hint = build_attachment_hint(spec)

        assert f"(and 5 more)" in hint
        assert f"file-{MAX_LISTED + 4}.md" not in hint

    def test_the_report_question_now_has_an_answerable_prompt(self):
        """The exact run that failed: two uploads, and a question about "this
        report" with nothing in the prompt to connect them."""
        hint = build_attachment_hint(
            _spec(
                "uploads/user_dev_localhost/chat-ms3vrecg-3ljbyq/full-report.md",
                "uploads/user_dev_localhost/d6bbd3eb-ad57/recipe-report.md",
            )
        )

        assert hint.startswith("Files attached to this conversation:")
        assert "full-report.md, recipe-report.md" in hint
