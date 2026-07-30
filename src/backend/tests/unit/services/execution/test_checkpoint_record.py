"""Unit tests for the checkpoint storage contract and the resume payloads.

The migration tests matter most: checkpoint payloads outlive the code that
wrote them, and the pre-unification crew shape is already sitting in real
databases.
"""

from src.services.execution.checkpointing import lifecycle
from src.services.execution.checkpointing.record import (
    CHECKPOINT_KEY,
    CHECKPOINT_VERSION,
    KIND_CREW,
    KIND_FLOW,
    LEGACY_CREW_KEY,
    build_unit,
    is_truncated,
    merge_unit,
    normalize,
    ordered_units,
)
from src.services.execution.checkpointing.resume import (
    build_crew_payload,
    build_flow_outputs,
    next_unit_index,
    select_prefix,
)

LEGACY_PAYLOAD = {
    "version": 1,
    "task_count": 3,
    "process": "sequential",
    "completed": {
        "1": {"index": 1, "task_key": "k1", "output_raw": "b"},
        "0": {"index": 0, "task_key": "k0", "output_raw": "a"},
    },
}


class TestBuildUnit:
    def test_stringifies_the_key_for_json_stability(self):
        unit = build_unit(key=3, name="t", output_raw="o")
        assert unit["key"] == "3"

    def test_truncates_and_flags_a_long_output(self):
        unit = build_unit(key=0, name="t", output_raw="x" * 600_001)
        assert len(unit["output_raw"]) == 500_000
        assert unit["truncated"] is True

    def test_omits_the_flag_when_nothing_was_truncated(self):
        assert "truncated" not in build_unit(key=0, name="t", output_raw="x")

    def test_non_dict_structured_output_is_dropped(self):
        unit = build_unit(key=0, name="t", output_raw="o", output_json="not a dict")
        assert unit["output_json"] is None

    def test_none_output_becomes_empty_rather_than_the_string_none(self):
        assert build_unit(key=0, name="t", output_raw=None)["output_raw"] == ""


class TestMergeUnit:
    def test_returns_a_new_dict_for_json_change_detection(self):
        original = merge_unit(None, KIND_CREW, build_unit(0, "a", "x"), unit_count=2)
        merged = merge_unit(original, KIND_CREW, build_unit(1, "b", "y"), unit_count=2)
        assert merged is not original
        assert set(merged["units"]) == {"0", "1"}

    def test_rewriting_a_unit_is_idempotent(self):
        record = merge_unit(None, KIND_CREW, build_unit(0, "a", "first"))
        record = merge_unit(record, KIND_CREW, build_unit(0, "a", "second"))
        assert list(record["units"]) == ["0"]
        assert record["units"]["0"]["output_raw"] == "second"

    def test_carries_the_current_version(self):
        record = merge_unit(None, KIND_FLOW, build_unit(1, "crew", "o"))
        assert record["version"] == CHECKPOINT_VERSION
        assert record["kind"] == KIND_FLOW


class TestNormalize:
    def test_reads_the_current_record(self):
        record = merge_unit(None, KIND_CREW, build_unit(0, "a", "x"), unit_count=1)
        assert normalize({CHECKPOINT_KEY: record})["units"]["0"]["output_raw"] == "x"

    def test_migrates_the_pre_unification_crew_payload(self):
        record = normalize({LEGACY_CREW_KEY: LEGACY_PAYLOAD})

        assert record["version"] == CHECKPOINT_VERSION
        assert record["kind"] == KIND_CREW
        assert record["unit_count"] == 3
        assert record["meta"]["process"] == "sequential"
        assert set(record["units"]) == {"0", "1"}
        # v0 called the content-addressed identity "task_key".
        assert record["units"]["0"]["identity"] == "k0"
        assert record["migrated_from_version"] == 0

    def test_the_current_key_wins_over_a_legacy_one(self):
        current = merge_unit(None, KIND_CREW, build_unit(0, "new", "new output"))
        record = normalize({CHECKPOINT_KEY: current, LEGACY_CREW_KEY: LEGACY_PAYLOAD})
        assert record["units"]["0"]["output_raw"] == "new output"

    def test_a_column_with_only_hitl_keys_holds_no_checkpoint(self):
        assert normalize({"edited_config": {"a": 1}, "ucmv_yaml_edits": "x"}) is None

    def test_missing_or_unrecognised_returns_none(self):
        assert normalize(None) is None
        assert normalize({}) is None
        assert normalize("not a dict") is None
        assert normalize({CHECKPOINT_KEY: {"version": 99, "surprise": True}}) is None


class TestOrderedUnits:
    def test_sorts_numerically_not_lexically(self):
        record = {"units": {str(i): build_unit(i, f"t{i}", "o") for i in (0, 2, 10, 1)}}
        assert [u["key"] for u in ordered_units(record)] == ["0", "1", "2", "10"]

    def test_a_malformed_key_does_not_make_the_rest_unreadable(self):
        record = {
            "units": {
                "0": build_unit(0, "a", "o"),
                "oops": build_unit("oops", "b", "o"),
            }
        }
        assert [u["key"] for u in ordered_units(record)] == ["0", "oops"]

    def test_empty_is_empty(self):
        assert ordered_units(None) == []
        assert ordered_units({"units": {}}) == []


class TestSelectPrefix:
    def _record(self):
        return {"units": {str(i): build_unit(i, f"t{i}", f"o{i}") for i in range(4)}}

    def test_without_a_boundary_the_whole_prefix_is_restored(self):
        assert len(select_prefix(self._record())) == 4

    def test_from_unit_restores_only_what_comes_before_it(self):
        # "Resume AT unit 2" → 0 and 1 are restored, 2 onward re-runs.
        assert [u["key"] for u in select_prefix(self._record(), 2)] == ["0", "1"]

    def test_from_unit_zero_restores_nothing(self):
        assert select_prefix(self._record(), 0) == []

    def test_an_unknown_boundary_restores_nothing_rather_than_everything(self):
        assert select_prefix(self._record(), "nonsense") == []


class TestBuildCrewPayload:
    def test_translates_into_the_runtimes_vocabulary(self):
        record = normalize({LEGACY_CREW_KEY: LEGACY_PAYLOAD})
        payload = build_crew_payload(record)

        assert [e["index"] for e in payload["completed"]] == [0, 1]
        assert [e["task_key"] for e in payload["completed"]] == ["k0", "k1"]
        assert payload["task_count"] == 3
        assert payload["process"] == "sequential"

    def test_honours_a_resume_boundary(self):
        record = normalize({LEGACY_CREW_KEY: LEGACY_PAYLOAD})
        payload = build_crew_payload(record, from_unit=1)
        assert [e["index"] for e in payload["completed"]] == [0]

    def test_nothing_to_resume_returns_none(self):
        assert build_crew_payload(None) is None
        assert build_crew_payload({"kind": KIND_CREW, "units": {}}) is None

    def test_a_flow_record_is_not_a_crew_payload(self):
        record = merge_unit(None, KIND_FLOW, build_unit(1, "crew", "o"))
        assert build_crew_payload(record) is None


class TestBuildFlowOutputs:
    def test_maps_crew_name_to_output(self):
        record = merge_unit(None, KIND_FLOW, build_unit(1, "research", "found it"))
        record = merge_unit(record, KIND_FLOW, build_unit(2, "write", "wrote it"))

        assert build_flow_outputs(record) == {
            "research": "found it",
            "write": "wrote it",
        }

    def test_honours_a_resume_boundary(self):
        record = merge_unit(None, KIND_FLOW, build_unit(1, "research", "found it"))
        record = merge_unit(record, KIND_FLOW, build_unit(2, "write", "wrote it"))
        assert build_flow_outputs(record, from_unit=2) == {"research": "found it"}

    def test_a_crew_record_is_not_flow_outputs(self):
        record = merge_unit(None, KIND_CREW, build_unit(0, "task", "o"))
        assert build_flow_outputs(record) == {}


class TestNextUnitIndex:
    def test_is_the_end_of_the_contiguous_prefix(self):
        record = {"units": {str(i): build_unit(i, "t", "o") for i in (0, 1, 2)}}
        assert next_unit_index(record) == 3

    def test_a_gap_stops_the_prefix(self):
        # Units after a gap cannot be trusted as context for a re-run.
        record = {"units": {str(i): build_unit(i, "t", "o") for i in (0, 2, 3)}}
        assert next_unit_index(record) == 1

    def test_nothing_recorded_starts_at_zero(self):
        assert next_unit_index(None) == 0


class TestIsTruncated:
    def test_reports_a_bounded_output(self):
        record = merge_unit(None, KIND_CREW, build_unit(0, "t", "x" * 600_000))
        assert is_truncated(record) is True

    def test_reports_nothing_when_all_units_are_whole(self):
        record = merge_unit(None, KIND_CREW, build_unit(0, "t", "small"))
        assert is_truncated(record) is False


class TestLifecycle:
    def test_any_finished_execution_is_resumable(self):
        assert lifecycle.is_resumable_execution("FAILED") is True
        assert lifecycle.is_resumable_execution("STOPPED") is True
        assert lifecycle.is_resumable_execution("CANCELLED") is True
        # A SUCCESSFUL run is resumable too: re-running a flow from the middle
        # after changing a downstream crew is the point of keeping it.
        assert lifecycle.is_resumable_execution("COMPLETED") is True

    def test_an_in_flight_execution_is_not_resumable(self):
        # Still writing units; resuming would race the process.
        assert lifecycle.is_resumable_execution("RUNNING") is False
        assert lifecycle.is_resumable_execution("PENDING") is False
        assert lifecycle.is_resumable_execution("QUEUED") is False

    def test_only_an_active_checkpoint_is_resumable(self):
        assert lifecycle.is_resumable_status("active") is True
        assert lifecycle.is_resumable_status("resumed") is False
        assert lifecycle.is_resumable_status("expired") is False
        assert lifecycle.is_resumable_status(None) is False

    def test_blocker_explains_itself(self):
        assert lifecycle.resumable_blocker("FAILED", "active") is None
        assert "already been resumed" in lifecycle.resumable_blocker(
            "FAILED", "resumed"
        )
        assert "expired" in lifecycle.resumable_blocker("FAILED", "expired")
        assert "no active checkpoint" in lifecycle.resumable_blocker("FAILED", None)
        assert "RUNNING" in lifecycle.resumable_blocker("RUNNING", "active")
        # A completed run with a live checkpoint is resumable, not blocked.
        assert lifecycle.resumable_blocker("COMPLETED", "active") is None
