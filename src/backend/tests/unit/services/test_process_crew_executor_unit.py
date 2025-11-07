import pytest

from src.services.process_crew_executor import run_crew_in_process


class TestProcessCrewExecutorValidation:
    def test_run_crew_in_process_none_config(self):
        out = run_crew_in_process(execution_id="e-1", crew_config=None)
        assert isinstance(out, dict)
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-1"
        assert "crew_config is None" in out.get("error", "")

    def test_run_crew_in_process_invalid_json_string(self):
        out = run_crew_in_process(execution_id="e-2", crew_config="{not-json}")
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-2"
        assert "Failed to parse crew_config JSON" in out.get("error", "")

    def test_run_crew_in_process_json_string_not_dict(self):
        # Valid JSON but not a dict (a list) should be rejected by type validation
        out = run_crew_in_process(execution_id="e-3", crew_config="[1,2,3]")
        assert out.get("status") == "FAILED"
        assert out.get("execution_id") == "e-3"
        assert "crew_config must be a dict" in out.get("error", "")

