"""The bundle says which runtime it runs on, and warns when that is not yours.

The exported app vendors Kasal's runtime and ships no agent framework — that is
what lets it stand alone. So a workspace configured for CrewAI still exports a
Kasal-runtime app: correct, and silent. `Capability.EXPORT` has recorded that
CrewAI cannot be exported since the harness landed, and nothing read it, so a
crew tuned against CrewAI's executor could be deployed with no indication that
it would behave differently.
"""

import pytest

from src.services.execution.harnesses import bind
from src.services.export.databricks_app_exporter import DatabricksAppExporter

CREW = {
    "id": "c1",
    "name": "Demo Crew",
    "agents": [{"name": "a1", "role": "R", "goal": "g", "backstory": "b", "tools": []}],
    "tasks": [
        {"name": "t1", "description": "d", "expected_output": "e", "agent": "a1"}
    ],
}


async def _export(harness: str):
    with bind(harness):
        result = await DatabricksAppExporter().export(CREW, {})
    readme = next(
        f for f in result["files"] if f["path"].endswith("README.md")
    )["content"]
    return result["metadata"], readme


class TestTheBundleNamesItsRuntime:
    @pytest.mark.asyncio
    async def test_every_export_records_the_runtime_it_produced(self):
        metadata, _ = await _export("kasal")
        assert metadata["bundle_runtime"] == "kasal"

    @pytest.mark.asyncio
    async def test_a_matching_harness_adds_no_noise(self):
        """The common case must read exactly as it did before."""
        metadata, readme = await _export("kasal")
        assert "runtime_notice" not in metadata
        assert "**Note:**" not in readme

    @pytest.mark.asyncio
    async def test_a_harness_that_cannot_export_is_called_out(self):
        metadata, readme = await _export("crewai")
        notice = metadata.get("runtime_notice", "")
        assert "crewai" in notice
        assert "Kasal's own runtime" in notice
        # …and where the person deploying it will actually read it.
        assert "**Note:**" in readme

    @pytest.mark.asyncio
    async def test_the_export_still_succeeds(self):
        """A warning, never a refusal: the bundle it produces is correct."""
        with bind("crewai"):
            result = await DatabricksAppExporter().export(CREW, {})
        assert result["files"]
        assert result["metadata"]["agents_count"] == 1

    @pytest.mark.asyncio
    async def test_no_placeholder_survives_into_the_bundle(self):
        for harness in ("kasal", "crewai"):
            _, readme = await _export(harness)
            assert "{{RUNTIME_NOTICE}}" not in readme
