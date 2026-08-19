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
    async def test_the_configured_harness_decides_the_bundle(self):
        """A CrewAI workspace exports a CrewAI bundle — no warning, because
        nothing surprising happened. This used to fall back to Kasal with a
        notice, before a CrewAI bundle existed."""
        metadata, readme = await _export("crewai")
        assert metadata["bundle_runtime"] == "crewai"
        assert "runtime_notice" not in metadata
        assert "**Note:**" not in readme

    @pytest.mark.asyncio
    async def test_a_runtime_nothing_can_produce_falls_back_and_says_so(self):
        """The notice is for the case that is actually surprising: you asked for
        a bundle this export cannot make, and got a working one anyway."""
        with bind("kasal"):
            result = await DatabricksAppExporter().export(CREW, {"runtime": "langgraph"})
        notice = result["metadata"].get("runtime_notice", "")
        assert result["metadata"]["bundle_runtime"] == "kasal"
        assert "langgraph" in notice
        readme = next(
            f for f in result["files"] if f["path"].endswith("README.md")
        )["content"]
        assert "**Note:**" in readme

    @pytest.mark.asyncio
    async def test_an_explicit_request_beats_the_configured_harness(self):
        """Export is a deliberate act: what you ask for is what you get."""
        with bind("kasal"):
            result = await DatabricksAppExporter().export(CREW, {"runtime": "crewai"})
        assert result["metadata"]["bundle_runtime"] == "crewai"

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


class TestTheOptionReachesTheExporter:
    """The runtime is only useful if a caller can actually ask for it. Each of
    these was broken at some point between the schema and the exporter, and none
    of the failures raised — you simply got the other runtime's bundle."""

    def test_the_schema_carries_it(self):
        from src.schemas.crew_export import ExportOptions

        # ``CrewExportService`` passes options through ``.dict()``, so a field
        # missing here is silently dropped rather than rejected.
        assert ExportOptions(runtime="crewai").dict()["runtime"] == "crewai"

    def test_it_defaults_to_unset_rather_than_to_kasal(self):
        """Unset must mean "follow the configured harness". A default of
        "kasal" here would override a CrewAI workspace on every export."""
        from src.schemas.crew_export import ExportOptions

        assert ExportOptions().runtime is None
