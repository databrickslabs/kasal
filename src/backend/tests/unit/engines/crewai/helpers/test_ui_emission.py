"""Unit tests for the modular Predefined UI emission helper."""
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.engines.crewai.helpers.ui_emission import build_ui_instruction, apply_ui_emission


class _FakeSessionCM:
    """Stand-in async context manager for request_scoped_session()."""
    async def __aenter__(self):
        return AsyncMock()

    async def __aexit__(self, *exc):
        return False


def _patch_config(config):
    """Patch request_scoped_session + UIConfigService so get_config returns `config`."""
    rss = patch("src.db.session.request_scoped_session", return_value=_FakeSessionCM())
    svc_patch = patch("src.services.ui_config_service.UIConfigService")
    return rss, svc_patch


# --- build_ui_instruction -------------------------------------------------

def test_build_ui_instruction_without_accent():
    s = build_ui_instruction()
    assert "OUTPUT FORMAT" in s
    assert "JSON" in s
    assert "accent color" not in s


def test_build_ui_instruction_with_accent():
    s = build_ui_instruction("#5aa2ff")
    assert "#5aa2ff" in s
    assert "accent color" in s


# --- apply_ui_emission: no-ops --------------------------------------------

@pytest.mark.asyncio
async def test_apply_ui_emission_noop_without_tasks():
    await apply_ui_emission([], "g1")  # must not raise


@pytest.mark.asyncio
async def test_apply_ui_emission_noop_without_group():
    tasks = [{"description": "orig"}]
    await apply_ui_emission(tasks, None)
    assert tasks[0]["description"] == "orig"


@pytest.mark.asyncio
async def test_apply_ui_emission_noop_when_disabled():
    tasks = [{"description": "orig"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(return_value=MagicMock(enabled=False))
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert tasks[0]["description"] == "orig"


# --- apply_ui_emission: enabled -------------------------------------------

@pytest.mark.asyncio
async def test_apply_ui_emission_appends_to_last_task_with_accent():
    tasks = [{"description": "first"}, {"description": "last"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(return_value=MagicMock(enabled=True, style_json='{"accent":"#123456"}'))
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert tasks[0]["description"] == "first"  # earlier tasks untouched
    assert tasks[1]["description"].startswith("last")
    assert "OUTPUT FORMAT" in tasks[1]["description"]
    assert "#123456" in tasks[1]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_enabled_with_no_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(return_value=MagicMock(enabled=True, style_json=None))
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_tolerates_invalid_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(return_value=MagicMock(enabled=True, style_json="not json"))
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    # Bad style JSON -> accent ignored, instruction still appended.
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_swallows_errors():
    """UI formatting must never break a run — any error is logged and ignored."""
    tasks = [{"description": "orig"}]
    with patch("src.db.session.request_scoped_session", side_effect=RuntimeError("boom")):
        await apply_ui_emission(tasks, "g1")  # must not raise
    assert tasks[0]["description"] == "orig"
