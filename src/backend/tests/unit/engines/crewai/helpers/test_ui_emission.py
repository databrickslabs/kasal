"""Unit tests for the modular Predefined UI emission helper."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.engines.crewai.helpers.ui_emission import (
    _build_directives_block,
    _build_theme_block,
    _palette_str,
    apply_ui_emission,
    build_ui_instruction,
)


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


def test_build_ui_instruction_with_themes_emits_palette_block_and_suppresses_accent():
    themes = {
        "default": {"accent": "#2272B4", "background": "#FFFFFF", "font": "sans"},
        "dashboard": {"accent": "#38BDF8", "background": "#0F172A"},
    }
    s = build_ui_instruction("#5aa2ff", themes)
    assert "THEME / BRANDING" in s
    assert "Default (any deliverable" in s
    assert "Dashboard / metrics" in s
    assert "#38BDF8" in s
    # When per-type themes drive the palette, the bare legacy accent hint is dropped.
    assert "accent color where relevant" not in s


def test_build_ui_instruction_with_directives_emits_settings_block():
    directives = {
        "presentation": "Aim for about 8 slides; at most 4 bullet points per slide.",
        "default": "",  # default is never emitted as a deliverable line
    }
    s = build_ui_instruction(None, None, directives)
    assert "DELIVERABLE SETTINGS" in s
    assert "Presentation / slides" in s
    assert "about 8 slides" in s
    assert "Default (any deliverable" not in s  # default key skipped


def test_palette_str_only_includes_present_keys():
    assert (
        _palette_str({"accent": "#111", "font": "serif"}) == "accent #111, font serif"
    )
    assert _palette_str({}) == ""


def test_theme_and_directive_blocks_ignore_empty_or_non_dict():
    assert _build_theme_block({}) == []
    assert _build_theme_block("nope") == []
    assert _build_theme_block({"dashboard": {}}) == []  # no usable palette tokens
    assert _build_directives_block({}) == []
    assert _build_directives_block("nope") == []
    assert (
        _build_directives_block({"default": "x", "quiz": "  "}) == []
    )  # only default/blank


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
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json='{"accent":"#123456"}')
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert tasks[0]["description"] == "first"  # earlier tasks untouched
    assert tasks[1]["description"].startswith("last")
    assert "OUTPUT FORMAT" in tasks[1]["description"]
    assert "#123456" in tasks[1]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_appends_themes_and_directives():
    style = {
        "accent": "#2272B4",
        "density": "comfortable",
        "themes": {
            "default": {"accent": "#2272B4", "background": "#FFFFFF"},
            "genie": {"accent": "#38BDF8"},
        },
        "directives": {
            "genie": "Add a chart when it aids understanding; show at most 20 rows.",
            "default": "",
        },
    }
    tasks = [{"description": "last"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=json.dumps(style))
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    desc = tasks[0]["description"]
    assert "THEME / BRANDING" in desc
    assert "DELIVERABLE SETTINGS" in desc
    assert "#38BDF8" in desc
    assert "at most 20 rows" in desc
    # expected_output overwritten to the single-UI-document target.
    assert tasks[0]["expected_output"].startswith("A single JSON")


@pytest.mark.asyncio
async def test_apply_ui_emission_enabled_with_no_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json=None)
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_tolerates_invalid_style_json():
    tasks = [{"description": "only"}]
    rss, svc_patch = _patch_config(None)
    with rss, svc_patch as Svc:
        inst = MagicMock()
        inst.get_config = AsyncMock(
            return_value=MagicMock(enabled=True, style_json="not json")
        )
        Svc.return_value = inst
        await apply_ui_emission(tasks, "g1")
    # Bad style JSON -> accent ignored, instruction still appended.
    assert "OUTPUT FORMAT" in tasks[0]["description"]


@pytest.mark.asyncio
async def test_apply_ui_emission_swallows_errors():
    """UI formatting must never break a run — any error is logged and ignored."""
    tasks = [{"description": "orig"}]
    with patch(
        "src.db.session.request_scoped_session", side_effect=RuntimeError("boom")
    ):
        await apply_ui_emission(tasks, "g1")  # must not raise
    assert tasks[0]["description"] == "orig"
