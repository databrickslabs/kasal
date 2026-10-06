"""UCMVCorrectionLearningService: fetch originals → diff → distil README."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.powerbi.correction_learning_service import (
    UCMVCorrectionLearningService,
)

_ORIG = "measures:\n  - name: cost\n    expr: SUM(source.cost_components)\n"
_CORR = "measures:\n  - name: cost\n    expr: SUM(source.booked_cost)\n"

_CS = "src.services.powerbi.correction_learning_service.ConverterService"
_COMPLETION = "src.services.powerbi.correction_learning_service.LLMManager.completion"


def _history_resp(yaml_map):
    h = MagicMock()
    h.output_data = {"yaml": yaml_map}
    resp = MagicMock()
    resp.history = [h]
    return resp


@pytest.mark.asyncio
async def test_learn_distills_readme_from_corrections():
    svc = UCMVCorrectionLearningService(session=MagicMock(), group_context=MagicMock())
    with (
        patch(_CS) as cs,
        patch(
            _COMPLETION, new=AsyncMock(return_value="# Learned\n- prefer booked_cost")
        ) as comp,
    ):
        cs.return_value.list_history = AsyncMock(
            return_value=_history_resp({"fact_x": _ORIG})
        )
        out = await svc.learn({"fact_x": _CORR}, model_hint="Total SC")

    assert out["readme"].startswith("# Learned")
    assert out["views_with_changes"] == 1
    assert out["matched_views"] == ["fact_x"]
    # the concrete correction reached the LLM prompt
    msgs = comp.call_args.kwargs["messages"]
    assert any("booked_cost" in m["content"] for m in msgs)


@pytest.mark.asyncio
async def test_learn_no_changes_returns_none_and_skips_llm():
    svc = UCMVCorrectionLearningService(session=MagicMock(), group_context=MagicMock())
    with patch(_CS) as cs, patch(_COMPLETION, new=AsyncMock()) as comp:
        cs.return_value.list_history = AsyncMock(
            return_value=_history_resp({"fact_x": _ORIG})
        )
        out = await svc.learn({"fact_x": _ORIG})  # identical to original

    assert out["readme"] is None
    assert out["views_with_changes"] == 0
    comp.assert_not_awaited()


@pytest.mark.asyncio
async def test_learn_rejects_empty_input():
    svc = UCMVCorrectionLearningService(session=MagicMock())
    out = await svc.learn({})
    assert out["readme"] is None
    assert "error" in out


@pytest.mark.asyncio
async def test_differently_named_upload_fuzzy_matches_by_measure_overlap():
    # Upload "fact_x" but our original is stored as "other_view"; they share the
    # same measure set, so measure-overlap pairs them (vs the old exact-only match
    # which would have dropped it as unmatched and learned nothing).
    svc = UCMVCorrectionLearningService(session=MagicMock(), group_context=MagicMock())
    with (
        patch(_CS) as cs,
        patch(_COMPLETION, new=AsyncMock(return_value="# x")),
    ):
        cs.return_value.list_history = AsyncMock(
            return_value=_history_resp({"other_view": _ORIG})
        )
        out = await svc.learn({"fact_x": _CORR})

    assert out["matched_views"] == []  # not an exact-name match
    assert out["unmatched_views"] == []  # but NOT dropped — fuzzy-paired
    assert out["fuzzy_matched"]["fact_x"]["matched_to"] == "other_view"
    assert out["fuzzy_matched"]["fact_x"]["how"] == "fuzzy_measures"
    assert out["readme"] == "# x"  # the cost expr change was learned


@pytest.mark.asyncio
async def test_truly_unrelated_upload_is_reported_unmatched():
    svc = UCMVCorrectionLearningService(session=MagicMock(), group_context=MagicMock())
    with patch(_CS) as cs, patch(_COMPLETION, new=AsyncMock()) as comp:
        cs.return_value.list_history = AsyncMock(
            return_value=_history_resp(
                {"zzz_unrelated": "measures:\n  - name: q\n    expr: SUM(source.z)\n"}
            )
        )
        out = await svc.learn({"fact_x": _CORR})

    assert out["unmatched_views"] == ["fact_x"]
    assert out["fuzzy_matched"] == {}
    comp.assert_not_awaited()
