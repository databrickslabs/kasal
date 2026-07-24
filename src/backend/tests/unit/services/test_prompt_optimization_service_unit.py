"""
Unit tests for PromptOptimizationService.

Covers the format scorer, example mining (dedupe/status/cutoff filters),
run lifecycle (start → background completion / failure), group-scoped
visibility, reflection-model resolution, and the apply flow.
"""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.schemas.prompt_optimization import PromptOptimizationRequest
from src.services import prompt_optimization_service as svc_module
from src.services.prompt_optimization_service import (
    PromptOptimizationService,
    _checklist_grade,
    _distill_requirements,
    _parse_requirement_lines,
    _extract_user_from_log,
    _intent_format_score,
    _json_keys_score,
)
from src.utils.user_context import GroupContext

EXAMPLES = [
    "get the news",
    "run crew",
    "create an agent for support",
    "analyze sales",
    "make a report",
]


def _group(gid="grp1"):
    return GroupContext(
        group_ids=[gid], group_email="user@example.com", email_domain="example.com"
    )


@pytest.fixture(autouse=True)
def clear_runs():
    svc_module._RUNS.clear()
    yield
    svc_module._RUNS.clear()


def _service(template="TEMPLATE", sync_result=None, sync_error=None):
    svc = PromptOptimizationService(MagicMock())
    svc.model_repository = MagicMock()
    svc.model_repository.find_by_key = AsyncMock(
        return_value=SimpleNamespace(provider="databricks")
    )
    svc._resolve_registry = AsyncMock(
        return_value=("http://127.0.0.1:5555", "kasal_detect_intent_grp1")
    )
    patches = [
        patch.object(
            svc_module.TemplateService,
            "get_effective_template_content",
            AsyncMock(return_value=template),
        ),
    ]
    if sync_error is not None:
        patches.append(
            patch.object(
                PromptOptimizationService,
                "_execute_optimization_sync",
                MagicMock(side_effect=sync_error),
            )
        )
    else:
        patches.append(
            patch.object(
                PromptOptimizationService,
                "_execute_optimization_sync",
                MagicMock(
                    return_value=sync_result
                    or {
                        "optimized_template": "BETTER TEMPLATE",
                        "initial_score": 0.5,
                        "final_score": 0.9,
                    }
                ),
            )
        )
    return svc, patches


class TestIntentFormatScore:
    def test_full_contract_scores_one(self):
        out = (
            '{"intent": "generate_crew", "confidence": 0.95, '
            '"extracted_info": {"goal": "x"}, "suggested_prompt": "do it"}'
        )
        assert _intent_format_score(out) == pytest.approx(1.0)

    def test_invalid_intent_loses_main_weight(self):
        out = '{"intent": "nonsense", "confidence": 0.9, "extracted_info": {}, "suggested_prompt": "p"}'
        assert _intent_format_score(out) == pytest.approx(0.4)

    def test_garbage_scores_zero(self):
        assert _intent_format_score("not json at all {{{") == 0.0

    def test_out_of_range_confidence_not_counted(self):
        out = '{"intent": "generate_crew", "confidence": 1.7}'
        assert _intent_format_score(out) == pytest.approx(0.6)


class TestStartOptimization:
    @pytest.mark.asyncio
    async def test_inline_examples_run_completes(self):
        svc, patches = _service()
        with patches[0], patches[1]:
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group(),
            )
            run_id = result["run_id"]
            assert result["status"] == "pending"
            assert result["dataset_size"] == len(EXAMPLES)
            await svc_module._RUNS[run_id]["task"]
        run = svc.get_run(run_id, _group())
        assert run["status"] == "completed"
        assert run["optimized_template"] == "BETTER TEMPLATE"
        assert run["initial_score"] == 0.5
        assert run["final_score"] == 0.9
        assert run["baseline_template"] == "TEMPLATE"

    @pytest.mark.asyncio
    async def test_failure_is_captured_on_the_run(self):
        svc, patches = _service(sync_error=RuntimeError("gepa exploded"))
        with patches[0], patches[1]:
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group(),
            )
            await svc_module._RUNS[result["run_id"]]["task"]
        run = svc.get_run(result["run_id"], _group())
        assert run["status"] == "failed"
        assert "gepa exploded" in run["error"]

    @pytest.mark.asyncio
    async def test_too_few_examples_rejected(self):
        svc, patches = _service()
        with patches[0], patches[1]:
            with pytest.raises(ValueError, match="at least"):
                await svc.start_optimization(
                    PromptOptimizationRequest(
                        template_name="detect_intent", examples=["one", "two"]
                    ),
                    _group(),
                )

    @pytest.mark.asyncio
    async def test_empty_template_rejected(self):
        svc, patches = _service(template="   ")
        with patches[0], patches[1]:
            with pytest.raises(ValueError, match="template"):
                await svc.start_optimization(
                    PromptOptimizationRequest(
                        template_name="detect_intent", examples=EXAMPLES
                    ),
                    _group(),
                )


class TestMineExamples:
    @pytest.mark.asyncio
    async def test_filters_dedupes_and_respects_cutoff(self):
        svc = PromptOptimizationService(MagicMock())
        now = datetime.utcnow()
        rows = [
            SimpleNamespace(prompt="get the news", status="success", created_at=now),
            SimpleNamespace(
                prompt="Get The News", status="success", created_at=now
            ),  # dup (case)
            SimpleNamespace(
                prompt="broken", status="error", created_at=now
            ),  # not success
            SimpleNamespace(prompt="  ", status="success", created_at=now),  # empty
            SimpleNamespace(
                prompt="ancient", status="success", created_at=now - timedelta(days=99)
            ),
            SimpleNamespace(
                prompt="/load crew Some Crew", status="success", created_at=now
            ),  # slash command
            SimpleNamespace(
                prompt="Crew generation failed: no credentials", status="success", created_at=now
            ),  # system error string
            SimpleNamespace(prompt="run crew", status="success", created_at=now),
        ]
        svc.log_repository = MagicMock()
        svc.log_repository.get_logs_paginated_by_group = AsyncMock(
            side_effect=[rows, []]
        )
        examples = await svc._mine_examples(
            "detect-intent", _group(), lookback_days=30, max_examples=10
        )
        assert examples == ["get the news", "run crew"]

    @pytest.mark.asyncio
    async def test_no_group_returns_empty(self):
        svc = PromptOptimizationService(MagicMock())
        assert await svc._mine_examples("detect-intent", None, 30, 10) == []


class TestReflectionModelResolution:
    @pytest.mark.asyncio
    async def test_databricks_provider(self):
        svc = PromptOptimizationService(MagicMock())
        svc.model_repository.find_by_key = AsyncMock(
            return_value=SimpleNamespace(provider="databricks")
        )
        uri, env = await svc._resolve_reflection_model("some-endpoint")
        assert uri == "databricks:/some-endpoint"
        assert env == {}

    @pytest.mark.asyncio
    async def test_vllm_provider_sets_openai_env(self):
        svc = PromptOptimizationService(MagicMock())
        svc.model_repository.find_by_key = AsyncMock(
            return_value=SimpleNamespace(provider="vllm")
        )
        uri, env = await svc._resolve_reflection_model("Qwen3-Coder-30B-A3B-Instruct")
        assert uri == "openai:/Qwen3-Coder-30B-A3B-Instruct"
        assert "OPENAI_API_BASE" in env and "OPENAI_API_KEY" in env

    @pytest.mark.asyncio
    async def test_unknown_provider_raises(self):
        svc = PromptOptimizationService(MagicMock())
        svc.model_repository.find_by_key = AsyncMock(return_value=None)
        with pytest.raises(ValueError, match="unsupported provider"):
            await svc._resolve_reflection_model("mystery-model")


class TestGenericScoringHelpers:
    def test_extract_user_from_generation_log(self):
        assert (
            _extract_user_from_log("System: tpl text\nUser: build a report")
            == "build a report"
        )
        assert _extract_user_from_log("just a raw message") is None
        assert _extract_user_from_log("System: tpl\nUser:   ") is None

    def test_json_keys_score_fraction(self):
        keys = ("name", "role", "goal", "backstory")
        full = '{"name": "A", "role": "B", "goal": "C", "backstory": "D"}'
        assert _json_keys_score(full, keys) == pytest.approx(1.0)
        half = '{"name": "A", "role": "B", "goal": "", "backstory": null}'
        assert _json_keys_score(half, keys) == pytest.approx(0.5)
        assert _json_keys_score("not json {{{", keys) == 0.0

    def test_json_keys_score_accepts_lists_and_objects(self):
        keys = ("agents", "tasks")
        crew = '{"agents": [{"name": "A"}], "tasks": [{"name": "T"}]}'
        assert _json_keys_score(crew, keys) == pytest.approx(1.0)
        empty = '{"agents": [], "tasks": [{"name": "T"}]}'
        assert _json_keys_score(empty, keys) == pytest.approx(0.5)

    def test_job_name_score(self):
        from src.services.prompt_optimization_service import _job_name_score

        assert _job_name_score("Swiss News Digest") == pytest.approx(1.0)
        assert _job_name_score('"Sales Analysis"') == pytest.approx(1.0)
        assert _job_name_score("Run") == pytest.approx(0.5)
        assert _job_name_score('{"name": "Swiss News"}') == 0.0
        assert _job_name_score("") == 0.0


class TestRequirementsDistillation:
    def test_dedupes_repeated_complaints(self):
        notes = [
            "it is giving french side we need german side of switzerland",
            "It is giving FRENCH side, we need german side of switzerland!",
            "you provided me a link of rent and not buy.",
            "I am expecting apartments for sale and not rent",
            "",
            "it is giving french side we need german side of switzerland",
        ]
        reqs = _distill_requirements(notes)
        assert len(reqs) == 3
        assert reqs[0].startswith("it is giving french side")

    def test_respects_limit_and_order(self):
        notes = [f"requirement {i}" for i in range(12)]
        reqs = _distill_requirements(notes, limit=8)
        assert len(reqs) == 8
        assert reqs[0] == "requirement 0"

    def test_empty_input(self):
        assert _distill_requirements([]) == []
        assert _distill_requirements(["", "   ", None]) == []


class TestParseRequirementLines:
    def test_parses_numbered_lines(self):
        text = (
            "R1. Only German-speaking Switzerland.\n\n"
            "R2: Apartments for sale, not rent.\n"
            "Some trailing chatter."
        )
        assert _parse_requirement_lines(text) == [
            "Only German-speaking Switzerland.",
            "Apartments for sale, not rent.",
        ]

    def test_empty_or_chatter_returns_nothing(self):
        assert _parse_requirement_lines("") == []
        assert _parse_requirement_lines("I could not produce a list.") == []


class TestChecklistGrade:
    def test_grade_computed_from_marks_not_model_arithmetic(self):
        # The model claims "40" — the grade must come from the marks.
        verdict = "R1: FAIL — quotes Geneva\nR2: PASS\nR3: PASS\n\n40"
        grade = _checklist_grade(verdict, 3)
        # 0.8 * (2/3) + 0.2 * 0.5 (no Q mark -> default 5/10)
        assert grade == pytest.approx(0.8 * (2 / 3) + 0.1)

    def test_quality_mark_blends_in(self):
        verdict = "R1: PASS\nR2: PASS\nQ: 8"
        assert _checklist_grade(verdict, 2) == pytest.approx(0.8 + 0.2 * 0.8)

    def test_all_fail_scores_low_but_not_none(self):
        verdict = "R1: FAIL — x\nR2: FAIL — y\nQ: 0"
        assert _checklist_grade(verdict, 2) == pytest.approx(0.0)

    def test_no_marks_returns_none_for_fallback(self):
        assert _checklist_grade("I think this is a 7.", 3) is None
        assert _checklist_grade("", 3) is None

    def test_duplicate_marks_first_wins(self):
        verdict = "R1: PASS\nR1: FAIL — restated\nQ: 5"
        assert _checklist_grade(verdict, 1) == pytest.approx(0.8 + 0.1)

    def test_case_insensitive_marks(self):
        verdict = "r1: pass\nr2: fail — z"
        assert _checklist_grade(verdict, 2) == pytest.approx(0.8 * 0.5 + 0.1)


class TestRegistryResolution:
    @pytest.mark.asyncio
    async def test_local_mode_requires_both_env_vars(self, monkeypatch):
        svc = PromptOptimizationService(MagicMock())
        monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
        monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        uri, name = await svc._resolve_registry("detect_intent", _group())
        assert uri == "http://127.0.0.1:5555"
        assert name == "kasal_detect_intent_grp1"

    @pytest.mark.asyncio
    async def test_launch_value_survives_runtime_override(self, monkeypatch):
        # main.py overwrites MLFLOW_TRACKING_URI to "databricks" at startup but
        # preserves the launch value — local mode must use the launch value.
        svc = PromptOptimizationService(MagicMock())
        monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
        monkeypatch.setenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "databricks")
        uri, name = await svc._resolve_registry("detect_intent", _group())
        assert uri == "http://127.0.0.1:5555"
        assert name == "kasal_detect_intent_grp1"

    @pytest.mark.asyncio
    async def test_tracking_uri_alone_is_not_local_mode(self, monkeypatch):
        svc = PromptOptimizationService(MagicMock())
        monkeypatch.delenv("MCP_SERVER_ENABLED", raising=False)
        monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
        fake_db = MagicMock()
        fake_db.get_databricks_config = AsyncMock(
            return_value=SimpleNamespace(catalog="main", db_schema="kasal")
        )
        with patch(
            "src.services.databricks_service.DatabricksService", return_value=fake_db
        ):
            uri, name = await svc._resolve_registry("detect_intent", _group())
        assert uri == "databricks-uc"
        assert name == "main.kasal.kasal_detect_intent_grp1"

    @pytest.mark.asyncio
    async def test_managed_without_uc_config_raises(self, monkeypatch):
        svc = PromptOptimizationService(MagicMock())
        monkeypatch.delenv("MCP_SERVER_ENABLED", raising=False)
        monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
        monkeypatch.delenv("MLFLOW_TRACKING_URI", raising=False)
        fake_db = MagicMock()
        fake_db.get_databricks_config = AsyncMock(return_value=None)
        with patch(
            "src.services.databricks_service.DatabricksService", return_value=fake_db
        ):
            with pytest.raises(ValueError, match="catalog and schema"):
                await svc._resolve_registry("detect_intent", _group())


class TestVisibilityAndApply:
    @pytest.mark.asyncio
    async def test_runs_are_group_scoped(self):
        svc, patches = _service()
        with patches[0], patches[1]:
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group("grp1"),
            )
            await svc_module._RUNS[result["run_id"]]["task"]
        assert svc.get_run(result["run_id"], _group("other")) is None
        assert svc.list_runs(_group("other")) == []
        assert len(svc.list_runs(_group("grp1"))) == 1

    @pytest.mark.asyncio
    async def test_apply_writes_group_override(self):
        svc, patches = _service()
        with patches[0], patches[1]:
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group(),
            )
            await svc_module._RUNS[result["run_id"]]["task"]

        fake_row = SimpleNamespace(id=42)
        fake_template_service = MagicMock()
        fake_template_service.find_by_name_with_group_check = AsyncMock(
            return_value=fake_row
        )
        fake_template_service.update_with_group_check = AsyncMock(
            return_value=SimpleNamespace(id=42)
        )
        with patch.object(
            svc_module, "TemplateService", return_value=fake_template_service
        ):
            applied = await svc.apply_run(result["run_id"], _group())
        assert applied["applied"] is True
        update_args = fake_template_service.update_with_group_check.call_args
        assert update_args.args[0] == 42
        assert update_args.args[1].template == "BETTER TEMPLATE"
        assert svc.get_run(result["run_id"], _group())["applied"] is True

    @pytest.mark.asyncio
    async def test_apply_rejects_unfinished_run(self):
        svc = PromptOptimizationService(MagicMock())
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "template_name": "detect_intent",
            "status": "running",
            "group_id": "grp1",
            "created_at": datetime.utcnow(),
        }
        with pytest.raises(ValueError, match="no completed proposal"):
            await svc.apply_run("r1", _group("grp1"))

    @pytest.mark.asyncio
    async def test_apply_rejects_other_groups_run(self):
        svc = PromptOptimizationService(MagicMock())
        svc_module._RUNS["r2"] = {
            "run_id": "r2",
            "template_name": "detect_intent",
            "status": "completed",
            "optimized_template": "X",
            "group_id": "grp1",
            "created_at": datetime.utcnow(),
        }
        with pytest.raises(ValueError, match="not found"):
            await svc.apply_run("r2", _group("other"))
