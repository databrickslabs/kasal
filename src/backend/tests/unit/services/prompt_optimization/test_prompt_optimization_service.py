"""
Unit tests for PromptOptimizationService.

Covers the format scorer, example mining (dedupe/status/cutoff filters),
run lifecycle (start → background completion / failure), group-scoped
visibility, reflection-model resolution, and the apply flow.

Runs are DURABLE (a `prompt_optimization_runs` row), so reads/cancel/apply
are async and go through a repository. `_FakeRunRepo` below is an in-memory
stand-in that honors the real repository's group scoping and staleness
semantics, so these tests exercise the service's own merge/overlay logic
rather than mocking it away.
"""

import asyncio
import contextlib
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.exceptions import BadRequestError
from src.schemas.prompt_optimization import PromptOptimizationRequest
from src.services.catalog.agents import AgentService
from src.services.catalog.tasks import TaskService
from src.services.prompt_optimization import run_state as run_state_mod
from src.services.prompt_optimization import runs as runs_mod
from src.services.prompt_optimization import service as svc_module

# Shared helpers the service was split across now live in these modules, and the
# callers reach them through the module object — so patching HERE is what
# intercepts every caller. Patching them on ``svc_module`` would rebind only that
# module's own name and silently let the real implementation run.
from src.services.prompt_optimization.gepa import reflection as gepa_reflection
from src.services.prompt_optimization.service import (
    PromptOptimizationService,
    _checklist_grade,
    _distill_requirements,
    _extract_user_from_log,
    _grade_judge_verdict,
    _intent_format_score,
    _json_keys_score,
    _judge_sample_count,
    _median_sample,
    _parse_requirement_lines,
    _resolve_judge_model,
)
from src.utils.user_context import GroupContext

EXAMPLES = [
    "get the news",
    "run crew",
    "create an agent for support",
    "analyze sales",
    "make a report",
]

# Every column `_row_to_public` reads, so a fake row is shaped like the model.
_ROW_DEFAULTS = {
    "id": "r",
    "kind": "template",
    "target_name": "detect_intent",
    "crew_id": None,
    "status": "pending",
    "error": None,
    "model": None,
    "judge_model": None,
    "reflection_model": None,
    "budget": None,
    "dataset_size": 0,
    "executions_used": None,
    "execution_cap": None,
    "candidates_tried": None,
    "human_feedback_count": None,
    "initial_score": None,
    "final_score": None,
    "baseline_template": None,
    "optimized_template": None,
    "baseline_fields": None,
    "optimized_fields": None,
    "before_image": None,
    "applied": False,
    "applied_at": None,
    "applied_by": None,
    "group_id": None,
    "group_email": None,
    "created_by_email": None,
}


class _FakeRunRepo:
    """In-memory PromptOptimizationRunRepository over SimpleNamespace rows."""

    def __init__(self):
        self.rows = {}

    async def create(self, data):
        now = datetime.utcnow()
        row = SimpleNamespace(
            **{
                **_ROW_DEFAULTS,
                "created_at": now,
                "updated_at": now,
                **data,
            }
        )
        self.rows[row.id] = row
        return row

    async def get(self, run_id):
        return self.rows.get(run_id)

    async def get_by_group(self, run_id, group_id):
        row = self.rows.get(run_id)
        return row if row is not None and row.group_id == group_id else None

    async def list_by_group(self, group_id, limit=50):
        rows = [r for r in self.rows.values() if r.group_id == group_id]
        rows.sort(key=lambda r: r.created_at, reverse=True)
        return rows[:limit]

    async def update_fields(self, run_id, changes):
        row = self.rows.get(run_id)
        if row is None:
            return False
        for key, value in changes.items():
            setattr(row, key, value)
        row.updated_at = datetime.utcnow()
        return True

    async def delete(self, run_id, group_id):
        row = self.rows.get(run_id)
        if row is None or row.group_id != group_id:
            return False
        del self.rows[run_id]
        return True

    async def find_stale_active(self, group_id, stale_after_seconds):
        cutoff = datetime.utcnow() - timedelta(seconds=stale_after_seconds)
        return [
            r
            for r in self.rows.values()
            if r.status in ("pending", "running")
            and r.updated_at < cutoff
            and r.group_id == group_id
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


@pytest.fixture(autouse=True)
def configured_judge(monkeypatch):
    """Every test runs against a PROPERLY CONFIGURED system.

    Starting an optimization now refuses when no judge is set, because a judge
    that defaults to the model under optimization grades its own work. Tests
    that exercise judge resolution itself override this with their own
    monkeypatch.setenv / delenv.
    """
    monkeypatch.setenv("GEPA_JUDGE_MODEL", "independent-judge")


@contextlib.contextmanager
def _all(patches):
    """Enter every patch `_service` produced (count varies per scenario)."""
    with contextlib.ExitStack() as stack:
        for patch_cm in patches:
            stack.enter_context(patch_cm)
        yield


def _attach_run_repo(svc):
    """Give a service the fake repo AND route background persistence to it.

    `_persist_run_changes` opens its OWN session (background work must never
    reuse the request session), so it has to be redirected here or it would hit
    a real database.
    """
    repo = _FakeRunRepo()
    svc.run_repository = repo

    async def fake_persist(run_id, changes):
        await repo.update_fields(run_id, changes)

    return repo, patch.object(run_state_mod, "_persist_run_changes", fake_persist)


def _service(template="TEMPLATE", sync_result=None, sync_error=None):
    svc = PromptOptimizationService(MagicMock())
    svc.model_repository = MagicMock()
    svc.model_repository.find_by_key = AsyncMock(
        return_value=SimpleNamespace(provider="databricks")
    )
    svc._resolve_registry = AsyncMock(
        return_value=("http://127.0.0.1:5555", "kasal_detect_intent_grp1")
    )
    repo, persist_patch = _attach_run_repo(svc)
    svc._test_run_repo = repo
    patches = [
        patch.object(
            svc_module.TemplateService,
            "get_effective_template_content",
            AsyncMock(return_value=template),
        ),
        persist_patch,
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
        with _all(patches):
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
        run = await svc.get_run(run_id, _group())
        assert run["status"] == "completed"
        assert run["optimized_template"] == "BETTER TEMPLATE"
        assert run["initial_score"] == 0.5
        assert run["final_score"] == 0.9
        assert run["baseline_template"] == "TEMPLATE"

    @pytest.mark.asyncio
    async def test_failure_is_captured_on_the_run(self):
        svc, patches = _service(sync_error=RuntimeError("gepa exploded"))
        with _all(patches):
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group(),
            )
            await svc_module._RUNS[result["run_id"]]["task"]
        run = await svc.get_run(result["run_id"], _group())
        assert run["status"] == "failed"
        assert "gepa exploded" in run["error"]

    @pytest.mark.asyncio
    async def test_too_few_examples_rejected(self):
        svc, patches = _service()
        with _all(patches):
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
        with _all(patches):
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
                prompt="Crew generation failed: no credentials",
                status="success",
                created_at=now,
            ),  # system error string
            SimpleNamespace(prompt="run crew", status="success", created_at=now),
        ]
        # LLM logs are read through LLMLogService (execution's domain), not through
        # LLMLogRepository directly.
        svc.log_service = MagicMock()
        svc.log_service.get_logs_paginated_by_group = AsyncMock(side_effect=[rows, []])
        examples = await svc._mine_examples(
            "detect-intent", _group(), lookback_days=30, max_examples=10
        )
        assert examples == ["get the news", "run crew"]

    @pytest.mark.asyncio
    async def test_no_group_returns_empty(self):
        svc = PromptOptimizationService(MagicMock())
        assert await svc._mine_examples("detect-intent", None, 30, 10) == []


class TestLLMManagerRouting:
    """All LLM calls route through LLMManager — this covers the pure helpers
    that replaced the old URI/env resolver."""

    def test_stored_judge_model_to_key_strips_uri_schemes(self):
        to_key = svc_module._stored_judge_model_to_key
        assert to_key("openai:/qwen-30b") == "qwen-30b"
        assert to_key("databricks:/databricks-llama-4-maverick") == (
            "databricks-llama-4-maverick"
        )
        assert to_key("deepseek:/deepseek-v4-pro") == "deepseek-v4-pro"
        assert to_key("bare-kasal-key") == "bare-kasal-key"
        assert to_key(None) is None
        assert to_key("") is None

    def test_parse_grade_from_text(self):
        parse = svc_module._parse_grade_from_text
        assert parse("Solid answer.\n7") == pytest.approx(0.7)
        assert parse("thinking 3 things...\nfinal grade: 9") == pytest.approx(0.9)
        # (10, 100] read as a percentage — clamping alone made "40" a perfect 10
        assert parse("40") == pytest.approx(0.4)
        assert parse("no digits here") is None
        assert parse("") is None

    def test_reflection_bridge_overrides_reflection_lm(self, monkeypatch):
        import sys
        import types

        calls = {}
        fake_gepa = types.ModuleType("gepa")

        def original_optimize(**kwargs):
            calls.update(kwargs)
            return "result"

        fake_gepa.optimize = original_optimize
        monkeypatch.setitem(sys.modules, "gepa", fake_gepa)

        svc_module._install_gepa_reflection_bridge()
        assert getattr(fake_gepa.optimize, "_kasal_reflection_bridge", False)
        # Idempotent — a second install must not double-wrap.
        wrapped = fake_gepa.optimize
        svc_module._install_gepa_reflection_bridge()
        assert fake_gepa.optimize is wrapped

        marker = object()
        svc_module._GEPA_REFLECTION_STATE.reflection_fn = marker
        try:
            fake_gepa.optimize(reflection_lm="openai/placeholder")
            assert calls["reflection_lm"] is marker
        finally:
            svc_module._GEPA_REFLECTION_STATE.reflection_fn = None

        # With no override armed, the caller's reflection_lm passes through.
        calls.clear()
        fake_gepa.optimize(reflection_lm="openai/placeholder")
        assert calls["reflection_lm"] == "openai/placeholder"

    def test_preflight_wraps_provider_errors(self):
        with patch.object(
            gepa_reflection,
            "_sync_llm_completion",
            MagicMock(side_effect=RuntimeError("connection refused")),
        ):
            with pytest.raises(ValueError, match="failed a test call"):
                svc_module._preflight_reflection(MagicMock(), "dead-model", None, None)

    def test_reflection_fn_shapes_messages_and_params(self):
        captured = {}

        def fake_completion(loop, messages, model, max_tokens, **kwargs):
            captured.update(
                {
                    "messages": messages,
                    "model": model,
                    "max_tokens": max_tokens,
                    **kwargs,
                }
            )
            return "IMPROVED DOC"

        with patch.object(gepa_reflection, "_sync_llm_completion", fake_completion):
            fn = svc_module._make_reflection_fn(MagicMock(), "qwen-30b", None, None)
            assert fn("improve this") == "IMPROVED DOC"
        assert captured["model"] == "qwen-30b"
        assert captured["temperature"] == 0.8
        assert captured["max_tokens"] == 6000
        # cache-buster system line + the actual prompt
        assert captured["messages"][0]["role"] == "system"
        assert "reflection request" in captured["messages"][0]["content"]
        assert captured["messages"][1] == {
            "role": "user",
            "content": "improve this",
        }
        # A second call must produce a DIFFERENT cache-buster.
        first_buster = captured["messages"][0]["content"]
        with patch.object(gepa_reflection, "_sync_llm_completion", fake_completion):
            fn = svc_module._make_reflection_fn(MagicMock(), "qwen-30b", None, None)
            fn("improve this")
        assert captured["messages"][0]["content"] != first_buster


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
        from src.services.prompt_optimization.service import _job_name_score

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


class TestCrewDocFenceRescue:
    DOC = (
        "[AGENT a1]\nROLE: r\nGOAL: g\nBACKSTORY: b\n\n"
        "[TASK t1]\nDESCRIPTION: d\nEXPECTED_OUTPUT: e"
    )

    def test_fenced_doc_parses(self):
        fenced = f"```\n{self.DOC}\n```"
        fields = svc_module._parse_crew_doc(fenced)
        assert fields is not None
        assert fields["agent.a1.role"] == "r"
        assert fields["task.t1.expected_output"] == "e"

    def test_language_tagged_fence_parses(self):
        fenced = f"```text\n{self.DOC}\n```"
        assert svc_module._parse_crew_doc(fenced) is not None

    def test_json_blob_still_rejected(self):
        assert svc_module._parse_crew_doc('{"instruction": "be better"}') is None


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
            "src.services.databricks.workspace.service.DatabricksService",
            return_value=fake_db,
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
            "src.services.databricks.workspace.service.DatabricksService",
            return_value=fake_db,
        ):
            with pytest.raises(ValueError, match="catalog and schema"):
                await svc._resolve_registry("detect_intent", _group())


class TestVisibilityAndApply:
    @pytest.mark.asyncio
    async def test_runs_are_group_scoped(self):
        svc, patches = _service()
        with _all(patches):
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent", examples=EXAMPLES
                ),
                _group("grp1"),
            )
            await svc_module._RUNS[result["run_id"]]["task"]
        assert await svc.get_run(result["run_id"], _group("other")) is None
        assert await svc.list_runs(_group("other")) == []
        assert len(await svc.list_runs(_group("grp1"))) == 1

    @pytest.mark.asyncio
    async def test_apply_writes_group_override(self):
        svc, patches = _service()
        with _all(patches):
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
            runs_mod, "TemplateService", return_value=fake_template_service
        ):
            applied = await svc.apply_run(result["run_id"], _group())
        assert applied["applied"] is True
        update_args = fake_template_service.update_with_group_check.call_args
        assert update_args.args[0] == 42
        assert update_args.args[1].template == "BETTER TEMPLATE"
        assert (await svc.get_run(result["run_id"], _group()))["applied"] is True

    @pytest.mark.asyncio
    async def test_apply_rejects_unfinished_run(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "r1",
                "target_name": "detect_intent",
                "status": "running",
                "group_id": "grp1",
            }
        )
        with pytest.raises(ValueError, match="no completed proposal"):
            await svc.apply_run("r1", _group("grp1"))

    @pytest.mark.asyncio
    async def test_apply_rejects_other_groups_run(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "r2",
                "target_name": "detect_intent",
                "status": "completed",
                "optimized_template": "X",
                "group_id": "grp1",
            }
        )
        with pytest.raises(ValueError, match="not found"):
            await svc.apply_run("r2", _group("other"))


class TestCrewDocSerialization:
    """The serialize/parse pair is the GEPA mutation contract: candidates that
    survive parsing execute for real; everything else free-rejects."""

    @staticmethod
    def _crew():
        agent = SimpleNamespace(
            id="a1",
            role="Researcher",
            goal="Find facts",
            backstory="line1\nline2",
            group_id=None,
        )
        task = SimpleNamespace(
            id="t1",
            description="Do the research",
            expected_output="A table",
            group_id=None,
        )
        return [agent], [task]

    def test_round_trip_preserves_fields_and_keys(self):
        agents, tasks = self._crew()
        doc, keys = svc_module._serialize_crew_doc(agents, tasks)
        fields = svc_module._parse_crew_doc(doc)
        assert fields is not None
        assert set(fields) == set(keys)
        assert fields["agent.a1.role"] == "Researcher"
        assert fields["task.t1.expected_output"] == "A table"

    def test_multiline_field_survives_via_continuation_lines(self):
        agents, tasks = self._crew()
        doc, _ = svc_module._serialize_crew_doc(agents, tasks)
        fields = svc_module._parse_crew_doc(doc)
        assert fields["agent.a1.backstory"] == "line1\nline2"

    def test_doc_layout_uses_labeled_sections(self):
        agents, tasks = self._crew()
        doc, _ = svc_module._serialize_crew_doc(agents, tasks)
        assert "[AGENT a1]" in doc
        assert "[TASK t1]" in doc
        assert "ROLE: Researcher" in doc
        assert "EXPECTED_OUTPUT: A table" in doc

    def test_label_before_any_entity_is_rejected(self):
        assert svc_module._parse_crew_doc("ROLE: orphan") is None

    def test_plain_prose_is_rejected(self):
        assert svc_module._parse_crew_doc("Here is a better prompt for you.") is None

    def test_json_blob_is_rejected(self):
        assert svc_module._parse_crew_doc('{"instruction": "be better"}') is None

    def test_empty_doc_is_rejected(self):
        assert svc_module._parse_crew_doc("") is None
        assert svc_module._parse_crew_doc(None) is None

    def test_mutated_doc_with_changed_key_set_detectable(self):
        # A mutation that drops a section parses, but the key set differs —
        # the caller compares against expected_keys and rejects for free.
        agents, tasks = self._crew()
        _, keys = svc_module._serialize_crew_doc(agents, tasks)
        partial = "[AGENT a1]\nROLE: Only role"
        fields = svc_module._parse_crew_doc(partial)
        assert fields is not None
        assert set(fields) != set(keys)


class TestJudgeValueToGrade:
    def test_numeric_scales(self):
        grade = svc_module._judge_value_to_grade
        assert grade(7) == 0.7
        assert grade(0.4) == 0.4
        assert grade("3") == pytest.approx(0.3)
        assert grade(10) == 1.0
        # Above the 0-10 scale clamps rather than exploding
        assert grade(15) == 1.0

    def test_booleans(self):
        assert svc_module._judge_value_to_grade(True) == 1.0
        assert svc_module._judge_value_to_grade(False) == 0.0

    def test_categorical_words(self):
        grade = svc_module._judge_value_to_grade
        assert grade("excellent") == 1.0
        assert grade("Satisfactory") == 0.75
        assert grade("partial") == 0.5
        assert grade("poor") == 0.25
        assert grade("fail") == 0.0

    def test_unusable_values_return_none(self):
        assert svc_module._judge_value_to_grade(None) is None
        assert svc_module._judge_value_to_grade("gibberish verdict") is None


class _FakeMlflowRegistry:
    """Fake mlflow + mlflow.genai.{judges,scorers} module tree that records
    registrations so judge-lifecycle tests never touch a real server."""

    def __init__(self):
        import types

        self.registered = []  # (name, instructions, model)
        self.deleted = []
        self.experiments = []
        self.scorers = {}

        registry = self

        class _Judge:
            def __init__(self, name, instructions, model):
                self.name = name
                self.instructions = instructions
                self.model = model

            def register(self):
                registry.registered.append((self.name, self.instructions, self.model))
                registry.scorers[self.name] = self

        mlflow = types.ModuleType("mlflow")
        mlflow.get_tracking_uri = lambda: "prev://"
        mlflow.set_tracking_uri = lambda uri: None
        mlflow.set_experiment = lambda name: registry.experiments.append(name)

        genai = types.ModuleType("mlflow.genai")
        judges = types.ModuleType("mlflow.genai.judges")
        judges.make_judge = (
            lambda name, instructions, model, feedback_value_type: _Judge(
                name, instructions, model
            )
        )
        scorers = types.ModuleType("mlflow.genai.scorers")
        scorers.get_scorer = lambda name: registry.scorers[name]
        scorers.delete_scorer = lambda name, version: registry.deleted.append(
            (name, version)
        )
        scorers.list_scorers = lambda: list(registry.scorers.values())
        mlflow.genai = genai
        genai.judges = judges
        genai.scorers = scorers
        self.modules = {
            "mlflow": mlflow,
            "mlflow.genai": genai,
            "mlflow.genai.judges": judges,
            "mlflow.genai.scorers": scorers,
        }


@pytest.fixture()
def fake_mlflow(monkeypatch):
    import sys

    registry = _FakeMlflowRegistry()
    for name, module in registry.modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setenv("MCP_SERVER_ENABLED", "true")
    monkeypatch.setenv("MLFLOW_TRACKING_URI", "http://127.0.0.1:5555")
    monkeypatch.delenv("KASAL_LAUNCH_MLFLOW_TRACKING_URI", raising=False)
    return registry


def _judge_service():
    return PromptOptimizationService(MagicMock())


class TestJudgeLifecycle:
    @pytest.mark.asyncio
    async def test_create_from_crew_registers_library_and_scoped_copy(
        self, fake_mlflow
    ):
        svc = _judge_service()
        crew_id = "88ab4478-823c-4f12-b1ca-8e74c568995e"
        result = await svc.create_judge(
            "accuracy", "Rate accuracy 0-10.", crew_id=crew_id, group_context=_group()
        )
        names = [name for name, _, _ in fake_mlflow.registered]
        assert names == ["accuracy", "crew_88ab4478823c__accuracy"]
        assert result["full_name"] == "crew_88ab4478823c__accuracy"
        # {{ outputs }} template variable auto-appended when missing
        assert "{{ outputs }}" in fake_mlflow.registered[0][1]

    @pytest.mark.asyncio
    async def test_create_without_crew_registers_library_only(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("style", "Judge style of {{ outputs }}.")
        assert [n for n, _, _ in fake_mlflow.registered] == ["style"]

    @pytest.mark.asyncio
    async def test_create_validates_inputs(self, fake_mlflow):
        svc = _judge_service()
        with pytest.raises(ValueError, match="name"):
            await svc.create_judge("   ", "criteria")
        with pytest.raises(ValueError, match="instructions"):
            await svc.create_judge("ok", "   ")

    @pytest.mark.asyncio
    async def test_create_requires_local_mode(self, fake_mlflow, monkeypatch):
        monkeypatch.setenv("MCP_SERVER_ENABLED", "false")
        svc = _judge_service()
        with pytest.raises(ValueError, match="local MLflow"):
            await svc.create_judge("x", "y")

    @pytest.mark.asyncio
    async def test_update_replaces_instructions_keeps_model(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("acc", "Old criteria for {{ outputs }}.")
        fake_mlflow.registered.clear()
        result = await svc.update_judge(
            "acc", instructions="New criteria.", group_context=_group()
        )
        assert len(fake_mlflow.registered) == 1
        name, instructions, model = fake_mlflow.registered[0]
        assert name == "acc"
        assert "New criteria." in instructions
        assert "{{ outputs }}" in instructions
        # unchanged from creation (the wrapped default Kasal key)
        assert model == f"openai:/{svc_module.DEFAULT_TARGET_MODEL}"
        assert result["model"] == f"openai:/{svc_module.DEFAULT_TARGET_MODEL}"

    @pytest.mark.asyncio
    async def test_update_model_keeps_instructions(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("acc", "Keep these criteria for {{ outputs }}.")
        fake_mlflow.registered.clear()
        await svc.update_judge("acc", model="deepseek-v4-pro", group_context=_group())
        name, instructions, model = fake_mlflow.registered[0]
        assert model == "openai:/deepseek-v4-pro"
        assert "Keep these criteria" in instructions

    @pytest.mark.asyncio
    async def test_update_with_nothing_to_change_rejected(self, fake_mlflow):
        svc = _judge_service()
        with pytest.raises(ValueError, match="Nothing to update"):
            await svc.update_judge("acc")

    @pytest.mark.asyncio
    async def test_assign_copies_source_into_crew_scope(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("shared", "Shared criteria for {{ outputs }}.")
        fake_mlflow.registered.clear()
        result = await svc.assign_judge(
            "shared", "11112222-3333-4444-5555-666677778888"
        )
        assert result["full_name"] == "crew_111122223333__shared"
        name, instructions, _ = fake_mlflow.registered[0]
        assert name == "crew_111122223333__shared"
        assert "Shared criteria" in instructions

    @pytest.mark.asyncio
    async def test_delete_removes_all_versions(self, fake_mlflow):
        svc = _judge_service()
        assert await svc.delete_judge("obsolete") is True
        assert fake_mlflow.deleted == [("obsolete", "all")]

    @pytest.mark.asyncio
    async def test_list_splits_library_and_crew_judges(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("lib", "Library judge for {{ outputs }}.")
        await svc.assign_judge("lib", "aaaabbbbccccdddd")
        judges = await svc.list_judges()
        by_full = {j["full_name"]: j for j in judges}
        assert by_full["lib"]["crew_id"] is None
        assert by_full["crew_aaaabbbbcccc__lib"]["crew_id"] == "aaaabbbbcccc"
        assert by_full["crew_aaaabbbbcccc__lib"]["name"] == "lib"

    @pytest.mark.asyncio
    async def test_every_operation_pins_the_experiment(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("a", "b {{ outputs }}")
        await svc.list_judges()
        await svc.delete_judge("a")
        # create, list and delete each pinned (assign/update covered above via
        # the same helper); the exact count just needs to be one per call.
        assert len(fake_mlflow.experiments) == 3


class TestRunRegistryBehaviors:
    def test_public_fields_expose_progress_chips(self):
        assert "human_feedback_count" in svc_module._PUBLIC_FIELDS
        assert "candidates_tried" in svc_module._PUBLIC_FIELDS
        assert "executions_used" in svc_module._PUBLIC_FIELDS

    @pytest.mark.asyncio
    async def test_get_run_never_leaks_internal_keys(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "r1",
                "target_name": "detect_intent",
                "status": "running",
                "group_id": "grp1",
            }
        )
        # The in-process entry carries un-serializable internals; the projection
        # must never surface them (the task handle would break the response).
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "running",
            "group_id": "grp1",
            "task": object(),
            "cancel_requested": False,
        }
        run = await svc.get_run("r1", _group("grp1"))
        assert "task" not in run
        assert "cancel_requested" not in run
        assert set(run) == set(svc_module._PUBLIC_FIELDS)

    @pytest.mark.asyncio
    async def test_list_runs_sorts_descending_and_returns_aware_timestamps(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        # Rows store NAIVE utc (the models' convention).
        await repo.create(
            {
                "id": "old",
                "status": "completed",
                "group_id": "grp1",
                "created_at": datetime.utcnow() - timedelta(minutes=5),
                "updated_at": datetime.utcnow(),
            }
        )
        await repo.create(
            {
                "id": "new",
                "status": "completed",
                "group_id": "grp1",
                "created_at": datetime.utcnow(),
                "updated_at": datetime.utcnow(),
            }
        )
        runs = await svc.list_runs(_group("grp1"))
        assert [r["run_id"] for r in runs] == ["new", "old"]
        # tz-AWARE on the way out so browsers localize (a naive stamp rendered a
        # 01:20 local run as "11:20 PM" — observed live).
        assert runs[0]["created_at"].tzinfo is not None

    @pytest.mark.asyncio
    async def test_live_counters_overlay_the_row(self):
        """The worker thread bumps counters in memory between heartbeats — a
        read must show those, not the row's lagging values."""
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "r1",
                "status": "running",
                "group_id": "grp1",
                "executions_used": 1,
                "candidates_tried": 1,
            }
        )
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "running",
            "group_id": "grp1",
            "executions_used": 4,
            "candidates_tried": 3,
        }
        run = await svc.get_run("r1", _group("grp1"))
        assert run["executions_used"] == 4
        assert run["candidates_tried"] == 3

    @pytest.mark.asyncio
    async def test_live_counters_do_not_cross_groups(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {"id": "r1", "status": "running", "group_id": "grp1", "executions_used": 1}
        )
        # A cache entry recorded under a DIFFERENT group must not be overlaid.
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "running",
            "group_id": "other",
            "executions_used": 99,
        }
        run = await svc.get_run("r1", _group("grp1"))
        assert run["executions_used"] == 1

    @pytest.mark.asyncio
    async def test_cancel_run_transitions_and_guards(self):
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create({"id": "r1", "status": "running", "group_id": "grp1"})
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "running",
            "group_id": "grp1",
        }
        with persist:
            result = await svc.cancel_run("r1", _group("grp1"))
            assert result["cancelling"] is True
            assert svc_module._RUNS["r1"]["cancel_requested"] is True

            await repo.update_fields("r1", {"status": "completed"})
            with pytest.raises(ValueError, match="not active"):
                await svc.cancel_run("r1", _group("grp1"))
            with pytest.raises(ValueError, match="not found"):
                await svc.cancel_run("missing", _group("grp1"))

    @pytest.mark.asyncio
    async def test_delete_run_removes_row_and_cache_group_scoped(self):
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create({"id": "r1", "status": "pending", "group_id": "grp1"})
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "pending",
            "group_id": "grp1",
        }
        with persist:
            # Wrong group cannot delete it.
            other = await svc.delete_run("r1", _group("other"))
            assert other["deleted"] is False
            assert "r1" in repo.rows

            # Owning group deletes the row and evicts the cache entry.
            result = await svc.delete_run("r1", _group("grp1"))
            assert result["deleted"] is True
            assert "r1" not in repo.rows
            assert "r1" not in svc_module._RUNS

            # Deleting a missing run is idempotent, not an error.
            again = await svc.delete_run("r1", _group("grp1"))
            assert again["deleted"] is False

    @pytest.mark.asyncio
    async def test_cancel_settles_a_run_orphaned_by_a_restart(self):
        """Active in the DB but absent from this process: nothing to signal, so
        the record is settled instead of hanging at 'running' forever."""
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create({"id": "r1", "status": "running", "group_id": "grp1"})
        with persist:
            result = await svc.cancel_run("r1", _group("grp1"))
        assert result["cancelling"] is True
        assert repo.rows["r1"].status == "cancelled"

    @pytest.mark.asyncio
    async def test_stale_active_run_is_failed_on_read(self):
        """A run whose heartbeat died with its backend must not keep the UI's
        'run in progress' lock engaged forever."""
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "dead",
                "status": "running",
                "group_id": "grp1",
                "updated_at": datetime.utcnow()
                - timedelta(seconds=svc_module.RUN_STALE_SECONDS + 60),
            }
        )
        with persist:
            runs = await svc.list_runs(_group("grp1"))
        assert runs[0]["status"] == "failed"
        assert "restarted" in runs[0]["error"]

    @pytest.mark.asyncio
    async def test_long_running_live_run_is_never_settled(self):
        """A legitimately long crew run (hours) is in _RUNS, so a lagging
        heartbeat must not be mistaken for an orphan."""
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "slow",
                "status": "running",
                "group_id": "grp1",
                "updated_at": datetime.utcnow()
                - timedelta(seconds=svc_module.RUN_STALE_SECONDS + 60),
            }
        )
        svc_module._RUNS["slow"] = {
            "run_id": "slow",
            "status": "running",
            "group_id": "grp1",
        }
        with persist:
            runs = await svc.list_runs(_group("grp1"))
        assert runs[0]["status"] == "running"
        assert repo.rows["slow"].status == "running"

    @pytest.mark.asyncio
    async def test_cached_run_with_finished_task_is_settled(self):
        """A run whose task DIED before writing a terminal status is still in
        _RUNS, but its task is done — it must be settled, not treated as alive
        forever (the bug that wedged the UI's 'run in progress' lock until a
        manual DB delete)."""
        svc = PromptOptimizationService(MagicMock())
        repo, persist = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "dead_task",
                "status": "running",
                "group_id": "grp1",
                "updated_at": datetime.utcnow()
                - timedelta(seconds=svc_module.RUN_STALE_SECONDS + 60),
            }
        )
        done_task = asyncio.get_event_loop().create_future()
        done_task.set_result(None)  # a finished task
        svc_module._RUNS["dead_task"] = {
            "run_id": "dead_task",
            "status": "running",
            "group_id": "grp1",
            "task": done_task,
        }
        with persist:
            runs = await svc.list_runs(_group("grp1"))
        assert runs[0]["status"] == "failed"
        assert repo.rows["dead_task"].status == "failed"

    def test_prune_keeps_active_runs(self):
        from datetime import timezone

        base = datetime.now(timezone.utc)
        for i in range(svc_module._MAX_KEPT_RUNS + 5):
            svc_module._RUNS[f"r{i}"] = {
                "run_id": f"r{i}",
                "status": "completed" if i else "running",
                "group_id": "grp1",
                "created_at": base + timedelta(seconds=i),
            }
        PromptOptimizationService._prune_runs()
        assert len(svc_module._RUNS) == svc_module._MAX_KEPT_RUNS
        assert "r0" in svc_module._RUNS  # the running one survived pruning


class TestJudgeModelResolution:
    """Target == judge is self-preference: the judge prefers its own outputs, so
    the score climbs whether or not the prompts improved. Because that score is
    the fitness function GEPA optimises against, the run is REFUSED rather than
    warned about — a warning still produced an authoritative-looking number that
    nobody could distinguish from a real gain."""

    def test_explicit_request_value_wins(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "configured-judge")
        assert _resolve_judge_model("asked-for", "target", "x") == "asked-for"

    def test_configured_default_used_when_unset(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "configured-judge")
        assert _resolve_judge_model(None, "target", "x") == "configured-judge"
        assert _resolve_judge_model("   ", "target", "x") == "configured-judge"

    def test_configured_default_equal_to_target_is_refused(self, monkeypatch):
        """A GEPA_JUDGE_MODEL that happens to BE the target buys nothing, and
        must not look like a deliberate, safe choice."""
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "target")
        with pytest.raises(BadRequestError) as err:
            _resolve_judge_model(None, "target", "run x")
        assert "GEPA_JUDGE_MODEL" in str(err.value)

    def test_no_judge_configured_is_refused(self, monkeypatch):
        """Silently falling back to the target is what made every unconfigured
        run report a meaningless score."""
        monkeypatch.delenv("GEPA_JUDGE_MODEL", raising=False)
        with pytest.raises(BadRequestError) as err:
            _resolve_judge_model(None, "the-target", "run x")
        assert "the-target" in str(err.value)
        assert "judge_model" in str(err.value)

    def test_explicitly_choosing_the_target_is_refused(self, monkeypatch):
        """The hand-picked case: the old guard only covered defaulting, so
        selecting the same model in both dropdowns sailed through."""
        monkeypatch.delenv("GEPA_JUDGE_MODEL", raising=False)
        with pytest.raises(BadRequestError):
            _resolve_judge_model("t", "t", "run x")

    @pytest.mark.asyncio
    async def test_start_optimization_records_a_non_target_judge(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_MODEL", "independent-judge")
        svc, patches = _service()
        with _all(patches):
            result = await svc.start_optimization(
                PromptOptimizationRequest(
                    template_name="detect_intent",
                    examples=EXAMPLES,
                    model="target-model",
                ),
                _group(),
            )
            await svc_module._RUNS[result["run_id"]]["task"]
            kwargs = (
                PromptOptimizationService._execute_optimization_sync.call_args.kwargs
            )
        assert kwargs["target_model"] == "target-model"
        assert kwargs["judge_model"] == "independent-judge"
        run = await svc.get_run(result["run_id"], _group())
        # Recorded on the run so a reader can tell whether it judged itself.
        assert run["judge_model"] == "independent-judge"
        assert run["model"] == "target-model"


class TestJudgeSampling:
    def test_default_and_env_override(self, monkeypatch):
        monkeypatch.delenv("GEPA_JUDGE_SAMPLES", raising=False)
        assert _judge_sample_count() == svc_module.DEFAULT_JUDGE_SAMPLES
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "5")
        assert _judge_sample_count() == 5
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        assert _judge_sample_count() == 1

    def test_bounds_and_garbage(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "0")
        assert _judge_sample_count() == 1
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "99")
        assert _judge_sample_count() == 9
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "three")
        assert _judge_sample_count() == svc_module.DEFAULT_JUDGE_SAMPLES
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "  ")
        assert _judge_sample_count() == svc_module.DEFAULT_JUDGE_SAMPLES

    def test_median_ignores_a_wild_outlier(self):
        """The observed failure: identical prompts graded 0.0 and 4/10 minutes
        apart. A mean would let the 0.0 move the score; the median does not."""
        grade, rationale = _median_sample(
            [(0.0, "zero"), (0.8, "eight"), (0.7, "seven")]
        )
        assert grade == pytest.approx(0.7)
        assert rationale == "seven"

    def test_single_sample_is_returned_untouched(self):
        assert _median_sample([(0.42, "why")]) == (0.42, "why")

    def test_even_count_averages_the_middle_pair(self):
        grade, _ = _median_sample([(0.2, "a"), (0.4, "b"), (0.6, "c"), (0.8, "d")])
        assert grade == pytest.approx(0.5)

    def test_rationale_is_the_sample_nearest_the_median(self):
        # Even count -> median 0.5 lies between samples; the nearest one wins so
        # the text GEPA reads explains the score it was given.
        _, rationale = _median_sample([(0.4, "low"), (0.6, "high")])
        assert rationale in ("low", "high")

    def test_no_samples_is_a_zero(self):
        assert _median_sample([]) == (0.0, "")

    def test_wide_spread_is_reported(self, caplog):
        with caplog.at_level("WARNING"):
            _median_sample([(0.0, "a"), (0.5, "b"), (1.0, "c")])
        assert "disagreed with itself" in caplog.text

    def test_tight_spread_is_quiet(self, caplog):
        with caplog.at_level("WARNING"):
            _median_sample([(0.70, "a"), (0.75, "b"), (0.72, "c")])
        assert "disagreed with itself" not in caplog.text


class TestGradeJudgeVerdict:
    """The parsing contract the multi-sample loop reuses per sample."""

    def test_checklist_wins_over_the_models_own_arithmetic(self):
        # "40" at the end would clamp to a perfect 10/10 if last-number ran.
        grade, rationale = _grade_judge_verdict(
            "R1: FAIL — quotes Geneva\nR2: PASS\nR3: PASS\n\n40", 3
        )
        assert grade == pytest.approx(0.8 * (2 / 3) + 0.1)
        assert "Geneva" in rationale

    def test_last_number_wins_when_no_checklist(self):
        grade, _ = _grade_judge_verdict("thinking about 3 things...\n7", 0)
        assert grade == pytest.approx(0.7)

    def test_above_ten_is_read_as_a_percentage(self):
        assert _grade_judge_verdict("40", 0)[0] == pytest.approx(0.4)

    def test_checklist_mode_falls_back_to_last_number_without_marks(self):
        # No R<n> marks at all — the run still has requirements, but this reply
        # cannot be scored from marks, so last-number parsing applies.
        assert _grade_judge_verdict("Overall I would say 6", 3)[0] == pytest.approx(0.6)

    def test_ungradable_reply_is_none_not_zero(self):
        """A parse miss is not evidence the deliverable was bad — the sample is
        discarded rather than scored 0."""
        assert _grade_judge_verdict("no digits at all", 0) is None
        assert _grade_judge_verdict("", 2) is None


class TestApplyIsReversible:
    """A crew apply overwrites live agent/task prompt text. Crew-level GEPA gain
    INVERTS with team size (measured positive at 2 agents, negative at 10), so a
    good-looking proposal can permanently degrade a large crew — apply must take
    a before-image and revert must restore it."""

    @staticmethod
    async def _completed_crew_run(svc, repo, optimized=None):
        await repo.create(
            {
                "id": "c1",
                "kind": "crew",
                "target_name": "crew:Research",
                "crew_id": "crew-uuid",
                "status": "completed",
                "group_id": "grp1",
                "optimized_template": "[AGENT a1]\nROLE: New role",
                "baseline_fields": {"agent.a1.role": "Old role"},
                "optimized_fields": optimized
                or {"agent.a1.role": "New role", "task.t1.description": "New desc"},
            }
        )

    @staticmethod
    def _entity_repos(agent_current="LIVE role", task_current="LIVE desc"):
        """Agent/task repositories whose CURRENT values differ from the run's
        baseline — the before-image must capture what is actually overwritten,
        not what the run started from.

        ``group_id`` matches the run's ("grp1"): apply and revert now go through
        AgentService/TaskService, which refuse an entity outside the caller's group.
        """
        agent_repo = MagicMock()
        agent_repo.get = AsyncMock(
            return_value=SimpleNamespace(id="a1", role=agent_current, group_id="grp1")
        )
        agent_repo.update = AsyncMock(return_value=True)
        task_repo = MagicMock()
        task_repo.get = AsyncMock(
            return_value=SimpleNamespace(
                id="t1", description=task_current, group_id="grp1"
            )
        )
        task_repo.update = AsyncMock(return_value=True)
        return agent_repo, task_repo

    @contextlib.contextmanager
    def _patched_entity_repos(self, agent_repo, task_repo):
        """Stub the SERVICE methods apply/revert now go through.

        These used to patch AgentRepository/TaskRepository, because the optimiser
        called them directly. It goes through AgentService/TaskService now (for the
        group check), so the seam moved up one layer — the fakes still carry the
        agent/task rows, they are just reached via the service.
        """
        with (
            patch.object(
                AgentService,
                "get_with_group_check",
                new=AsyncMock(side_effect=lambda i, g: agent_repo.get.return_value),
            ),
            patch.object(
                TaskService,
                "get_with_group_check",
                new=AsyncMock(side_effect=lambda i, g: task_repo.get.return_value),
            ),
            # Real async funcs, not AsyncMock(side_effect=lambda ...): a lambda
            # returning a coroutine leaves it un-awaited, so the tests'
            # assert_awaited_* on the repo mock would never see the call.
            patch.object(
                AgentService,
                "update_prompt_text_with_group_check",
                new=_forward_to(agent_repo.update),
            ),
            patch.object(
                TaskService,
                "update_prompt_text_with_group_check",
                new=_forward_to(task_repo.update),
            ),
        ):
            yield

    @pytest.mark.asyncio
    async def test_crew_apply_snapshots_the_live_values(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await self._completed_crew_run(svc, repo)
        agent_repo, task_repo = self._entity_repos()
        with self._patched_entity_repos(agent_repo, task_repo):
            result = await svc.apply_run("c1", _group("grp1"))
        assert result["applied"] is True
        row = repo.rows["c1"]
        # The before-image is the LIVE value, not the run's stale baseline.
        assert row.before_image == {
            "agent.a1.role": "LIVE role",
            "task.t1.description": "LIVE desc",
        }
        assert row.applied is True
        assert row.applied_at is not None
        assert row.applied_by == "user@example.com"
        agent_repo.update.assert_awaited_once_with("a1", {"role": "New role"})
        task_repo.update.assert_awaited_once_with("t1", {"description": "New desc"})

    @pytest.mark.asyncio
    async def test_revert_writes_the_before_image_back(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await self._completed_crew_run(svc, repo)
        agent_repo, task_repo = self._entity_repos()
        with self._patched_entity_repos(agent_repo, task_repo):
            await svc.apply_run("c1", _group("grp1"))
            agent_repo.update.reset_mock()
            task_repo.update.reset_mock()
            result = await svc.revert_run("c1", _group("grp1"))
        assert result["reverted"] is True
        assert result["applied"] is False
        assert result["restored"] == 2
        agent_repo.update.assert_awaited_once_with("a1", {"role": "LIVE role"})
        task_repo.update.assert_awaited_once_with("t1", {"description": "LIVE desc"})
        row = repo.rows["c1"]
        assert row.applied is False
        # CONSUMED: a second revert would push a stale snapshot over fresh edits.
        assert row.before_image is None
        assert row.applied_at is None

    @pytest.mark.asyncio
    async def test_second_revert_is_refused(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await self._completed_crew_run(svc, repo)
        agent_repo, task_repo = self._entity_repos()
        with self._patched_entity_repos(agent_repo, task_repo):
            await svc.apply_run("c1", _group("grp1"))
            await svc.revert_run("c1", _group("grp1"))
            with pytest.raises(ValueError, match="has not been applied"):
                await svc.revert_run("c1", _group("grp1"))

    @pytest.mark.asyncio
    async def test_revert_refuses_a_run_applied_without_a_before_image(self):
        """Rows written by an older backend carry no snapshot — refuse loudly
        rather than pretend a revert happened."""
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "legacy",
                "kind": "crew",
                "target_name": "crew:Old",
                "status": "completed",
                "group_id": "grp1",
                "applied": True,
                "before_image": None,
            }
        )
        with pytest.raises(ValueError, match="no before-image"):
            await svc.revert_run("legacy", _group("grp1"))

    @pytest.mark.asyncio
    async def test_revert_is_group_scoped(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "x1",
                "target_name": "detect_intent",
                "status": "completed",
                "group_id": "grp1",
                "applied": True,
                "before_image": {"template": "OLD"},
            }
        )
        with pytest.raises(ValueError, match="not found"):
            await svc.revert_run("x1", _group("other"))

    @pytest.mark.asyncio
    async def test_template_apply_and_revert_round_trip(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await repo.create(
            {
                "id": "t1",
                "kind": "template",
                "target_name": "detect_intent",
                "status": "completed",
                "group_id": "grp1",
                "optimized_template": "NEW TEMPLATE",
            }
        )
        template_service = MagicMock()
        template_service.find_by_name_with_group_check = AsyncMock(
            return_value=SimpleNamespace(id=7, template="CURRENT TEMPLATE")
        )
        template_service.update_with_group_check = AsyncMock(
            return_value=SimpleNamespace(id=7)
        )
        with patch.object(runs_mod, "TemplateService", return_value=template_service):
            await svc.apply_run("t1", _group("grp1"))
            assert repo.rows["t1"].before_image == {"template": "CURRENT TEMPLATE"}
            await svc.revert_run("t1", _group("grp1"))
        last = template_service.update_with_group_check.await_args
        assert last.args[1].template == "CURRENT TEMPLATE"
        assert repo.rows["t1"].applied is False

    @pytest.mark.asyncio
    async def test_revertible_flag_tracks_the_before_image(self):
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await self._completed_crew_run(svc, repo)
        assert (await svc.get_run("c1", _group("grp1")))["revertible"] is False
        agent_repo, task_repo = self._entity_repos()
        with self._patched_entity_repos(agent_repo, task_repo):
            await svc.apply_run("c1", _group("grp1"))
            assert (await svc.get_run("c1", _group("grp1")))["revertible"] is True
            await svc.revert_run("c1", _group("grp1"))
        assert (await svc.get_run("c1", _group("grp1")))["revertible"] is False

    @pytest.mark.asyncio
    async def test_apply_survives_a_restart(self):
        """The whole point of the durable row: the in-process cache is empty
        (as after `--reload`) and the proposal is still appliable."""
        svc = PromptOptimizationService(MagicMock())
        repo, _ = _attach_run_repo(svc)
        await self._completed_crew_run(svc, repo)
        assert svc_module._RUNS == {}
        agent_repo, task_repo = self._entity_repos()
        with self._patched_entity_repos(agent_repo, task_repo):
            result = await svc.apply_run("c1", _group("grp1"))
        assert result["applied"] is True


# ---------------------------------------------------------------------------
# Orchestration: the two blocking optimization bodies. Every other test in this
# file patches these out, leaving ~700 lines (MLflow span setup, caching, cap
# enforcement, gepa_kwargs, judge orchestration) unexercised. The fakes below
# stand in for mlflow + gepa + the LLM + crew execution so the real control flow
# runs end to end without spending a token or executing a crew.
# ---------------------------------------------------------------------------

_LAZY_IMPORTS = (
    "openai",
    "openai.resources",
    "openai.resources.chat",
    "openai.resources.chat.completions",
    "openai.resources.completions",
    "openai.resources.embeddings",
    "openai.resources.images",
    "openai.resources.beta",
    "openai.resources.beta.chat",
    "openai.resources.beta.chat.completions",
    "openai.resources.responses",
    "litellm",
    "databricks",
    "databricks.sdk",
    "mlflow.openai",
)


class _FakeFeedback:
    def __init__(self, name=None, value=None, rationale=None):
        self.name = name
        self.value = value
        self.rationale = rationale


class _FakePromptClient:
    """Stands in for MlflowClient's prompt-registry surface. `template` is what
    load_prompt hands back — the fake optimizer moves it to simulate GEPA
    writing a new candidate to the registry."""

    def __init__(self, template=""):
        self.template = template
        self.registered = []

    def register_prompt(self, name, template, commit_message=None):
        self.registered.append((name, template))
        self.template = template
        return SimpleNamespace(uri=f"prompts:/{name}/1")

    def load_prompt(self, uri):
        return SimpleNamespace(template=self.template, uri=uri)


class _FakeOptimizeStack:
    """The mlflow + gepa module tree both sync bodies import."""

    def __init__(self, optimize_prompts):
        import types

        self.client = _FakePromptClient()
        self.registry_uris = []
        self.tracking_uris = []
        self.gepa_optimize_calls = []

        registry = self

        mlflow = types.ModuleType("mlflow")
        mlflow.set_registry_uri = lambda uri: registry.registry_uris.append(uri)
        mlflow.get_registry_uri = lambda: (
            registry.registry_uris[-1] if registry.registry_uris else ""
        )
        mlflow.get_tracking_uri = lambda: "prev://"
        mlflow.set_tracking_uri = lambda uri: registry.tracking_uris.append(uri)
        mlflow.set_experiment = lambda name: None
        mlflow.MlflowClient = lambda **kwargs: registry.client
        mlflow.search_traces = lambda **kwargs: []
        mlflow.update_current_trace = lambda **kwargs: None

        entities = types.ModuleType("mlflow.entities")
        entities.Feedback = _FakeFeedback

        genai = types.ModuleType("mlflow.genai")
        genai.optimize_prompts = optimize_prompts

        optimize = types.ModuleType("mlflow.genai.optimize")

        class GepaPromptOptimizer:
            def __init__(self, reflection_model, max_metric_calls, gepa_kwargs=None):
                self.reflection_model = reflection_model
                self.max_metric_calls = max_metric_calls
                self.gepa_kwargs = gepa_kwargs or {}

        optimize.GepaPromptOptimizer = GepaPromptOptimizer

        scorers = types.ModuleType("mlflow.genai.scorers")
        scorers.scorer = lambda fn: fn  # identity: the body's plain functions
        scorers.list_scorers = lambda: []

        trace_mod = types.ModuleType("mlflow.models.evaluation.utils.trace")
        trace_mod.FLAVOR_TO_MODULE_NAME = {"openai": "openai", "crewai": "crewai"}

        autolog_mod = types.ModuleType("mlflow.utils.autologging_utils")
        # Pre-seeded with a value that must be RESTORED, not left disabled.
        autolog_mod.AUTOLOGGING_INTEGRATIONS = {"openai": {"disable": False}}
        self.autolog = autolog_mod.AUTOLOGGING_INTEGRATIONS

        gepa = types.ModuleType("gepa")

        def gepa_optimize(**kwargs):
            registry.gepa_optimize_calls.append(kwargs)
            return None

        gepa.optimize = gepa_optimize

        self.modules = {
            "mlflow": mlflow,
            "mlflow.entities": entities,
            "mlflow.genai": genai,
            "mlflow.genai.optimize": optimize,
            "mlflow.genai.scorers": scorers,
            "mlflow.models": types.ModuleType("mlflow.models"),
            "mlflow.models.evaluation": types.ModuleType("mlflow.models.evaluation"),
            "mlflow.models.evaluation.utils": types.ModuleType(
                "mlflow.models.evaluation.utils"
            ),
            "mlflow.models.evaluation.utils.trace": trace_mod,
            "mlflow.utils": types.ModuleType("mlflow.utils"),
            "mlflow.utils.autologging_utils": autolog_mod,
            "gepa": gepa,
        }
        for name in _LAZY_IMPORTS:
            self.modules.setdefault(name, types.ModuleType(name))


@contextlib.contextmanager
def _fake_stack(optimize_prompts, completion):
    """Install the fake mlflow/gepa tree and the fake LLM for one body call."""
    import sys

    stack = _FakeOptimizeStack(optimize_prompts)
    with (
        patch.dict(sys.modules, stack.modules),
        patch.object(gepa_reflection, "_sync_llm_completion", completion),
    ):
        # The bridge patches gepa.optimize in place; it must re-install against
        # the fake module rather than reuse a wrapper from a previous test.
        yield stack


def _fake_completion(handler, calls):
    def completion(loop, messages, model, max_tokens, **kwargs):
        calls.append(
            {
                "model": model,
                "messages": messages,
                "max_tokens": max_tokens,
                "text": " ".join(str(m.get("content", "")) for m in messages),
                **kwargs,
            }
        )
        return handler(calls[-1])

    return completion


class TestTemplateOptimizationOrchestration:
    """_execute_optimization_sync driven end to end against fake mlflow/gepa."""

    @staticmethod
    def _handler(call):
        # The judge is instructed to answer CORRECT or WRONG.
        if "You judge an intent classifier" in call["text"]:
            return "The mapping looks right. CORRECT"
        return '{"intent": "generate_crew", "confidence": 0.9, "extracted_info": {}, "suggested_prompt": "p"}'

    def _run(self, calls, target="target-model", judge="judge-model"):
        captured = {}

        def optimize_prompts(
            *,
            predict_fn,
            train_data,
            prompt_uris,
            optimizer,
            scorers,
            aggregation,
            enable_tracking,
        ):
            fmt, correct = scorers
            captured["optimizer"] = optimizer
            captured["prompt_uris"] = prompt_uris
            captured["enable_tracking"] = enable_tracking
            captured["train_data"] = train_data
            # The reflection callable must be ARMED while gepa runs — the bridge
            # can only swap it in if it is on the thread-local right now.
            captured["reflection_armed"] = (
                getattr(svc_module._GEPA_REFLECTION_STATE, "reflection_fn", None)
                is not None
            )
            captured["scored"] = []
            for record in train_data:
                output = predict_fn(**record["inputs"])
                scores = {
                    "output_format": fmt(outputs=output),
                    "output_correct": correct(inputs=record["inputs"], outputs=output),
                }
                captured["scored"].append((output, scores, aggregation(scores)))
            return SimpleNamespace(
                optimized_prompts=[
                    SimpleNamespace(template="IMPROVED", uri="prompts:/p/2")
                ],
                initial_eval_score=0.5,
                final_eval_score=0.82,
            )

        with _fake_stack(
            optimize_prompts, _fake_completion(self._handler, calls)
        ) as st:
            result = PromptOptimizationService._execute_optimization_sync(
                loop=MagicMock(),
                template_name="detect_intent",
                baseline="BASELINE TEMPLATE",
                examples=["book a flight", "research the market"],
                input_key="message",
                target_model=target,
                judge_model=judge,
                reflection_model="reflect-model",
                max_metric_calls=12,
                registry_uri="databricks-uc",
                prompt_name="main.kasal.kasal_detect_intent_grp1",
                group_context=None,
            )
        return result, captured, st

    def test_result_is_mapped_from_the_optimizer(self):
        result, captured, stack = self._run([])
        assert result["optimized_template"] == "IMPROVED"
        assert result["initial_score"] == pytest.approx(0.5)
        assert result["final_score"] == pytest.approx(0.82)
        assert result["prompt_uri"] == "prompts:/p/2"
        # Baseline registered, and the registry (not global tracking) retargeted.
        assert stack.client.registered == [
            ("main.kasal.kasal_detect_intent_grp1", "BASELINE TEMPLATE")
        ]
        assert stack.registry_uris == ["databricks-uc"]
        # Managed mode: tracking left alone, tracking writes skipped.
        assert stack.tracking_uris == []
        assert captured["enable_tracking"] is False
        assert captured["prompt_uris"] == [
            "prompts:/main.kasal.kasal_detect_intent_grp1/1"
        ]

    def test_predict_uses_the_candidate_template_and_target_model(self):
        calls = []
        self._run(calls)
        predicts = [c for c in calls if c["model"] == "target-model"]
        assert len(predicts) == 2  # one per training example
        assert predicts[0]["messages"][0] == {
            "role": "system",
            "content": "BASELINE TEMPLATE",
        }
        assert predicts[0]["messages"][1]["content"] == "book a flight"

    def test_judge_runs_on_the_judge_model_not_the_target(self):
        """The self-preference guard, asserted where it matters: the grading call
        must not be issued to the model under optimization."""
        calls = []
        self._run(calls, target="target-model", judge="independent-judge")
        judge_calls = [
            c for c in calls if "You judge an intent classifier" in c["text"]
        ]
        assert judge_calls
        assert all(c["model"] == "independent-judge" for c in judge_calls)
        assert all(c["model"] != "target-model" for c in judge_calls)

    def test_scorers_and_aggregation_are_wired(self):
        _, captured, _ = self._run([])
        output, scores, aggregate = captured["scored"][0]
        assert scores["output_format"] == pytest.approx(1.0)
        assert scores["output_correct"] == pytest.approx(1.0)
        assert aggregate == pytest.approx(0.4 * 1.0 + 0.6 * 1.0)

    def test_training_rows_carry_the_input_key(self):
        _, captured, _ = self._run([])
        assert captured["train_data"][0] == {
            "inputs": {"message": "book a flight"},
            "expectations": {},
        }

    def test_budget_reaches_the_optimizer(self):
        _, captured, _ = self._run([])
        assert captured["optimizer"].max_metric_calls == 12

    def test_reflection_bridge_is_armed_during_and_cleared_after(self):
        _, captured, _ = self._run([])
        assert captured["reflection_armed"] is True
        assert getattr(svc_module._GEPA_REFLECTION_STATE, "reflection_fn", None) is None

    def test_autolog_flags_are_restored(self):
        """Flavors are force-disabled for the span to dodge the importlib
        deadlock; the caller's original values must come back."""
        _, _, stack = self._run([])
        assert stack.autolog["openai"]["disable"] is False
        # A flavor that had NO prior setting must not be left with one.
        assert "disable" not in stack.autolog.get("crewai", {})


def _forward_to(repo_update):
    """A group-checked service updater that delegates to a repository mock.

    Awaits the mock so ``assert_awaited_once_with`` in the tests still describes
    the write that happened.
    """

    async def _update(_self, entity_id, fields, group_context=None):
        # `_self`: patch.object replaces an unbound method, so the instance is
        # passed positionally.
        return bool(await repo_update(entity_id, fields))

    return _update


def _crew_fixture():
    """A one-agent/one-task crew, serialized exactly as the service does."""
    agent = SimpleNamespace(
        id="a1",
        name="Researcher",
        role="Researcher",
        goal="Find facts",
        backstory="Experienced",
        tools=[],
        llm="m",
        # Real Agent rows always have this; the apply path is group-checked.
        group_id=None,
    )
    task = SimpleNamespace(
        id="t1",
        name="Research",
        description="Do the research",
        expected_output="A table",
        tools=[],
        agent_id="a1",
        group_id=None,
    )
    doc, keys = svc_module._serialize_crew_doc([agent], [task])
    agents_yaml = {
        "Researcher": {
            "name": "Researcher",
            "role": agent.role,
            "goal": agent.goal,
            "backstory": agent.backstory,
            "tools": [],
            "llm": "m",
            "_field_prefix": "agent.a1",
        }
    }
    tasks_yaml = {
        "Research": {
            "name": "Research",
            "description": task.description,
            "expected_output": task.expected_output,
            "tools": [],
            "agent": "Researcher",
            "async_execution": False,
            "context": [],
            "_field_prefix": "task.t1",
        }
    }
    return doc, keys, agents_yaml, tasks_yaml


def _variant(doc, marker):
    """A structurally VALID mutation of the crew doc (parses, same key set)."""
    return doc.replace("GOAL: Find facts", f"GOAL: Find facts about {marker}")


class TestCrewOptimizationOrchestration:
    """_execute_crew_optimization_sync end to end: cap enforcement, the caches
    that keep real crew executions down to one per distinct candidate, the
    cancel flag, answer-first judging, and median sampling."""

    RUN_ID = "run123"

    def _drive(
        self,
        docs,
        judge_replies=None,
        max_metric_calls=10,
        on_candidate=None,
        run_entry=True,
        samples_env=None,
        monkeypatch=None,
    ):
        """Run the body while a fake optimizer walks `docs` as GEPA candidates."""
        baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
        calls = []
        replies = list(judge_replies or [])
        executions = {"n": 0, "payloads": []}
        scored = []

        if run_entry:
            svc_module._RUNS[self.RUN_ID] = {
                "run_id": self.RUN_ID,
                "status": "running",
                "group_id": "grp1",
                "executions_used": 0,
                "candidates_tried": 0,
            }

        def handler(call):
            if "reply with OK" in call["text"]:
                return "OK"
            if "GROUND TRUTH" in call["text"]:
                return "REFERENCE ANSWER: expect a table.\nMUST INCLUDE: sources"
            return replies.pop(0) if replies else "7"

        def fake_run_crew(loop, agents_yaml, tasks_yaml, model, timeout, **kwargs):
            executions["n"] += 1
            executions["payloads"].append((agents_yaml, tasks_yaml))
            return (
                f"Deliverable number {executions['n']} — a long enough answer "
                f"to clear the fifty character format floor."
            )

        def optimize_prompts(
            *,
            predict_fn,
            train_data,
            prompt_uris,
            optimizer,
            scorers,
            aggregation,
            enable_tracking,
        ):
            fmt, correct = scorers
            for index, doc in enumerate(docs):
                if on_candidate:
                    on_candidate(index)
                stack.client.template = doc
                output = predict_fn(**train_data[0]["inputs"])
                scores = {
                    "output_format": fmt(outputs=output),
                    "output_correct": correct(
                        inputs=train_data[0]["inputs"], outputs=output
                    ),
                }
                scored.append((output, scores, aggregation(scores)))
            return SimpleNamespace(
                optimized_prompts=[
                    SimpleNamespace(template=docs[-1], uri="prompts:/c/2")
                ],
                initial_eval_score=0.4,
                final_eval_score=0.7,
            )

        if samples_env is not None:
            monkeypatch.setenv("GEPA_JUDGE_SAMPLES", samples_env)

        with _fake_stack(optimize_prompts, _fake_completion(handler, calls)) as stack:
            with patch.object(gepa_reflection, "_sync_run_crew", fake_run_crew):
                result = PromptOptimizationService._execute_crew_optimization_sync(
                    loop=MagicMock(),
                    baseline_doc=baseline_doc,
                    field_keys=keys,
                    objective="Crew 'Research': survey the market",
                    rubric="- Research: A table",
                    agents_yaml=agents_yaml,
                    tasks_yaml=tasks_yaml,
                    target_model="crew-model",
                    judge_model="independent-judge",
                    reflection_model="reflect-model",
                    max_metric_calls=max_metric_calls,
                    execution_timeout=60,
                    registry_uri="databricks-uc",
                    prompt_name="main.kasal.kasal_crew_x_grp1",
                    crew_id="crew-uuid",
                    cancel_run_id=self.RUN_ID,
                    group_context=None,
                )
        return SimpleNamespace(
            result=result,
            calls=calls,
            executions=executions,
            scored=scored,
            baseline_doc=baseline_doc,
            stack=stack,
        )

    # -- cap -----------------------------------------------------------------

    def test_execution_cap_is_hard(self, monkeypatch):
        """The user's budget is a promise about REAL crew executions (tools,
        emails, DB writes). GEPA overshoots its own metric budget, so the cap is
        enforced here — over-budget candidates get a free empty result."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        docs = [base] + [_variant(base, m) for m in ("a", "b", "c")]
        run = self._drive(docs, max_metric_calls=2)
        assert run.executions["n"] == 2
        assert svc_module._RUNS[self.RUN_ID]["executions_used"] == 2
        # Over-cap candidates scored 0 without executing and without a judge call.
        assert run.scored[2][0] == ""
        assert run.scored[2][1]["output_format"] == 0.0
        assert run.scored[2][1]["output_correct"].value == 0.0
        assert "Empty deliverable" in run.scored[2][1]["output_correct"].rationale

    def test_cap_needs_the_run_entry_to_count_against(self, monkeypatch):
        """Sanity-check the mechanism: the counter lives on the in-process run
        entry, which start_crew_optimization always creates."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base], max_metric_calls=2)
        assert svc_module._RUNS[self.RUN_ID]["candidates_tried"] == 1

    # -- caching -------------------------------------------------------------

    def test_repeated_candidate_never_executes_twice(self, monkeypatch):
        """GEPA re-evaluates the same doc many times (smoke test, baseline valset
        pass, a fresh minibatch pass every iteration). Uncached, those re-runs
        ate a small budget re-measuring the baseline."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        other = _variant(base, "z")
        run = self._drive([base, base, other, base], max_metric_calls=10)
        assert run.executions["n"] == 2  # two DISTINCT docs, four evaluations
        # The repeat returned the cached deliverable, not an empty string.
        assert run.scored[1][0] == run.scored[0][0]

    def test_identical_deliverable_is_judged_once(self, monkeypatch):
        """judge_cache: re-scoring the same text must be free, which is also what
        keeps baseline comparisons stable inside a run."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base, base, base], max_metric_calls=10)
        grading = [c for c in run.calls if "HARSH grader" in c["text"]]
        assert len(grading) == 1
        assert (
            run.scored[0][1]["output_correct"].value
            == run.scored[2][1]["output_correct"].value
        )

    def test_malformed_candidate_is_rejected_for_free(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base, '{"instruction": "be better"}'], max_metric_calls=10)
        assert run.executions["n"] == 1
        assert run.scored[1][0] == ""

    # -- cancel --------------------------------------------------------------

    def test_cancel_flag_stops_the_loop(self, monkeypatch):
        """Honored BEFORE the next crew execution — an in-flight one finishes."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        docs = [base] + [_variant(base, m) for m in ("a", "b")]

        def cancel_before_second(index):
            if index == 1:
                svc_module._RUNS[self.RUN_ID]["cancel_requested"] = True

        with pytest.raises(RuntimeError, match="Cancelled by user"):
            self._drive(docs, max_metric_calls=10, on_candidate=cancel_before_second)
        # The first candidate executed; nothing after the flag did.
        assert svc_module._RUNS[self.RUN_ID]["executions_used"] == 1

    # -- answer-first judging ------------------------------------------------

    def test_judge_commits_a_reference_before_grading_anything(self, monkeypatch):
        """A reference-free judge is exploitable by the optimizer; the judge must
        write its own answer BEFORE it sees a candidate."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base, _variant(base, "a")], max_metric_calls=10)
        kinds = [
            (
                "reference"
                if "GROUND TRUTH" in c["text"]
                else "grade" if "HARSH grader" in c["text"] else "other"
            )
            for c in run.calls
        ]
        assert "reference" in kinds and "grade" in kinds
        assert kinds.index("reference") < kinds.index("grade")
        # ONE extra call per RUN, not per candidate.
        assert kinds.count("reference") == 1
        # The reference request must not leak a candidate deliverable into it.
        reference_call = next(c for c in run.calls if "GROUND TRUTH" in c["text"])
        assert "Deliverable number" not in reference_call["text"]

    def test_grading_prompt_carries_the_committed_reference(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base], max_metric_calls=10)
        grading = next(c for c in run.calls if "HARSH grader" in c["text"])
        assert "REFERENCE ANSWER: expect a table." in grading["text"]
        assert "committed" in grading["text"]
        assert grading["model"] == "independent-judge"

    def test_reference_failure_degrades_instead_of_failing_the_run(
        self, monkeypatch, caplog
    ):
        """If the reference call dies the run still completes — reference-free —
        but says so, because that is the exploitable mode."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        calls = []
        scored = []

        def handler(call):
            if "reply with OK" in call["text"]:
                return "OK"
            if "GROUND TRUTH" in call["text"]:
                raise RuntimeError("reference model unavailable")
            return "6"

        def optimize_prompts(
            *,
            predict_fn,
            train_data,
            prompt_uris,
            optimizer,
            scorers,
            aggregation,
            enable_tracking,
        ):
            fmt, correct = scorers
            output = predict_fn(**train_data[0]["inputs"])
            scored.append(correct(inputs=train_data[0]["inputs"], outputs=output))
            return SimpleNamespace(
                optimized_prompts=[SimpleNamespace(template=base, uri="u")],
                initial_eval_score=0.1,
                final_eval_score=0.2,
            )

        baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
        svc_module._RUNS[self.RUN_ID] = {
            "run_id": self.RUN_ID,
            "status": "running",
            "group_id": "grp1",
            "executions_used": 0,
        }
        with caplog.at_level("WARNING"):
            with _fake_stack(optimize_prompts, _fake_completion(handler, calls)):
                with patch.object(
                    gepa_reflection,
                    "_sync_run_crew",
                    lambda *a, **k: "A deliverable long enough to clear the format floor easily.",
                ):
                    PromptOptimizationService._execute_crew_optimization_sync(
                        loop=MagicMock(),
                        baseline_doc=baseline_doc,
                        field_keys=keys,
                        objective="obj",
                        rubric="- r",
                        agents_yaml=agents_yaml,
                        tasks_yaml=tasks_yaml,
                        target_model="crew-model",
                        judge_model="independent-judge",
                        reflection_model="reflect-model",
                        max_metric_calls=10,
                        execution_timeout=60,
                        registry_uri="databricks-uc",
                        prompt_name="p",
                        crew_id="crew-uuid",
                        cancel_run_id=self.RUN_ID,
                        group_context=None,
                    )
        assert "REFERENCE-FREE" in caplog.text
        assert scored[0].value == pytest.approx(0.6)  # grading still happened

    # -- median sampling -----------------------------------------------------

    def test_median_of_samples_is_used_not_the_mean(self, monkeypatch):
        """The judge has graded identical prompts 0.0 then 4/10 minutes apart.
        Median of three ignores the outlier; a mean would not."""
        base, _, _, _ = _crew_fixture()
        run = self._drive(
            [base],
            judge_replies=["0", "8", "7"],
            max_metric_calls=10,
            samples_env="3",
            monkeypatch=monkeypatch,
        )
        grading = [c for c in run.calls if "HARSH grader" in c["text"]]
        assert len(grading) == 3
        assert run.scored[0][1]["output_correct"].value == pytest.approx(0.7)

    def test_samples_are_cache_busted_so_they_differ(self, monkeypatch):
        """Without a per-sample buster, litellm's process-global cache would
        replay sample 1 as samples 2..N."""
        base, _, _, _ = _crew_fixture()
        run = self._drive(
            [base],
            judge_replies=["5", "6", "7"],
            max_metric_calls=10,
            samples_env="3",
            monkeypatch=monkeypatch,
        )
        grading = [c for c in run.calls if "HARSH grader" in c["text"]]
        busters = [c["messages"][0]["content"] for c in grading]
        assert all(b.startswith("grading pass ") for b in busters)
        assert len(set(busters)) == 3

    def test_single_sample_takes_the_no_op_path(self, monkeypatch):
        """N=1 must be byte-identical to the old single-draw prompt: no buster."""
        base, _, _, _ = _crew_fixture()
        run = self._drive(
            [base],
            judge_replies=["6"],
            max_metric_calls=10,
            samples_env="1",
            monkeypatch=monkeypatch,
        )
        grading = [c for c in run.calls if "HARSH grader" in c["text"]]
        assert len(grading) == 1
        assert grading[0]["messages"][0]["content"].startswith("You are a HARSH grader")
        assert run.scored[0][1]["output_correct"].value == pytest.approx(0.6)

    def test_partial_judge_outage_still_yields_a_median(self, monkeypatch):
        """Some samples failing is survivable; only a total outage is fatal."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "3")
        base, _, _, _ = _crew_fixture()
        baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
        calls = []
        scored = []
        state = {"grades": 0}

        def handler(call):
            if "reply with OK" in call["text"]:
                return "OK"
            if "GROUND TRUTH" in call["text"]:
                return "REFERENCE"
            state["grades"] += 1
            if state["grades"] < 3:
                raise RuntimeError("judge provider 500")
            return "8"

        def optimize_prompts(
            *,
            predict_fn,
            train_data,
            prompt_uris,
            optimizer,
            scorers,
            aggregation,
            enable_tracking,
        ):
            _, correct = scorers
            output = predict_fn(**train_data[0]["inputs"])
            scored.append(correct(inputs=train_data[0]["inputs"], outputs=output))
            return SimpleNamespace(
                optimized_prompts=[SimpleNamespace(template=base, uri="u")],
                initial_eval_score=0.1,
                final_eval_score=0.2,
            )

        svc_module._RUNS[self.RUN_ID] = {
            "run_id": self.RUN_ID,
            "status": "running",
            "group_id": "grp1",
            "executions_used": 0,
        }
        with _fake_stack(optimize_prompts, _fake_completion(handler, calls)):
            with patch.object(
                gepa_reflection,
                "_sync_run_crew",
                lambda *a, **k: "A deliverable long enough to clear the format floor easily.",
            ):
                PromptOptimizationService._execute_crew_optimization_sync(
                    loop=MagicMock(),
                    baseline_doc=baseline_doc,
                    field_keys=keys,
                    objective="obj",
                    rubric="- r",
                    agents_yaml=agents_yaml,
                    tasks_yaml=tasks_yaml,
                    target_model="crew-model",
                    judge_model="independent-judge",
                    reflection_model="reflect-model",
                    max_metric_calls=10,
                    execution_timeout=60,
                    registry_uri="databricks-uc",
                    prompt_name="p",
                    crew_id="crew-uuid",
                    cancel_run_id=self.RUN_ID,
                    group_context=None,
                )
        assert scored[0].value == pytest.approx(0.8)

    def test_total_judge_outage_is_loud(self, monkeypatch):
        """Silent zeros flatten the landscape and make a run look like 'no
        improvement possible' — a dead judge must raise."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "2")
        base, _, _, _ = _crew_fixture()

        def handler(call):
            if "reply with OK" in call["text"]:
                return "OK"
            if "GROUND TRUTH" in call["text"]:
                return "REFERENCE"
            raise RuntimeError("judge provider is down")

        with pytest.raises(RuntimeError, match="judge provider is down"):
            self._drive_with_handler(handler, [base])

    def _drive_with_handler(self, handler, docs):
        baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
        calls = []
        svc_module._RUNS[self.RUN_ID] = {
            "run_id": self.RUN_ID,
            "status": "running",
            "group_id": "grp1",
            "executions_used": 0,
        }

        def optimize_prompts(
            *,
            predict_fn,
            train_data,
            prompt_uris,
            optimizer,
            scorers,
            aggregation,
            enable_tracking,
        ):
            _, correct = scorers
            output = predict_fn(**train_data[0]["inputs"])
            correct(inputs=train_data[0]["inputs"], outputs=output)
            return SimpleNamespace(
                optimized_prompts=[SimpleNamespace(template=docs[0], uri="u")],
                initial_eval_score=0.1,
                final_eval_score=0.2,
            )

        with _fake_stack(optimize_prompts, _fake_completion(handler, calls)):
            with patch.object(
                gepa_reflection,
                "_sync_run_crew",
                lambda *a, **k: "A deliverable long enough to clear the format floor easily.",
            ):
                return PromptOptimizationService._execute_crew_optimization_sync(
                    loop=MagicMock(),
                    baseline_doc=baseline_doc,
                    field_keys=keys,
                    objective="obj",
                    rubric="- r",
                    agents_yaml=agents_yaml,
                    tasks_yaml=tasks_yaml,
                    target_model="crew-model",
                    judge_model="independent-judge",
                    reflection_model="reflect-model",
                    max_metric_calls=10,
                    execution_timeout=60,
                    registry_uri="databricks-uc",
                    prompt_name="p",
                    crew_id="crew-uuid",
                    cancel_run_id=self.RUN_ID,
                    group_context=None,
                )

    # -- wiring --------------------------------------------------------------

    def test_candidate_fields_are_overlaid_onto_the_execution_payload(
        self, monkeypatch
    ):
        """The mutated GOAL must actually reach the crew that gets executed."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([_variant(base, "solar")], max_metric_calls=10)
        agents_over, _ = run.executions["payloads"][0]
        assert agents_over["Researcher"]["goal"] == "Find facts about solar"
        # The internal routing key must never reach the engine.
        assert "_field_prefix" not in agents_over["Researcher"]

    def test_result_reports_the_parsed_optimized_fields(self, monkeypatch):
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([_variant(base, "wind")], max_metric_calls=10)
        assert (
            run.result["optimized_fields"]["agent.a1.goal"] == "Find facts about wind"
        )
        assert run.result["initial_score"] == pytest.approx(0.4)
        assert run.result["final_score"] == pytest.approx(0.7)

    def test_reflection_is_preflighted_before_any_execution(self, monkeypatch):
        """A dead reflection model does not fail a run — GEPA just proposes
        nothing after burning the whole budget. So it is pinged first."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        run = self._drive([base], max_metric_calls=10)
        assert "reply with OK" in run.calls[0]["text"]
        assert run.calls[0]["model"] == "reflect-model"

    def test_gepa_kwargs_keep_the_document_contract(self, monkeypatch):
        """These were each learned from a failure mode; losing one regresses it."""
        monkeypatch.setenv("GEPA_JUDGE_SAMPLES", "1")
        base, _, _, _ = _crew_fixture()
        captured = {}

        def optimize_prompts(*, optimizer, predict_fn, train_data, **kwargs):
            captured["optimizer"] = optimizer
            return SimpleNamespace(
                optimized_prompts=[SimpleNamespace(template=base, uri="u")],
                initial_eval_score=0.1,
                final_eval_score=0.2,
            )

        baseline_doc, keys, agents_yaml, tasks_yaml = _crew_fixture()
        with _fake_stack(optimize_prompts, _fake_completion(lambda c: "OK", [])):
            PromptOptimizationService._execute_crew_optimization_sync(
                loop=MagicMock(),
                baseline_doc=baseline_doc,
                field_keys=keys,
                objective="obj",
                rubric="- r",
                agents_yaml=agents_yaml,
                tasks_yaml=tasks_yaml,
                target_model="crew-model",
                judge_model="j",
                reflection_model="reflect-model",
                max_metric_calls=7,
                execution_timeout=60,
                registry_uri="databricks-uc",
                prompt_name="p",
                crew_id="c",
                cancel_run_id="",
                group_context=None,
            )
        gepa_kwargs = captured["optimizer"].gepa_kwargs
        # Minibatch 1: the default 3 sampled our SINGLE example three times, so
        # every candidate cost 3 crew executions racing the cache.
        assert gepa_kwargs["reflection_minibatch_size"] == 1
        # Ties must survive: a fully requirement-compliant candidate scored
        # 0.9 vs 0.9 and was discarded under strict improvement.
        assert gepa_kwargs["acceptance_criterion"] == "improvement_or_equal"
        assert gepa_kwargs["cache_evaluation"] is True
        # The pinned template stops the reflection model returning JSON blobs
        # that lose the [AGENT]/[TASK] structure (11/11 malformed, observed live).
        assert "[AGENT <id>]" in gepa_kwargs["reflection_prompt_template"]
        assert "Do NOT output JSON" in gepa_kwargs["reflection_prompt_template"]
        # Metric calls are DECOUPLED from executions: cached re-scores must not
        # consume the user's execution budget.
        assert captured["optimizer"].max_metric_calls == 7 * 2 + 3


class TestCrewNodeSync:
    """A crew's graph is stored TWICE: the agents/tasks rows, and a
    denormalised copy in ``crews.nodes`` that the canvas renders and the JSON
    export serialises. Applying to only the rows reported success while the
    canvas still showed the old prompts — real change, invisible where the user
    looks for it. Revert has the same shape mirrored.
    """

    @staticmethod
    def _crew(nodes):
        return SimpleNamespace(nodes=nodes)

    @staticmethod
    def _nodes():
        return [
            {
                "type": "agentNode",
                "data": {"agentId": "a1", "role": "Old role", "goal": "Old goal"},
            },
            {
                "type": "taskNode",
                "data": {
                    "taskId": "t1",
                    "description": "Old description",
                    "expected_output": "Old output",
                },
            },
            {
                "type": "taskNode",
                "data": {"taskId": "other", "description": "Untouched"},
            },
        ]

    @pytest.mark.asyncio
    async def test_apply_patches_the_matching_nodes(self, monkeypatch):
        import src.services.catalog.crews as crew_service_module

        crew = self._crew(self._nodes())
        committed = {"n": 0}

        class FakeRepo:
            def __init__(self, session):
                pass

            async def get(self, _id):
                return crew

        class FakeSession:
            async def commit(self):
                committed["n"] += 1

        monkeypatch.setattr(crew_service_module, "CrewService", FakeRepo)
        svc = PromptOptimizationService.__new__(PromptOptimizationService)
        svc.session = FakeSession()

        patched = await svc._sync_crew_nodes(
            "11dc3d57-8798-4b59-8d70-9f504f03ecef",
            {
                ("agent", "a1"): {"goal": "New goal"},
                ("task", "t1"): {"expected_output": "New output"},
            },
        )

        assert patched == 2
        assert committed["n"] == 1
        by_id = {
            (n["data"].get("agentId") or n["data"].get("taskId")): n["data"]
            for n in crew.nodes
        }
        assert by_id["a1"]["goal"] == "New goal"
        assert by_id["a1"]["role"] == "Old role", "untouched fields survive"
        assert by_id["t1"]["expected_output"] == "New output"
        assert by_id["t1"]["description"] == "Old description"
        assert by_id["other"]["description"] == "Untouched", "other tasks untouched"

    @pytest.mark.asyncio
    async def test_sync_is_best_effort_and_never_raises(self, monkeypatch):
        """The rows are the system of record and are already written by the time
        this runs, so a snapshot failure must not fail or half-undo the apply."""
        import src.services.catalog.crews as crew_service_module

        class ExplodingRepo:
            def __init__(self, session):
                pass

            async def get(self, _id):
                raise RuntimeError("crew table on fire")

        monkeypatch.setattr(crew_service_module, "CrewService", ExplodingRepo)
        svc = PromptOptimizationService.__new__(PromptOptimizationService)
        svc.session = SimpleNamespace()

        result = await svc._sync_crew_nodes("some-crew", {("task", "t1"): {"x": "y"}})
        assert result == 0

    @pytest.mark.asyncio
    async def test_no_crew_or_no_changes_is_a_noop(self):
        svc = PromptOptimizationService.__new__(PromptOptimizationService)
        svc.session = SimpleNamespace()
        assert await svc._sync_crew_nodes(None, {("task", "t"): {"a": "b"}}) == 0
        assert await svc._sync_crew_nodes("crew-1", {}) == 0
