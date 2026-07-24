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
                prompt="Crew generation failed: no credentials",
                status="success",
                created_at=now,
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
        uri, env, provider = await svc._resolve_reflection_model("some-endpoint")
        assert uri == "databricks:/some-endpoint"
        assert env == {}
        assert provider == "databricks"

    @pytest.mark.asyncio
    async def test_vllm_provider_sets_openai_env(self):
        svc = PromptOptimizationService(MagicMock())
        svc.model_repository.find_by_key = AsyncMock(
            return_value=SimpleNamespace(provider="vllm")
        )
        uri, env, provider = await svc._resolve_reflection_model(
            "Qwen3-Coder-30B-A3B-Instruct"
        )
        assert uri == "openai:/Qwen3-Coder-30B-A3B-Instruct"
        assert provider == "vllm"
        assert "OPENAI_API_BASE" in env and "OPENAI_API_KEY" in env

    @pytest.mark.asyncio
    async def test_kimi_provider_resolves_with_key_and_provider_tag(self):
        # The provider tag drives request quirks downstream — Kimi rejects any
        # explicit temperature, so the caller must know it is talking to Kimi.
        svc = PromptOptimizationService(MagicMock())
        svc.model_repository.find_by_key = AsyncMock(
            return_value=SimpleNamespace(
                provider="kimi", name="kimi-k2.7-code-highspeed"
            )
        )
        with patch(
            "src.services.api_keys_service.ApiKeysService.get_provider_api_key",
            AsyncMock(return_value="sk-test"),
        ):
            uri, env, provider = await svc._resolve_reflection_model(
                "kimi-k2.7-code-highspeed", _group()
            )
        assert uri == "openai:/kimi-k2.7-code-highspeed"
        assert env["OPENAI_API_KEY"] == "sk-test"
        assert provider == "kimi"

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


class TestCrewDocSerialization:
    """The serialize/parse pair is the GEPA mutation contract: candidates that
    survive parsing execute for real; everything else free-rejects."""

    @staticmethod
    def _crew():
        agent = SimpleNamespace(
            id="a1", role="Researcher", goal="Find facts", backstory="line1\nline2"
        )
        task = SimpleNamespace(
            id="t1", description="Do the research", expected_output="A table"
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
    svc = PromptOptimizationService(MagicMock())
    svc._resolve_reflection_model = AsyncMock(return_value=("openai:/qwen", {}, "vllm"))
    return svc


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
        assert model == "openai:/qwen"  # unchanged from creation
        assert result["model"] == "openai:/qwen"

    @pytest.mark.asyncio
    async def test_update_model_keeps_instructions(self, fake_mlflow):
        svc = _judge_service()
        await svc.create_judge("acc", "Keep these criteria for {{ outputs }}.")
        svc._resolve_reflection_model = AsyncMock(
            return_value=("deepseek:/v4", {}, "deepseek")
        )
        fake_mlflow.registered.clear()
        await svc.update_judge("acc", model="deepseek-v4-pro", group_context=_group())
        name, instructions, model = fake_mlflow.registered[0]
        assert model == "deepseek:/v4"
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

    def test_get_run_never_leaks_internal_keys(self):
        from datetime import timezone

        svc = PromptOptimizationService(MagicMock())
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "template_name": "detect_intent",
            "status": "running",
            "group_id": "grp1",
            "created_at": datetime.now(timezone.utc),
            "task": object(),
            "cancel_requested": False,
        }
        run = svc.get_run("r1", _group("grp1"))
        assert "task" not in run
        assert "cancel_requested" not in run

    def test_list_runs_sorts_aware_timestamps_descending(self):
        from datetime import timezone

        svc = PromptOptimizationService(MagicMock())
        older = datetime.now(timezone.utc) - timedelta(minutes=5)
        newer = datetime.now(timezone.utc)
        svc_module._RUNS["old"] = {
            "run_id": "old",
            "status": "completed",
            "group_id": "grp1",
            "created_at": older,
        }
        svc_module._RUNS["new"] = {
            "run_id": "new",
            "status": "completed",
            "group_id": "grp1",
            "created_at": newer,
        }
        runs = svc.list_runs(_group("grp1"))
        assert [r["run_id"] for r in runs] == ["new", "old"]

    def test_cancel_run_transitions_and_guards(self):
        svc = PromptOptimizationService(MagicMock())
        svc_module._RUNS["r1"] = {
            "run_id": "r1",
            "status": "running",
            "group_id": "grp1",
        }
        result = svc.cancel_run("r1", _group("grp1"))
        assert result["cancelling"] is True
        assert svc_module._RUNS["r1"]["cancel_requested"] is True

        svc_module._RUNS["r1"]["status"] = "completed"
        with pytest.raises(ValueError, match="not active"):
            svc.cancel_run("r1", _group("grp1"))
        with pytest.raises(ValueError, match="not found"):
            svc.cancel_run("missing", _group("grp1"))

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
