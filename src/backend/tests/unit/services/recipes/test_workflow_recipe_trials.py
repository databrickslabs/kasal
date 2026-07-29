"""Tests for the workflow-recipe measurement ledger.

What these pin down, in order of how badly getting them wrong would mislead:

1. The control arm is RECORDED when exemplars are withheld. A withheld
   generation that looks identical to one that never had a match collapses the
   two populations the report exists to keep apart.
2. Trials link to runs by AGENT ID, exactly. Any fuzzier join would silently
   attribute the wrong run and corrupt the comparison it feeds.
3. Completion rates are computed over LINKED runs, not over generations, so a
   crew nobody ran is not counted as a crew that failed.
4. Recording never breaks generation.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.models.workflow_recipe_trial import ARM_CONTROL, ARM_EXEMPLAR, ARM_NONE
from src.services.recipes.recipes import (
    ExemplarDecision,
    WorkflowRecipeService,
)


class _Recipe:
    """Minimal stand-in for a retrieved recipe row."""

    def __init__(self, recipe_id, curation="good"):
        self.id = recipe_id
        self.curation = curation
        self.intent_text = f"Load the {recipe_id} companies"
        self.agents_yaml = {"a": {"role": "Analyst"}}
        self.tasks_yaml = {"t": {"description": "Load them"}}
        self.tool_names = ["GenieTool"]
        self.mcp_servers = []


async def _engine():
    from src.models.execution_history import ExecutionHistory
    from src.models.execution_trace import ExecutionTrace
    from src.models.workflow_recipe_trial import WorkflowRecipeTrial

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        for model in (ExecutionHistory, ExecutionTrace, WorkflowRecipeTrial):
            await conn.run_sync(model.__table__.create)
    return async_sessionmaker(engine, expire_on_commit=False)


def _decision(arm=ARM_EXEMPLAR, injected=(1,), prompt="build me a loader"):
    return ExemplarDecision(
        text="exemplars" if arm == ARM_EXEMPLAR else "",
        arm=arm,
        candidates=[{"recipe_id": 1, "similarity": 0.82, "curation": "good"}],
        injected_recipe_ids=list(injected),
        prompt=prompt,
    )


_GENERATED = {
    "agents": [{"id": "agent-1"}, {"id": "agent-2"}],
    "tasks": [{"id": "task-1"}],
}


class TestArmAssignment:
    @pytest.mark.asyncio
    async def test_blessed_match_is_injected_when_no_holdout(self):
        service = WorkflowRecipeService(session=None)
        with (
            patch.object(
                WorkflowRecipeService,
                "find_similar_for_prompt",
                return_value=[(_Recipe(7), 0.9)],
            ),
            patch("src.services.recipes.recipes.HOLDOUT_FRACTION", 0.0),
        ):
            decision = await service.prepare_exemplars("load companies", ["g1"])

        assert decision.arm == ARM_EXEMPLAR
        assert decision.injected_recipe_ids == [7]
        assert "PREVIOUSLY SUCCESSFUL CREWS" in decision.text

    @pytest.mark.asyncio
    async def test_holdout_withholds_exemplars_but_still_records_the_arm(self):
        """The whole point of the control: same eligibility, no treatment, and
        an explicit record that it was denied rather than never available."""
        service = WorkflowRecipeService(session=None)
        with (
            patch.object(
                WorkflowRecipeService,
                "find_similar_for_prompt",
                return_value=[(_Recipe(7), 0.9)],
            ),
            patch("src.services.recipes.recipes.HOLDOUT_FRACTION", 1.0),
        ):
            decision = await service.prepare_exemplars("load companies", ["g1"])

        assert decision.arm == ARM_CONTROL
        assert decision.text == "", "control generations must get no exemplars"
        assert decision.injected_recipe_ids == []
        assert decision.candidates, "but what was available is still recorded"

    @pytest.mark.asyncio
    async def test_uncurated_matches_are_none_available_not_control(self):
        """A recipe nobody blessed was never eligible, so withholding it is not
        a treatment decision — filing it as a control would put untreated
        baseline work in the comparison arm."""
        service = WorkflowRecipeService(session=None)
        with (
            patch.object(
                WorkflowRecipeService,
                "find_similar_for_prompt",
                return_value=[(_Recipe(7, curation=None), 0.9)],
            ),
            patch("src.services.recipes.recipes.HOLDOUT_FRACTION", 1.0),
        ):
            decision = await service.prepare_exemplars("load companies", ["g1"])

        assert decision.arm == ARM_NONE
        assert decision.blessed_count == 0
        assert len(decision.candidates) == 1, "it was still considered, and recorded"

    @pytest.mark.asyncio
    async def test_below_threshold_match_is_not_injected(self):
        service = WorkflowRecipeService(session=None)
        with patch.object(
            WorkflowRecipeService,
            "find_similar_for_prompt",
            return_value=[(_Recipe(7), 0.10)],
        ):
            decision = await service.prepare_exemplars(
                "something else entirely", ["g1"]
            )

        assert decision.arm == ARM_NONE
        assert decision.text == ""

    @pytest.mark.asyncio
    async def test_retrieval_failure_degrades_to_no_exemplars(self):
        service = WorkflowRecipeService(session=None)

        async def boom(*args, **kwargs):
            raise RuntimeError("embedder down")

        with patch.object(WorkflowRecipeService, "find_similar_for_prompt", boom):
            decision = await service.prepare_exemplars("load companies", ["g1"])

        assert decision.arm == ARM_NONE
        assert decision.text == ""


class TestTrialRecording:
    @pytest.mark.asyncio
    async def test_records_arm_candidates_and_generated_ids(self):
        factory = await _engine()
        async with factory() as session:
            trial = await WorkflowRecipeService(session).record_trial(
                _decision(), generated=_GENERATED, group_id="g1"
            )

        assert trial is not None
        assert trial.arm == ARM_EXEMPLAR
        assert trial.injected_recipe_ids == [1]
        assert trial.blessed_count == 1
        assert trial.best_similarity == 0.82
        # The join key for linking this generation to the run it becomes.
        assert trial.agent_ids == ["agent-1", "agent-2"]
        assert trial.agent_count == 2 and trial.task_count == 1

    @pytest.mark.asyncio
    async def test_recording_failure_never_raises(self):
        """Generation predates this ledger and must survive without it — here,
        a session with no table behind it."""
        factory = await _engine()
        async with factory() as session:
            service = WorkflowRecipeService(session)

            async def boom(*args, **kwargs):
                raise RuntimeError("table missing")

            service.trial_repository.create = boom
            assert await service.record_trial(_decision(), generated=_GENERATED) is None


class TestLinking:
    @pytest.mark.asyncio
    async def test_links_the_run_that_used_the_generated_agents(self):
        from src.models.execution_history import ExecutionHistory
        from src.models.execution_trace import ExecutionTrace

        factory = await _engine()
        async with factory() as session:
            await WorkflowRecipeService(session).record_trial(
                _decision(), generated=_GENERATED, group_id="g1"
            )
            # The run that actually exercised those agents, plus an unrelated
            # run that must NOT be picked up.
            session.add(
                ExecutionHistory(
                    job_id="job-ours",
                    status="COMPLETED",
                    run_name="Loader",
                    group_id="g1",
                    inputs={
                        "execution_type": "crew",
                        "agents_yaml": {"agent_agent-1": {"role": "Analyst"}},
                        "tasks_yaml": {"t": {"description": "Load"}},
                    },
                )
            )
            session.add(
                ExecutionHistory(
                    job_id="job-theirs",
                    status="FAILED",
                    run_name="Unrelated",
                    group_id="g1",
                    inputs={
                        "execution_type": "crew",
                        "agents_yaml": {"agent_someone-else": {"role": "Other"}},
                        "tasks_yaml": {"t": {"description": "Other"}},
                    },
                )
            )
            session.add(
                ExecutionTrace(
                    job_id="job-ours",
                    event_source="agent",
                    event_context="Analyst",
                    event_type="tool_usage",
                    # tool_name lives in trace_metadata, NOT output — this
                    # mirrors what the engine actually writes. The fixture used
                    # to put it in ``output``, which is where the reader looked,
                    # so both agreed and both were wrong: against real traces
                    # every recipe recorded tool_names=[] and tool_call_count=0.
                    span_name="CrewAI.tool.execute",
                    trace_metadata={"tool_name": "GenieTool", "tool_args": {}},
                    output={"duration_ms": 1.0, "extra_data": {}},
                )
            )
            await session.commit()

        async with factory() as session:
            linked = await WorkflowRecipeService(session).link_trials()
        assert linked == 1

        async with factory() as session:
            trials = await WorkflowRecipeService(
                session
            ).trial_repository.list_for_report(["g1"])
        assert trials[0].linked_job_id == "job-ours"
        assert trials[0].outcome_status == "COMPLETED"
        assert trials[0].outcome_tool_calls == 1

    @pytest.mark.asyncio
    async def test_failed_runs_are_linked_too(self):
        """Mining only reads COMPLETED runs; measurement must not. Dropping
        failures would leave only successes in both arms and flatten the very
        difference the report is looking for."""
        from src.models.execution_history import ExecutionHistory

        factory = await _engine()
        async with factory() as session:
            await WorkflowRecipeService(session).record_trial(
                _decision(), generated=_GENERATED, group_id="g1"
            )
            session.add(
                ExecutionHistory(
                    job_id="job-failed",
                    status="FAILED",
                    run_name="Loader",
                    group_id="g1",
                    inputs={
                        "execution_type": "crew",
                        "agents_yaml": {"agent_agent-2": {"role": "Analyst"}},
                        "tasks_yaml": {"t": {"description": "Load"}},
                    },
                )
            )
            await session.commit()

        async with factory() as session:
            assert await WorkflowRecipeService(session).link_trials() == 1

        async with factory() as session:
            trials = await WorkflowRecipeService(
                session
            ).trial_repository.list_for_report(["g1"])
        assert trials[0].outcome_status == "FAILED"

    @pytest.mark.asyncio
    async def test_trial_with_no_matching_run_stays_unlinked(self):
        factory = await _engine()
        async with factory() as session:
            await WorkflowRecipeService(session).record_trial(
                _decision(), generated=_GENERATED, group_id="g1"
            )
        async with factory() as session:
            assert await WorkflowRecipeService(session).link_trials() == 0


class TestEffectivenessReport:
    @pytest.mark.asyncio
    async def test_reports_arms_separately_and_flags_comparability(self):
        factory = await _engine()
        async with factory() as session:
            service = WorkflowRecipeService(session)
            await service.record_trial(
                _decision(ARM_EXEMPLAR), _GENERATED, group_id="g1"
            )
            await service.record_trial(
                _decision(ARM_CONTROL, injected=()), _GENERATED, group_id="g1"
            )
            await service.record_trial(
                _decision(ARM_NONE, injected=()), _GENERATED, group_id="g1"
            )

        async with factory() as session:
            report = await WorkflowRecipeService(session).effectiveness(["g1"])

        assert report["generations"] == 3
        assert report["arms"][ARM_EXEMPLAR]["generations"] == 1
        assert report["arms"][ARM_CONTROL]["generations"] == 1
        assert report["arms"][ARM_NONE]["generations"] == 1
        assert report["injection_rate"] == round(1 / 3, 4)
        assert report["comparable"] is True

    @pytest.mark.asyncio
    async def test_not_comparable_without_a_control_arm(self):
        """With no holdout running there is no unconfounded comparison, and the
        report has to say so rather than let a reader infer causation."""
        factory = await _engine()
        async with factory() as session:
            service = WorkflowRecipeService(session)
            await service.record_trial(
                _decision(ARM_EXEMPLAR), _GENERATED, group_id="g1"
            )

        async with factory() as session:
            report = await WorkflowRecipeService(session).effectiveness(["g1"])
        assert report["comparable"] is False

    @pytest.mark.asyncio
    async def test_completion_rate_is_over_linked_runs_not_generations(self):
        """Two generations, one ever run, and it completed. The rate is 100% of
        what ran — not 50% of what was generated, which would count a crew the
        user simply never started as a crew that failed."""
        from src.models.execution_history import ExecutionHistory

        factory = await _engine()
        async with factory() as session:
            service = WorkflowRecipeService(session)
            await service.record_trial(_decision(), _GENERATED, group_id="g1")
            await service.record_trial(
                _decision(), {"agents": [{"id": "agent-9"}], "tasks": []}, group_id="g1"
            )
            session.add(
                ExecutionHistory(
                    job_id="job-ours",
                    status="COMPLETED",
                    run_name="Loader",
                    group_id="g1",
                    inputs={
                        "execution_type": "crew",
                        "agents_yaml": {"agent_agent-1": {"role": "Analyst"}},
                        "tasks_yaml": {"t": {"description": "Load"}},
                    },
                )
            )
            await session.commit()

        async with factory() as session:
            await WorkflowRecipeService(session).link_trials()
        async with factory() as session:
            report = await WorkflowRecipeService(session).effectiveness(["g1"])

        arm = report["arms"][ARM_EXEMPLAR]
        assert arm["generations"] == 2
        assert arm["linked_runs"] == 1
        assert arm["completion_rate"] == 1.0

    @pytest.mark.asyncio
    async def test_group_scoped(self):
        factory = await _engine()
        async with factory() as session:
            await WorkflowRecipeService(session).record_trial(
                _decision(), _GENERATED, group_id="other-group"
            )
        async with factory() as session:
            report = await WorkflowRecipeService(session).effectiveness(["g1"])
        assert report["generations"] == 0


class TestProgressivePathIsWired:
    """The progressive path (`/crew/create-crew-streaming`) is what BOTH the
    canvas chat input and ChatMode use. It originally had no recipe hook at all,
    so a workspace could curate a recipe, generate a crew, and be silently given
    nothing — no exemplars, and no trial recorded to show why. These pin the
    three seams that fix it.
    """

    @pytest.mark.asyncio
    async def test_exemplars_reach_the_plan_prompt(self):
        """The PLAN call decides the crew's shape, which is exactly what a past
        crew is evidence about — so exemplars belong there, not in the later
        per-agent/per-task calls that run after those decisions are made."""
        from src.services.generation.crews import CrewGenerationService

        service = CrewGenerationService(session=None)
        captured = {}

        async def fake_completion(messages=None, **kwargs):
            captured["system"] = messages[0]["content"]
            return '{"complexity":"light","process_type":"sequential","agents":[],"tasks":[]}'

        with (
            patch(
                "src.services.generation.crews.TemplateService."
                "get_effective_template_content",
                return_value="PLAN TEMPLATE",
            ),
            patch(
                "src.services.generation.crew.progressive.LLMManager.completion",
                side_effect=fake_completion,
            ),
            patch.object(
                CrewGenerationService, "_log_llm_interaction", return_value=None
            ),
        ):
            request = SimpleNamespace(prompt="write about Khalil Gibran", model="m")
            try:
                await service._generate_crew_plan(
                    request, None, "m", exemplars="\n\nPREVIOUSLY SUCCESSFUL CREWS"
                )
            except Exception:
                # The plan may reject an empty agent list downstream; the prompt
                # was already captured, which is what this asserts on.
                pass

        assert "PREVIOUSLY SUCCESSFUL CREWS" in captured.get("system", "")
        assert (
            "PLAN TEMPLATE" in captured["system"]
        ), "exemplars must ADD to the template"

    @pytest.mark.asyncio
    async def test_recipe_work_never_uses_the_shared_connection(self):
        """Regression guard, inherited: on SQLite the shared StaticPool is ONE
        connection, and a concurrent commit on it can discard an agent the
        generation already committed. Recipe lookups and trial writes must take
        a private connection like the rest of the flow."""
        from src.services.generation.crews import CrewGenerationService

        service = CrewGenerationService(session=None)

        with (
            patch("src.db.database_router.is_lakebase_enabled", return_value=False),
            patch("src.db.session.get_isolated_db_session") as isolated,
            patch("src.db.session.async_session_factory") as shared,
        ):
            await service._isolated_session_ctx()
            isolated.assert_called_once()
            shared.assert_not_called()


class TestRecipesByJob:
    """Powers the run list's Reusable column: one request maps every mined run
    to its recipe."""

    async def _service_with(self, recipes):
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from src.models.workflow_recipe import WorkflowRecipe

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(WorkflowRecipe.__table__.create)
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            for r in recipes:
                session.add(WorkflowRecipe(**r))
            await session.commit()
        return factory

    def _recipe(self, **over):
        base = dict(
            group_id="g1",
            intent_text="Load US and EU\nsecond line",
            intent_hash="h1",
            agents_yaml={"a": {"role": "Analyst"}},
            tasks_yaml={"t": {"description": "Load"}},
            source_job_id="job-newest",
            mined_job_ids=["job-old-1", "job-old-2", "job-newest"],
            run_count=3,
        )
        base.update(over)
        return base

    @pytest.mark.asyncio
    async def test_keys_every_mined_run_not_just_the_newest(self):
        """Dedup rewrites source_job_id to the newest run of an intent. Keying
        on that alone would leave the 28 earlier runs of a repeated intent
        looking as though they had never been mined — no control on their rows."""
        factory = await self._service_with([self._recipe(curation="good")])
        async with factory() as session:
            index = await WorkflowRecipeService(session).recipes_by_job(["g1"])

        assert set(index) == {"job-old-1", "job-old-2", "job-newest"}
        assert index["job-old-1"]["curation"] == "good"
        assert index["job-old-1"]["run_count"] == 3
        # Only the first line of the intent — the row shows it in a tooltip.
        assert index["job-newest"]["intent_text"] == "Load US and EU"

    @pytest.mark.asyncio
    async def test_group_scoped(self):
        factory = await self._service_with(
            [self._recipe(group_id="other", mined_job_ids=["job-theirs"])]
        )
        async with factory() as session:
            index = await WorkflowRecipeService(session).recipes_by_job(["g1"])
        assert index == {}

    @pytest.mark.asyncio
    async def test_empty_group_list_returns_nothing(self):
        factory = await self._service_with([self._recipe()])
        async with factory() as session:
            assert await WorkflowRecipeService(session).recipes_by_job([]) == {}


class TestLinkerWorkQueue:
    """``list_unlinked`` is the linker's queue. Most trials never link at all —
    a generated crew that was discarded has no run to find — so the queue has to
    stop retrying permanent misses or every sweep rescans them forever."""

    @pytest.mark.asyncio
    async def test_excludes_already_linked_and_ancient_trials(self):
        from datetime import datetime, timedelta

        from src.models.workflow_recipe_trial import WorkflowRecipeTrial
        from src.repositories.workflow_recipe_trial_repository import (
            WorkflowRecipeTrialRepository,
        )

        factory = await _engine()
        async with factory() as session:
            repo = WorkflowRecipeTrialRepository(session)
            common = dict(
                group_id="g1",
                prompt_hash="h",
                candidates=[],
                candidate_count=0,
                blessed_count=0,
                arm=ARM_NONE,
                injected_recipe_ids=[],
                agent_ids=[],
                task_ids=[],
            )
            session.add(WorkflowRecipeTrial(**common, created_at=datetime.utcnow()))
            session.add(
                WorkflowRecipeTrial(
                    **common, created_at=datetime.utcnow(), linked_job_id="job-1"
                )
            )
            session.add(
                WorkflowRecipeTrial(
                    **common, created_at=datetime.utcnow() - timedelta(days=45)
                )
            )
            await session.commit()

            pending = await repo.list_unlinked()

        assert len(pending) == 1, "only the recent, still-unlinked trial is queued"
        assert pending[0].linked_job_id is None
