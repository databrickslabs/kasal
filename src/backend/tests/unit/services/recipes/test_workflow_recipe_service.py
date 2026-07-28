"""Workflow recipe mining — distilling completed crew runs into reusable recipes.

The two properties that matter and are easy to get wrong:

1. **Convergence.** Dedup rewrites a recipe's ``source_job_id`` to the newest
   run, so a bulk "already mined?" check on that column silently forgets every
   run it folded in — they get swept again on every pass and ``run_count``
   inflates forever. Membership is tracked in ``mined_job_ids`` instead.

2. **Provenance.** A run that was itself started from a recipe must never be
   mined back in, or the corpus learns from its own output and collapses onto
   whatever was cached first while looking healthier each round.
"""

from types import SimpleNamespace

import pytest

from src.repositories.workflow_recipe_repository import WorkflowRecipeRepository
from src.services.recipes.recipes import (
    WorkflowRecipeService,
    build_intent_text,
    intent_hash,
)


def _execution(**overrides):
    inputs = {
        "execution_type": "crew",
        "agents_yaml": {"agent_a": {"role": "Analyst"}},
        "tasks_yaml": {"task_a": {"description": "Load the companies"}},
    }
    inputs.update(overrides.pop("inputs", {}))
    base = {
        "job_id": "job-1",
        "run_name": "Load US and EU",
        "group_id": "user_dev_localhost",
        "group_email": "dev@localhost",
        "inputs": inputs,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


def _distil(execution):
    return WorkflowRecipeService._distil(execution)


class TestIntentIdentity:
    def test_hash_ignores_case_and_whitespace(self):
        assert intent_hash("Load  US\nand EU") == intent_hash("load us and eu")

    def test_different_intents_differ(self):
        assert intent_hash("load companies") != intent_hash("summarise news")

    def test_intent_text_combines_run_name_and_task_descriptions(self):
        text = build_intent_text(
            "Load US and EU", {"t1": {"description": "Fetch tickers"}}
        )
        assert "Load US and EU" in text
        assert "Fetch tickers" in text

    def test_run_name_alone_is_insufficient_so_descriptions_are_included(self):
        """Several unrelated crews share a run_name like "Direct User Helper";
        the descriptions are what actually distinguish them."""
        a = build_intent_text(
            "Direct User Helper", {"t": {"description": "Query postgres"}}
        )
        b = build_intent_text(
            "Direct User Helper", {"t": {"description": "Summarise news"}}
        )
        assert intent_hash(a) != intent_hash(b)


class TestMineability:
    def test_crew_run_is_mineable(self):
        assert _distil(_execution()) is not None

    def test_light_agent_run_is_skipped(self):
        """A single agent with one task has no graph to reuse."""
        assert _distil(_execution(inputs={"execution_type": "agent"})) is None

    def test_flow_run_is_skipped(self):
        assert _distil(_execution(inputs={"execution_type": "flow"})) is None

    def test_run_derived_from_a_recipe_is_skipped(self):
        """The provenance guard — without it the corpus feeds on its own output."""
        assert _distil(_execution(inputs={"source_recipe_id": 7})) is None

    def test_run_without_a_graph_is_skipped(self):
        assert _distil(_execution(inputs={"agents_yaml": {}})) is None
        assert _distil(_execution(inputs={"tasks_yaml": {}})) is None

    def test_run_with_no_describable_intent_is_skipped(self):
        execution = _execution(run_name=None, inputs={"tasks_yaml": {"t": {}}})
        assert _distil(execution) is None

    def test_group_is_carried_onto_the_recipe(self):
        fields, _ = _distil(_execution())
        assert fields["group_id"] == "user_dev_localhost"
        assert fields["group_email"] == "dev@localhost"


class TestMcpExtraction:
    def test_servers_are_collected_from_agents_and_tasks_and_deduped(self):
        agents = {"a": {"tool_configs": {"MCP_SERVERS": {"servers": ["postgres"]}}}}
        tasks = {
            "t": {
                "tool_configs": {
                    "MCP_SERVERS": {"servers": ["postgres", "yahoo_finance"]}
                }
            }
        }
        assert WorkflowRecipeService._mcp_servers(agents, tasks) == [
            "postgres",
            "yahoo_finance",
        ]

    def test_legacy_list_format_is_handled(self):
        agents = {"a": {"tool_configs": {"MCP_SERVERS": ["postgres"]}}}
        assert WorkflowRecipeService._mcp_servers(agents, {}) == ["postgres"]

    def test_absent_or_malformed_configs_yield_nothing(self):
        assert WorkflowRecipeService._mcp_servers({"a": {}}, {}) == []
        assert (
            WorkflowRecipeService._mcp_servers({"a": {"tool_configs": "nope"}}, {})
            == []
        )
        assert WorkflowRecipeService._mcp_servers({}, {}) == []


class TestDedupIdentity:
    def test_repeat_runs_of_one_intent_share_a_hash(self):
        """29 runs of "Load US and EU" must collapse to ONE recipe, not 29 rows
        each competing for the same retrieval slot."""
        first, _ = _distil(_execution(job_id="job-1"))
        second, _ = _distil(_execution(job_id="job-2"))
        assert first["intent_hash"] == second["intent_hash"]
        assert first["source_job_id"] != second["source_job_id"]


class TestConvergence:
    """The bug this guards: the first implementation pre-filtered candidates on
    ``source_job_id``, which dedup rewrites to the newest run. Every run it had
    folded in then looked unmined, so each sweep re-folded them and ``run_count``
    grew without bound. Sweeping must reach a fixed point."""

    @pytest.mark.asyncio
    async def test_repeated_sweeps_reach_a_fixed_point(self):
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from src.models.execution_history import ExecutionHistory
        from src.models.execution_trace import ExecutionTrace
        from src.models.workflow_recipe import WorkflowRecipe

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            for model in (ExecutionHistory, ExecutionTrace, WorkflowRecipe):
                await conn.run_sync(model.__table__.create)

        factory = async_sessionmaker(engine, expire_on_commit=False)

        # Three runs of ONE intent, plus one run of a different intent.
        async with factory() as session:
            for i in range(3):
                session.add(
                    ExecutionHistory(
                        job_id=f"job-{i}",
                        status="COMPLETED",
                        run_name="Load US and EU",
                        group_id="g1",
                        inputs={
                            "execution_type": "crew",
                            "agents_yaml": {"a": {"role": "Analyst"}},
                            "tasks_yaml": {"t": {"description": "Load the companies"}},
                        },
                    )
                )
            session.add(
                ExecutionHistory(
                    job_id="job-other",
                    status="COMPLETED",
                    run_name="Swiss News",
                    group_id="g1",
                    inputs={
                        "execution_type": "crew",
                        "agents_yaml": {"a": {"role": "Reporter"}},
                        "tasks_yaml": {"t": {"description": "Collect news"}},
                    },
                )
            )
            await session.commit()

        async def sweep():
            async with factory() as session:
                return await WorkflowRecipeService(session).mine_new_executions()

        first = await sweep()
        assert first == 4, "all four runs mined on the first pass"

        assert await sweep() == 0, "second sweep must find nothing new"
        assert await sweep() == 0, "and stay converged"

        async with factory() as session:
            recipes = await WorkflowRecipeRepository(session).list_by_group(["g1"])
        assert (
            len(recipes) == 2
        ), "3 repeats collapse to 1 recipe, +1 for the other intent"

        by_intent = {r.intent_text.splitlines()[0]: r for r in recipes}
        repeated = by_intent["Load US and EU"]
        assert (
            repeated.run_count == 3
        ), "run_count counts real runs, and does not inflate"
        assert sorted(repeated.mined_job_ids) == ["job-0", "job-1", "job-2"]
        assert repeated.source_job_id in repeated.mined_job_ids

        await engine.dispose()


class TestRetrievalThreshold:
    """Retrieval must be able to say "nothing like this".

    A ranked list always has a best row, so a caller that forgets to check the
    score would cheerfully propose an unrelated crew. The threshold lives in the
    service, below every reuse path, so it cannot be skipped.

    Calibration (dev corpus, 51 recipes, local nomic-embed-text): genuine
    matches scored 0.818/0.826/0.831, an unrelated prompt topped out at 0.456.
    """

    @pytest.mark.asyncio
    async def test_only_matches_above_the_floor_are_suggested(self, monkeypatch):
        from src.services.recipes import recipes as module

        good = SimpleNamespace(
            id=1,
            intent_text="Load US and EU",
            run_count=5,
            agents_yaml={"a": {}},
            tasks_yaml={"t": {}},
            tool_names=["postgres_execute_sql"],
            mcp_servers=["postgres"],
            source_job_id="job-1",
            curation=None,
            times_reused=0,
            updated_at=None,
        )
        noise = SimpleNamespace(
            id=2,
            intent_text="Swiss News",
            run_count=1,
            agents_yaml={},
            tasks_yaml={},
            tool_names=[],
            mcp_servers=[],
            source_job_id="job-2",
            curation=None,
            times_reused=0,
            updated_at=None,
        )

        async def fake_find(self, prompt, group_ids, limit=3):
            return [(good, 0.818), (noise, 0.456)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )

        service = module.WorkflowRecipeService(session=None)
        out = await service.suggest_for_prompt("load companies", ["g1"])

        assert [r.recipe_id for r in out] == [1], "the 0.456 row must not be offered"
        assert out[0].similarity == 0.818
        assert out[0].mcp_servers == ["postgres"]

    @pytest.mark.asyncio
    async def test_nothing_close_yields_no_suggestion(self, monkeypatch):
        from src.services.recipes import recipes as module

        row = SimpleNamespace(
            id=9,
            intent_text="Anything",
            run_count=1,
            agents_yaml={},
            tasks_yaml={},
            tool_names=[],
            mcp_servers=[],
            source_job_id="j",
        )

        async def fake_find(self, prompt, group_ids, limit=3):
            return [(row, 0.456)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        service = module.WorkflowRecipeService(session=None)
        assert await service.suggest_for_prompt("snake game", ["g1"]) == []

    @pytest.mark.asyncio
    async def test_no_group_never_retrieves(self):
        """Group scoping is a data-leak boundary: a recipe carries another
        tenant's crew structure and tool bindings."""
        from src.services.recipes.recipes import WorkflowRecipeService

        service = WorkflowRecipeService(session=None)
        assert await service.find_similar_for_prompt("anything", []) == []
        assert await service.suggest_for_prompt("anything", []) == []

    @pytest.mark.asyncio
    async def test_missing_embedder_degrades_to_no_suggestions(self, monkeypatch):
        """An embedder outage must not raise into crew generation — it just
        means nothing is retrievable right now."""
        from src.services.recipes import recipes as module

        async def no_embedder(text, group_id=None):
            return None

        monkeypatch.setattr(
            module.WorkflowRecipeService, "embed", staticmethod(no_embedder)
        )
        service = module.WorkflowRecipeService(session=None)
        assert await service.find_similar_for_prompt("anything", ["g1"]) == []


class TestCuration:
    """Explicit human judgement is the only trustworthy signal here — everything
    mined is merely "a crew that finished", which says nothing about whether its
    output was right."""

    @pytest.mark.asyncio
    async def test_suppressed_recipes_are_filtered_in_the_query_itself(self):
        """'bad' and 'hidden' are excluded by the retrieval query, not by each
        caller — so no present or future reuse path can forget to honour a
        human's "never suggest this again"."""
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from src.models.workflow_recipe import WorkflowRecipe
        from src.repositories.workflow_recipe_repository import (
            SUPPRESSED_CURATIONS,
            WorkflowRecipeRepository,
        )

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            await conn.run_sync(WorkflowRecipe.__table__.create)
        factory = async_sessionmaker(engine, expire_on_commit=False)

        vector = [1.0, 0.0, 0.0]
        async with factory() as session:
            for name, curation in (
                ("keeper", None),
                ("blessed", "good"),
                ("rejected", "bad"),
                ("muted", "hidden"),
            ):
                session.add(
                    WorkflowRecipe(
                        group_id="g1",
                        intent_text=name,
                        intent_hash=name,
                        agents_yaml={"a": {}},
                        tasks_yaml={"t": {}},
                        source_job_id=f"job-{name}",
                        mined_job_ids=[f"job-{name}"],
                        embedding=vector,
                        curation=curation,
                    )
                )
            await session.commit()

        async with factory() as session:
            hits = await WorkflowRecipeRepository(session).find_similar(
                vector, ["g1"], limit=10
            )
        names = {r.intent_text for r, _ in hits}

        assert names == {"keeper", "blessed"}
        assert not names & {"rejected", "muted"}
        assert set(SUPPRESSED_CURATIONS) == {"bad", "hidden"}

        await engine.dispose()

    @pytest.mark.asyncio
    async def test_a_good_recipe_outranks_a_closer_uncurated_one(self, monkeypatch):
        """Both have already cleared the relevance floor, so a human's explicit
        "this is the good one" beats a few hundredths of cosine distance."""
        from src.services.recipes import recipes as module

        def row(rid, curation):
            return SimpleNamespace(
                id=rid,
                intent_text=f"r{rid}",
                run_count=1,
                agents_yaml={},
                tasks_yaml={},
                tool_names=[],
                mcp_servers=[],
                source_job_id="j",
                curation=curation,
                times_reused=0,
                updated_at=None,
            )

        async def fake_find(self, prompt, group_ids, limit=3):
            return [(row(1, None), 0.83), (row(2, "good"), 0.79)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        out = await module.WorkflowRecipeService(session=None).suggest_for_prompt(
            "anything", ["g1"]
        )
        assert [r.recipe_id for r in out] == [2, 1]

    @pytest.mark.asyncio
    async def test_curation_below_the_floor_is_still_not_suggested(self, monkeypatch):
        """Curation reorders, it never rescues an irrelevant recipe."""
        from src.services.recipes import recipes as module

        good_but_irrelevant = SimpleNamespace(
            id=5,
            intent_text="unrelated",
            run_count=1,
            agents_yaml={},
            tasks_yaml={},
            tool_names=[],
            mcp_servers=[],
            source_job_id="j",
            curation="good",
            times_reused=0,
            updated_at=None,
        )

        async def fake_find(self, prompt, group_ids, limit=3):
            return [(good_but_irrelevant, 0.40)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        out = await module.WorkflowRecipeService(session=None).suggest_for_prompt(
            "snake game", ["g1"]
        )
        assert out == []

    @pytest.mark.asyncio
    async def test_unknown_curation_value_is_rejected(self):
        """An unrecognised value would read as "uncurated" to the filter and
        quietly resurrect a rejected recipe, so it is refused up front."""
        from src.services.recipes.recipes import WorkflowRecipeService

        with pytest.raises(ValueError):
            await WorkflowRecipeService(session=None).curate(1, "excellent", ["g1"])


class TestExemplars:
    """Generation only ever learns from recipes a HUMAN marked good.

    Mining can say a crew finished; it cannot say it was correct. Feeding
    merely-completed runs back into generation would teach whatever shape
    happened to survive and — since generated crews get mined in turn —
    reinforce it each round until the library only agrees with itself.
    """

    @staticmethod
    def _row(rid, curation, intent="Load US and EU\nsearch yahoo finance"):
        return SimpleNamespace(
            id=rid,
            intent_text=intent,
            curation=curation,
            agents_yaml={"a": {"role": "Data Engineer"}},
            tasks_yaml={"t": {"description": "Load the companies. Then verify."}},
            tool_names=["postgres_execute_sql"],
            mcp_servers=["postgres"],
        )

    @pytest.mark.asyncio
    async def test_uncurated_recipes_are_never_used_as_exemplars(self, monkeypatch):
        """The safety property: a merely-completed run must not shape generation."""
        from src.services.recipes import recipes as module

        async def fake_find(self, prompt, group_ids, limit=8):
            return [(TestExemplars._row(1, None), 0.90)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        text = await module.WorkflowRecipeService(session=None).exemplars_for_prompt(
            "load companies", ["g1"]
        )
        assert text == ""

    @pytest.mark.asyncio
    async def test_blessed_recipes_are_used(self, monkeypatch):
        from src.services.recipes import recipes as module

        async def fake_find(self, prompt, group_ids, limit=8):
            return [(TestExemplars._row(1, "good"), 0.90)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        text = await module.WorkflowRecipeService(session=None).exemplars_for_prompt(
            "load companies", ["g1"]
        )
        assert "PREVIOUSLY SUCCESSFUL CREWS" in text
        assert "Data Engineer" in text
        assert "postgres_execute_sql" in text
        assert "not as a template to copy" in text

    @pytest.mark.asyncio
    async def test_blessed_but_irrelevant_is_not_used(self, monkeypatch):
        """Curation never rescues a recipe that failed the relevance floor."""
        from src.services.recipes import recipes as module

        async def fake_find(self, prompt, group_ids, limit=8):
            return [(TestExemplars._row(1, "good"), 0.40)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        text = await module.WorkflowRecipeService(session=None).exemplars_for_prompt(
            "snake game", ["g1"]
        )
        assert text == ""

    @pytest.mark.asyncio
    async def test_retrieval_failure_never_breaks_generation(self, monkeypatch):
        from src.services.recipes import recipes as module

        async def boom(self, prompt, group_ids, limit=8):
            raise RuntimeError("embedder exploded")

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", boom
        )
        text = await module.WorkflowRecipeService(session=None).exemplars_for_prompt(
            "anything", ["g1"]
        )
        assert text == ""

    @pytest.mark.asyncio
    async def test_kill_switch_disables_injection(self, monkeypatch):
        from src.services.recipes import recipes as module

        async def fake_find(self, prompt, group_ids, limit=8):
            return [(TestExemplars._row(1, "good"), 0.90)]

        monkeypatch.setattr(
            module.WorkflowRecipeService, "find_similar_for_prompt", fake_find
        )
        monkeypatch.setattr(module, "EXEMPLARS_ENABLED", False)
        text = await module.WorkflowRecipeService(session=None).exemplars_for_prompt(
            "load companies", ["g1"]
        )
        assert text == ""

    @pytest.mark.asyncio
    async def test_no_group_yields_no_exemplars(self):
        from src.services.recipes.recipes import WorkflowRecipeService

        assert (
            await WorkflowRecipeService(session=None).exemplars_for_prompt("x", [])
            == ""
        )


class TestDeletion:
    """Recipes are distilled FROM runs, so they must not outlive them: a recipe
    left behind keeps pointing at a source_job_id that no longer exists and
    keeps feeding exemplars from crews the user believes they erased."""

    @staticmethod
    async def _factory():
        from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

        from src.models.workflow_recipe import WorkflowRecipe
        from src.models.workflow_recipe_trial import WorkflowRecipeTrial

        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        async with engine.begin() as conn:
            for model in (WorkflowRecipe, WorkflowRecipeTrial):
                await conn.run_sync(model.__table__.create)
        return async_sessionmaker(engine, expire_on_commit=False)

    @staticmethod
    def _recipe(group_id, intent="load companies", job_id="job-1"):
        from src.models.workflow_recipe import WorkflowRecipe

        return WorkflowRecipe(
            group_id=group_id,
            intent_text=intent,
            intent_hash=intent_hash(intent),
            agents_yaml={"a": {"role": "Analyst"}},
            tasks_yaml={"t": {"description": intent}},
            source_job_id=job_id,
            mined_job_ids=[job_id],
        )

    @staticmethod
    def _trial(group_id):
        from src.models.workflow_recipe_trial import ARM_EXEMPLAR, WorkflowRecipeTrial

        return WorkflowRecipeTrial(
            group_id=group_id, arm=ARM_EXEMPLAR, prompt_hash="h", prompt_text="p"
        )

    @pytest.mark.asyncio
    async def test_delete_removes_one_recipe(self):
        factory = await self._factory()
        async with factory() as session:
            session.add(self._recipe("g1"))
            await session.commit()

        async with factory() as session:
            service = WorkflowRecipeService(session)
            recipe_id = (await WorkflowRecipeRepository(session).list_by_group(["g1"]))[0].id
            assert await service.delete(recipe_id, ["g1"]) is True

        async with factory() as session:
            assert await WorkflowRecipeRepository(session).list_by_group(["g1"]) == []

    @pytest.mark.asyncio
    async def test_delete_refuses_a_recipe_from_another_workspace(self):
        factory = await self._factory()
        async with factory() as session:
            session.add(self._recipe("g1"))
            await session.commit()

        async with factory() as session:
            recipe_id = (await WorkflowRecipeRepository(session).list_by_group(["g1"]))[0].id
            # Reported as "not found" rather than deleted — the caller 404s.
            assert await WorkflowRecipeService(session).delete(recipe_id, ["g2"]) is False

        async with factory() as session:
            assert len(await WorkflowRecipeRepository(session).list_by_group(["g1"])) == 1

    @pytest.mark.asyncio
    async def test_delete_for_groups_clears_only_those_workspaces(self):
        factory = await self._factory()
        async with factory() as session:
            session.add(self._recipe("g1"))
            session.add(self._recipe("g2", intent="summarise news", job_id="job-2"))
            session.add(self._trial("g1"))
            session.add(self._trial("g2"))
            await session.commit()

        async with factory() as session:
            counts = await WorkflowRecipeService(session).delete_for_groups(["g1"])
            await session.commit()
        assert counts == {"recipe_count": 1, "trial_count": 1}

        async with factory() as session:
            repo = WorkflowRecipeRepository(session)
            assert await repo.list_by_group(["g1"]) == []
            assert len(await repo.list_by_group(["g2"])) == 1

    @pytest.mark.asyncio
    async def test_no_groups_is_not_an_invitation_to_delete_everything(self):
        """An empty list means "these zero workspaces". Treating it as unscoped
        would turn a group-filtered delete into a cross-tenant wipe."""
        factory = await self._factory()
        async with factory() as session:
            session.add(self._recipe("g1"))
            await session.commit()

        async with factory() as session:
            counts = await WorkflowRecipeService(session).delete_for_groups([])
            await session.commit()
        assert counts == {"recipe_count": 0, "trial_count": 0}

        async with factory() as session:
            assert len(await WorkflowRecipeRepository(session).list_by_group(["g1"])) == 1

    @pytest.mark.asyncio
    async def test_omitting_groups_clears_every_workspace(self):
        """The admin arm of run deletion passes no groups at all."""
        factory = await self._factory()
        async with factory() as session:
            session.add(self._recipe("g1"))
            session.add(self._recipe("g2", intent="summarise news", job_id="job-2"))
            await session.commit()

        async with factory() as session:
            counts = await WorkflowRecipeService(session).delete_for_groups()
            await session.commit()
        assert counts["recipe_count"] == 2

        async with factory() as session:
            repo = WorkflowRecipeRepository(session)
            assert await repo.list_by_group(["g1", "g2"]) == []
