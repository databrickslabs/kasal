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
from src.services.workflow_recipe_service import (
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
