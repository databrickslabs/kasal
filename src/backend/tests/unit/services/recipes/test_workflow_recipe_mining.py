"""Mining is triggered by a crew finishing, not by a clock.

The properties that matter here are about NOT hurting the run that triggered it:
mining is fire-and-forget, it never raises into the executor, and a burst of
completions does not turn into a burst of sweeps over the same rows.
"""

import asyncio

import pytest

from src.services.recipes import mining


@pytest.fixture(autouse=True)
def _reset_module_state():
    mining._sweep_pending = False
    yield
    mining._sweep_pending = False


class TestMineNow:
    @pytest.mark.asyncio
    async def test_returns_what_the_sweep_mined(self, monkeypatch):
        async def fake_sweep():
            return 3

        monkeypatch.setattr(
            "src.services.recipes.recipes.WorkflowRecipeService.sweep",
            staticmethod(fake_sweep),
        )
        assert await mining.mine_now() == 3

    @pytest.mark.asyncio
    async def test_concurrent_completions_coalesce_into_two_passes(self, monkeypatch):
        """Several crews finishing at once must not each start a sweep over the
        same rows — but the extra pass IS run, since a sweep already in flight
        may have read past the run that asked."""
        calls = 0
        started = asyncio.Event()
        release = asyncio.Event()

        async def fake_sweep():
            nonlocal calls
            calls += 1
            if calls == 1:
                started.set()
                await release.wait()
            return 1

        monkeypatch.setattr(
            "src.services.recipes.recipes.WorkflowRecipeService.sweep",
            staticmethod(fake_sweep),
        )

        first = asyncio.create_task(mining.mine_now())
        await started.wait()

        # Three more completions land while the first sweep is still running.
        assert await mining.mine_now() == 0
        assert await mining.mine_now() == 0
        assert await mining.mine_now() == 0

        release.set()
        assert await first == 2, "its own pass, plus one covering the others"
        assert calls == 2, "four completions, two sweeps"


class TestScheduleMiningAfterRun:
    @pytest.mark.asyncio
    async def test_schedules_a_sweep_without_awaiting_it(self, monkeypatch):
        swept = asyncio.Event()

        async def fake_sweep():
            swept.set()
            return 1

        monkeypatch.setattr(
            "src.services.recipes.recipes.WorkflowRecipeService.sweep",
            staticmethod(fake_sweep),
        )

        mining.schedule_mining_after_run("job-1")
        assert not swept.is_set(), "must not block the caller"

        await asyncio.wait_for(swept.wait(), timeout=1)

    @pytest.mark.asyncio
    async def test_a_failing_sweep_never_reaches_the_run(self, monkeypatch):
        attempted = asyncio.Event()

        async def boom():
            attempted.set()
            raise RuntimeError("mining exploded")

        monkeypatch.setattr(
            "src.services.recipes.recipes.WorkflowRecipeService.sweep",
            staticmethod(boom),
        )

        mining.schedule_mining_after_run("job-1")  # must not raise
        await asyncio.wait_for(attempted.wait(), timeout=1)
        await asyncio.sleep(0)  # let the task finish swallowing it

    def test_no_event_loop_is_not_an_error(self, monkeypatch):
        """Called from a thread with no loop: the recipe arrives on a later
        completion rather than the executor blowing up."""
        mining.schedule_mining_after_run("job-1")
