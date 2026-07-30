"""Requests-per-minute throttling, which until now did nothing at all.

``max_rpm`` was on Agent and Crew, offered in the agent form, carried through
the config builders, and read by no one — so "Max RPM: 10" on screen throttled
exactly nothing.
"""

import pytest

from src.core.llm.transport.rpm import WINDOW_SECONDS, RPMController, throttle


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def _controller(max_rpm: int) -> tuple[RPMController, _Clock]:
    clock = _Clock()
    return RPMController(max_rpm, clock=clock, sleep=clock.sleep), clock


class TestPacing:
    def test_calls_within_the_limit_never_wait(self):
        controller, clock = _controller(3)
        for _ in range(3):
            controller.acquire()
        assert clock.slept == []

    def test_the_call_over_the_limit_waits_out_the_window(self):
        controller, clock = _controller(2)
        controller.acquire()
        controller.acquire()
        controller.acquire()
        assert clock.slept == [WINDOW_SECONDS]

    def test_time_already_spent_is_deducted_from_the_wait(self):
        """Waiting a full minute after 50s of the window has passed would make
        the effective limit half what the user asked for."""
        controller, clock = _controller(1)
        controller.acquire()
        clock.now += 50
        controller.acquire()
        assert clock.slept == [pytest.approx(10.0)]

    def test_a_new_window_restores_the_full_allowance(self):
        controller, clock = _controller(2)
        controller.acquire()
        controller.acquire()
        clock.now += WINDOW_SECONDS
        controller.acquire()
        controller.acquire()
        assert clock.slept == []

    def test_the_limit_is_per_window_not_per_lifetime(self):
        controller, clock = _controller(1)
        for _ in range(4):
            controller.acquire()
        # Three waits for the three calls beyond the first.
        assert len(clock.slept) == 3

    @pytest.mark.parametrize("bad", [0, -1])
    def test_a_nonsense_limit_is_rejected_rather_than_ignored(self, bad):
        """0 would mean 'no requests ever', which is never what anyone means."""
        with pytest.raises(ValueError):
            RPMController(bad)


class TestThrottleHook:
    def test_an_agent_without_a_limiter_is_not_paced(self):
        class _Agent:
            rpm_controller = None

        throttle(_Agent())  # must not raise

    def test_no_agent_at_all_is_not_paced(self):
        throttle(None)

    def test_an_agent_with_a_limiter_is_paced(self):
        controller, clock = _controller(1)

        class _Agent:
            pass

        agent = _Agent()
        agent.rpm_controller = controller
        throttle(agent)
        throttle(agent)
        assert clock.slept == [WINDOW_SECONDS]

    def test_something_that_is_not_a_controller_is_ignored(self):
        """Duck-typing must not turn a stray attribute into an AttributeError
        deep inside the transport."""

        class _Agent:
            rpm_controller = "10"

        throttle(_Agent())


class TestWiring:
    def test_an_agent_with_max_rpm_builds_its_own_limiter(self):
        """The Chat path runs one agent and never enters Crew.kickoff, so the
        agent has to carry its own."""
        from src.services.execution.runtime import Agent

        agent = Agent(role="r", goal="g", backstory="b", max_rpm=5)
        assert isinstance(agent.rpm_controller, RPMController)
        assert agent.rpm_controller.max_rpm == 5

    def test_an_agent_without_max_rpm_has_no_limiter(self):
        from src.services.execution.runtime import Agent

        agent = Agent(role="r", goal="g", backstory="b")
        assert agent.rpm_controller is None

    def test_a_crew_limit_is_shared_so_it_is_not_multiplied_by_agent_count(self):
        """Per-agent limiters would let a six-agent crew issue six times the
        requests per minute the user asked for."""
        from src.services.execution.runtime import Agent, Crew, Task

        agents = [
            Agent(role=f"r{i}", goal="g", backstory="b", max_rpm=99) for i in range(3)
        ]
        crew = Crew(
            agents=agents,
            tasks=[Task(description="d", expected_output="e", agent=agents[0])],
            max_rpm=10,
        )
        try:
            crew.kickoff()
        except Exception:  # noqa: BLE001 — no LLM configured; the stamping ran
            pass

        shared = {id(a.rpm_controller) for a in agents}
        assert len(shared) == 1, "every agent must share one limiter"
        assert agents[0].rpm_controller.max_rpm == 10, "crew limit wins over agent"
