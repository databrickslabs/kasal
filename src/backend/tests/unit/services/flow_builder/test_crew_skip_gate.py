"""Unit tests for the resume skip gate.

This is the decision that used to replay an edited crew's stale output. It sits
between "this crew is before the resume point" and "therefore do not run it".

The gate now also carries the consequence of a re-run: a crew AFTER one that
changed has stale input even when its own tasks are untouched, so it re-runs
too. That makes the policy stateful, and the order it is asked in — build
order, which is sequence order — part of its contract.
"""

from types import SimpleNamespace

from src.services.flow_builder.checkpoint_identity import compute_crew_identity
from src.services.flow_builder.checkpoint_skip import CrewSkipPolicy

# Far enough past every sequence used below that the resume point itself is
# never the thing under test.
RESUME_FROM = 99


def make_agent(role="researcher", model="gpt-x"):
    return SimpleNamespace(
        key=f"{role}|find|expert", llm=SimpleNamespace(model=model), tools=[]
    )


def make_task(description="do it", agent=None):
    return SimpleNamespace(
        key=f"{description}|a result",
        agent=agent if agent is not None else make_agent(),
        tools=[],
    )


def sp(name, task_id, tasks):
    """A starting-point tuple in the builder's shape."""
    return ("starting_point_0", [task_id], tasks, name, None)


def listener(name, crew_id, task_id, tasks, listen_to):
    """A listener tuple in the builder's shape."""
    return ("listener", crew_id, [task_id], tasks, name, listen_to, "NONE", None)


def policy(
    identities,
    resume_from=RESUME_FROM,
    starting_points=None,
    listeners=None,
    flow_config=None,
):
    """A policy over a single starting-point crew unless told otherwise."""
    if starting_points is None and listeners is None:
        starting_points = [sp("research", "t1", [make_task()])]
    return CrewSkipPolicy.decide(
        resume_from,
        identities,
        starting_points=starting_points or [],
        listener_crews=listeners or [],
        flow_config=flow_config or {},
    )


class TestCrewMayBeSkipped:
    def test_an_unchanged_crew_is_skipped(self):
        tasks = [make_task()]
        identities = {"research": compute_crew_identity("research", tasks)}

        # Same crew, same work — its stored output stands in for running it.
        gate = policy(identities, starting_points=[sp("research", "t1", tasks)])
        assert gate.may_skip("research", tasks, 1) is True

    def test_an_edited_crew_is_re_run(self):
        stored = {"research": compute_crew_identity("research", [make_task()])}
        edited = [make_task(description="do it much better")]

        # The whole point: the edit must not be silently discarded.
        gate = policy(stored, starting_points=[sp("research", "t1", edited)])
        assert gate.may_skip("research", edited, 1) is False

    def test_a_re_modelled_crew_is_re_run(self):
        stored = {
            "research": compute_crew_identity(
                "research", [make_task(agent=make_agent(model="gpt-x"))]
            )
        }
        re_modelled = [make_task(agent=make_agent(model="claude-y"))]

        gate = policy(stored, starting_points=[sp("research", "t1", re_modelled)])
        assert gate.may_skip("research", re_modelled, 1) is False

    def test_a_retooled_crew_is_re_run(self):
        """Changing which tools a crew may use changes what it produces.

        ``Task.key`` covers description and expected output only, so this edit
        moved nothing until tools joined the identity.
        """
        unchanged_agent = make_agent()
        stored = {
            "research": compute_crew_identity(
                "research", [make_task(agent=unchanged_agent)]
            )
        }

        retooled_agent = make_agent()
        retooled_agent.tools = [SimpleNamespace(name="SerperDevTool")]
        retooled = [make_task(agent=retooled_agent)]

        gate = policy(stored, starting_points=[sp("research", "t1", retooled)])
        assert gate.may_skip("research", retooled, 1) is False

    def test_a_checkpoint_without_identities_still_skips(self):
        """Pre-identity checkpoints keep working exactly as before.

        Refusing them would make every existing checkpoint useless, which is a
        worse outcome than the unverified skip they always had.
        """
        assert policy({}).may_skip("research", [make_task()], 1) is True
        assert policy(None).may_skip("research", [make_task()], 1) is True

    def test_a_crew_missing_from_the_checkpoint_still_skips(self):
        identities = {"other-crew": "abc123"}
        assert policy(identities).may_skip("research", [make_task()], 1) is True

    def test_an_uncomputable_crew_still_skips(self):
        # No tasks -> no identity -> unverified, not "changed".
        gate = policy(
            {"research": "abc123"}, starting_points=[sp("research", "t1", [])]
        )
        assert gate.may_skip("research", [], 1) is True


class TestTheResumePoint:
    def test_nothing_is_skipped_without_a_resume_point(self):
        tasks = [make_task()]
        identities = {"research": compute_crew_identity("research", tasks)}
        gate = policy(identities, resume_from=None)
        assert gate.may_skip("research", tasks, 1) is False

    def test_the_crew_at_the_resume_point_runs(self):
        tasks = [make_task()]
        identities = {"research": compute_crew_identity("research", tasks)}
        # resume_from names the crew TO RUN, so the comparison is < not <=.
        gate = policy(identities, resume_from=3)
        assert gate.may_skip("research", tasks, 3) is False
        assert gate.may_skip("research", tasks, 2) is True


class TestChangePropagatesAlongTheGraph:
    """Invalidation follows dependencies, not sequence numbers.

    Sequence is BUILD order — starting points then listeners, in config order —
    and a listener declared early can depend on one declared late. Following
    sequence instead of the graph is how a stale descendant got replayed.
    """

    @staticmethod
    def crew(name, tasks=None):
        return name, tasks if tasks is not None else [make_task(description=name)]

    def test_a_downstream_crew_is_re_run_when_its_input_changed(self):
        black = [make_task(description="black")]
        green = [make_task(description="green")]
        identities = {
            "black": compute_crew_identity("black", black),
            "green": compute_crew_identity("green", green),
        }
        # black --t1--> green, and black is edited.
        edited_black = [make_task(description="black rewritten")]
        gate = policy(
            identities,
            starting_points=[sp("black", "t1", edited_black)],
            listeners=[listener("green", "c-green", "t2", green, ["t1"])],
        )

        assert gate.may_skip("black", edited_black, 1) is False
        # green is untouched and must STILL re-run: its input just changed.
        assert gate.may_skip("green", green, 2) is False

    def test_an_independent_branch_keeps_its_checkpoint(self):
        """The gain from using the graph: a sibling is not a descendant."""
        black = [make_task(description="black")]
        green = [make_task(description="green")]
        white = [make_task(description="white")]
        identities = {
            "black": compute_crew_identity("black", black),
            "green": compute_crew_identity("green", green),
            "white": compute_crew_identity("white", white),
        }
        edited_green = [make_task(description="green rewritten")]
        # black --> green (edited), black --> white. white is a SIBLING.
        gate = policy(
            identities,
            starting_points=[sp("black", "t1", black)],
            listeners=[
                listener("green", "c-green", "t2", edited_green, ["t1"]),
                listener("white", "c-white", "t3", white, ["t1"]),
            ],
        )

        assert gate.may_skip("black", black, 1) is True
        assert gate.may_skip("green", edited_green, 2) is False
        # Under the old sequence rule this re-ran, purely for sorting after
        # green. It consumes nothing green produced.
        assert gate.may_skip("white", white, 3) is True

    def test_declaration_order_does_not_decide_dependency(self):
        """The exact shape that exposed the bug in a real flow.

        black (seq 1) -> white (seq 3) -> green (seq 2). green is declared
        BEFORE white, so it takes the lower sequence while depending on it.
        Skipping "everything below the resume point" replayed green against a
        white that was being re-run.
        """
        black = [make_task(description="black")]
        white = [make_task(description="white")]
        green = [make_task(description="green")]
        identities = {
            "black": compute_crew_identity("black", black),
            "white": compute_crew_identity("white", white),
            "green": compute_crew_identity("green", green),
        }
        edited_white = [make_task(description="white rewritten")]
        gate = policy(
            identities,
            starting_points=[sp("black", "t-black", black)],
            listeners=[
                # green is declared first -> sequence 2, but listens to white.
                listener("green", "c-green", "t-green", green, ["t-white"]),
                listener("white", "c-white", "t-white", edited_white, ["t-black"]),
            ],
        )

        assert gate.may_skip("black", black, 1) is True
        assert gate.may_skip("white", edited_white, 3) is False
        # The bug: green has the LOWER sequence but is DOWNSTREAM of white.
        assert gate.may_skip("green", green, 2) is False

    def test_a_cycle_terminates(self):
        """A malformed config must not hang the build."""
        a = [make_task(description="a")]
        b = [make_task(description="b")]
        gate = policy(
            {"a": "stale-identity"},
            starting_points=[],
            listeners=[
                listener("a", "c-a", "t-a", a, ["t-b"]),
                listener("b", "c-b", "t-b", b, ["t-a"]),
            ],
        )
        assert gate.may_skip("a", a, 1) is False

    def test_an_unchanged_flow_skips_everything_before_the_point(self):
        black = [make_task(description="black")]
        green = [make_task(description="green")]
        identities = {
            "black": compute_crew_identity("black", black),
            "green": compute_crew_identity("green", green),
        }
        gate = policy(
            identities,
            resume_from=4,
            starting_points=[sp("black", "t1", black)],
            listeners=[listener("green", "c-green", "t2", green, ["t1"])],
        )
        assert gate.may_skip("black", black, 1) is True
        assert gate.may_skip("green", green, 2) is True
