"""Unit tests for the resume skip gate.

This is the decision that used to replay an edited crew's stale output. It sits
between "this crew is before the resume point" and "therefore do not run it".
"""

from types import SimpleNamespace

from src.services.flow_builder.checkpoint_identity import compute_crew_identity
from src.services.flow_builder.modules.flow_builder import _crew_may_be_skipped


def make_agent(role="researcher", model="gpt-x"):
    return SimpleNamespace(key=f"{role}|find|expert", llm=SimpleNamespace(model=model))


def make_task(description="do it", agent=None):
    return SimpleNamespace(
        key=f"{description}|a result",
        agent=agent if agent is not None else make_agent(),
    )


class TestCrewMayBeSkipped:
    def test_an_unchanged_crew_is_skipped(self):
        tasks = [make_task()]
        identities = {"research": compute_crew_identity("research", tasks)}

        # Same crew, same work — its stored output stands in for running it.
        assert _crew_may_be_skipped("research", tasks, identities) is True

    def test_an_edited_crew_is_re_run(self):
        stored = {"research": compute_crew_identity("research", [make_task()])}
        edited = [make_task(description="do it much better")]

        # The whole point: the edit must not be silently discarded.
        assert _crew_may_be_skipped("research", edited, stored) is False

    def test_a_re_modelled_crew_is_re_run(self):
        stored = {
            "research": compute_crew_identity(
                "research", [make_task(agent=make_agent(model="gpt-x"))]
            )
        }
        re_modelled = [make_task(agent=make_agent(model="claude-y"))]

        assert _crew_may_be_skipped("research", re_modelled, stored) is False

    def test_a_checkpoint_without_identities_still_skips(self):
        """Pre-identity checkpoints keep working exactly as before.

        Refusing them would make every existing checkpoint useless, which is a
        worse outcome than the unverified skip they always had.
        """
        assert _crew_may_be_skipped("research", [make_task()], {}) is True
        assert _crew_may_be_skipped("research", [make_task()], None) is True

    def test_a_crew_missing_from_the_checkpoint_still_skips(self):
        identities = {"other-crew": "abc123"}
        assert _crew_may_be_skipped("research", [make_task()], identities) is True

    def test_an_uncomputable_crew_still_skips(self):
        # No tasks -> no identity -> unverified, not "changed".
        assert _crew_may_be_skipped("research", [], {"research": "abc123"}) is True
