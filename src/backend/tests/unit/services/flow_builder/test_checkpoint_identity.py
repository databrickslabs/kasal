"""Unit tests for flow crew content identity.

The point of the identity is that editing a crew must not be silently ignored
on resume. Every test here is a way a crew can change that used to look
identical, because only its NAME was stored.
"""

from types import SimpleNamespace

from src.services.flow_builder.checkpoint_identity import (
    CHANGED,
    MATCH,
    UNVERIFIED,
    compute_crew_identity,
    verify_crew_identity,
)


def make_agent(role="researcher", goal="find", backstory="expert", model="gpt-x"):
    # Agent.key is a hash of role|goal|backstory; the model sits outside it,
    # which is why the fingerprint has to reach for llm.model separately.
    key = f"{role}|{goal}|{backstory}"
    return SimpleNamespace(key=key, llm=SimpleNamespace(model=model))


def make_task(description="do it", expected="a result", agent=None):
    return SimpleNamespace(
        key=f"{description}|{expected}",
        agent=agent if agent is not None else make_agent(),
    )


class TestComputeCrewIdentity:
    def test_same_crew_hashes_the_same(self):
        a = compute_crew_identity("research", [make_task()])
        b = compute_crew_identity("research", [make_task()])
        assert a == b

    def test_changing_a_task_changes_the_identity(self):
        before = compute_crew_identity("research", [make_task(description="do it")])
        after = compute_crew_identity("research", [make_task(description="do it well")])
        assert before != after

    def test_changing_expected_output_changes_the_identity(self):
        before = compute_crew_identity("research", [make_task(expected="a result")])
        after = compute_crew_identity("research", [make_task(expected="a table")])
        assert before != after

    def test_changing_the_agent_changes_the_identity(self):
        before = compute_crew_identity("research", [make_task(agent=make_agent())])
        after = compute_crew_identity(
            "research", [make_task(agent=make_agent(role="analyst"))]
        )
        assert before != after

    def test_changing_the_model_changes_the_identity(self):
        """The most common tuning edit, and the one Agent.key alone misses."""
        before = compute_crew_identity(
            "research", [make_task(agent=make_agent(model="gpt-x"))]
        )
        after = compute_crew_identity(
            "research", [make_task(agent=make_agent(model="claude-y"))]
        )
        assert before != after

    def test_renaming_the_crew_changes_the_identity(self):
        assert compute_crew_identity(
            "research", [make_task()]
        ) != compute_crew_identity("analysis", [make_task()])

    def test_reordering_tasks_changes_the_identity(self):
        one, two = make_task(description="first"), make_task(description="second")
        assert compute_crew_identity("c", [one, two]) != compute_crew_identity(
            "c", [two, one]
        )

    def test_adding_a_task_changes_the_identity(self):
        assert compute_crew_identity("c", [make_task()]) != compute_crew_identity(
            "c", [make_task(), make_task(description="extra")]
        )

    def test_no_tasks_is_unverifiable(self):
        assert compute_crew_identity("c", []) is None
        assert compute_crew_identity("c", None) is None

    def test_a_task_without_a_key_makes_the_crew_unverifiable(self):
        # Better to report "cannot verify" than to hash around the hole.
        assert (
            compute_crew_identity("c", [SimpleNamespace(key=None, agent=None)]) is None
        )

    def test_an_agentless_task_still_hashes(self):
        assert compute_crew_identity("c", [SimpleNamespace(key="k", agent=None)])

    def test_a_string_llm_is_still_part_of_the_identity(self):
        task_a = SimpleNamespace(key="k", agent=SimpleNamespace(key="a", llm="gpt-x"))
        task_b = SimpleNamespace(key="k", agent=SimpleNamespace(key="a", llm="other"))
        assert compute_crew_identity("c", [task_a]) != compute_crew_identity(
            "c", [task_b]
        )


class TestVerifyCrewIdentity:
    def test_identical_is_a_match(self):
        assert verify_crew_identity("abc", "abc") == MATCH

    def test_different_is_changed(self):
        assert verify_crew_identity("abc", "def") == CHANGED

    def test_a_missing_stored_identity_is_unverified_not_changed(self):
        """Checkpoints written before identities existed must keep working.

        Treating them as CHANGED would make every existing checkpoint worthless
        the moment this shipped.
        """
        assert verify_crew_identity("abc", None) == UNVERIFIED
        assert verify_crew_identity("abc", "") == UNVERIFIED

    def test_an_uncomputable_current_identity_is_unverified(self):
        assert verify_crew_identity(None, "abc") == UNVERIFIED
        assert verify_crew_identity(None, None) == UNVERIFIED
