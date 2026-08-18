"""How the active harness is resolved, and what happens when it is not stated.

The property under test throughout: **resolution never fails a run**. A bad
config value, an absent environment variable, a harness nobody registered — all
of them degrade to the Kasal harness, which is the one that has always been
here. An exception on this path would turn a typo in one settings row into
every execution failing.
"""

import pytest

from src.services.execution.harnesses import selection
from src.services.execution.harnesses.binding import HarnessName


@pytest.fixture(autouse=True)
def _clean_selection():
    """No harness may leak from one test into the next.

    The process default and the environment stamp are process-wide by design
    (a spawned crew interpreter serves one run), so a test that pins one and
    does not clean up takes the rest of the xdist worker with it — and the
    failure lands in a suite that has nothing to do with harnesses.
    """
    selection.reset_for_tests()
    yield
    selection.reset_for_tests()


class TestCoerce:
    @pytest.mark.parametrize(
        "value,expected",
        [
            ("kasal", HarnessName.KASAL),
            ("KASAL", HarnessName.KASAL),
            ("  crewai  ", HarnessName.CREWAI),
            (HarnessName.CREWAI, HarnessName.CREWAI),
        ],
    )
    def test_accepts_known_engines_in_any_casing(self, value, expected):
        assert selection.coerce(value) is expected

    @pytest.mark.parametrize("value", [None, "", "   ", "kasl", "langgraph", 7])
    def test_unknown_values_are_none_rather_than_an_exception(self, value):
        assert selection.coerce(value) is None

    def test_crewai_is_not_an_alias_for_kasal(self):
        """It used to be, in engineconfig rows, and must never be again.

        ``db/session.py`` still rewrites legacy ``engine_name='crewai'`` rows to
        'kasal'. If the word also coerced to the Kasal harness here, selecting
        CrewAI would silently do nothing and the setting would look ignored.
        """
        assert selection.coerce("crewai") is HarnessName.CREWAI


class TestActiveName:
    def test_defaults_to_kasal_with_nothing_configured(self):
        assert selection.active_name() is HarnessName.KASAL

    def test_environment_is_read_when_nothing_else_says(self, monkeypatch):
        monkeypatch.setenv(selection.HARNESS_ENV_VAR, "crewai")
        assert selection.active_name() is HarnessName.CREWAI

    def test_a_bad_environment_value_degrades_rather_than_raises(self, monkeypatch):
        monkeypatch.setenv(selection.HARNESS_ENV_VAR, "not-an-harness")
        assert selection.active_name() is HarnessName.KASAL

    def test_process_default_beats_the_environment(self, monkeypatch):
        monkeypatch.setenv(selection.HARNESS_ENV_VAR, "kasal")
        selection.set_process_default("crewai")
        assert selection.active_name() is HarnessName.CREWAI

    def test_setting_the_process_default_also_stamps_the_environment(self):
        """A grandchild process, and anything reading the env directly, agrees."""
        import os

        selection.set_process_default(HarnessName.CREWAI)
        assert os.environ[selection.HARNESS_ENV_VAR] == "crewai"

    def test_binding_beats_the_process_default_and_is_restored(self):
        selection.set_process_default("kasal")
        with selection.bind("crewai") as bound:
            assert bound is HarnessName.CREWAI
            assert selection.active_name() is HarnessName.CREWAI
        assert selection.active_name() is HarnessName.KASAL

    def test_binding_nests_and_unwinds_in_order(self):
        with selection.bind("kasal"):
            with selection.bind("crewai"):
                assert selection.active_name() is HarnessName.CREWAI
            assert selection.active_name() is HarnessName.KASAL

    def test_binding_an_unknown_engine_binds_the_default(self):
        with selection.bind("nonsense") as bound:
            assert bound is HarnessName.KASAL

    def test_binding_is_restored_even_when_the_block_raises(self):
        selection.set_process_default("kasal")
        with pytest.raises(RuntimeError):
            with selection.bind("crewai"):
                raise RuntimeError("boom")
        assert selection.active_name() is HarnessName.KASAL
