"""A model that exists only in the database must still get a window.

The module-level loop registers the SEEDED catalogue. A user-created model is
in the `modelconfig` table and nowhere else, so it reached the transport
unregistered — and unregistered is not a small thing: `_raw_context_window()`
returns 0, which switches the output clamp off entirely and drops the input
budget onto a guess made from the agent rather than the model.
"""

import pytest

from src.core.llm.transport import LLM_CONTEXT_WINDOW_SIZES
from src.services.llm.manager import _register_context_window

CUSTOM = "Test-Custom-Model-V9"


@pytest.fixture(autouse=True)
def _clean_registry():
    yield
    for key in (CUSTOM, f"openai/{CUSTOM}"):
        LLM_CONTEXT_WINDOW_SIZES.pop(key, None)


def test_the_row_teaches_the_transport_its_window():
    _register_context_window(CUSTOM, 131072)

    assert LLM_CONTEXT_WINDOW_SIZES[CUSTOM] == 131072


def test_both_spellings_are_registered():
    """The lookup is by exact key and callers differ on the prefix."""
    _register_context_window(CUSTOM, 131072)

    assert LLM_CONTEXT_WINDOW_SIZES[f"openai/{CUSTOM}"] == 131072


@pytest.mark.parametrize("bad", [None, 0, -1, "", "many", float("inf")])
def test_a_window_that_is_not_a_positive_int_is_refused(bad):
    """A wrong window is worse than none: unknown routes through the cautious
    fallbacks, while a lie is budgeted against confidently."""
    _register_context_window(CUSTOM, bad)

    assert CUSTOM not in LLM_CONTEXT_WINDOW_SIZES


def test_registering_twice_is_harmless():
    _register_context_window(CUSTOM, 131072)
    _register_context_window(CUSTOM, 131072)

    assert LLM_CONTEXT_WINDOW_SIZES[CUSTOM] == 131072


def test_a_nameless_model_is_ignored():
    _register_context_window("", 131072)

    assert "" not in LLM_CONTEXT_WINDOW_SIZES


class TestOneWriter:
    """The registry had three inline writers that agreed on the hard part (key
    spellings) and differed on everything else — only one validated its value,
    none were idempotent, and a missing window silently became 128,000."""

    def test_the_seeded_loop_goes_through_the_same_writer(self):
        """Not a mock check: if the loop still wrote the dict directly, these
        spellings would exist without the validation the writer applies."""
        import src.services.llm.manager  # noqa: F401  (import runs the loop)
        from src.seeds.model_configs import MODEL_CONFIGS

        databricks = [
            name
            for name, cfg in MODEL_CONFIGS.items()
            if cfg.get("provider") == "databricks"
        ]
        assert databricks, "the catalogue should still seed databricks models"
        name = databricks[0]

        assert LLM_CONTEXT_WINDOW_SIZES.get(name)
        assert LLM_CONTEXT_WINDOW_SIZES.get(f"databricks/{name}")

    def test_extra_spellings_are_registered_alongside_the_bare_name(self):
        _register_context_window(CUSTOM, 4096, keys=("databricks/" + CUSTOM,))

        assert LLM_CONTEXT_WINDOW_SIZES[CUSTOM] == 4096
        assert LLM_CONTEXT_WINDOW_SIZES["databricks/" + CUSTOM] == 4096
        LLM_CONTEXT_WINDOW_SIZES.pop("databricks/" + CUSTOM, None)

    def test_a_bad_window_registers_NOTHING_rather_than_a_default(self):
        """The loops defaulted a missing window to 128,000, which is a confident
        lie: the transport budgets against it instead of using its fallbacks."""
        _register_context_window(CUSTOM, None, keys=("openai/" + CUSTOM,))

        assert CUSTOM not in LLM_CONTEXT_WINDOW_SIZES
        assert "openai/" + CUSTOM not in LLM_CONTEXT_WINDOW_SIZES
