"""Reading back the ceiling a model was actually built with.

Two field names carry the same setting: most endpoints take ``max_tokens``,
GPT-5 and the newer OpenAI reasoning models take ``max_completion_tokens``.
A diagnostic that reads only one reports "None" for a model that is capped —
which is how a run with a 128,000-token ceiling was read as having none, and
sent someone looking for a setting that was already there.
"""

from types import SimpleNamespace

from src.core.llm.output_cap import UNSET, output_cap


class TestReadingTheCeiling:
    def test_a_plain_model_reports_max_tokens(self):
        assert output_cap(SimpleNamespace(max_tokens=4000)) == 4000

    def test_a_gpt5_model_reports_max_completion_tokens(self):
        # The case that was being misread: max_tokens is genuinely None here,
        # and the model is still capped.
        llm = SimpleNamespace(max_tokens=None, max_completion_tokens=128000)

        assert output_cap(llm) == 128000

    def test_max_completion_tokens_wins_when_both_are_set(self):
        # Mirrors the transport, which prefers it when deciding what to send.
        llm = SimpleNamespace(max_tokens=4000, max_completion_tokens=8000)

        assert output_cap(llm) == 8000

    def test_a_genuinely_uncapped_model_says_so(self):
        assert output_cap(SimpleNamespace(max_tokens=None)) == UNSET
        assert output_cap(SimpleNamespace()) == UNSET

    def test_a_nonsense_value_is_not_reported_as_a_ceiling(self):
        # A zero or negative cap bounds nothing; saying "0" would read as an
        # extremely tight limit rather than an absent one.
        assert output_cap(SimpleNamespace(max_tokens=0)) == UNSET
        assert output_cap(SimpleNamespace(max_tokens=-1)) == UNSET
        assert output_cap(SimpleNamespace(max_tokens="lots")) == UNSET
