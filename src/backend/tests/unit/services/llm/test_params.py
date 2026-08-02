"""Resolving what gets sent with a request.

Before this existed, the model catalogue could express exactly two knobs —
``temperature`` and ``max_output_tokens`` — so the transport's ``top_p``,
``frequency_penalty``, ``presence_penalty`` and ``stop`` fields were declared,
forwarded on every request, and assigned by nothing anywhere in the backend.
Influencing anything else meant editing Python in a provider handler, which is
what made every fix per-model.

Two properties matter more than the mechanics and are pinned hardest here:

* **Unset means absent.** A model that declares nothing must send exactly what
  it sent before these columns existed. Anything else silently changes the
  behaviour of every existing workspace.
* **A refused parameter cannot reach the wire.** There is no litellm
  ``drop_params`` net on this path — what is set IS sent, and OpenAI's reasoning
  models answer a stray ``frequency_penalty`` with a 400.
"""

import pytest

from src.services.llm.params import RESERVED, rejects, resolve


class TestUnsetMeansAbsent:
    def test_a_model_declaring_nothing_sends_nothing(self):
        assert resolve(None) == {}

    def test_an_empty_declaration_sends_nothing(self):
        assert resolve({}) == {}

    def test_an_explicit_null_is_not_a_value(self):
        """A config row carrying null must not become a null on the request."""
        assert resolve({"top_p": None, "frequency_penalty": 0.3}) == {
            "frequency_penalty": 0.3
        }


class TestPrecedence:
    def test_the_model_declaration_beats_the_kasal_default(self):
        assert resolve({"top_p": 0.8}, defaults={"top_p": 0.5})["top_p"] == 0.8

    def test_the_call_site_beats_the_model_declaration(self):
        assert resolve({"top_p": 0.8}, overrides={"top_p": 0.5})["top_p"] == 0.5

    def test_layers_combine_rather_than_replace_each_other_wholesale(self):
        merged = resolve(
            {"top_p": 0.8}, overrides={"frequency_penalty": 0.2}, defaults={"seed": 7}
        )

        assert merged == {"seed": 7, "top_p": 0.8, "frequency_penalty": 0.2}


class TestCapabilityFilter:
    def test_a_refused_parameter_is_dropped(self):
        """The GPT-5 / o-series case: both penalties are a 400."""
        merged = resolve(
            {"frequency_penalty": 0.3, "top_p": 0.8},
            unsupported=["frequency_penalty", "presence_penalty"],
        )

        assert merged == {"top_p": 0.8}

    def test_the_filter_reaches_inside_extra_body(self):
        """An endpoint that refuses a parameter refuses it wherever it is
        written — and vLLM-only knobs can only travel inside extra_body."""
        merged = resolve(
            {"extra_body": {"top_k": 20, "repetition_penalty": 1.05}},
            unsupported=["top_k"],
        )

        assert merged == {"extra_body": {"repetition_penalty": 1.05}}

    def test_an_emptied_extra_body_is_removed_entirely(self):
        """Sending extra_body={} is not the same as not sending it."""
        merged = resolve({"extra_body": {"top_k": 20}}, unsupported=["top_k"])

        assert "extra_body" not in merged

    def test_the_filter_applies_to_the_MERGED_bag(self):
        """Filtering each layer separately lets a later layer reintroduce what
        an earlier one was filtered for."""
        merged = resolve(
            {"top_p": 0.8},
            overrides={"frequency_penalty": 0.9},
            unsupported=["frequency_penalty"],
        )

        assert "frequency_penalty" not in merged

    def test_no_refusals_means_nothing_is_dropped(self):
        assert resolve({"top_p": 0.8}, unsupported=[]) == {"top_p": 0.8}


class TestReservedParameters:
    @pytest.mark.parametrize("name", sorted(RESERVED))
    def test_the_transport_owns_these(self, name):
        """A catalogue row must not be able to redirect traffic or swap
        credentials by declaring a 'sampling parameter'."""
        assert resolve({name: "hijacked", "top_p": 0.9}) == {"top_p": 0.9}


class TestRejects:
    def test_it_answers_from_data_not_from_the_model_name(self):
        """Replaces the substring tests that lived in three files and
        disagreed: model_rejects_temperature, supports_stop_words, is_gpt5."""
        assert rejects(["temperature"], "temperature") is True
        assert rejects(["temperature"], "top_p") is False
        assert rejects(None, "temperature") is False
