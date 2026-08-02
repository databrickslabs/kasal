"""The declared sampling surface has to survive the trip to the transport.

`ModelConfig.params` / `.unsupported_params` are columns, `services/llm/params.py`
resolves them, and `configure_kasal_llm` reads both off the dict this service
returns. That dict never carried them — so a row declaring
``{"frequency_penalty": 0.3}`` resolved to an empty bag and an endpoint's
refusal list resolved to nothing, on every model, always.

Both directions failed silently, which is why it survived: an absent parameter
looks exactly like a parameter nobody declared, and an unfiltered one only
shows up as a 400 from an endpoint that happens to refuse it.

Two builders produce this shape and BOTH must carry the columns —
``_as_config`` serves the substitution path, which is how a Databricks model on
a workspace-less deployment becomes the local vLLM one. That is precisely the
row that carries a penalty.
"""

from types import SimpleNamespace

from src.services.settings.models import ModelConfigService


def _row(**overrides):
    row = SimpleNamespace(
        key="Qwen3-Coder-30B-A3B-Instruct",
        name="Qwen3-Coder-30B-A3B-Instruct",
        provider="vllm",
        temperature=0.6,
        context_window=28672,
        max_output_tokens=4096,
        extended_thinking=False,
        enabled=True,
        params={"frequency_penalty": 0.3},
        unsupported_params=None,
    )
    for key, value in overrides.items():
        setattr(row, key, value)
    return row


class TestSubstitutionPathCarriesDeclaredParams:
    def test_params_survive(self):
        assert ModelConfigService._as_config(_row())["params"] == {
            "frequency_penalty": 0.3
        }

    def test_unsupported_params_survive(self):
        row = _row(unsupported_params=["temperature"])

        assert ModelConfigService._as_config(row)["unsupported_params"] == [
            "temperature"
        ]

    def test_a_row_declaring_nothing_yields_none(self):
        """Unset must stay unset — `resolve` treats None as "not specified", so
        an empty dict here would be a different statement."""
        config = ModelConfigService._as_config(_row(params=None))

        assert config["params"] is None

    def test_a_row_object_without_the_attributes_does_not_explode(self):
        """The fallback seed dicts and older cached rows are plain objects; a
        substitution must never raise on the way to a working model."""
        bare = SimpleNamespace(
            key="k",
            name="n",
            provider="ollama",
            temperature=0.7,
            context_window=8192,
            max_output_tokens=4096,
            enabled=True,
        )

        config = ModelConfigService._as_config(bare)

        assert config["params"] is None
        assert config["unsupported_params"] is None


class TestTheShapesAgree:
    def test_as_config_declares_every_key_configure_kasal_llm_reads(self):
        """`_as_config` is documented as "the config dict shape
        get_model_config returns" — a claim worth checking, since the two
        drifting apart is what made the substitution path lose the columns."""
        config = ModelConfigService._as_config(_row())

        for key in (
            "key",
            "name",
            "provider",
            "temperature",
            "context_window",
            "max_output_tokens",
            "enabled",
            "params",
            "unsupported_params",
        ):
            assert key in config, f"{key} missing from the config dict"
