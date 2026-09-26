"""A model's endpoint comes from Configuration → Models, never from env vars."""

import pytest

from src.services.llm import endpoints


@pytest.fixture
def outside_apps(monkeypatch):
    monkeypatch.setattr(endpoints, "on_databricks_apps", lambda: False)


@pytest.fixture
def inside_apps(monkeypatch):
    monkeypatch.setattr(endpoints, "on_databricks_apps", lambda: True)


class TestModelApiBase:
    def test_the_models_own_endpoint_wins(self, inside_apps):
        params = {"api_base": " https://vllm.example.com/v1/ "}
        assert endpoints.model_api_base("vllm", params) == "https://vllm.example.com/v1"

    def test_hosted_providers_default_to_their_public_api(self, inside_apps):
        assert (
            endpoints.model_api_base("anthropic", None) == "https://api.anthropic.com"
        )
        assert endpoints.model_api_base("deepseek", {}) == "https://api.deepseek.com"

    def test_self_hosted_defaults_to_localhost_outside_apps(self, outside_apps):
        assert endpoints.model_api_base("vllm", None) == "http://localhost:8081/v1"
        assert endpoints.model_api_base("custom", None) == "http://127.0.0.1:8082/v1"
        assert endpoints.ollama_base_url() == "http://localhost:11434"

    def test_self_hosted_has_no_default_inside_apps(self, inside_apps):
        assert endpoints.model_api_base("vllm", None) is None
        assert endpoints.ollama_base_url() is None

    @pytest.mark.parametrize(
        "var",
        ["VLLM_BASE_URL", "KAT_BASE_URL", "OLLAMA_API_BASE", "ANTHROPIC_API_BASE"],
    )
    def test_env_vars_are_ignored(self, outside_apps, monkeypatch, var):
        monkeypatch.setenv(var, "https://env.example.com/v1")
        for provider in ("vllm", "custom", "ollama", "anthropic"):
            assert "env.example.com" not in (
                endpoints.model_api_base(provider, None) or ""
            )


class TestRequireApiBase:
    def test_missing_self_hosted_endpoint_in_apps_is_a_clear_error(self, inside_apps):
        with pytest.raises(ValueError, match="Configuration → Models"):
            endpoints.require_api_base("vllm", None, "Qwen3")

    def test_configured_endpoint_is_returned(self, inside_apps):
        params = {"api_base": "https://kat.example.com/v1"}
        assert (
            endpoints.require_api_base("custom", params, "KAT")
            == "https://kat.example.com/v1"
        )
