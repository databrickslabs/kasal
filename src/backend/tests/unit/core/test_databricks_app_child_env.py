"""The allow-listed environment a crew/flow child (or the MCP helper) inherits."""

from unittest.mock import patch

from src.core.databricks_app import (
    child_env_allowed,
    child_environment,
    restrict_environment_to_child_allow_list,
)

HOSTED = {
    "DATABRICKS_APP_NAME": "kasal",
    "DATABRICKS_APP_PORT": "8000",
    "DATABRICKS_WORKSPACE_ID": "123",
    "DATABRICKS_HOST": "https://example.com",
}


class TestChildEnvAllowed:
    def test_workspace_credentials_never_pass(self):
        for name in (
            "OPENAI_API_KEY",
            "ANTHROPIC_API_KEY",
            "SERPER_API_KEY",
            "PERPLEXITY_API_KEY",
            "VLLM_API_KEY",
            "KAT_API_KEY",
            "POWERBI_PASSWORD",
            "POWERBI_CLIENT_SECRET",
            "LITELLM_CACHE_REDIS_PASSWORD",
        ):
            assert not child_env_allowed(name, hosted=True), name
            assert not child_env_allowed(name, hosted=False), name

    def test_platform_injected_vars_pass_including_the_app_service_principal(self):
        for name in (
            *HOSTED,
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
            "PGHOST",
            "PGDATABASE",
            "PGUSER",
            "PGPORT",
            "KASAL_SQL_WAREHOUSE_ID",
            "KASAL_LAKEBASE_RESOURCE",
            "KASAL_OUTPUT_VOLUME",
            "MLFLOW_TRACING_SQL_WAREHOUSE_ID",
            "OTEL_EXPORTER_OTLP_ENDPOINT",
        ):
            assert child_env_allowed(name, hosted=True), name

    def test_process_db_and_logging_configuration_pass(self):
        for name in (
            "PATH",
            "HOME",
            "PYTHONPATH",
            "KASAL_EXECUTION_ID",
            "KASAL_HARNESS",
            "KASAL_LOG_LEVEL",
            "LOG_DIR",
            "DATABASE_TYPE",
            "SQLITE_DB_PATH",
            "POSTGRES_PASSWORD",  # the operator's own DB, not a workspace key
            "ENCRYPTION_KEY",  # children decrypt the workspace's stored keys
            "CREW_SUBPROCESS_MODE",
        ):
            assert child_env_allowed(name, hosted=True), name

    def test_an_env_pat_passes_only_outside_apps(self):
        for name in ("DATABRICKS_TOKEN", "DATABRICKS_API_KEY"):
            assert child_env_allowed(name, hosted=False)
            assert not child_env_allowed(name, hosted=True)

    def test_unknown_variables_do_not_pass(self):
        assert not child_env_allowed("SOME_RANDOM_THING", hosted=True)


class TestChildEnvironment:
    def test_filters_a_hosted_environment(self):
        env = {
            **HOSTED,
            "DATABRICKS_CLIENT_SECRET": "sp",
            "DATABRICKS_TOKEN": "pat",
            "SERPER_API_KEY": "k",
            "PATH": "/bin",
        }
        child = child_environment(env)
        assert child["DATABRICKS_CLIENT_SECRET"] == "sp"
        assert child["PATH"] == "/bin"
        assert "DATABRICKS_TOKEN" not in child
        assert "SERPER_API_KEY" not in child

    def test_restrict_scrubs_this_process(self, monkeypatch):
        monkeypatch.setenv("SERPER_API_KEY", "leak")
        monkeypatch.setenv("KASAL_LOG_LEVEL", "INFO")
        with patch("src.core.databricks_app.is_databricks_app", return_value=True):
            removed = restrict_environment_to_child_allow_list()
        import os

        assert "SERPER_API_KEY" in removed
        assert "SERPER_API_KEY" not in os.environ
        assert os.environ["KASAL_LOG_LEVEL"] == "INFO"
