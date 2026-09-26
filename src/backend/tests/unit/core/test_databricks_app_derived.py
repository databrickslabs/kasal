"""Platform facts DERIVED from the Databricks Apps context, not configured."""

import pytest

from src.core import databricks_app as app
from src.core.exceptions import LakebaseNotConfiguredError

_APP = {
    "DATABRICKS_APP_NAME": "kasal",
    "DATABRICKS_APP_PORT": "8000",
    "DATABRICKS_WORKSPACE_ID": "123",
    "DATABRICKS_HOST": "https://example.com",
}
_CLEAR = (
    *_APP,
    "ENVIRONMENT",
    "KASAL_DEPLOYMENT_MODE",
    "KASAL_LAKEBASE_RESOURCE",
    "PGHOST",
    "PGDATABASE",
    "PGUSER",
    "MLFLOW_CREW_TRACES_EXPERIMENT",
    "KASAL_OUTPUT_VOLUME",
)


@pytest.fixture
def env(monkeypatch):
    for key in _CLEAR:
        monkeypatch.delenv(key, raising=False)
    return monkeypatch


@pytest.fixture
def hosted(env):
    for key, value in _APP.items():
        env.setenv(key, value)
    return env


class TestProduction:
    @pytest.mark.parametrize("value", ["", "development", "dev", "local"])
    def test_apps_is_production_whatever_environment_says(self, hosted, value):
        hosted.setenv("ENVIRONMENT", value)
        assert app.is_production() is True
        assert app.is_local_dev() is False

    def test_app_name_alone_fails_closed(self, env):
        env.setenv("DATABRICKS_APP_NAME", "kasal")
        assert app.is_production() is True

    def test_outside_apps_environment_decides(self, env):
        assert app.is_local_dev() is True
        assert app.is_production() is False
        env.setenv("ENVIRONMENT", "production")
        assert app.is_production() is True
        assert app.is_local_dev() is False


class TestLakebaseInstance:
    def test_configured_name_wins(self, hosted):
        hosted.setenv("KASAL_LAKEBASE_RESOURCE", "bound")
        assert app.resolve_lakebase_instance_name("configured") == "configured"

    def test_then_the_apps_binding(self, env):
        env.setenv("KASAL_LAKEBASE_RESOURCE", "bound")
        assert app.resolve_lakebase_instance_name(None) == "bound"
        assert app.lakebase_instance_from_config({"enabled": True}) == "bound"

    def test_then_the_bound_pg_host(self, hosted):
        for key, value in {"PGHOST": "pg", "PGDATABASE": "d", "PGUSER": "u"}.items():
            hosted.setenv(key, value)
        assert app.resolve_lakebase_instance_name("") == "pg"

    def test_never_an_invented_default(self, env):
        with pytest.raises(LakebaseNotConfiguredError):
            app.resolve_lakebase_instance_name(None)
        with pytest.raises(LakebaseNotConfiguredError):
            app.lakebase_instance_from_config(None)


class TestTraceExperimentFallback:
    def test_inside_apps_it_is_per_teamspace(self, hosted):
        hosted.setenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Shared/ignored")
        name = app.fallback_trace_experiment("team-a")
        assert name == app.DatabricksAppInstallation.from_env().experiment_name(
            "team-a"
        )
        assert name != app.fallback_trace_experiment("team-b")
        assert "kasal-crew-execution-traces" not in name

    def test_inside_apps_without_a_teamspace_there_is_none(self, hosted):
        assert app.fallback_trace_experiment(None) is None

    def test_local_dev_reads_the_env_override_only(self, env):
        assert app.fallback_trace_experiment("team-a") is None
        env.setenv("MLFLOW_CREW_TRACES_EXPERIMENT", "/Users/me/traces")
        assert app.fallback_trace_experiment("team-a") == "/Users/me/traces"
