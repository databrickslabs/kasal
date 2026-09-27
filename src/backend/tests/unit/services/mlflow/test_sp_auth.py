"""sp_auth scopes MLflow's Databricks credential to a context, not the process.

The previous design wrote the token into ``os.environ`` behind one process-wide
lock: a long GEPA/evaluation run blocked every other identity's MLflow call for
up to 60 s (then failed it) and exposed its token to every thread. These tests
pin the replacement's contract: nothing is written to the env, concurrent
identities neither wait for nor see each other, and the credential still
reaches every place MLflow reads one (the SDK ``Config``, MLflow's env provider,
and MLflow's own worker pools).
"""

from __future__ import annotations

import os
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from src.services.mlflow import sp_auth

_ENV_KEYS = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_API_KEY",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_AUTH_TYPE",
)


@pytest.fixture
def app_env(monkeypatch):
    """The Databricks Apps shape: SP OAuth creds injected, auth type oauth-m2m."""
    monkeypatch.setenv("DATABRICKS_HOST", "https://ambient.example.com")
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "cid")
    monkeypatch.setenv("DATABRICKS_CLIENT_SECRET", "csec")
    monkeypatch.setenv("DATABRICKS_AUTH_TYPE", "oauth-m2m")
    monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)
    monkeypatch.delenv("DATABRICKS_API_KEY", raising=False)


def _env() -> dict:
    return {k: os.environ.get(k) for k in _ENV_KEYS}


def _loaded_config(**inner):
    """A Config after ``_load_from_env`` only — no host discovery, no network."""
    from databricks.sdk.config import Config
    from databricks.sdk.credentials_provider import DefaultCredentials

    cfg = Config.__new__(Config)
    cfg._inner = {}
    cfg._credentials_strategy = DefaultCredentials()
    for key, value in inner.items():
        setattr(cfg, key, value)
    cfg._load_from_env()
    return cfg


class TestNothingTouchesTheEnvironment:
    def test_scope_leaves_os_environ_exactly_as_it_was(self, app_env):
        before = _env()
        with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
            assert _env() == before
            creds = sp_auth.current_credentials()
            assert creds is not None
            assert (creds.host, creds.token) == ("https://ws.example.com", "tok")
        assert _env() == before
        assert sp_auth.current_credentials() is None

    def test_no_token_is_a_no_op(self):
        with sp_auth.single_auth_env(host="https://ws.example.com", token=None):
            assert sp_auth.current_credentials() is None

    def test_repr_never_prints_the_token(self):
        creds = sp_auth.ScopedCredentials(host="h", token="secret-token")
        assert "secret-token" not in repr(creds)


class TestTheCredentialReachesMlflowAndTheSdk:
    def test_bare_sdk_config_uses_the_scoped_token_as_pat(self, app_env):
        with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
            cfg = _loaded_config()
        assert cfg.token == "tok"
        assert cfg.host == "https://ws.example.com"
        # Pinned, so the injected OAuth vars can't make it "oauth and pat".
        assert cfg.auth_type == "pat"

    def test_sdk_config_outside_a_scope_reads_the_ambient_env(self, app_env):
        sp_auth.install_hooks()
        cfg = _loaded_config()
        assert cfg.token is None
        assert cfg.auth_type == "oauth-m2m"
        assert cfg.client_id == "cid"

    def test_explicit_credentials_are_never_overridden(self, app_env):
        with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
            cfg = _loaded_config(
                host="https://other.example.com",
                client_id="x",
                client_secret="y",
                auth_type="oauth-m2m",
            )
        assert cfg.token is None
        assert cfg.auth_type == "oauth-m2m"
        assert cfg.host == "https://other.example.com"

    def test_full_workspace_client_authenticates_with_the_scoped_token(self, app_env):
        from databricks.sdk import WorkspaceClient

        with (
            patch(
                "databricks.sdk.config.get_host_metadata",
                side_effect=RuntimeError("no network in unit tests"),
            ),
            sp_auth.single_auth_env(host="https://ws.example.com", token="tok"),
        ):
            headers = WorkspaceClient().config.authenticate()
        assert headers["Authorization"] == "Bearer tok"

    def test_mlflow_host_creds_carry_the_scoped_identity(self, app_env):
        """The function every MLflow Databricks store calls per request."""
        from mlflow.utils.databricks_utils import get_databricks_host_creds
        from mlflow.utils.rest_utils import get_workspace_client

        with (
            patch(
                "databricks.sdk.config.get_host_metadata",
                side_effect=RuntimeError("no network in unit tests"),
            ),
            sp_auth.single_auth_env(host="https://ws.example.com", token="tok"),
        ):
            creds = get_databricks_host_creds("databricks")
            client = get_workspace_client(
                creds.use_secret_scope_token,
                creds.host,
                creds.token,
                creds.databricks_auth_profile,
            )
            headers = client.config.authenticate()
        assert (creds.host, creds.token) == ("https://ws.example.com", "tok")
        assert headers["Authorization"] == "Bearer tok"

    def test_mlflow_env_provider_returns_the_scoped_credential(self, app_env):
        from mlflow.legacy_databricks_cli.configure.provider import (
            EnvironmentVariableConfigProvider,
        )

        with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
            cfg = EnvironmentVariableConfigProvider().get_config()
        assert (cfg.host, cfg.token) == ("https://ws.example.com", "tok")
        # Outside the scope MLflow sees only what the environment holds.
        outside = EnvironmentVariableConfigProvider().get_config()
        assert outside is None or outside.token is None


class TestConcurrency:
    def test_two_identities_run_in_parallel_and_see_only_their_own(self):
        """Both scopes are open AT THE SAME TIME (the barrier would time out if
        one waited for the other), and each sees only its own token — in
        current_credentials and in a bare SDK Config built inside."""
        sp_auth.install_hooks()
        barrier = threading.Barrier(2, timeout=5)
        seen: dict = {}
        errors: list = []

        def run(name: str) -> None:
            try:
                with sp_auth.single_auth_env(
                    host=f"https://{name}.example.com", token=f"tok-{name}"
                ):
                    barrier.wait()  # both scopes open now
                    seen[name] = (
                        sp_auth.current_credentials().token,
                        _loaded_config().token,
                    )
                    barrier.wait()  # neither closes before the other has looked
            except Exception as exc:  # noqa: BLE001 — surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=run, args=(n,)) for n in ("a", "b")]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10)
        assert not errors
        assert seen == {"a": ("tok-a", "tok-a"), "b": ("tok-b", "tok-b")}
        assert "DATABRICKS_TOKEN" not in os.environ or os.environ[
            "DATABRICKS_TOKEN"
        ] not in ("tok-a", "tok-b")

    def test_a_long_running_scope_does_not_starve_others(self):
        """A GEPA-length scope stays open while another identity's calls come
        and go immediately — the old lock made them wait 60 s, then fail."""
        hold_open, a_open = threading.Event(), threading.Event()

        def long_run() -> None:
            with sp_auth.single_auth_env(host="https://a.example.com", token="a"):
                a_open.set()
                hold_open.wait(10)

        runner = threading.Thread(target=long_run)
        runner.start()
        try:
            assert a_open.wait(5)
            done = threading.Event()

            def quick_calls() -> None:
                for _ in range(20):
                    with sp_auth.single_auth_env(
                        host="https://b.example.com", token="b"
                    ):
                        assert sp_auth.current_credentials().token == "b"
                done.set()

            quick = threading.Thread(target=quick_calls)
            quick.start()
            assert done.wait(2), "another identity's MLflow calls were blocked"
            quick.join(2)
        finally:
            hold_open.set()
            runner.join(5)


class TestWorkerPools:
    def test_work_submitted_in_a_scope_carries_it(self):
        with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
            with ThreadPoolExecutor(max_workers=2) as pool:
                tokens = list(
                    pool.submit(lambda: sp_auth.current_credentials().token).result()
                    for _ in range(3)
                )
        assert tokens == ["tok", "tok", "tok"]

    def test_a_pool_thread_does_not_keep_a_scope_after_it_closes(self):
        pool = ThreadPoolExecutor(max_workers=1)
        try:
            with sp_auth.single_auth_env(host="https://ws.example.com", token="tok"):
                assert pool.submit(sp_auth.current_credentials).result() is not None
            # Same worker thread, submitted outside any scope: nothing leaks.
            assert pool.submit(sp_auth.current_credentials).result() is None
            with sp_auth.single_auth_env(host="https://o.example.com", token="other"):
                assert pool.submit(sp_auth.current_credentials).result().token == (
                    "other"
                )
        finally:
            pool.shutdown()


class TestSpSingleAuth:
    def test_scopes_the_derived_sp_bearer(self, app_env, monkeypatch):
        monkeypatch.setattr(sp_auth, "derive_sp_bearer", lambda *a: "sp-bearer")
        before = _env()
        with sp_auth.sp_single_auth() as active:
            assert active is True
            creds = sp_auth.current_credentials()
            assert (creds.host, creds.token) == (
                "https://ambient.example.com",
                "sp-bearer",
            )
            # The OAuth vars stay where the platform put them; nothing is added.
            assert _env() == before
        assert sp_auth.current_credentials() is None

    def test_local_dev_pat_is_scoped_when_no_sp(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.delenv("DATABRICKS_CLIENT_SECRET", raising=False)
        monkeypatch.setenv("DATABRICKS_HOST", "https://dev.example.com")
        monkeypatch.setenv("DATABRICKS_TOKEN", "dev-pat")
        monkeypatch.setattr("src.core.databricks_app.is_databricks_app", lambda: False)
        with sp_auth.pat_auth_env() as active:
            assert active is True
            assert sp_auth.current_credentials().token == "dev-pat"

    def test_an_env_token_inside_apps_is_nobodys(self, monkeypatch):
        monkeypatch.delenv("DATABRICKS_CLIENT_ID", raising=False)
        monkeypatch.setenv("DATABRICKS_TOKEN", "leaked")
        monkeypatch.setattr("src.core.databricks_app.is_databricks_app", lambda: True)
        with sp_auth.pat_auth_env() as active:
            assert active is False
            assert sp_auth.current_credentials() is None

    def test_no_op_without_any_credentials(self, monkeypatch):
        for key in _ENV_KEYS:
            monkeypatch.delenv(key, raising=False)
        with sp_auth.sp_single_auth() as active:
            assert active is False
            assert sp_auth.current_credentials() is None

    def test_derive_sp_bearer_names_its_auth_type(self):
        with patch("databricks.sdk.WorkspaceClient") as wc:
            wc.return_value.config.authenticate.return_value = {
                "Authorization": "Bearer tok"
            }
            assert sp_auth.derive_sp_bearer("https://h", "cid", "sec") == "tok"
        assert wc.call_args.kwargs["auth_type"] == "oauth-m2m"


class TestNotImportedByTheCrewSubprocess:
    def test_the_execution_package_does_not_import_sp_auth(self):
        """services/execution/CLAUDE.md: the spawned interpreter must not pick up
        these hooks by import; only MLflow callers in the server install them."""
        import pathlib

        root = pathlib.Path(sp_auth.__file__).resolve().parents[1] / "execution"
        offenders = [
            str(p)
            for p in root.rglob("*.py")
            if "mlflow.sp_auth" in p.read_text(encoding="utf-8")
        ]
        assert offenders == []
