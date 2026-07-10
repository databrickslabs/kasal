"""Unit tests for the Databricks-secret-scope encryption key provisioner.

The provisioner materializes a STABLE RSA private key onto the filesystem at
startup (sourced from a Databricks secret scope) so encrypted secrets stay
decryptable across redeploys. Off Databricks — or on any failure — it must be a
no-op that leaves the existing local-filesystem key behavior untouched and never
raises.
"""
import os
from unittest.mock import AsyncMock, patch

import pytest

from src.utils import encryption_key_provisioner as prov
from src.utils.encryption_utils import EncryptionUtils


@pytest.fixture
def key_dir(tmp_path):
    """Point the key directory at a temp dir so nothing touches ~/.backendcrew."""
    d = tmp_path / "keys"
    d.mkdir()
    with patch.object(EncryptionUtils, "get_key_directory", return_value=d):
        yield d


def _databricks_env(**extra):
    env = {
        "DATABRICKS_APP_NAME": "kasal",
        "DATABRICKS_CLIENT_ID": "cid",
        "DATABRICKS_CLIENT_SECRET": "csecret",
    }
    env.update(extra)
    return env


class _Auth:
    token = "spn-token"
    workspace_url = "https://example.cloud.databricks.com"


@pytest.mark.asyncio
async def test_noop_when_not_databricks(key_dir):
    """No Databricks signal → skip entirely: no auth, no REST, no files."""
    with patch.dict(os.environ, {"DATABRICKS_CLIENT_ID": "cid", "DATABRICKS_CLIENT_SECRET": "c"}, clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock) as auth, \
         patch.object(prov, "_get_secret", new_callable=AsyncMock) as get_secret:
        await prov.provision_encryption_keys()

    auth.assert_not_awaited()
    get_secret.assert_not_awaited()
    assert not (key_dir / "private_key.pem").exists()


@pytest.mark.asyncio
async def test_noop_when_disabled(key_dir):
    """Disable flag forces filesystem mode even on Databricks."""
    with patch.dict(os.environ, _databricks_env(KASAL_ENCRYPTION_KEY_SCOPE_DISABLE="1"), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock) as auth:
        await prov.provision_encryption_keys()
    auth.assert_not_awaited()
    assert not (key_dir / "private_key.pem").exists()


@pytest.mark.asyncio
async def test_loads_existing_key_from_scope(key_dir):
    """Key present in the scope → materialize it, never write to the scope."""
    private_pem, _ = EncryptionUtils.generate_ssh_key_pair()

    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, return_value=private_pem.decode()) as get_secret, \
         patch.object(prov, "_put_secret", new_callable=AsyncMock) as put_secret, \
         patch.object(prov, "_ensure_scope", new_callable=AsyncMock) as ensure_scope:
        await prov.provision_encryption_keys()

    get_secret.assert_awaited_once()
    put_secret.assert_not_awaited()      # nothing generated → nothing stored
    ensure_scope.assert_not_awaited()
    # Materialized both files; private key round-trips (decryption would work).
    assert (key_dir / "private_key.pem").read_bytes() == private_pem
    assert (key_dir / "public_key.pem").exists()


@pytest.mark.asyncio
async def test_generates_and_stores_when_absent(key_dir):
    """Key absent → generate, ensure scope, store in scope, materialize locally."""
    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, return_value=None), \
         patch.object(prov, "_put_secret", new_callable=AsyncMock, return_value=True) as put_secret, \
         patch.object(prov, "_ensure_scope", new_callable=AsyncMock) as ensure_scope:
        await prov.provision_encryption_keys()

    ensure_scope.assert_awaited_once()
    put_secret.assert_awaited_once()
    # The stored value is a PEM private key, written to the fixed key name.
    _host, _tok, scope, key = put_secret.await_args.args[:4]
    stored_pem = put_secret.await_args.args[4]
    assert key == prov.PRIVATE_KEY_SECRET
    assert scope == prov.DEFAULT_SCOPE
    assert "PRIVATE KEY" in stored_pem
    # Materialized to disk so the cipher (and subprocesses) read the same key.
    assert (key_dir / "private_key.pem").exists()
    assert (key_dir / "public_key.pem").exists()


@pytest.mark.asyncio
async def test_adopts_running_key_when_scope_empty(key_dir):
    """Scope empty but this container already has a live key → ADOPT it (store
    the SAME key) rather than generate a fresh one, so secrets already encrypted
    this boot stay decryptable."""
    live_private, _ = EncryptionUtils.generate_ssh_key_pair()
    (key_dir / "private_key.pem").write_bytes(live_private)

    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, return_value=None), \
         patch.object(prov, "_put_secret", new_callable=AsyncMock, return_value=True) as put_secret, \
         patch.object(prov, "_ensure_scope", new_callable=AsyncMock):
        await prov.provision_encryption_keys()

    put_secret.assert_awaited_once()
    stored_pem = put_secret.await_args.args[4]
    # The EXISTING key was uploaded verbatim — not a newly generated one.
    assert stored_pem == live_private.decode("utf-8")
    # On-disk key is unchanged.
    assert (key_dir / "private_key.pem").read_bytes() == live_private


@pytest.mark.asyncio
async def test_ensure_scope_is_creator_only_no_broad_grant():
    """_ensure_scope must NOT send initial_manage_principal — otherwise every
    workspace user would get MANAGE (and read access to the encryption key).
    Omitting it makes the scope readable only by the creating service principal,
    which is what removes the manual grant step."""
    captured = {}

    class _Resp:
        status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        async def text(self): return ""

    class _Session:
        async def __aenter__(self): return self
        async def __aexit__(self, *a): return False
        def post(self, url, json=None, headers=None):
            captured["url"] = url
            captured["json"] = json
            return _Resp()

    with patch("aiohttp.ClientSession", return_value=_Session()):
        await prov._ensure_scope("https://host", "tok", "kasal")

    assert captured["url"].endswith("/api/2.0/secrets/scopes/create")
    assert captured["json"] == {"scope": "kasal"}
    assert "initial_manage_principal" not in captured["json"]


@pytest.mark.asyncio
async def test_uses_custom_scope_env(key_dir):
    with patch.dict(os.environ, _databricks_env(KASAL_ENCRYPTION_KEY_SCOPE="my-scope"), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, return_value=None) as get_secret, \
         patch.object(prov, "_put_secret", new_callable=AsyncMock, return_value=True), \
         patch.object(prov, "_ensure_scope", new_callable=AsyncMock):
        await prov.provision_encryption_keys()
    # Scope name threaded from the env override.
    assert get_secret.await_args.args[2] == "my-scope"


@pytest.mark.asyncio
async def test_store_failure_still_materializes_and_never_raises(key_dir):
    """If the scope write fails (e.g. no MANAGE), the run must not raise; the
    generated key is still materialized so this boot works (it just won't
    persist across redeploys — logged as a warning)."""
    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, return_value=None), \
         patch.object(prov, "_put_secret", new_callable=AsyncMock, return_value=False), \
         patch.object(prov, "_ensure_scope", new_callable=AsyncMock):
        await prov.provision_encryption_keys()  # must not raise
    assert (key_dir / "private_key.pem").exists()


@pytest.mark.asyncio
async def test_never_raises_on_rest_error(key_dir):
    """Any error during provisioning is swallowed (filesystem fallback)."""
    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=_Auth()), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock, side_effect=RuntimeError("boom")):
        await prov.provision_encryption_keys()  # must not raise
    # Nothing materialized (we never got past the read), and no exception.
    assert not (key_dir / "private_key.pem").exists()


@pytest.mark.asyncio
async def test_skips_when_auth_unavailable(key_dir):
    """SPN present in env but auth context can't resolve → skip, don't raise."""
    with patch.dict(os.environ, _databricks_env(), clear=True), \
         patch("src.utils.databricks_auth.get_auth_context", new_callable=AsyncMock, return_value=None), \
         patch.object(prov, "_get_secret", new_callable=AsyncMock) as get_secret:
        await prov.provision_encryption_keys()
    get_secret.assert_not_awaited()
    assert not (key_dir / "private_key.pem").exists()
