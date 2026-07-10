"""Startup provisioning of the app's encryption private key from a Databricks
secret scope.

WHY THIS EXISTS
---------------
Kasal encrypts stored secrets (API keys, MCP server keys, Databricks PATs, tool
configs) with an RSA private key that ``EncryptionUtils.get_or_create_ssh_keys``
keeps on the container filesystem (``~/.backendcrew/keys/``) and *regenerates
whenever the files are missing*. On Databricks Apps the container filesystem is
ephemeral, so every redeploy/restart mints a new key — while the ciphertext now
lives in Lakebase and persists. The mismatch makes every previously stored
secret undecryptable after a redeploy ("Error decrypting value with SSH key").

THE FIX
-------
On Databricks, source a STABLE private key from a Databricks secret scope and
materialize it onto the filesystem BEFORE any encryption runs (called at the top
of the app lifespan, before ``init_db`` and before any crew/flow subprocess is
spawned). The cipher and every encrypt/decrypt call site are unchanged — only
the *origin* of the key bytes moves from "regenerated on disk" to "the scope".
The crew/flow subprocess is spawned on the same container filesystem, so it
reads the same materialized files and never regenerates a divergent key.

On non-Databricks installs (or any failure) this is a no-op: the existing
filesystem behavior is left exactly as-is. It never raises — a failure only logs
an actionable warning and lets startup continue.
"""

import base64
import logging
import os
from typing import Optional

import aiohttp

from src.utils.encryption_utils import EncryptionUtils
from src.utils.telemetry import KasalProduct, get_user_agent_header

logger = logging.getLogger(__name__)

# Scope is overridable per-deployment; the key name within the scope is fixed.
DEFAULT_SCOPE = "kasal"
PRIVATE_KEY_SECRET = "kasal-encryption-private-key"


def _on_databricks() -> bool:
    """Request-independent 'deployed on Databricks' signal (matches config/logging.py)."""
    return bool(os.getenv("DATABRICKS_APP_NAME") or os.getenv("DATABRICKS_RUNTIME_VERSION"))


def _spn_creds_present() -> bool:
    return bool(os.getenv("DATABRICKS_CLIENT_ID") and os.getenv("DATABRICKS_CLIENT_SECRET"))


async def provision_encryption_keys() -> None:
    """Materialize the canonical RSA private key onto the filesystem at startup.

    No-op (and never raises) on non-Databricks installs or on any error, leaving
    the local filesystem key behavior unchanged.
    """
    try:
        if os.getenv("KASAL_ENCRYPTION_KEY_SCOPE_DISABLE"):
            logger.info("[encryption-key] skipped: KASAL_ENCRYPTION_KEY_SCOPE_DISABLE set")
            return
        if not _on_databricks() or not _spn_creds_present():
            logger.info(
                "[encryption-key] skipped: not a Databricks deployment — using local filesystem key"
            )
            return

        scope = os.getenv("KASAL_ENCRYPTION_KEY_SCOPE", DEFAULT_SCOPE)

        # SPN auth resolves from env at startup (no request, no DB) with
        # skip_db_auth=True; auth.workspace_url is normalized to https, no slash.
        from src.utils.databricks_auth import get_auth_context

        auth = await get_auth_context(skip_db_auth=True)
        if not auth or not getattr(auth, "token", None) or not getattr(auth, "workspace_url", None):
            logger.warning(
                "[encryption-key] skipped: could not resolve Databricks SPN auth at startup "
                "— using local filesystem key"
            )
            return
        host = auth.workspace_url
        token = auth.token

        existing_pem = await _get_secret(host, token, scope, PRIVATE_KEY_SECRET)
        if existing_pem:
            _materialize(existing_pem.encode("utf-8"))
            logger.info(f"[encryption-key] loaded private key from secret scope '{scope}'")
            return

        # Absent from the scope. Prefer ADOPTING the key this container is
        # already using (if any) over generating a fresh one — otherwise we'd
        # orphan every secret this container has already encrypted. Only
        # generate when there is no key anywhere.
        adopted_pem = _read_local_private_key()
        if adopted_pem:
            private_pem, action = adopted_pem, "adopted the running key into"
        else:
            private_pem, _ = EncryptionUtils.generate_ssh_key_pair()
            action = "generated a new key + stored it in"

        # Create the scope creator-only (the app SP is the creator → it alone
        # gets MANAGE). This is why no manual grant is needed, and it keeps the
        # encryption key unreadable by other workspace principals.
        await _ensure_scope(host, token, scope)
        stored = await _put_secret(host, token, scope, PRIVATE_KEY_SECRET, private_pem.decode("utf-8"))
        _materialize(private_pem)
        if stored:
            logger.info(f"[encryption-key] {action} secret scope '{scope}'")
        else:
            logger.warning(
                f"[encryption-key] FAILED to store the key in scope '{scope}': it will NOT persist "
                "across redeploys. Ensure the app service principal can create secret scopes (or "
                "pre-create the scope and grant the SP MANAGE)."
            )
    except Exception as e:  # noqa: BLE001 — startup must never be blocked by this
        logger.warning(
            f"[encryption-key] provisioning skipped due to error — using local filesystem key: {e}"
        )


def _read_local_private_key() -> Optional[bytes]:
    """Return the private key bytes this container is currently using, if any.

    Lets us adopt an already-live key into the scope instead of generating a
    fresh one (which would orphan secrets already encrypted this boot)."""
    try:
        path = EncryptionUtils.get_key_directory() / "private_key.pem"
        if path.exists() and path.stat().st_size > 0:
            return path.read_bytes()
    except Exception:  # noqa: BLE001 — best-effort; fall back to generating
        pass
    return None


def _materialize(private_pem: bytes) -> None:
    """Write the private key (+ derived public key) to the filesystem key dir,
    so the unchanged EncryptionUtils cipher — in this process and in spawned
    crew/flow subprocesses — reads the canonical key instead of regenerating one."""
    from cryptography.hazmat.backends import default_backend
    from cryptography.hazmat.primitives import serialization

    private_key = serialization.load_pem_private_key(
        private_pem, password=None, backend=default_backend()
    )
    public_pem = private_key.public_key().public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
    key_dir = EncryptionUtils.get_key_directory()
    private_path = key_dir / "private_key.pem"
    public_path = key_dir / "public_key.pem"
    private_path.write_bytes(private_pem)
    try:
        os.chmod(str(private_path), 0o600)
    except (OSError, TypeError):
        pass  # Best-effort on platforms where chmod may not apply
    public_path.write_bytes(public_pem)


# --- Databricks Secrets REST helpers (shapes mirror DatabricksSecretsService) ---

def _headers(token: str) -> dict:
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
        **get_user_agent_header(KasalProduct.SECRET),
    }


async def _get_secret(host: str, token: str, scope: str, key: str) -> Optional[str]:
    """Return the decoded secret value, or None if it does not exist / not readable."""
    url = f"{host}/api/2.0/secrets/get"
    data = {"scope": scope, "key": key}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data, headers=_headers(token)) as response:
            if response.status == 200:
                result = await response.json()
                encoded = result.get("value", "")
                if not encoded:
                    return None
                try:
                    return base64.b64decode(encoded).decode("utf-8")
                except Exception:  # noqa: BLE001 — value wasn't base64; use as-is
                    return encoded
            # 404 / RESOURCE_DOES_NOT_EXIST (or a permission error) → treat as absent.
            return None


async def _put_secret(host: str, token: str, scope: str, key: str, value: str) -> bool:
    url = f"{host}/api/2.0/secrets/put"
    data = {"scope": scope, "key": key, "string_value": value}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data, headers=_headers(token)) as response:
            if response.status == 200:
                return True
            error_text = await response.text()
            logger.warning(f"[encryption-key] secrets/put failed ({response.status}): {error_text}")
            return False


async def _ensure_scope(host: str, token: str, scope: str) -> None:
    # NOTE: deliberately NO ``initial_manage_principal`` — that would grant every
    # workspace user MANAGE (and thus read access to the encryption private key).
    # Omitting it means only the creator (this app's service principal) gets
    # MANAGE, so the key stays app-private and no manual grant is required.
    url = f"{host}/api/2.0/secrets/scopes/create"
    data = {"scope": scope}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=data, headers=_headers(token)) as response:
            if response.status == 200:
                return
            error_text = await response.text()
            if "already exists" in error_text or "RESOURCE_ALREADY_EXISTS" in error_text:
                return
            logger.warning(f"[encryption-key] scopes/create failed ({response.status}): {error_text}")
