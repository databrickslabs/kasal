"""Persist installation encryption material without a user-supplied resource.

The app owns a private Databricks secret scope. Lakebase serializes first-time
initialization; key material is never written to application tables or volumes.
All SDK, crypto, and filesystem work runs off the server event loop.
"""

import asyncio
import base64
import hashlib
import json
import os
import tempfile
from pathlib import Path

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from sqlalchemy import text

from src.core.databricks_app import DatabricksAppInstallation
from src.utils.databricks_app_auth import get_app_client
from src.utils.encryption_utils import EncryptionUtils

_SECRET_KEY = "encryption-material-v1"


def _scope_name() -> str:
    installation = DatabricksAppInstallation.from_env()
    client_id = os.getenv("DATABRICKS_CLIENT_ID", "").strip()
    if not installation.hosted or not client_id:
        raise RuntimeError("App encryption requires the Databricks App identity")
    digest = hashlib.sha256(
        f"{installation.workspace_id}:{client_id}".encode()
    ).hexdigest()[:32]
    return f"kasal-app-{digest}"


def _read_secret(client, scope: str, key: str) -> str:
    result = client.secrets.get_secret(scope=scope, key=key)
    return base64.b64decode(result.value, validate=True).decode()


def _legacy_fernet_key(client) -> str:
    """Retain explicit or previously assigned keys during upgrade."""
    from databricks.sdk.errors import PermissionDenied, ResourceDoesNotExist

    explicit = os.getenv("ENCRYPTION_KEY", "").strip()
    if explicit:
        return explicit
    app = client.apps.get(name=DatabricksAppInstallation.from_env().app_name)
    for resource in app.resources or []:
        secret = getattr(resource, "secret", None)
        if resource.name == "encryption-key" and secret:
            # This was explicitly selected by the installer. A read failure is
            # not an absent key: propagate it rather than silently replacing it.
            return _read_secret(client, secret.scope, secret.key)
    try:
        return _read_secret(
            client,
            EncryptionUtils.ENCRYPTION_SCOPE,
            EncryptionUtils.ENCRYPTION_KEY_NAME,
        )
    except (ResourceDoesNotExist, PermissionDenied):
        # The old CLI used one workspace-wide scope, which a fresh app may not
        # have. Newly generated material belongs only to this app's scope.
        return ""


def _validate_material(raw: str) -> dict:
    material = json.loads(raw)
    if material.get("version") != 1:
        raise ValueError("Unsupported app encryption material version")
    Fernet(material["fernet"].encode())
    private = serialization.load_pem_private_key(
        material["private_key"].encode(), password=None
    )
    public = serialization.load_pem_public_key(material["public_key"].encode())
    if private.public_key().public_numbers() != public.public_numbers():
        raise ValueError("App encryption key pair does not match")
    return material


def _write_private_file(path: Path, content: str) -> None:
    # Atomic replacement avoids exposing a partial PEM to spawned workers.
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temp_path = Path(handle.name)
        try:
            os.chmod(temp_path, 0o600)
            handle.write(content.encode())
            handle.flush()
            os.fsync(handle.fileno())
        except BaseException:
            temp_path.unlink(missing_ok=True)
            raise
    try:
        temp_path.replace(path)
    finally:
        temp_path.unlink(missing_ok=True)


def _ensure_material(scope: str) -> None:
    from databricks.sdk.errors import ResourceAlreadyExists, ResourceDoesNotExist

    client = get_app_client()
    try:
        raw = _read_secret(client, scope, _SECRET_KEY)
    except ResourceDoesNotExist:
        # Prepare and validate before creating the scope so an invalid legacy
        # key cannot leave a newly created scope empty on every future restart.
        key = _legacy_fernet_key(client) or Fernet.generate_key().decode()
        Fernet(key.encode())
        # Persist existing local RSA keys too: legacy encrypt_value uses these
        # preferentially, so a stable Fernet key alone is insufficient.
        private, public = EncryptionUtils.get_or_create_ssh_keys()
        candidate = json.dumps(
            {
                "version": 1,
                "fernet": key,
                "private_key": private.decode(),
                "public_key": public.decode(),
            }
        )
        _validate_material(candidate)
        try:
            # Omitting initial_manage_principal grants MANAGE only to the app
            # principal creating the scope, never the workspace-wide users group.
            client.secrets.create_scope(scope=scope)
            created = True
        except ResourceAlreadyExists:
            created = False
        if not created:
            # A different deployment may have created the scope. Never overwrite
            # its key, including during a concurrent deployment to another DB.
            raw = _read_secret(client, scope, _SECRET_KEY)
        else:
            client.secrets.put_secret(
                scope=scope, key=_SECRET_KEY, string_value=candidate
            )
            # Read back from durable storage before allowing any encrypted writes.
            raw = _read_secret(client, scope, _SECRET_KEY)
    # Do not trust a same-named scope pre-created with access for other users.
    # Workspace admins inherently retain administrative access to secrets.
    from databricks.sdk.service.workspace import AclPermission

    principal = os.environ["DATABRICKS_CLIENT_ID"].strip()
    acls = list(client.secrets.list_acls(scope=scope))
    if any(acl.principal not in {principal, "admins"} for acl in acls) or not any(
        acl.principal == principal and acl.permission == AclPermission.MANAGE
        for acl in acls
    ):
        raise ValueError(
            "The app encryption scope must be private to its service principal"
        )
    material = _validate_material(raw)
    explicit = os.getenv("ENCRYPTION_KEY", "").strip()
    if explicit and explicit != material["fernet"]:
        raise ValueError("ENCRYPTION_KEY differs from the existing app encryption key")
    directory = EncryptionUtils.get_key_directory()
    _write_private_file(directory / "private_key.pem", material["private_key"])
    _write_private_file(directory / "public_key.pem", material["public_key"])
    # Subprocesses inherit this key and read the restored RSA files. No SDK call
    # or key generation is needed on their encrypt/decrypt paths.
    os.environ["ENCRYPTION_KEY"] = material["fernet"]
    EncryptionUtils._cached_key = material["fernet"].encode()


async def initialize_app_keys(connection) -> None:
    """Called during native Lakebase startup, before seeding or user requests."""
    scope = _scope_name()
    lock_id = int.from_bytes(
        hashlib.sha256(scope.encode()).digest()[:8], "big", signed=True
    )
    await connection.execute(
        text("SELECT pg_advisory_xact_lock(:key)"), {"key": lock_id}
    )
    try:
        await asyncio.to_thread(_ensure_material, scope)
    except Exception as exc:
        # SDK messages can contain request data; do not log secret-bearing errors.
        raise RuntimeError(
            "Kasal could not initialize its private encryption secret. The app "
            "service principal must be allowed to create and access its own "
            "Databricks secret scope. Existing keys were not replaced "
            f"({type(exc).__name__})."
        ) from None
