"""Restoring the app's key material resets the parsed-key cache."""

import base64
import json
from types import SimpleNamespace
from unittest.mock import patch

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization

from src.utils import databricks_app_keys
from src.utils.encryption_utils import EncryptionUtils


def _material():
    private, public = EncryptionUtils.generate_ssh_key_pair()
    return {
        "version": 1,
        "fernet": Fernet.generate_key().decode(),
        "private_key": private.decode(),
        "public_key": public.decode(),
    }


def _client(material, principal):
    from databricks.sdk.service.workspace import AclPermission

    raw = base64.b64encode(json.dumps(material).encode()).decode()
    secrets = SimpleNamespace(
        get_secret=lambda scope, key: SimpleNamespace(value=raw),
        list_acls=lambda scope: [
            SimpleNamespace(principal=principal, permission=AclPermission.MANAGE)
        ],
    )
    return SimpleNamespace(secrets=secrets)


def test_restored_keys_replace_the_ones_parsed_before(tmp_path, monkeypatch):
    # This test rewrites the key files: give it its own, not the session's.
    monkeypatch.setattr(EncryptionUtils, "get_key_directory", lambda: tmp_path)
    # A key pair is in use (and parsed) before startup restores the stored one.
    sealed_before = EncryptionUtils.encrypt_value("before")
    assert EncryptionUtils.decrypt_value(sealed_before) == "before"

    material = _material()
    monkeypatch.setenv("DATABRICKS_CLIENT_ID", "app-sp")
    # _ensure_material exports the key and caches it; undo both afterwards.
    monkeypatch.setenv("ENCRYPTION_KEY", material["fernet"])
    monkeypatch.setattr(EncryptionUtils, "_cached_key", None)
    with (
        patch.object(
            databricks_app_keys,
            "get_app_client",
            return_value=_client(material, "app-sp"),
        ),
        patch.object(
            EncryptionUtils,
            "reset_key_cache",
            wraps=EncryptionUtils.reset_key_cache,
        ) as reset,
    ):
        databricks_app_keys._ensure_material("kasal-app-test")
    reset.assert_called_once()

    # Everything from here on uses the restored pair.
    restored = serialization.load_pem_private_key(
        material["private_key"].encode(), password=None
    )
    parsed = EncryptionUtils._private_key()
    assert parsed.private_numbers() == restored.private_numbers()
