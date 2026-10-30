"""The persisted app key material must be a matching RSA pair."""

import json

import pytest
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec, rsa

from src.utils.databricks_app_keys import _validate_material


def _pem_pair(private_key):
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return private_pem, public_pem


def _material(private_pem: str, public_pem: str) -> str:
    return json.dumps(
        {
            "version": 1,
            "fernet": Fernet.generate_key().decode(),
            "private_key": private_pem,
            "public_key": public_pem,
        }
    )


def _rsa():
    return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def test_matching_rsa_pair_is_accepted():
    private_pem, public_pem = _pem_pair(_rsa())
    material = _validate_material(_material(private_pem, public_pem))
    assert material["private_key"] == private_pem


def test_mismatched_rsa_pair_is_rejected():
    private_pem, _ = _pem_pair(_rsa())
    _, other_public_pem = _pem_pair(_rsa())
    with pytest.raises(ValueError, match="does not match"):
        _validate_material(_material(private_pem, other_public_pem))


def test_non_rsa_pair_is_rejected():
    private_pem, public_pem = _pem_pair(ec.generate_private_key(ec.SECP256R1()))
    with pytest.raises(ValueError, match="must be RSA"):
        _validate_material(_material(private_pem, public_pem))


def test_unsupported_version_is_rejected():
    private_pem, public_pem = _pem_pair(_rsa())
    raw = json.loads(_material(private_pem, public_pem))
    raw["version"] = 2
    with pytest.raises(ValueError, match="version"):
        _validate_material(json.dumps(raw))
