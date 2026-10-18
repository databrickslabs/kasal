"""A count or a limit of tokens is not a credential.

``is_sensitive_key`` is the one rule the run-input masker, the API maskers, the
YAML export and the SensitiveJSON column share. It must keep catching every
credential-shaped key, and must stop treating ``max_tokens`` and its kin as one:
a string such as ``"4000"`` under them is configuration, not a secret.
"""

from unittest.mock import patch

import pytest

from src.services.execution.stored_inputs import mask_run_inputs
from src.utils.sensitive_data_utils import (
    REDACTED_PLACEHOLDER,
    decrypt_sensitive_fields,
    encrypt_sensitive_fields,
    is_sensitive_key,
    mask_sensitive_strings,
)

SECRETS = [
    "token",
    "TOKEN",
    "api_key",
    "apiKey",
    "access_token",
    "accessToken",
    "x-forwarded-access-token",
    "refresh_token",
    "refresh_tokens",
    "auth_token",
    "authtoken",
    "user_token",
    "databricks_token",
    "DATABRICKS_TOKEN",
    "slack_token",
    "client_secret",
    "powerbi_client_secret",
    "password",
    "db_password",
    "private_key",
    "credentials",
    "bearer",
]

COUNTS_AND_LIMITS = [
    "max_tokens",
    "MAX_TOKENS",
    "maxTokens",
    "num_tokens",
    "max_output_tokens",
    "max_completion_tokens",
    "thinking_budget_tokens",
    "budget_tokens",
    "prompt_tokens",
    "completion_tokens",
    "total_tokens",
    "token_count",
    "token_limit",
    "token_budget",
    "output_token_cap",
    "tokenizer",
]


@pytest.mark.parametrize("key", SECRETS)
def test_credential_keys_are_sensitive(key):
    assert is_sensitive_key(key) is True


@pytest.mark.parametrize("key", COUNTS_AND_LIMITS)
def test_counts_and_limits_are_not_sensitive(key):
    assert is_sensitive_key(key) is False


def test_the_run_input_masker_keeps_a_string_count_and_masks_the_secrets():
    inputs = {
        "inputs": {
            "max_tokens": "4000",
            "num_tokens": "12",
            "api_key": "sk-value",
            "access_token": "at-value",
            "client_secret": "cs-value",
        }
    }
    masked = mask_run_inputs(inputs)["inputs"]
    assert masked["max_tokens"] == "4000"
    assert masked["num_tokens"] == "12"
    assert masked["api_key"] == REDACTED_PLACEHOLDER
    assert masked["access_token"] == REDACTED_PLACEHOLDER
    assert masked["client_secret"] == REDACTED_PLACEHOLDER


def test_the_api_masker_keeps_a_string_count():
    shown = mask_sensitive_strings({"max_tokens": "4000", "token": "t-value"})
    assert shown == {"max_tokens": "4000", "token": REDACTED_PLACEHOLDER}


def _encryption():
    fake = patch("src.utils.sensitive_data_utils.EncryptionUtils")
    mock = fake.start()
    mock.encrypt_value.side_effect = lambda v: "x" + v
    mock.decrypt_value.side_effect = lambda v: v[1:]
    return fake


def test_a_string_count_is_stored_as_plaintext():
    fake = _encryption()
    try:
        sealed = encrypt_sensitive_fields({"max_tokens": "4000", "token": "t"})
    finally:
        fake.stop()
    assert sealed == {"max_tokens": "4000", "token": "ENC:xt"}


def test_a_count_encrypted_under_the_old_rule_still_decrypts():
    """Rows written before the rule narrowed hold ciphertext under these keys.

    A guard for the change, not a proof of the defect: reading must keep
    opening them, or a tool would get the ciphertext as its setting.
    """
    fake = _encryption()
    try:
        opened = decrypt_sensitive_fields({"max_tokens": "ENC:x4000"})
    finally:
        fake.stop()
    assert opened == {"max_tokens": "4000"}
