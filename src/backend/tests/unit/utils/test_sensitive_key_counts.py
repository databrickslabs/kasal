"""A count or a limit of tokens is not a credential, for masking.

``is_sensitive_key`` is the masking rule: the run-input masker, the API maskers,
logs and the YAML export. It must keep catching every credential-shaped key,
plurals included, and must stop treating ``max_tokens`` and its kin as one: a
string such as ``"4000"`` under them is configuration, not a secret.

Encryption at rest (``encrypts_at_rest``) stays broad, so those values are
still encrypted in the database.
"""

from unittest.mock import patch

import pytest

from src.services.execution.stored_inputs import mask_run_inputs
from src.utils.sensitive_data_utils import (
    REDACTED_PLACEHOLDER,
    decrypt_sensitive_fields,
    encrypt_sensitive_fields,
    encrypts_at_rest,
    is_sensitive_key,
    mask_sensitive_strings,
)

SECRETS = [
    "token",
    "TOKEN",
    "tokens",
    "api_key",
    "apiKey",
    "api_keys",
    "api_tokens",
    "github_tokens",
    "bot_tokens",
    "user_tokens",
    "accessTokens",
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
    "llm_token",
    "token_after_refresh",
    "before_token",
    "input_token",
    "cache_token",
    "client_secret",
    "powerbi_client_secret",
    "password",
    "db_password",
    "private_key",
    "credentials",
    "bearer",
]

#: Every count or limit of tokens quoted in src/backend/src, plus a few other
#: common spellings.
COUNTS_AND_LIMITS = [
    "budget_tokens",
    "cache_creation_input_tokens",
    "cache_creation_tokens",
    "cache_read_input_tokens",
    "cache_read_tokens",
    "cached_prompt_tokens",
    "cached_tokens",
    "completion_tokens",
    "input_tokens",
    "max_completion_tokens",
    "max_output_tokens",
    "max_tokens",
    "MAX_TOKENS",
    "maxTokens",
    "num_tokens",
    "output_token_cap",
    "output_tokens",
    "prompt_tokens",
    "prompt_tokens_details",
    "reasoning_tokens",
    "thinking_budget_tokens",
    "thinking_tokens",
    "token_budget",
    "token_count",
    "token_limit",
    "token_threshold",
    "token_usage",
    "tokens_after",
    "tokens_before",
    "tokens_count",
    "tokens_used",
    "total_tokens",
    "tokenizer",
]


@pytest.mark.parametrize("key", SECRETS)
def test_credential_keys_are_sensitive(key):
    assert is_sensitive_key(key) is True
    assert encrypts_at_rest(key) is True


@pytest.mark.parametrize("key", COUNTS_AND_LIMITS)
def test_counts_and_limits_are_not_masked(key):
    assert is_sensitive_key(key) is False


@pytest.mark.parametrize("key", COUNTS_AND_LIMITS)
def test_counts_and_limits_are_still_encrypted_at_rest(key):
    assert encrypts_at_rest(key) is True


def test_the_run_input_masker_keeps_a_string_count_and_masks_the_secrets():
    inputs = {
        "inputs": {
            "max_tokens": "4000",
            "num_tokens": "12",
            "api_key": "sk-value",
            "access_token": "at-value",
            "client_secret": "cs-value",
            "api_tokens": "tk-value",
        }
    }
    masked = mask_run_inputs(inputs)["inputs"]
    assert masked["max_tokens"] == "4000"
    assert masked["num_tokens"] == "12"
    assert masked["api_key"] == REDACTED_PLACEHOLDER
    assert masked["access_token"] == REDACTED_PLACEHOLDER
    assert masked["client_secret"] == REDACTED_PLACEHOLDER
    assert masked["api_tokens"] == REDACTED_PLACEHOLDER


def test_the_api_masker_keeps_a_string_count():
    shown = mask_sensitive_strings({"max_tokens": "4000", "token": "t-value"})
    assert shown == {"max_tokens": "4000", "token": REDACTED_PLACEHOLDER}


def _encryption():
    fake = patch("src.utils.sensitive_data_utils.EncryptionUtils")
    mock = fake.start()
    mock.encrypt_value.side_effect = lambda v: "x" + v
    mock.decrypt_value.side_effect = lambda v: v[1:]
    return fake


def test_a_string_count_is_encrypted_at_rest_and_reads_back():
    fake = _encryption()
    try:
        sealed = encrypt_sensitive_fields({"max_tokens": "4000", "token": "t"})
        opened = decrypt_sensitive_fields(sealed)
    finally:
        fake.stop()
    assert sealed == {"max_tokens": "ENC:x4000", "token": "ENC:xt"}
    assert opened == {"max_tokens": "4000", "token": "t"}


def test_an_encrypted_count_read_and_saved_again_stays_encrypted():
    fake = _encryption()
    try:
        opened = decrypt_sensitive_fields({"max_tokens": "ENC:x4000"})
        resealed = encrypt_sensitive_fields(opened)
    finally:
        fake.stop()
    assert opened == {"max_tokens": "4000"}
    assert resealed == {"max_tokens": "ENC:x4000"}
