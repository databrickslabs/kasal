"""
Unit tests for sensitive_data_utils module.

Tests encryption, decryption, masking, and sensitivity detection utilities
for tool configurations and log sanitization.
"""

import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_encryption_utils_mock():
    """Return a mock for EncryptionUtils with deterministic behaviour."""
    mock = MagicMock()
    mock.encrypt_value.side_effect = lambda v: "encrypted_" + v
    mock.decrypt_value.side_effect = lambda v: v.replace("encrypted_", "")
    return mock


# ---------------------------------------------------------------------------
# is_sensitive_key
# ---------------------------------------------------------------------------

class TestIsSensitiveKey:
    """Tests for is_sensitive_key()."""

    def test_exact_match_client_secret(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("client_secret") is True

    def test_exact_match_databricks_token(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("databricks_token") is True

    def test_exact_match_openai_api_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("openai_api_key") is True

    def test_exact_match_anthropic_api_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("anthropic_api_key") is True

    def test_exact_match_powerbi_client_secret(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("powerbi_client_secret") is True

    def test_pattern_match_secret_in_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("my_secret") is True

    def test_pattern_match_password_in_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("db_password") is True

    def test_pattern_match_token_in_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("access_token") is True

    def test_pattern_match_api_key_substring(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("some_api_key_here") is True

    def test_pattern_match_apikey_substring(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("myapikey") is True

    def test_pattern_match_credential(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("db_credential") is True

    def test_pattern_match_bearer(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("bearer_value") is True

    def test_case_insensitive_SECRET(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("MY_SECRET_KEY") is True

    def test_case_insensitive_PASSWORD(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("PASSWORD") is True

    def test_non_sensitive_key_name(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("workspace_name") is False

    def test_non_sensitive_url(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("base_url") is False

    def test_non_sensitive_description(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("description") is False

    def test_pattern_match_private_key(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("rsa_private_key") is True

    def test_pattern_match_refresh_token(self):
        from src.utils.sensitive_data_utils import is_sensitive_key
        assert is_sensitive_key("user_refresh_token") is True


# ---------------------------------------------------------------------------
# is_encrypted
# ---------------------------------------------------------------------------

class TestIsEncrypted:
    """Tests for is_encrypted()."""

    def test_returns_true_for_enc_prefix(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted("ENC:somedata") is True

    def test_returns_false_for_plain_string(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted("plaintext") is False

    def test_returns_false_for_empty_string(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted("") is False

    def test_returns_false_for_non_string(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted(12345) is False  # type: ignore[arg-type]

    def test_returns_false_for_none(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted(None) is False  # type: ignore[arg-type]

    def test_case_sensitive_prefix(self):
        from src.utils.sensitive_data_utils import is_encrypted
        # prefix must be uppercase "ENC:"
        assert is_encrypted("enc:data") is False

    def test_enc_prefix_only_no_data(self):
        from src.utils.sensitive_data_utils import is_encrypted
        assert is_encrypted("ENC:") is True


# ---------------------------------------------------------------------------
# encrypt_value
# ---------------------------------------------------------------------------

class TestEncryptValue:
    """Tests for encrypt_value()."""

    def test_encrypts_plain_value(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            # Reload to pick up patch
            result = sensitive_data_utils.encrypt_value("mysecret")
        assert result == "ENC:encrypted_mysecret"
        mock_eu.encrypt_value.assert_called_once_with("mysecret")

    def test_skips_already_encrypted_value(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_value("ENC:already_encrypted")
        assert result == "ENC:already_encrypted"
        mock_eu.encrypt_value.assert_not_called()

    def test_returns_empty_string_unchanged(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_value("")
        assert result == ""
        mock_eu.encrypt_value.assert_not_called()

    def test_raises_on_encryption_error(self):
        mock_eu = _make_encryption_utils_mock()
        mock_eu.encrypt_value.side_effect = RuntimeError("crypto failure")
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            with pytest.raises(RuntimeError, match="crypto failure"):
                sensitive_data_utils.encrypt_value("value")

    def test_result_starts_with_enc_prefix(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_value("anyvalue")
        assert result.startswith("ENC:")


# ---------------------------------------------------------------------------
# decrypt_value
# ---------------------------------------------------------------------------

class TestDecryptValue:
    """Tests for decrypt_value()."""

    def test_decrypts_enc_prefixed_value(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_value("ENC:encrypted_mysecret")
        assert result == "mysecret"

    def test_returns_plain_value_as_is(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_value("plaintext")
        assert result == "plaintext"
        mock_eu.decrypt_value.assert_not_called()

    def test_returns_empty_string_unchanged(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_value("")
        assert result == ""

    def test_returns_empty_string_on_decryption_failure(self):
        mock_eu = _make_encryption_utils_mock()
        mock_eu.decrypt_value.side_effect = Exception("bad key")
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_value("ENC:corrupted")
        assert result == ""

    def test_strips_enc_prefix_before_calling_decrypt(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            sensitive_data_utils.decrypt_value("ENC:payload")
        mock_eu.decrypt_value.assert_called_once_with("payload")


# ---------------------------------------------------------------------------
# encrypt_sensitive_fields
# ---------------------------------------------------------------------------

class TestEncryptSensitiveFields:
    """Tests for encrypt_sensitive_fields()."""

    def test_encrypts_known_sensitive_field(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_sensitive_fields({"api_key": "mykey"})
        assert result["api_key"] == "ENC:encrypted_mykey"

    def test_leaves_non_sensitive_field_unchanged(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_sensitive_fields({"name": "agent1"})
        assert result["name"] == "agent1"

    def test_skips_already_encrypted_field(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_sensitive_fields({"token": "ENC:existing"})
        assert result["token"] == "ENC:existing"
        mock_eu.encrypt_value.assert_not_called()

    def test_recursive_nested_dict(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            data = {"config": {"password": "secret123", "host": "localhost"}}
            result = sensitive_data_utils.encrypt_sensitive_fields(data)
        assert result["config"]["password"] == "ENC:encrypted_secret123"
        assert result["config"]["host"] == "localhost"

    def test_non_recursive_skips_nested_dict(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            data = {"config": {"password": "secret123"}}
            result = sensitive_data_utils.encrypt_sensitive_fields(data, recursive=False)
        # nested dict not processed - returned as-is
        assert result["config"] == {"password": "secret123"}

    def test_returns_empty_dict_unchanged(self):
        from src.utils.sensitive_data_utils import encrypt_sensitive_fields
        assert encrypt_sensitive_fields({}) == {}

    def test_returns_none_unchanged(self):
        from src.utils.sensitive_data_utils import encrypt_sensitive_fields
        assert encrypt_sensitive_fields(None) is None  # type: ignore[arg-type]

    def test_skips_empty_string_value(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.encrypt_sensitive_fields({"password": ""})
        # empty string is falsy, so it should be left unchanged
        assert result["password"] == ""
        mock_eu.encrypt_value.assert_not_called()

    def test_returns_original_on_encrypt_failure(self):
        mock_eu = _make_encryption_utils_mock()
        mock_eu.encrypt_value.side_effect = RuntimeError("fail")
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            # Should not raise; falls back to original value on error
            result = sensitive_data_utils.encrypt_sensitive_fields({"api_key": "mykey"})
        assert result["api_key"] == "mykey"


# ---------------------------------------------------------------------------
# decrypt_sensitive_fields
# ---------------------------------------------------------------------------

class TestDecryptSensitiveFields:
    """Tests for decrypt_sensitive_fields()."""

    def test_decrypts_enc_prefixed_value(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_sensitive_fields(
                {"token": "ENC:encrypted_mytoken"}
            )
        assert result["token"] == "mytoken"

    def test_leaves_non_encrypted_value_unchanged(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_sensitive_fields(
                {"token": "plaintoken", "name": "agent"}
            )
        assert result["token"] == "plaintoken"
        assert result["name"] == "agent"

    def test_recursive_nested_dict(self):
        mock_eu = _make_encryption_utils_mock()
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            data = {"config": {"password": "ENC:encrypted_pw", "host": "db.example.com"}}
            result = sensitive_data_utils.decrypt_sensitive_fields(data)
        assert result["config"]["password"] == "pw"
        assert result["config"]["host"] == "db.example.com"

    def test_returns_empty_string_on_decrypt_error(self):
        mock_eu = _make_encryption_utils_mock()
        mock_eu.decrypt_value.side_effect = Exception("bad")
        with patch("src.utils.sensitive_data_utils.EncryptionUtils", mock_eu):
            from src.utils import sensitive_data_utils
            result = sensitive_data_utils.decrypt_sensitive_fields({"secret": "ENC:bad_data"})
        assert result["secret"] == ""

    def test_returns_none_unchanged(self):
        from src.utils.sensitive_data_utils import decrypt_sensitive_fields
        assert decrypt_sensitive_fields(None) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# mask_sensitive_fields
# ---------------------------------------------------------------------------

class TestMaskSensitiveFields:
    """Tests for mask_sensitive_fields()."""

    def test_masks_sensitive_key_value(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        result = mask_sensitive_fields({"password": "hunter2", "name": "alice"})
        assert result["password"] == REDACTED_PLACEHOLDER
        assert result["name"] == "alice"

    def test_leaves_empty_sensitive_value_unmasked(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        # falsy values should not be replaced
        result = mask_sensitive_fields({"password": ""})
        assert result["password"] == ""

    def test_recursive_nested_dict(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        data = {"db": {"password": "secret", "host": "localhost"}}
        result = mask_sensitive_fields(data)
        assert result["db"]["password"] == REDACTED_PLACEHOLDER
        assert result["db"]["host"] == "localhost"

    def test_processes_list_of_dicts(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        data = {"tools": [{"api_key": "k1"}, {"name": "tool2"}]}
        result = mask_sensitive_fields(data)
        assert result["tools"][0]["api_key"] == REDACTED_PLACEHOLDER
        assert result["tools"][1]["name"] == "tool2"

    def test_list_of_non_dicts_unchanged(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields
        data = {"items": [1, 2, 3]}
        result = mask_sensitive_fields(data)
        assert result["items"] == [1, 2, 3]

    def test_returns_none_unchanged(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields
        assert mask_sensitive_fields(None) is None  # type: ignore[arg-type]

    def test_non_recursive_skips_nested(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        data = {"config": {"password": "pw"}}
        result = mask_sensitive_fields(data, recursive=False)
        # Without recursion, nested dicts are returned as-is
        assert result["config"] == {"password": "pw"}

    def test_masks_token_field(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields, REDACTED_PLACEHOLDER
        result = mask_sensitive_fields({"auth_token": "abc123"})
        assert result["auth_token"] == REDACTED_PLACEHOLDER

    def test_non_sensitive_values_preserved(self):
        from src.utils.sensitive_data_utils import mask_sensitive_fields
        data = {"url": "https://example.com", "timeout": 30}
        result = mask_sensitive_fields(data)
        assert result == data


# ---------------------------------------------------------------------------
# mask_sensitive_string
# ---------------------------------------------------------------------------

class TestMaskSensitiveString:
    """Tests for mask_sensitive_string()."""

    def test_masks_bearer_token(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig"  # nosec - mock token for testing
        result = mask_sensitive_string(text)
        assert "Bearer ***REDACTED***" in result
        assert "eyJhbGciOiJIUzI1NiJ9" not in result  # nosec - mock token for testing

    def test_masks_bearer_token_case_insensitive(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "BEARER mytoken1234"
        result = mask_sensitive_string(text)
        assert "***REDACTED***" in result

    def test_masks_password_equals(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "password=supersecret123"
        result = mask_sensitive_string(text)
        assert "password=***REDACTED***" in result
        assert "supersecret123" not in result

    def test_masks_secret_colon(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "secret:myverysecretvalue"
        result = mask_sensitive_string(text)
        assert "***REDACTED***" in result

    def test_masks_32_plus_char_alphanumeric_string(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        long_key = "A" * 32
        text = f"key={long_key}"
        result = mask_sensitive_string(text)
        assert "***REDACTED***" in result

    def test_does_not_mask_short_alphanumeric_string(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "ref=abc123"  # 6 chars, below 32 threshold
        result = mask_sensitive_string(text)
        assert "abc123" in result

    def test_returns_non_string_unchanged(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        assert mask_sensitive_string(None) is None  # type: ignore[arg-type]

    def test_returns_empty_string_unchanged(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        assert mask_sensitive_string("") == ""

    def test_plain_text_not_modified(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "Hello, this is a normal log message."
        result = mask_sensitive_string(text)
        assert result == text

    def test_masks_api_key_pattern(self):
        from src.utils.sensitive_data_utils import mask_sensitive_string
        text = "api_key=abcdefghijklmnopqrstuvwxyz123456"
        result = mask_sensitive_string(text)
        assert "***REDACTED***" in result


# ---------------------------------------------------------------------------
# safe_log_tool_configs
# ---------------------------------------------------------------------------

class TestSafeLogToolConfigs:
    """Tests for safe_log_tool_configs()."""

    def test_returns_none_message_for_none_input(self):
        from src.utils.sensitive_data_utils import safe_log_tool_configs
        result = safe_log_tool_configs(None)
        assert result == "tool_configs: None"

    def test_returns_none_message_for_empty_dict(self):
        from src.utils.sensitive_data_utils import safe_log_tool_configs
        result = safe_log_tool_configs({})
        assert "tool_configs: None" in result

    def test_masks_sensitive_fields_in_output(self):
        from src.utils.sensitive_data_utils import safe_log_tool_configs, REDACTED_PLACEHOLDER
        configs = {"api_key": "secret123", "workspace": "my_ws"}
        result = safe_log_tool_configs(configs)
        assert REDACTED_PLACEHOLDER in result
        assert "secret123" not in result
        assert "my_ws" in result

    def test_prefix_applied(self):
        from src.utils.sensitive_data_utils import safe_log_tool_configs
        configs = {"name": "tool1"}
        result = safe_log_tool_configs(configs, prefix="[DEBUG] ")
        assert result.startswith("[DEBUG] ")

    def test_non_sensitive_fields_visible(self):
        from src.utils.sensitive_data_utils import safe_log_tool_configs
        configs = {"workspace": "ws1", "timeout": 30}
        result = safe_log_tool_configs(configs)
        assert "ws1" in result


class TestMaskSensitiveHeaders:
    """P1 (H9): full header dicts must never be logged with credentials intact."""

    def test_redacts_credential_headers(self):
        from src.utils.sensitive_data_utils import (
            mask_sensitive_headers,
            REDACTED_PLACEHOLDER,
        )
        headers = {
            "Authorization": "Bearer dapiSECRET",
            "X-Forwarded-Access-Token": "tok-123",
            "X-Auth-Request-Access-Token": "tok-456",
            "Cookie": "session=abc",
            "Content-Type": "application/json",
            "User-Agent": "Kasal/1.0",
        }
        masked = mask_sensitive_headers(headers)
        assert masked["Authorization"] == REDACTED_PLACEHOLDER
        assert masked["X-Forwarded-Access-Token"] == REDACTED_PLACEHOLDER
        assert masked["X-Auth-Request-Access-Token"] == REDACTED_PLACEHOLDER
        assert masked["Cookie"] == REDACTED_PLACEHOLDER
        # Non-sensitive headers are preserved verbatim.
        assert masked["Content-Type"] == "application/json"
        assert masked["User-Agent"] == "Kasal/1.0"

    def test_non_dict_input_returns_empty(self):
        from src.utils.sensitive_data_utils import mask_sensitive_headers
        assert mask_sensitive_headers(None) == {}
        assert mask_sensitive_headers("not-a-dict") == {}
