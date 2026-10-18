"""
Sensitive data utilities module.

This module provides utilities for handling sensitive data in tool configurations,
including encryption, decryption, and masking for logs and API responses.
"""

import logging
import re
import threading
from collections import OrderedDict
from typing import Any, Dict, List, Optional, Set

from src.utils.encryption_utils import EncryptionUtils

# Initialize logger
logger = logging.getLogger(__name__)

# Sensitive key patterns - any key containing these substrings will be treated as
# sensitive. A bare "token" is judged by words instead (see _names_a_token).
SENSITIVE_KEY_PATTERNS: Set[str] = {
    "secret",
    "password",
    "api_key",
    "apikey",
    "credential",
    "private_key",
    "privatekey",
    "auth_token",
    "access_token",
    "refresh_token",
    "bearer",
}

#: Words that make any key mentioning tokens a count or a limit of them, not a
#: credential: ``max_tokens``, ``token_count``, ``output_token_cap``.
TOKEN_LIMIT_WORDS: Set[str] = {
    "max",
    "min",
    "num",
    "count",
    "limit",
    "budget",
    "total",
    "usage",
    "cap",
    "threshold",
}

#: LLM-usage words that make a key a count only beside the PLURAL "tokens":
#: ``prompt_tokens`` and ``tokens_before`` are counts, while ``input_token``
#: and ``token_after_refresh`` still name a credential.
TOKEN_USAGE_WORDS: Set[str] = {
    "used",
    "prompt",
    "completion",
    "input",
    "output",
    "cache",
    "cached",
    "thinking",
    "reasoning",
    "before",
    "after",
}

# One word of a key: an ALLCAPS run, a Capitalised or lowercase word, or digits.
_KEY_WORD = re.compile(r"[A-Z]+(?![a-z])|[A-Z]?[a-z]+|\d+")

# Exact key matches that should always be treated as sensitive
SENSITIVE_EXACT_KEYS: Set[str] = {
    "powerbi_client_secret",
    "client_secret",
    "databricks_token",
    "openai_api_key",
    "anthropic_api_key",
}

# Prefix used to identify encrypted values
ENCRYPTED_PREFIX = "ENC:"

# Redacted placeholder for masked values
REDACTED_PLACEHOLDER = "***REDACTED***"

# HTTP header names whose VALUES carry credentials and must never be logged.
# Matched as case-insensitive substrings so e.g. "X-Forwarded-Access-Token"
# and "X-Auth-Request-Access-Token" are caught by "token".
SENSITIVE_HEADER_PATTERNS: Set[str] = {
    "authorization",
    "cookie",
    "token",
    "api-key",
    "apikey",
    "secret",
    "x-databricks",
}


def _key_words(key: str) -> List[str]:
    """``key`` as lowercase words: ``maxTokens`` and ``MAX_TOKENS`` -> max, tokens."""
    return [word.lower() for word in _KEY_WORD.findall(key)]


def _names_a_token(key: str) -> bool:
    """Whether ``key`` names token credentials rather than counting tokens.

    A word ending in "token" or "tokens" names them (``token``, ``user_token``,
    ``accessToken``, ``authtoken``, ``api_tokens``). The key counts them
    instead when it also has a limit word (:data:`TOKEN_LIMIT_WORDS`:
    ``max_tokens``, ``token_limit``), or the plural "tokens" with a usage word
    (:data:`TOKEN_USAGE_WORDS`: ``prompt_tokens``, ``tokens_before``).
    ``tokenizer`` names neither.
    """
    words = _key_words(key)
    plural = any(word.endswith("tokens") for word in words)
    if not plural and not any(word.endswith("token") for word in words):
        return False
    if any(word in TOKEN_LIMIT_WORDS for word in words):
        return False
    return not (plural and any(word in TOKEN_USAGE_WORDS for word in words))


def is_sensitive_key(key: str) -> bool:
    """
    Check if a key name indicates sensitive data.

    The rule for MASKING: API responses, run records, logs, display and
    export. A count or a limit of tokens (``max_tokens``, ``prompt_tokens``)
    is not sensitive, so it stays visible. Encryption at rest uses the broader
    ``encrypts_at_rest``, which also covers those keys.

    Args:
        key: The key name to check

    Returns:
        True if the key appears to contain sensitive data
    """
    key_lower = key.lower()

    # Check exact matches first
    if key_lower in SENSITIVE_EXACT_KEYS:
        return True

    # Check pattern matches
    for pattern in SENSITIVE_KEY_PATTERNS:
        if pattern in key_lower:
            return True

    return _names_a_token(key)


def encrypts_at_rest(key: str) -> bool:
    """Whether a string under ``key`` is encrypted at rest (SensitiveJSON).

    The rule for ENCRYPTION, on write and on read alike: every key
    ``is_sensitive_key`` names, plus any key containing "token". Broader than
    the masking rule on purpose, so a value encrypted before masking learned to
    tell a count from a credential stays encrypted when its row is saved again.
    """
    return is_sensitive_key(key) or "token" in key.lower()


def is_encrypted(value: str) -> bool:
    """
    Check if a value is already encrypted (has our encryption prefix).

    Args:
        value: The value to check

    Returns:
        True if the value appears to be encrypted
    """
    if not isinstance(value, str):
        return False
    return value.startswith(ENCRYPTED_PREFIX)


def encrypt_value(value: str) -> str:
    """
    Encrypt a sensitive value and add the encryption prefix.

    Args:
        value: The plain text value to encrypt

    Returns:
        The encrypted value with prefix
    """
    if not value or is_encrypted(value):
        return value

    try:
        encrypted = EncryptionUtils.encrypt_value(value)
        return f"{ENCRYPTED_PREFIX}{encrypted}"
    except Exception as e:
        logger.error(f"Failed to encrypt sensitive value: {e}")
        raise


def decrypt_value(encrypted_value: str) -> str:
    """
    Decrypt an encrypted value (removes the encryption prefix first).

    Args:
        encrypted_value: The encrypted value with prefix

    Returns:
        The decrypted plain text value
    """
    if not encrypted_value:
        return encrypted_value

    if not is_encrypted(encrypted_value):
        # Value is not encrypted, return as-is (for backward compatibility)
        return encrypted_value

    try:
        # Remove the prefix before decryption
        encrypted_data = encrypted_value[len(ENCRYPTED_PREFIX) :]
        return EncryptionUtils.decrypt_value(encrypted_data)
    except Exception as e:
        # Almost always a KEY MISMATCH: the value was encrypted with a different
        # (e.g. prior ephemeral) key. We blank it so the UI re-prompts — but make
        # the reason explicit, otherwise a downstream tool sees an empty credential
        # and reports a misleading "workspace_id missing" error instead of
        # "please re-enter your credential".
        logger.warning(
            f"Could not decrypt a stored credential — the encryption key likely "
            f"changed (e.g. a redeploy without a stable ENCRYPTION_KEY). Please "
            f"RE-ENTER this credential. ({type(e).__name__})"
        )
        return ""


def _seal(value: object, sensitive: bool, recursive: bool) -> object:
    """``value`` with each non-empty string under a key that
    ``encrypts_at_rest`` encrypted.

    A dict's entries are judged by their own key; a list's items by the key the
    list sits under, so ``{"servers": [{"token": ...}]}`` and
    ``{"api_keys": ["...", "..."]}`` are both covered.
    """
    if recursive and isinstance(value, dict):
        return encrypt_sensitive_fields(value)
    if recursive and isinstance(value, list):
        return [_seal(item, sensitive, True) for item in value]
    if sensitive and isinstance(value, str) and value and not is_encrypted(value):
        # Raises on failure: storing the plaintext instead would defeat the point.
        return encrypt_value(value)
    return value


def encrypt_sensitive_fields(
    data: Dict[str, Any], recursive: bool = True
) -> Dict[str, Any]:
    """
    Encrypt sensitive fields in a dictionary (e.g., tool_configs).

    Nested dictionaries and lists are walked too (see ``_seal``). A value that
    is already encrypted is left as it is, so this is safe to apply twice.

    Args:
        data: The dictionary containing potentially sensitive data
        recursive: Whether to recursively process nested dictionaries and lists

    Returns:
        A new dictionary with sensitive fields encrypted

    Raises:
        Exception: when a value cannot be encrypted. The caller must not store
            the plaintext in its place.
    """
    if not data or not isinstance(data, dict):
        return data
    return {
        key: _seal(value, isinstance(key, str) and encrypts_at_rest(key), recursive)
        for key, value in data.items()
    }


def encrypt_sensitive_json(value: object) -> object:
    """``encrypt_sensitive_fields`` for any JSON value, a list at the root too."""
    return _seal(value, False, True)


#: Plaintext of recently decrypted values, keyed by ciphertext. Every load of a
#: tool-config column decrypts every secret in it (~1.6 ms each, an RSA
#: private-key operation), and the same rows are read over and over. A
#: ciphertext only ever decrypts to one value, so a hit is always right; only
#: successful decrypts are kept, and the parsed-key reset clears it.
_DECRYPTED_MAX = 2048
_decrypted: "OrderedDict[str, str]" = OrderedDict()
_decrypted_lock = threading.Lock()


def forget_decrypted_values() -> None:
    """Drop every cached plaintext (``EncryptionUtils.reset_key_cache`` calls it)."""
    with _decrypted_lock:
        _decrypted.clear()


def _decrypt_or_keep(value: str) -> str:
    """The plaintext of ``value``, or ``value`` itself when it cannot be decrypted.

    Keeping the ciphertext means a later save writes it back unchanged, so the
    credential is recoverable once the right key is back. ``encrypt_value``
    never encrypts an empty string, so an empty result always means failure.
    """
    with _decrypted_lock:
        cached = _decrypted.get(value)
        if cached is not None:
            _decrypted.move_to_end(value)
            return cached
    try:
        plain = EncryptionUtils.decrypt_value(value[len(ENCRYPTED_PREFIX) :])
    except Exception:  # noqa: BLE001 — reported below; the caller gets the ciphertext
        plain = ""
    if plain:
        with _decrypted_lock:
            _decrypted[value] = plain
            if len(_decrypted) > _DECRYPTED_MAX:
                _decrypted.popitem(last=False)
        return plain
    logger.warning(
        "A stored credential could not be decrypted and is left encrypted; "
        "re-enter it if the encryption key changed."
    )
    return value


def _open(value: object, sensitive: bool, recursive: bool) -> object:
    """``value`` with each encrypted string under a sensitive key decrypted.

    The mirror of ``_seal``, by the same rule (``encrypts_at_rest``): only where
    encryption happens, so an ``ENC:``-looking string under an ordinary key (a
    prompt) is left alone.
    """
    if recursive and isinstance(value, dict):
        return decrypt_sensitive_fields(value)
    if recursive and isinstance(value, list):
        return [_open(item, sensitive, True) for item in value]
    if sensitive and isinstance(value, str) and is_encrypted(value):
        return _decrypt_or_keep(value)
    return value


def decrypt_sensitive_fields(
    data: Dict[str, Any], recursive: bool = True
) -> Dict[str, Any]:
    """
    Decrypt sensitive fields in a dictionary (e.g., tool_configs).

    Decrypts the ``ENC:`` strings under the keys ``encrypt_sensitive_fields``
    encrypts (``encrypts_at_rest``), in nested dictionaries and lists too.
    A value that cannot be decrypted is returned still encrypted, never blanked.

    Args:
        data: The dictionary containing potentially encrypted data
        recursive: Whether to recursively process nested dictionaries and lists

    Returns:
        A new dictionary with sensitive fields decrypted
    """
    if not data or not isinstance(data, dict):
        return data
    return {
        key: _open(value, isinstance(key, str) and encrypts_at_rest(key), recursive)
        for key, value in data.items()
    }


def decrypt_sensitive_json(value: object) -> object:
    """``decrypt_sensitive_fields`` for any JSON value, a list at the root too."""
    return _open(value, False, True)


def mask_sensitive_strings(value: object, sensitive: bool = False) -> object:
    """A copy of ``value`` with each non-empty string under a sensitive key masked.

    For API responses. Unlike ``mask_sensitive_fields`` it leaves numbers and
    booleans alone, handles a list at the root, and never mutates its input.
    """
    if isinstance(value, dict):
        return {
            key: mask_sensitive_strings(
                item, isinstance(key, str) and is_sensitive_key(key)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [mask_sensitive_strings(item, sensitive) for item in value]
    if sensitive and isinstance(value, str) and value:
        return REDACTED_PLACEHOLDER
    return value


def mask_sensitive_fields(
    data: Dict[str, Any], recursive: bool = True
) -> Dict[str, Any]:
    """
    Mask sensitive fields in a dictionary for logging or API responses.

    Args:
        data: The dictionary containing potentially sensitive data
        recursive: Whether to recursively process nested dictionaries

    Returns:
        A new dictionary with sensitive fields masked (redacted)
    """
    if not data or not isinstance(data, dict):
        return data

    result: Dict[str, Any] = {}

    for key, value in data.items():
        if isinstance(value, dict) and recursive:
            # Recursively process nested dictionaries
            result[key] = mask_sensitive_fields(value, recursive=True)
        elif isinstance(value, list) and recursive:
            # Process lists that may contain dictionaries
            result[key] = [
                (
                    mask_sensitive_fields(item, recursive=True)
                    if isinstance(item, dict)
                    else item
                )
                for item in value
            ]
        elif is_sensitive_key(key) and value:
            # Mask sensitive values
            result[key] = REDACTED_PLACEHOLDER
        else:
            result[key] = value

    return result


def mask_sensitive_headers(headers: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Return a copy of an HTTP header mapping with credential-bearing values
    redacted, safe for logging.

    SECURITY: full header dicts must never be logged because they carry
    `Authorization: Bearer <token>` (OBO user token / PAT / SPN token),
    `X-Forwarded-Access-Token`, cookies, etc. Any header whose name matches a
    SENSITIVE_HEADER_PATTERN has its value replaced with a placeholder.

    Args:
        headers: The HTTP header mapping to sanitize.

    Returns:
        A new dict with sensitive header values redacted (non-dict input is
        returned unchanged).
    """
    if not isinstance(headers, dict):
        return {}

    masked: Dict[str, Any] = {}
    for key, value in headers.items():
        key_lower = str(key).lower()
        if any(pattern in key_lower for pattern in SENSITIVE_HEADER_PATTERNS):
            masked[key] = REDACTED_PLACEHOLDER
        else:
            masked[key] = value
    return masked


def safe_log_tool_configs(
    tool_configs: Optional[Dict[str, Any]], prefix: str = ""
) -> str:
    """
    Create a safe string representation of tool_configs for logging.

    Args:
        tool_configs: The tool configurations to log
        prefix: Optional prefix for the log message

    Returns:
        A safe string representation with sensitive data masked
    """
    if not tool_configs:
        return f"{prefix}tool_configs: None"

    masked = mask_sensitive_fields(tool_configs)
    return f"{prefix}tool_configs: {masked}"
