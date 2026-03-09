"""
Secret-leak detector (log-only).

Scans agent output for common credential / secret patterns:
  - Databricks PAT  (dapi<32 hex chars>)
  - Databricks env var (DATABRICKS_TOKEN=...)
  - AWS access key  (AKIA<16 uppercase alphanumeric>)
  - Slack token     (xox[baprs]-<alphanumeric>)
  - PEM private key header
  - GitHub tokens   (ghp_, gho_, ghs_, github_pat_)
  - GCP service account key (type: "service_account")
  - Azure connection string (AccountKey=...)
  - Generic API key / secret assignment (tightened to reduce false positives)

Designed to be fast and non-blocking — always returns, never raises.

Usage:
    from src.engines.crewai.security.secret_leak_detector import detect
    result = detect(text)
    if result.detected:
        logger.warning("[SECURITY] Secret leak: %s", result.secret_types)
"""

import re
import logging
from dataclasses import dataclass, field
from typing import List

logger = logging.getLogger(__name__)

# Registry of (pattern_name, compiled_regex) pairs.
# Order matters: more specific patterns first.
_PATTERNS: List[tuple] = [
    ("databricks_pat", re.compile(r"dapi[0-9a-f]{32}", re.IGNORECASE)),
    ("databricks_env_token", re.compile(
        r"DATABRICKS_TOKEN\s*[=:]\s*\S{10,}",
    )),
    ("aws_access_key", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("slack_token",    re.compile(r"xox[baprs]-[0-9A-Za-z\-]+")),
    ("private_key",    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA |ENCRYPTED )?PRIVATE KEY-----")),
    # GitHub tokens: personal access (ghp_), OAuth (gho_), app install (ghs_), fine-grained (github_pat_)
    ("github_token",   re.compile(r"(?:ghp|gho|ghs|github_pat)_[A-Za-z0-9_]{20,}")),
    # GCP service account JSON key identifier
    ("gcp_service_account", re.compile(
        r'"type"\s*:\s*"service_account"',
    )),
    # Azure storage account key in connection string
    ("azure_connection_string", re.compile(
        r"AccountKey\s*=\s*[A-Za-z0-9+/=]{40,}",
    )),
    # Generic: api_key / secret followed by = : or whitespace and a long value
    # Tightened: require at least 24 chars (excludes UUIDs at 32 hex+hyphens)
    # and the value must start with a plausible key prefix (alphanumeric, not all digits)
    ("generic_api_key", re.compile(
        r'(?:api[_-]?key|api[_-]?secret|secret[_-]?key|auth[_-]?token)'
        r'["\s:=]+(?=[A-Za-z])[A-Za-z0-9/+_\-]{24,}',
        re.IGNORECASE,
    )),
]


@dataclass
class SecretLeakResult:
    """Result of a secret-leak scan."""
    detected: bool
    secret_types: List[str] = field(default_factory=list)


def detect(text: str) -> SecretLeakResult:
    """
    Scan *text* for known secret patterns.

    Args:
        text: Any string — agent step output, task result, etc.

    Returns:
        SecretLeakResult with detected=True and the matched pattern names
        if any secrets are found; detected=False otherwise.
    """
    if not text:
        return SecretLeakResult(detected=False)

    found: List[str] = []
    try:
        for name, pattern in _PATTERNS:
            if pattern.search(text):
                found.append(name)
    except Exception as exc:  # pragma: no cover — defensive
        logger.debug("[SECURITY] SecretLeakDetector scan error (ignored): %s", exc)

    return SecretLeakResult(detected=bool(found), secret_types=found)
