"""No token or key fragments in logs or responses, anywhere in ``src/`` (audit N4, V3-4).

``tool_factory`` logged the first 10 characters of OBO tokens and the first and
last 4 of decrypted API keys at INFO; the Jobs and Perplexity tools logged a
"masked" token; the UCMV generator logged the PowerBI token's first and last 4;
``GET /connections/test-api-key`` returned the first 4 of each LLM key; and a
debug endpoint returned 20 characters of a bearer token. For a PAT (``dapi`` +
32 hex) eight known characters are a real entropy reduction. Only ``bool(token)``
and the auth method may be logged or returned.

This used to scan three files, so a new fragment anywhere else went unnoticed.
It now scans every module under ``src/``.
"""

import re
from pathlib import Path

SRC = Path(__file__).resolve().parents[4] / "src"

# A slice of a variable whose name says it holds a secret, e.g. token[:10],
# decrypted_value[-4:], auth_header[7:11], client_secret[:6].
SECRET_SLICE = re.compile(
    r"\b(\w*(?:token|api_?key|decrypted_value|secret|auth_header|password"
    r"|credential|bearer)\w*)\s*\[\s*-?\d*\s*:\s*-?\d*\s*\]",
    re.IGNORECASE,
)

# A "first N ... last N" preview of any variable: f"{value[:4]}...{value[-4:]}".
HEAD_TAIL_PREVIEW = re.compile(r"\{(\w+)\[:\s*\d+\s*\]\}\.\.\.\{\1\[\s*-\d+\s*:\s*\]\}")

# Stripping the "Bearer " prefix (7 characters) keeps the WHOLE token for use; it
# is not a fragment.
BEARER_PREFIX_STRIP = re.compile(r"\[\s*7\s*:\s*\]$")

# Fragments outside this change's ownership, recorded rather than silently
# allowed. Each entry must still match, so fixing one fails this test until the
# entry is removed — the list can only shrink.
KNOWN_OFFENDERS = {
    # DEBUG, non-Bearer branch only: 20 characters of the SDK auth header.
    ("utils/databricks_auth.py", "auth_header[:20]"),
    # INFO in the MLflow subprocess: "prefix" of the SPN auth header. Seven
    # characters is "Bearer " on the normal path, but token text otherwise.
    ("services/mlflow/mlflow_setup.py", "auth_header[:7]"),
    # _mask_secret: now unused (its one log call logs bool(token) instead);
    # delete it with the dead llm_token fields (audit V3-5).
    ("services/tools/uc_metric_view_generator_tool.py", "{value[:4]}...{value[-4:]}"),
}


def _findings():
    for path in sorted(SRC.rglob("*.py")):
        rel = path.relative_to(SRC).as_posix()
        source = path.read_text(encoding="utf-8")
        for pattern in (SECRET_SLICE, HEAD_TAIL_PREVIEW):
            for match in pattern.finditer(source):
                text = match.group(0)
                if pattern is SECRET_SLICE and BEARER_PREFIX_STRIP.search(text):
                    continue
                line = source.count("\n", 0, match.start()) + 1
                yield rel, text, line


def test_no_secret_is_sliced_anywhere_in_src():
    offenders = [
        f"src/{rel}:{line}: {text}"
        for rel, text, line in _findings()
        if (rel, text) not in KNOWN_OFFENDERS
    ]
    assert not offenders, (
        "secret fragments sliced into a log or response; log bool(secret) or the "
        "auth method instead:\n" + "\n".join(offenders)
    )


def test_known_offenders_are_still_present():
    """A fixed site must leave the allowlist, so the list only ever shrinks."""
    found = {(rel, text) for rel, text, _ in _findings()}
    stale = sorted(KNOWN_OFFENDERS - found)
    assert not stale, f"remove fixed entries from KNOWN_OFFENDERS: {stale}"


def test_the_patterns_catch_what_they_are_for():
    for sample in (
        'logger.info(f"t={token[:10]}")',
        "key_prefix = openai_api_key[:4]",
        '"token_preview": auth_token[:20]',
        "x = client_secret[-4:]",
    ):
        assert SECRET_SLICE.search(sample), sample
    assert HEAD_TAIL_PREVIEW.search('f"{value[:4]}...{value[-4:]}"')
    assert BEARER_PREFIX_STRIP.search("auth_header[7:]")
    assert not SECRET_SLICE.search("parts = path_parts[4:]")
