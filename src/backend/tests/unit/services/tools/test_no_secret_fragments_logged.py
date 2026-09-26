"""No token or key fragments in tool logs (audit N4).

``tool_factory`` logged the first 10 characters of OBO tokens and the first and
last 4 of decrypted API keys at INFO; the Jobs and Perplexity tools logged a
"masked" token. For a PAT (``dapi`` + 32 hex) eight known characters are a real
entropy reduction. Only ``bool(token)`` and the auth method may be logged.
"""

import re
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parents[4] / "src" / "services" / "tools"

# A slice of a variable whose name says it holds a secret, e.g. token[:10],
# decrypted_value[-4:], auth_header[7:11].
SECRET_SLICE = re.compile(
    r"\b(\w*(?:token|api_key|decrypted_value|secret|auth_header)\w*)\s*\[\s*-?\d*\s*:"
    r"\s*-?\d*\s*\]",
    re.IGNORECASE,
)


@pytest.mark.parametrize(
    "name",
    [
        "tool_factory.py",
        "databricks_jobs_tool.py",
        "perplexity_tool.py",
    ],
)
def test_no_secret_is_sliced_in_the_tool_source(name):
    source = (TOOLS / name).read_text()
    offenders = [
        f"{name}:{source.count(chr(10), 0, m.start()) + 1}: {m.group(0)}"
        for m in SECRET_SLICE.finditer(source)
    ]
    assert not offenders, "secret fragments sliced for logging:\n" + "\n".join(
        offenders
    )
