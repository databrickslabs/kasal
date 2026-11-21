"""Source-DAX fingerprints carried in a measure's UC comment.

WHY: drift detection has to answer "was this deployed measure built from the DAX
Power BI has TODAY?". The deployed YAML only keeps the translated SQL, so without a
record of the source DAX a changed measure is indistinguishable from an unchanged
one. The generator stamps ``dax#<8 hex>`` into each DAX-translated measure's comment;
the drift monitor recomputes it from the live DAX and compares.

The hash is over a NORMALISED DAX string — comments stripped, whitespace collapsed,
case folded outside string literals — so re-formatting a measure in PBI Desktop is
not reported as drift. Only a change to what the DAX computes is.
"""

import hashlib
import re
from typing import Optional

FINGERPRINT_PREFIX = "dax#"
_FINGERPRINT_RE = re.compile(r"\bdax#([0-9a-f]{8})\b")
# "PBI: <original name>" — the generator writes it first in a DAX measure comment,
# followed by " · ..." / " [..." suffixes or the end of the string.
_PBI_TAG_RE = re.compile(r"(?:^|\s)PBI:\s*(.+?)(?=\s+·|\s+\[|\s+dax#|$)")

_LINE_COMMENT_RE = re.compile(r"(--|//)[^\n]*")
_BLOCK_COMMENT_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_STRING_RE = re.compile(r'"(?:[^"]|"")*"')


def normalize_dax(dax: str) -> str:
    """Canonical form of a DAX expression for change detection.

    String literals keep their case (``"EUR"`` vs ``"eur"`` is a real change);
    everything else is case-folded, because DAX identifiers and functions are
    case-insensitive.
    """
    if not dax:
        return ""
    text = _BLOCK_COMMENT_RE.sub(" ", dax)
    out: list[str] = []
    pos = 0
    for m in _STRING_RE.finditer(text):
        out.append(_LINE_COMMENT_RE.sub(" ", text[pos : m.start()]).lower())
        out.append(m.group(0))
        pos = m.end()
    out.append(_LINE_COMMENT_RE.sub(" ", text[pos:]).lower())
    joined = "".join(out)
    joined = re.sub(r"\s+", " ", joined).strip()
    # Whitespace around punctuation is formatting, not semantics.
    return re.sub(r"\s*([(),=<>+\-*/&|\[\]{}])\s*", r"\1", joined)


def dax_fingerprint(dax: str) -> str:
    """``dax#<8 hex>`` for a DAX expression, or "" when there is no DAX."""
    norm = normalize_dax(dax)
    if not norm:
        return ""
    digest = hashlib.sha256(norm.encode("utf-8")).hexdigest()[:8]
    return f"{FINGERPRINT_PREFIX}{digest}"


def extract_fingerprint(comment: Optional[str]) -> Optional[str]:
    """The ``dax#…`` token recorded in a measure comment, if any."""
    if not comment:
        return None
    m = _FINGERPRINT_RE.search(str(comment))
    return f"{FINGERPRINT_PREFIX}{m.group(1)}" if m else None


def extract_pbi_name(comment: Optional[str]) -> Optional[str]:
    """The original PBI measure name from a ``PBI: <name>`` comment tag, if any."""
    if not comment:
        return None
    m = _PBI_TAG_RE.search(str(comment))
    if not m:
        return None
    name = m.group(1).strip()
    return name or None
