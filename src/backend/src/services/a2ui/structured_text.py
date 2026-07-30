"""Turning a structured answer into text the composer can actually render.

Deep Research asks every task for a JSON envelope (``summary`` + ``findings``,
see ``schemas/deep_research``), and ``output_schema`` routes through
``output_json``, which REWRITES ``TaskOutput.raw`` to the JSON dump. So the
finished run's text is a JSON document, and that is what reached the A2UI
composer.

Observed end to end: the composer ran for 5.4s on the raw JSON, produced a
``document`` surface with no data component, and the prose gate correctly dropped
it ("would have repeated the answer's own words"). A dropped surface leaves the
result a plain string — so the user was shown the raw envelope JSON in chat.

The fix is NOT a hand-built surface. ``kernel/genie_formatting`` states the rule
this follows: every deliverable renders through the shared composer, so there is
no per-feature surface to keep in sync. What was wrong here is only the INPUT.
Rendering the envelope as markdown fixes both halves at once:

* the composer sees a markdown table and emits a ``Table`` — a data component,
  so the surface passes ``_has_data_component`` instead of being dropped;
* and when A2UI is off or the surface is still dropped, the fallback text is a
  readable table rather than JSON.

Deliberately duck-typed rather than importing the envelope model: this module
stays independent of any one schema, and a partial or degraded answer (a task
that hit ``guardrail_on_exhausted='degrade'`` and carries an annotation after
the JSON) still renders as far as it can.
"""

import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

#: Columns rendered for each finding, in order: (envelope key, header).
_FINDING_COLUMNS = (
    ("claim", "Finding"),
    ("evidence", "Evidence"),
    ("source", "Source"),
    ("confidence", "Confidence"),
)


def _cell(value: Any) -> str:
    """One markdown table cell: single-line, pipes escaped, links linked."""
    if value is None:
        return "—"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        # Confidence reads better as a percentage than as 0.8500000000000001.
        return f"{value:.0%}" if 0 <= float(value) <= 1 else str(value)
    text = str(value).strip()
    if not text:
        return "—"
    text = text.replace("|", "\\|").replace("\n", " ")
    if text.startswith(("http://", "https://")):
        return f"[{text}]({text})"
    return text


def _table(findings: List[Dict[str, Any]]) -> List[str]:
    """A markdown table over the findings, omitting columns nobody filled.

    An always-present Evidence column full of em-dashes tells the composer there
    is a column worth rendering when there is not, and the envelope marks
    ``evidence`` optional.
    """
    columns = [
        (key, header)
        for key, header in _FINDING_COLUMNS
        if any(str(f.get(key) or "").strip() for f in findings)
    ]
    if not columns:
        return []
    lines = [
        "| " + " | ".join(header for _, header in columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    lines += [
        "| " + " | ".join(_cell(f.get(key)) for key, _ in columns) + " |"
        for f in findings
    ]
    return lines


def _envelope(text: str) -> Optional[Dict[str, Any]]:
    """The parsed research envelope, or None if ``text`` is not one.

    Tolerates trailing prose: the degrade paths append ``⚠️ Truncated`` /
    ``⚠️ Unverified`` notes AFTER the JSON, and an answer that was degraded is
    exactly the one a reader most needs rendered.
    """
    candidate = (text or "").strip()
    if not candidate.startswith("{"):
        return None
    try:
        parsed = json.loads(candidate)
    except json.JSONDecodeError:
        try:
            parsed = json.JSONDecoder().raw_decode(candidate)[0]
        except ValueError:
            return None
    if not isinstance(parsed, dict):
        return None
    if not isinstance(parsed.get("findings"), list) or "summary" not in parsed:
        return None
    return parsed


def render_research_envelope(text: str) -> Optional[str]:
    """Markdown for a deep-research envelope, or None if ``text`` is not one.

    None rather than a passthrough so the caller can tell "nothing to do" from
    "converted", and leave a non-envelope result byte-identical.
    """
    envelope = _envelope(text)
    if envelope is None:
        return None

    findings = [f for f in envelope["findings"] if isinstance(f, dict)]
    parts: List[str] = []

    summary = str(envelope.get("summary") or "").strip()
    if summary:
        parts.append(summary)

    table = _table(findings)
    if table:
        parts.append("\n".join(table))

    questions = [
        str(q).strip()
        for q in (envelope.get("open_questions") or [])
        if str(q or "").strip()
    ]
    if questions:
        parts.append("**Open questions**\n" + "\n".join(f"- {q}" for q in questions))

    limitations = str(envelope.get("limitations") or "").strip()
    if limitations:
        parts.append(f"**Limitations** — {limitations}")

    if not parts:
        # A well-formed envelope with nothing in it. Leave the original alone
        # rather than replacing it with an empty string.
        return None

    trailing = (text or "").strip()
    note_at = max(trailing.find("⚠️"), -1)
    if note_at > 0:
        # Keep a degrade annotation visible; it is why the answer looks thin.
        parts.append(trailing[note_at:].strip())

    logger.info(
        "[a2ui] rendered a research envelope as markdown: "
        "%d finding(s), %d open question(s)",
        len(findings),
        len(questions),
    )
    return "\n\n".join(parts)
