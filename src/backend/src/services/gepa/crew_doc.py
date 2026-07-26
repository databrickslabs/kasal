"""The crew ⇄ text round-trip GEPA optimises over.

GEPA mutates text, so a crew's agents and tasks are serialised into one
document, reflected on, and parsed back. The parser is deliberately tolerant:
the text it reads was written by an LLM, so a malformed section costs that
section, not the candidate.
"""

import logging
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

_CREW_DOC_FIELD_LABELS = {
    "ROLE": "role",
    "GOAL": "goal",
    "BACKSTORY": "backstory",
    "DESCRIPTION": "description",
    "EXPECTED_OUTPUT": "expected_output",
}


def _serialize_crew_doc(agents: List[Any], tasks: List[Any]) -> tuple:
    """Serialize crew prompt fields into a labeled document + the key set.

    Returns (doc, field_keys) where keys look like 'agent.<id>.role'.
    """
    lines: List[str] = []
    keys: List[str] = []
    for agent in agents:
        lines.append(f"[AGENT {agent.id}]")
        for label, field in (
            ("ROLE", "role"),
            ("GOAL", "goal"),
            ("BACKSTORY", "backstory"),
        ):
            lines.append(f"{label}: {str(getattr(agent, field, '') or '')}")
            keys.append(f"agent.{agent.id}.{field}")
        lines.append("")
    for task in tasks:
        lines.append(f"[TASK {task.id}]")
        for label, field in (
            ("DESCRIPTION", "description"),
            ("EXPECTED_OUTPUT", "expected_output"),
        ):
            lines.append(f"{label}: {str(getattr(task, field, '') or '')}")
            keys.append(f"task.{task.id}.{field}")
        lines.append("")
    return "\n".join(lines).strip(), keys


def _parse_crew_doc(doc: str) -> Optional[Dict[str, str]]:
    """Parse a (possibly GEPA-mutated) crew document back into field values.

    Returns {key: text} or None when the document lost its structure —
    callers score such candidates 0 WITHOUT executing the crew.
    """
    doc = (doc or "").strip()
    # Fence rescue: reflection models sometimes wrap the document in markdown
    # code fences that survive gepa's extraction. The content inside is a
    # perfectly good document — losing the candidate over the wrapper wastes
    # the proposal.
    if doc.startswith("```"):
        doc = re.sub(r"^```\S*\n?", "", doc)
        doc = re.sub(r"\n?```\s*$", "", doc)
    fields: Dict[str, str] = {}
    entity_prefix: Optional[str] = None
    current_key: Optional[str] = None
    for raw_line in (doc or "").splitlines():
        line = raw_line.strip()
        if line.startswith("[AGENT ") and line.endswith("]"):
            entity_prefix = f"agent.{line[len('[AGENT '):-1].strip()}"
            current_key = None
            continue
        if line.startswith("[TASK ") and line.endswith("]"):
            entity_prefix = f"task.{line[len('[TASK '):-1].strip()}"
            current_key = None
            continue
        matched = False
        for label, field in _CREW_DOC_FIELD_LABELS.items():
            if line.startswith(f"{label}:"):
                if entity_prefix is None:
                    return None
                current_key = f"{entity_prefix}.{field}"
                fields[current_key] = line[len(label) + 1 :].strip()
                matched = True
                break
        if matched:
            continue
        if line and current_key:
            fields[current_key] = f"{fields[current_key]}\n{line}".strip()
    return fields or None


def _parse_requirement_lines(text: str) -> List[str]:
    """Parse 'R1. ...' numbered requirement lines from a distillation reply."""
    return [
        m.group(1).strip()
        for m in re.finditer(r"^\s*R\d+[.:]\s*(.+)$", text or "", re.MULTILINE)
        if m.group(1).strip()
    ]


def _distill_requirements(raw_notes: List[str], limit: int = 8) -> List[str]:
    """Collapse harvested human feedback into a deduplicated requirements list.

    The raw harvest repeats the same complaint many times ("french side" x8)
    and carries the grade numbers. Feeding that litany to the judge ANCHORED
    it — a compliant answer was graded 0/10 because every historical line said
    0.0 (verified live with an A/B judge experiment: same answer, litany
    rubric -> 0, requirements checklist -> 6). The judge needs constraints,
    not grade history.
    """
    requirements: List[str] = []
    seen: set = set()
    for note in raw_notes:
        text = str(note or "").strip()
        if not text:
            continue
        normalized = re.sub(r"[^a-z0-9 ]", "", text.lower())
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        requirements.append(text)
    return requirements[:limit]


def _extract_user_from_log(prompt: str) -> Optional[str]:
    """Generation services log 'System: <template>\\nUser: <request>' — return
    the user request part, or None when the marker is absent."""
    if "\nUser: " not in prompt:
        return None
    return prompt.split("\nUser: ", 1)[1].strip() or None
