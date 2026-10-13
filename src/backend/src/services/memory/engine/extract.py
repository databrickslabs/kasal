"""Fact extraction — the one model call a graph-memory write makes.

Text in (a chat turn, a task output), zero or more facts out. Zero is the
expected answer for most turns: the store is for what stays true about the
user and their world, not for a log of what a run did. That is the difference
from record memory, which stored the turn itself and then needed four dedupe
mechanisms to tidy up after it.

Never raises: no LLM, a malformed reply, a failed call — each means "no facts".
"""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence

from .analyze import extract_json_object
from .graph_types import (
    ENTITY_TYPES,
    FACT_EPISODIC,
    FACT_FINDING,
    FACT_PROCEDURAL,
    FACT_SEMANTIC,
    MAX_OBJECT_CHARS,
    MAX_STATEMENT_CHARS,
    NOTE_RELATION,
    RELATIONS,
    USER_ENTITY,
    ExtractedFact,
    Fact,
    entity_key,
    entity_type,
    is_single_valued,
    relation_key,
)

logger = logging.getLogger(__name__)

_TEXT_CHAR_CAP = 4000
MAX_FACTS_PER_WRITE = 8
_MIN_TEXT_CHARS = 20

MAX_FINDINGS_PER_WRITE = 5

_SYSTEM_PROMPT = (
    "You maintain an AI assistant's long-term memory as a graph of facts. Read "
    "the text and extract only DURABLE facts worth knowing in a later, unrelated "
    "conversation. Reply with ONE JSON object and no other text:\n"
    '{"facts": [{"kind": "semantic", "subject": "...", "subject_type": "...", '
    '"relation": "...", "object": "...", "object_type": null, "single": false, '
    '"qualifiers": {}, "standing_evidence": "", "statement": "..."}], '
    '"note": "", "note_subject": "", '
    '"note_subject_type": ""}\n'
    "The text is usually one exchange: a `User:` part, then an `Assistant:` "
    "part. A fact has a kind, and each kind has ONE source:\n"
    '1. kind "procedural" — how the assistant should respond in EVERY '
    "conversation from now on: a standing preference or instruction about "
    "format, language, tone, sources or method. ONLY from what the User says, "
    "and ONLY when the user says it holds beyond this request — with words "
    'such as "always", "from now on", "in general", "by default", "never", "I '
    'prefer". Copy those exact words into "standing_evidence". An instruction '
    'about THIS request — "check only from postgres", "just the titles", "use '
    'the web for this one" — is not procedural and is not a fact of any kind: '
    "do not extract it. When unsure, leave it out: a wrong standing rule "
    "changes every later answer.\n"
    '2. kind "semantic" — what the User states about themselves, their '
    "organisation, systems, datasets and projects: role, employer, decisions, "
    "properties. ONLY from what the User says. Never take 1 or 2 from the "
    "Assistant's part: what it restates, recalls, assumes or offers about the "
    "user is not a fact.\n"
    '3. kind "finding" — what the Assistant\'s answer ESTABLISHES about a '
    "NAMED real-world entity (a company, product, person, place, dataset), "
    "when it is specific and will still be true later: a figure for a closed "
    "period, a date, an owner, a location, a definition. Example: subject "
    '"Acme", relation "revenue_fy2025", object "USD 4.2 bn", single true. Put '
    "the period in the relation when the value belongs to one. At most "
    f"{MAX_FINDINGS_PER_WRITE}, the ones the user asked about first. The "
    "user's OWN systems and datasets count, as the run discovered them: which "
    "tables a database has, what a table holds, what its key columns are — "
    'e.g. subject "orders" (dataset), relation "part_of", object "postgres" '
    '(object_type system); subject "orders", relation "holds", object '
    '"customer orders with city and status". A count that changes (rows, '
    "totals as of today) belongs inside a statement, never as its own fact.\n"
    "NOT facts of any kind: what was asked or done in this exchange; errors "
    "and failed attempts; news of the day and headlines; forecasts, guidance "
    "and opinions; anything the answer is unsure of; generated artefacts such "
    'as code, slides or HTML. Most texts contain nothing durable — then "facts" '
    "is [].\n"
    'The note: only when "facts" holds NO finding and the exchange still did '
    "work worth referring back to (a news briefing, a report, a build, a query "
    'result), set "note" to at most 30 words: the subject as a short noun '
    'phrase, a colon, then the main findings — e.g. "Swiss news roundup: '
    'court president ousted; tax vote campaign opens". Write only the subject '
    'and findings: never "the user asked" or "the assistant produced", and no '
    'date. Set "note_subject" to the one named thing the note is mainly about '
    '(e.g. "Switzerland") and "note_subject_type" to its type. Otherwise leave '
    '"note" empty.\n'
    "Rules:\n"
    f'- The person speaking is always the subject "{USER_ENTITY}" (type person). '
    "Other subjects are named exactly as the text names them.\n"
    f"- subject_type and object_type: one of {', '.join(ENTITY_TYPES)}. Set "
    "object_type only when the object is itself a named thing; otherwise null.\n"
    f"- relation: lowercase snake_case. Prefer one of: {', '.join(RELATIONS)}. "
    "When a subject can hold only ONE value at a time for something (an "
    "employer, a deadline, a preferred output format, a figure for one "
    'period), use a specific relation for it (e.g. "preferred_output_format") '
    'and set "single": true, so a later value replaces this one.\n'
    "- object: the value, a short phrase. When it is a named thing, it is the "
    "BARE NAME and nothing else — no parentheses, no dates, no role, never two "
    'things in one object. What qualifies the relation goes in "qualifiers", '
    'short strings keyed by what they are: {"role": "Solutions Architect", '
    '"from": "Feb 2016", "to": "2023"} for a job, {"field": "Business '
    'Computing"} for a degree. So "worked at Acme as an engineer from 2016 to '
    '2023" is object "Acme", object_type "organization", with those '
    "qualifiers — not an object that contains them.\n"
    "- statement: one self-contained sentence stating the fact, understandable "
    "without the text.\n"
    "- You may be given facts ALREADY IN MEMORY as `subject | relation | "
    "object`. Never output one of them again, in any wording. Output a fact "
    "only when it is new, or when it changes one of them — and then reuse that "
    "fact's subject and relation exactly, so the new value replaces the old.\n"
    f"- At most {MAX_FACTS_PER_WRITE} facts. Invent nothing."
)

_KNOWN_HEADER = "ALREADY IN MEMORY:"


def _clean(value: object, cap: int) -> str:
    return " ".join(str(value or "").split())[:cap].strip()


_TRAILING_PARENS = re.compile(r"^(?P<name>[^()]+?)\s*\((?P<detail>[^()]*)\)\s*$")
_MAX_QUALIFIERS = 6


def _grounded(evidence: object, user_text: str | None) -> bool:
    """Whether ``evidence`` — the words that make a preference a standing one —
    was given, and (when the user's text is known) really is in it."""
    quote = " ".join(str(evidence or "").lower().split())
    if not quote:
        return False
    if user_text is None:
        return True
    return quote in " ".join(user_text.lower().split())


def user_part(text: str) -> str | None:
    """What the user said in a ``User: … Assistant: …`` exchange; ``None`` for
    a text that is not one (a task result)."""
    match = re.match(r"\s*User:\s*(?P<said>.*?)(?:\s+Assistant:|$)", text, re.DOTALL)
    return match["said"] if match else None


def _bare_name(text: str) -> tuple[str, str]:
    """``("AWS", "Solutions Architect, 2023-2024")`` from
    ``"AWS (Solutions Architect, 2023-2024)"``; the text itself otherwise."""
    match = _TRAILING_PARENS.match(text)
    if match and entity_key(match["name"]):
        return match["name"].strip(), match["detail"].strip()
    return text, ""


def _qualifiers(value: object) -> dict[str, str]:
    """A small ``{what: value}`` map of short strings; anything else is dropped."""
    if not isinstance(value, dict):
        return {}
    out: dict[str, str] = {}
    for key, item in value.items():
        name, text = relation_key(key), _clean(item, 80)
        if name and text and len(out) < _MAX_QUALIFIERS:
            out[name] = text
    return out


def parse_facts(payload: object, user_text: str | None = None) -> list[ExtractedFact]:
    """Validate an extraction reply. Malformed entries are dropped, not raised.

    ``user_text`` is what the user actually said, when known. A standing
    preference is only accepted with words from it that say it holds beyond
    this request: "check only from postgres", said about one lookup, was once
    stored as a standing rule, and a later news request then refused to search
    the web.
    """
    items = payload.get("facts") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        return []
    facts: list[ExtractedFact] = []
    seen: set[tuple[str, str, str]] = set()
    findings = 0
    for item in items:
        if not isinstance(item, dict):
            continue
        subject = _clean(item.get("subject"), 120)
        relation = relation_key(item.get("relation"))
        obj = _clean(item.get("object"), MAX_OBJECT_CHARS)
        statement = _clean(item.get("statement"), MAX_STATEMENT_CHARS)
        if not (entity_key(subject) and relation and entity_key(obj) and statement):
            continue
        if relation == NOTE_RELATION:  # reserved for the run note
            continue
        # A kind is only accepted from the source it can have: nothing about
        # the user is a finding, and only the user holds standing preferences.
        about_user = entity_key(subject) == USER_ENTITY
        kind = str(item.get("kind") or "").strip().lower()
        if kind == FACT_FINDING and not about_user:
            findings += 1
            if findings > MAX_FINDINGS_PER_WRITE:
                continue
        elif kind == FACT_PROCEDURAL and about_user:
            if not _grounded(item.get("standing_evidence"), user_text):
                continue  # a one-off instruction is not a fact of any kind
        else:
            kind = FACT_SEMANTIC
        qualifiers = _qualifiers(item.get("qualifiers"))
        if item.get("object_type"):
            # A model that still writes "AWS (Solutions Architect, 2023-2024)"
            # would mint a new entity per job; the name is what precedes the
            # parenthesis, and what is inside it qualifies the relation.
            obj, detail = _bare_name(obj)
            if detail and not qualifiers:
                qualifiers = {"detail": detail}
        identity = (entity_key(subject), relation, entity_key(obj))
        if identity in seen:
            continue
        seen.add(identity)
        object_type = item.get("object_type")
        facts.append(
            ExtractedFact(
                subject=subject,
                subject_type=entity_type(item.get("subject_type")),
                relation=relation,
                object=obj,
                object_type=entity_type(object_type) if object_type else None,
                single=is_single_valued(relation, item.get("single") is True),
                statement=statement,
                kind=kind,
                qualifiers=qualifiers,
            )
        )
        if len(facts) >= MAX_FACTS_PER_WRITE:
            break
    return facts


MAX_NOTE_CHARS = 240


def parse_note(payload: object) -> ExtractedFact | None:
    """The run note in an extraction reply, attached to the one thing it is
    mainly about (the user when the reply names nothing). ``None`` without one.
    Undated: the writer stamps the date, since a model writes the year it
    remembers."""
    if not isinstance(payload, dict) or not isinstance(payload.get("note"), str):
        return None
    text = _clean(payload.get("note"), MAX_NOTE_CHARS)
    if not text:
        return None
    subject = _clean(payload.get("note_subject"), 120)
    named = bool(entity_key(subject))
    return ExtractedFact(
        subject=subject if named else USER_ENTITY,
        subject_type=(
            entity_type(payload.get("note_subject_type")) if named else "person"
        ),
        relation=NOTE_RELATION,
        object=text,
        statement=text,
        kind=FACT_EPISODIC,
    )


def extract_turn(
    llm: object, text: str, known: Sequence[Fact] = ()
) -> tuple[list[ExtractedFact], ExtractedFact | None]:
    """The durable facts in ``text`` that ``known`` does not already hold, and
    the run note — what a run produced, when it left no finding — or ``None``.

    ``known`` is what the store has now. Showing it to the extractor is the
    read-before-write: the same preference restated in other words is not
    stored again, and a changed value comes back under the SAME relation name,
    which is what lets the store replace the old one. ``([], None)`` when there is
    nothing to keep, or no LLM. One model call for both.
    """
    call = getattr(llm, "call", None)
    content = " ".join((text or "").split())
    if not callable(call) or len(content) < _MIN_TEXT_CHARS:
        return [], None
    user = content[:_TEXT_CHAR_CAP]
    if known:
        lines = "\n".join(f"- {f.subject} | {f.relation} | {f.object}" for f in known)
        user = f"{_KNOWN_HEADER}\n{lines}\n\nTEXT:\n{user}"
    try:
        raw = call(
            [
                {"role": "system", "content": _SYSTEM_PROMPT},
                {"role": "user", "content": user},
            ]
        )
        payload = extract_json_object(str(raw or ""))
        return parse_facts(payload, user_part(content)), parse_note(payload)
    except Exception:  # noqa: BLE001 — extraction must never break a write
        logger.warning("memory fact extraction failed; nothing stored", exc_info=True)
        return [], None


def extract_facts(
    llm: object, text: str, known: Sequence[Fact] = ()
) -> list[ExtractedFact]:
    """The facts half of :func:`extract_turn`."""
    return extract_turn(llm, text, known)[0]
