"""Capability fingerprint for the DAX→UC-Metric-View transpiler.

WHY: Kasal's transpilation capability keeps improving — new patterns in
``dax_translator.py``, a better LLM-first path in ``dax_llm_fallback.py``, and an
evolving skill corpus under ``skills/``. To know whether it is worth re-trying the
measures that failed on an OLD run, we need a cheap answer to "has the transpiler
changed since then?".

Why a fingerprint and not a skill-file diff: most capability gains land in CODE
(pattern registry, LLM-first path, emitters), not in the ``.md`` skill files, so
diffing skills alone would miss them. And mapping "this skill paragraph changed" to
"measure X now works" is guesswork. Hashing the whole capability surface gives a
reliable "something changed" signal; the actual gain is then measured by RE-RUNNING
the transpiler (ground truth) rather than inferred.

Note: ``ConversionHistory.converter_version`` exists as a column but is never
written anywhere in the codebase, so it cannot serve as the gate today. This
fingerprint replaces it.

Fail-open by design: any error yields the ``_UNKNOWN`` sentinel, which callers treat
as "changed" (re-evaluate) rather than crashing a scheduled sweep.
"""
from __future__ import annotations

import hashlib
import logging
import os
from functools import lru_cache

logger = logging.getLogger(__name__)

_UNKNOWN = "unknown"

# The capability surface: source files whose content materially changes what the
# transpiler can translate. Kept explicit (not a directory walk) so unrelated
# helper edits don't churn the fingerprint.
_SOURCE_FILES = ("dax_translator.py", "dax_llm_fallback.py")

_SKILLS_DIRNAME = "skills"


def _here() -> str:
    return os.path.dirname(os.path.abspath(__file__))


def _read(path: str) -> bytes:
    try:
        with open(path, "rb") as fh:
            return fh.read()
    except OSError:
        return b""


def skill_files() -> list[str]:
    """Every skill-corpus file, as paths relative to the skills/ dir, sorted.

    Sorted so the fingerprint is stable regardless of filesystem walk order.
    """
    root = os.path.join(_here(), _SKILLS_DIRNAME)
    found: list[str] = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for name in filenames:
            if name.startswith("."):
                continue
            full = os.path.join(dirpath, name)
            found.append(os.path.relpath(full, root))
    return sorted(found)


def pattern_names() -> list[str]:
    """Registered translator pattern names — the deterministic capability surface.

    Imported lazily: this module is used by a scheduled sweep that should not pay
    the translator import cost unless it actually needs the fingerprint.
    """
    try:
        from .dax_translator import DaxTranslator

        translator = DaxTranslator({})
        return [name for name, _m, _t in getattr(translator, "_patterns", [])]
    except Exception as exc:  # fail-open — never block on introspection
        logger.debug("[capability] could not read pattern names: %s", exc)
        return []


@lru_cache(maxsize=1)
def capability_fingerprint() -> str:
    """Short stable hash over the transpiler's capability surface.

    Covers: every skill-corpus file's content, the source of the translator and the
    LLM-first fallback, and the registered pattern names. Cached per process (the
    files cannot change under a running interpreter without a reload).
    """
    try:
        digest = hashlib.sha256()

        root = os.path.join(_here(), _SKILLS_DIRNAME)
        for rel in skill_files():
            digest.update(rel.encode("utf-8"))
            digest.update(_read(os.path.join(root, rel)))

        for name in _SOURCE_FILES:
            digest.update(name.encode("utf-8"))
            digest.update(_read(os.path.join(_here(), name)))

        for name in pattern_names():
            digest.update(name.encode("utf-8"))

        return digest.hexdigest()[:16]
    except Exception as exc:  # fail-open
        logger.warning("[capability] fingerprint failed (%s) — treating as unknown", exc)
        return _UNKNOWN


def capability_summary() -> dict:
    """Human/report-friendly view of the current capability level."""
    names = pattern_names()
    files = skill_files()
    return {
        "fingerprint": capability_fingerprint(),
        "pattern_count": len(names),
        "skill_file_count": len(files),
        "skill_files": files,
    }


def has_capability_changed(stored_fingerprint: str | None) -> bool:
    """True when it is worth re-evaluating a run recorded under ``stored_fingerprint``.

    Unknown/missing stored fingerprint → True (old runs predate fingerprinting, so we
    cannot rule out a gain). Current fingerprint unknown → True (fail-open: prefer a
    wasted re-check over silently never proposing anything).
    """
    if not stored_fingerprint or stored_fingerprint == _UNKNOWN:
        return True
    current = capability_fingerprint()
    if current == _UNKNOWN:
        return True
    return current != stored_fingerprint
