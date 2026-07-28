"""Getting skills in and out as folders.

Portability is the reason to adopt this format rather than invent one, and
portability is only real if the round trip works: a skill authored anywhere
imports here, and a skill authored here exports to something Claude Code,
Cursor, Codex or Gemini CLI reads unchanged.

Both directions treat the archive as hostile. A zip is attacker-controlled input
with three well-known abuses — path traversal via ``../`` entries, absolute
paths, and decompression bombs — and all three are refused here rather than
downstream.
"""

import hashlib
import io
import logging
import zipfile
from typing import Any, Dict, List, Tuple

from src.services.skills.loader import ALLOWED_PREFIXES, normalise_path
from src.services.skills.parser import ParsedSkill, parse, to_skill_md

logger = logging.getLogger(__name__)

#: A skill is instructions, not a dataset. These bound the damage a malicious or
#: merely careless archive can do; each is refused with the limit named, so an
#: author knows what to fix.
MAX_ARCHIVE_BYTES = 5 * 1024 * 1024
MAX_UNCOMPRESSED_BYTES = 20 * 1024 * 1024
MAX_FILE_BYTES = 1 * 1024 * 1024
MAX_FILES = 100


class SkillPackageError(ValueError):
    """An archive Kasal will not accept, with the reason an author can act on."""


def read_zip(data: bytes) -> Tuple[ParsedSkill, List[Dict[str, Any]]]:
    """A zip -> a validated skill and its bundled files.

    The archive may wrap the skill in a top-level directory (what you get from
    ``zip -r my-skill.zip my-skill``) or contain ``SKILL.md`` at the root (what
    you get from zipping the folder's *contents*). Both are what people actually
    produce, so both are accepted and the prefix is stripped.
    """
    if len(data) > MAX_ARCHIVE_BYTES:
        raise SkillPackageError(
            f"The archive is larger than {MAX_ARCHIVE_BYTES // 1024 // 1024}MB. "
            "A skill is instructions, not a dataset."
        )

    try:
        archive = zipfile.ZipFile(io.BytesIO(data))
    except zipfile.BadZipFile as exc:
        raise SkillPackageError(f"That is not a readable zip file: {exc}") from exc

    entries = [i for i in archive.infolist() if not i.is_dir()]
    if len(entries) > MAX_FILES:
        raise SkillPackageError(f"A skill may bundle at most {MAX_FILES} files.")

    total = sum(i.file_size for i in entries)
    if total > MAX_UNCOMPRESSED_BYTES:
        # Checked against the DECLARED sizes before reading anything, which is
        # what stops a bomb rather than detecting one after it has expanded.
        raise SkillPackageError(
            "The archive expands to more than "
            f"{MAX_UNCOMPRESSED_BYTES // 1024 // 1024}MB."
        )

    prefix = _common_prefix(entries)
    skill_md = None
    files: List[Dict[str, Any]] = []

    for info in entries:
        relative = info.filename[len(prefix) :] if prefix else info.filename
        relative = relative.replace("\\", "/").lstrip("/")
        if not relative or relative.startswith("__MACOSX/"):
            continue
        if info.file_size > MAX_FILE_BYTES:
            raise SkillPackageError(
                f"'{relative}' is larger than {MAX_FILE_BYTES // 1024}KB."
            )

        if relative == "SKILL.md":
            skill_md = _text(archive.read(info))
            continue

        if relative.startswith("scripts/"):
            # Not stored at all. Being able to READ a bundled script is the
            # first half of being able to run one, and execution needs a
            # sandbox and an approval model that do not exist yet.
            raise SkillPackageError(
                "This skill bundles scripts/, which Kasal does not accept yet. "
                "Executing skill code needs a sandbox and an approval model; "
                "remove scripts/ to import the instructions."
            )

        if not relative.startswith(ALLOWED_PREFIXES):
            logger.info(
                "[skills] ignoring '%s' — outside references/ and assets/", relative
            )
            continue

        try:
            path = normalise_path(relative)
        except Exception as exc:  # noqa: BLE001 — the loader's refusals
            raise SkillPackageError(str(exc)) from exc

        content = _text(archive.read(info))
        files.append(
            {
                "path": path,
                "content": content,
                "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
                "size_bytes": len(content.encode("utf-8")),
            }
        )

    if skill_md is None:
        raise SkillPackageError(
            "No SKILL.md in the archive. Every skill is a folder with a SKILL.md "
            "at its root."
        )

    parsed = parse(skill_md, name_hint=prefix.rstrip("/") or None)
    return parsed, files


def write_zip(skill: Any) -> bytes:
    """A stored skill -> a zip another Agent Skills client can read.

    Written under a directory named after the skill, because the spec requires
    the directory name and the frontmatter ``name`` to match — a flat archive
    would import as invalid everywhere else.
    """
    buffer = io.BytesIO()
    root = skill.name
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            f"{root}/SKILL.md",
            to_skill_md(
                skill.name,
                skill.description,
                skill.body or "",
                skill.license,
                skill.compatibility,
                skill.skill_metadata or {},
            ),
        )
        for stored in skill.files or []:
            archive.writestr(f"{root}/{stored.path}", stored.content or "")
    return buffer.getvalue()


def _common_prefix(entries: List[zipfile.ZipInfo]) -> str:
    """The wrapping directory, if the archive has exactly one."""
    tops = {i.filename.replace("\\", "/").split("/")[0] for i in entries}
    tops.discard("__MACOSX")
    if len(tops) != 1:
        return ""
    top = tops.pop()
    if any(i.filename.replace("\\", "/") == top for i in entries):
        return ""  # a single file at the root, not a directory
    return f"{top}/"


def _text(raw: bytes) -> str:
    """Decode a bundled file.

    Skills are text. A file that is not valid UTF-8 is refused rather than
    coerced, because silently replacing bytes would corrupt instructions a model
    is about to follow.
    """
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise SkillPackageError(
            "A bundled file is not valid UTF-8 text. Skills bundle instructions "
            "and references, not binaries."
        ) from exc
