"""Zip in, folder out.

An uploaded zip is attacker-controlled input, so most of this is about what
happens when it is hostile. The rest is the round trip, which is the only thing
that makes "portable" true rather than aspirational.
"""

import io
import zipfile

import pytest

from src.services.skills import packaging
from src.services.skills.parser import to_skill_md


def _zip(entries: dict) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for path, content in entries.items():
            archive.writestr(path, content)
    return buffer.getvalue()


def _skill_md(name="pricing", description="How we price a deal.", body="# Steps\n"):
    return to_skill_md(name, description, body)


class TestIngest:
    def test_a_folder_wrapped_archive_is_accepted(self):
        """What you get from `zip -r my-skill.zip my-skill`."""
        parsed, files = packaging.read_zip(
            _zip(
                {
                    "pricing/SKILL.md": _skill_md(),
                    "pricing/references/margins.md": "# Margins",
                }
            )
        )
        assert parsed.name == "pricing"
        assert [f["path"] for f in files] == ["references/margins.md"]

    def test_a_flat_archive_is_also_accepted(self):
        """What you get from zipping the folder's CONTENTS. Both are things
        people actually produce."""
        parsed, _ = packaging.read_zip(_zip({"SKILL.md": _skill_md()}))
        assert parsed.name == "pricing"

    def test_an_archive_without_a_skill_md_is_refused(self):
        with pytest.raises(packaging.SkillPackageError, match="No SKILL.md"):
            packaging.read_zip(_zip({"pricing/references/a.md": "x"}))

    def test_macos_metadata_is_ignored_rather_than_refused(self):
        """__MACOSX turns up in every archive made from Finder; refusing it
        would reject most real uploads."""
        parsed, files = packaging.read_zip(
            _zip(
                {
                    "pricing/SKILL.md": _skill_md(),
                    "__MACOSX/pricing/._SKILL.md": "junk",
                }
            )
        )
        assert parsed.name == "pricing"
        assert files == []

    def test_files_outside_references_and_assets_are_dropped(self):
        _, files = packaging.read_zip(
            _zip({"pricing/SKILL.md": _skill_md(), "pricing/notes.txt": "x"})
        )
        assert files == []


class TestHostileArchives:
    def test_scripts_are_refused_with_the_reason(self):
        """Not silently dropped: an author who bundled scripts needs to know
        they will not run, or they will assume the skill is doing something it
        is not."""
        with pytest.raises(packaging.SkillPackageError, match="sandbox"):
            packaging.read_zip(
                _zip({"pricing/SKILL.md": _skill_md(), "pricing/scripts/run.py": "x"})
            )

    def test_a_traversal_entry_is_refused(self):
        with pytest.raises(packaging.SkillPackageError):
            packaging.read_zip(
                _zip(
                    {
                        "pricing/SKILL.md": _skill_md(),
                        "pricing/references/../../../etc/passwd": "x",
                    }
                )
            )

    def test_too_many_files_is_refused(self):
        entries = {"pricing/SKILL.md": _skill_md()}
        entries.update(
            {f"pricing/references/f{i}.md": "x" for i in range(packaging.MAX_FILES + 1)}
        )
        with pytest.raises(packaging.SkillPackageError, match="at most"):
            packaging.read_zip(_zip(entries))

    def test_a_decompression_bomb_is_refused_before_it_is_read(self):
        """Checked against the DECLARED sizes. Detecting it after expansion is
        detecting it too late."""
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("pricing/SKILL.md", _skill_md())
            archive.writestr(
                "pricing/references/big.md",
                "a" * (packaging.MAX_UNCOMPRESSED_BYTES + 1),
            )
        with pytest.raises(packaging.SkillPackageError, match="expands"):
            packaging.read_zip(buffer.getvalue())

    def test_a_binary_file_is_refused_rather_than_coerced(self):
        """Silently replacing bytes would corrupt instructions a model is about
        to follow."""
        with pytest.raises(packaging.SkillPackageError, match="UTF-8"):
            packaging.read_zip(
                _zip(
                    {
                        "pricing/SKILL.md": _skill_md(),
                        "pricing/assets/logo.png": b"\x89PNG\x00\xff\xfe",
                    }
                )
            )

    def test_a_file_that_is_not_a_zip_is_refused(self):
        with pytest.raises(packaging.SkillPackageError, match="readable zip"):
            packaging.read_zip(b"not a zip at all")


class TestRoundTrip:
    def test_what_is_exported_can_be_imported_again(self):
        """The only test that makes "portable" mean anything: the artefact Kasal
        produces has to be one Kasal — and therefore any client — accepts."""
        from types import SimpleNamespace

        skill = SimpleNamespace(
            name="pricing",
            description="How we price a deal: margins, floors, approvals.",
            body="# Steps\n1. Check the floor\n",
            license="MIT",
            compatibility=None,
            skill_metadata={"author": "kasal"},
            files=[SimpleNamespace(path="references/margins.md", content="# Margins")],
        )

        parsed, files = packaging.read_zip(packaging.write_zip(skill))

        assert parsed.name == skill.name
        assert parsed.description == skill.description
        assert parsed.body.strip() == skill.body.strip()
        assert parsed.license == "MIT"
        assert parsed.metadata["author"] == "kasal"
        assert files[0]["path"] == "references/margins.md"

    def test_the_export_puts_the_skill_in_a_directory_named_after_it(self):
        """The spec requires the directory name and the frontmatter name to
        match — a flat archive imports as invalid everywhere else."""
        from types import SimpleNamespace

        data = packaging.write_zip(
            SimpleNamespace(
                name="pricing",
                description="d",
                body="b",
                license=None,
                compatibility=None,
                skill_metadata={},
                files=[],
            )
        )
        assert zipfile.ZipFile(io.BytesIO(data)).namelist() == ["pricing/SKILL.md"]
