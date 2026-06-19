"""
Unit tests for the recursive documentation copy behavior in src/build.py.

The change under test: ``Builder.build_frontend`` copies documentation
RECURSIVELY. Instead of copying only top-level ``*.md`` files from
``self.docs_dir``, it now walks ``self.docs_dir.rglob('*')`` and copies every
file whose suffix is in ``{'.md', '.png', '.jpg', '.jpeg', '.gif', '.svg'}``,
preserving the relative subdirectory structure under BOTH the frontend
``public/docs`` directory and the ``frontend_static/docs`` directory.

These tests drive the *real* ``build_frontend`` method with ``subprocess.run``
(npm install / npm run build) mocked out to no-ops, and with the path
attributes redirected to temp directories. The ``dist`` build directory is
pre-created so the method gets past its existence check and reaches both
docs-copy loops.

src/build.py lives at the repository root's ``src/`` directory (NOT inside the
backend package), so it is loaded here via importlib from its absolute path.
"""
import importlib.util
import sys
from pathlib import Path
from unittest import mock

import pytest


# --- Locate and import src/build.py by absolute path -----------------------
# tests/unit/test_build_docs_copy.py -> backend -> src -> repo_src (build.py)
_REPO_SRC_DIR = Path(__file__).resolve().parents[3]  # .../kasal/src
_BUILD_PY = _REPO_SRC_DIR / "build.py"


def _load_build_module():
    """Import the repo-root src/build.py as an isolated module object."""
    assert _BUILD_PY.is_file(), f"build.py not found at {_BUILD_PY}"
    spec = importlib.util.spec_from_file_location("kasal_build_under_test", _BUILD_PY)
    module = importlib.util.module_from_spec(spec)
    # Register so any internal references resolve; harmless and isolated by name.
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def build_module():
    return _load_build_module()


@pytest.fixture
def docs_tree(tmp_path):
    """
    Build a docs source tree with nested subdirectories, an image asset, and a
    non-doc file that must NOT be copied.

    Layout:
        docs/
            index.md                 (top-level doc -> copied)
            x.txt                    (non-doc -> NOT copied)
            powerbi/
                foo.md               (nested doc -> copied)
                diagram.png          (nested image -> copied)
            archive/
                bar.md               (deeper nested doc -> copied)
                notes.log            (non-doc -> NOT copied)
            assets/
                logo.SVG             (uppercase suffix image -> copied)
    """
    docs_dir = tmp_path / "docs"
    (docs_dir / "powerbi").mkdir(parents=True)
    (docs_dir / "archive").mkdir(parents=True)
    (docs_dir / "assets").mkdir(parents=True)

    (docs_dir / "index.md").write_text("# Index\n")
    (docs_dir / "x.txt").write_text("not a doc\n")
    (docs_dir / "powerbi" / "foo.md").write_text("# Foo\n")
    (docs_dir / "powerbi" / "diagram.png").write_bytes(b"\x89PNG\r\n")
    (docs_dir / "archive" / "bar.md").write_text("# Bar\n")
    (docs_dir / "archive" / "notes.log").write_text("log line\n")
    # Uppercase suffix to exercise the .lower() normalization in the filter.
    (docs_dir / "assets" / "logo.SVG").write_text("<svg/>\n")
    return docs_dir


def _run_build_frontend(build_module, tmp_path, docs_dir):
    """
    Run the real Builder.build_frontend with the path attributes redirected to
    temp dirs and subprocess.run mocked out. Returns (builder, return_value).
    """
    root_dir = tmp_path / "root"
    frontend_dir = root_dir / "frontend"
    frontend_dir.mkdir(parents=True)
    # build_frontend chdir's into frontend_dir, then back to root_dir in finally.
    # Both must exist as real directories.

    # Pre-create the Vite build output dir so the .exists() check passes and the
    # method continues to the frontend_static copy + second docs-copy loop.
    dist_dir = frontend_dir / "dist"
    dist_dir.mkdir()
    # Put one real artifact in dist so the build->frontend_static copy has work.
    (dist_dir / "index.html").write_text("<html></html>")

    builder = build_module.Builder()
    builder.root_dir = root_dir
    builder.frontend_dir = frontend_dir
    builder.docs_dir = docs_dir

    with mock.patch.object(build_module.subprocess, "run") as mock_run:
        mock_run.return_value = None  # npm install / npm run build -> no-op
        result = builder.build_frontend()

    return builder, result, mock_run


def _collect_rel_files(base: Path):
    """Return the set of POSIX-style relative paths of files under base."""
    return {
        p.relative_to(base).as_posix()
        for p in base.rglob("*")
        if p.is_file()
    }


EXPECTED_DOCS = {
    "index.md",
    "powerbi/foo.md",
    "powerbi/diagram.png",
    "archive/bar.md",
    "assets/logo.SVG",
}
EXCLUDED_DOCS = {"x.txt", "archive/notes.log"}


def test_build_frontend_succeeds_with_mocked_npm(build_module, tmp_path, docs_tree):
    """The method runs to completion (returns True) when npm is mocked out."""
    _builder, result, mock_run = _run_build_frontend(build_module, tmp_path, docs_tree)
    assert result is True
    # npm install + npm run build => subprocess.run called twice, never really run.
    assert mock_run.call_count == 2


def test_public_docs_recursive_copy(build_module, tmp_path, docs_tree):
    """public/docs gets the full nested tree of docs + images, preserving dirs."""
    builder, result, _ = _run_build_frontend(build_module, tmp_path, docs_tree)
    assert result is True

    public_docs = builder.frontend_dir / "public" / "docs"
    copied = _collect_rel_files(public_docs)

    assert copied == EXPECTED_DOCS


def test_public_docs_excludes_non_doc_files(build_module, tmp_path, docs_tree):
    """Non-doc files (.txt, .log) are NOT copied into public/docs."""
    builder, _result, _ = _run_build_frontend(build_module, tmp_path, docs_tree)
    public_docs = builder.frontend_dir / "public" / "docs"
    copied = _collect_rel_files(public_docs)

    for excluded in EXCLUDED_DOCS:
        assert excluded not in copied
    # And the offending source files definitely existed to begin with.
    assert (docs_tree / "x.txt").exists()
    assert (docs_tree / "archive" / "notes.log").exists()


def test_public_docs_preserves_nested_structure(build_module, tmp_path, docs_tree):
    """Nested files land under their subdirectory, not flattened to the root."""
    builder, _result, _ = _run_build_frontend(build_module, tmp_path, docs_tree)
    public_docs = builder.frontend_dir / "public" / "docs"

    # Nested doc lives under powerbi/, NOT at the top level.
    assert (public_docs / "powerbi" / "foo.md").is_file()
    assert not (public_docs / "foo.md").exists()
    # Deeper nested doc preserved too.
    assert (public_docs / "archive" / "bar.md").is_file()


def test_frontend_static_docs_recursive_copy(build_module, tmp_path, docs_tree):
    """frontend_static/docs receives the same recursive tree as public/docs."""
    builder, result, _ = _run_build_frontend(build_module, tmp_path, docs_tree)
    assert result is True

    static_docs = builder.root_dir / "frontend_static" / "docs"
    copied = _collect_rel_files(static_docs)

    assert copied == EXPECTED_DOCS
    for excluded in EXCLUDED_DOCS:
        assert excluded not in copied


def test_uppercase_image_suffix_is_copied(build_module, tmp_path, docs_tree):
    """Suffix matching is case-insensitive (logo.SVG matches '.svg')."""
    builder, _result, _ = _run_build_frontend(build_module, tmp_path, docs_tree)
    public_docs = builder.frontend_dir / "public" / "docs"
    static_docs = builder.root_dir / "frontend_static" / "docs"

    assert (public_docs / "assets" / "logo.SVG").is_file()
    assert (static_docs / "assets" / "logo.SVG").is_file()
