"""Exercise the shared docs staging/publishing code and Python build entry point.

Node runs against temporary trees; npm installation and application compilation
are mocked only in the Python entry-point tests.
"""

import importlib.util
import os
import shutil
import subprocess
from pathlib import Path
from unittest import mock

import pytest

_REPO_SRC_DIR = Path(__file__).resolve().parents[3]
_BUILD_TASKS = _REPO_SRC_DIR / "scripts" / "build-tasks.cjs"


@pytest.fixture
def build_module():
    spec = importlib.util.spec_from_file_location(
        "kasal_build_under_test", _REPO_SRC_DIR / "build.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def source_tree(tmp_path):
    files = {
        "docs/index.md": "# Index\n",
        "docs/powerbi/foo.md": "# Foo\n",
        "docs/powerbi/diagram.png": "image bytes",
        "docs/archive/bar.md": "# Bar\n",
        "docs/assets/logo.SVG": "<svg/>\n",
        "docs/examples/workflow.json": '{"nodes": []}',
        "docs/css/extra.css": "body {}",
        "docs/x.txt": "not published",
        "docs/archive/notes.log": "not published",
        "frontend/public/favicon.ico": "icon bytes",
        "frontend/.generated/public/docs/deleted.md": "stale page",
        "frontend_static/docs/deleted.md": "stale page",
    }
    for name, content in files.items():
        file = tmp_path / name
        file.parent.mkdir(parents=True, exist_ok=True)
        file.write_text(content)
    return tmp_path


EXPECTED_DOCS = {
    "index.md",
    "powerbi/foo.md",
    "powerbi/diagram.png",
    "archive/bar.md",
    "assets/logo.SVG",
    "examples/workflow.json",
    "css/extra.css",
}


def _node(function, root):
    return subprocess.run(
        [
            "node",
            "-e",
            "require(process.argv[1])[process.argv[2]](process.argv[3]);",
            str(_BUILD_TASKS),
            function,
            str(root),
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def _files(root):
    return {
        file.relative_to(root).as_posix() for file in root.rglob("*") if file.is_file()
    }


def test_staging_preserves_nested_docs_assets_and_bytes(source_tree):
    _node("preparePublic", source_tree)
    staged = source_tree / "frontend/.generated/public"
    assert _files(staged / "docs") == EXPECTED_DOCS
    for name in EXPECTED_DOCS:
        assert (staged / "docs" / name).read_bytes() == (
            source_tree / "docs" / name
        ).read_bytes()
    assert (staged / "favicon.ico").read_text() == "icon bytes"
    # No generated docs leak back into the tracked public assets directory.
    assert _files(source_tree / "frontend/public") == {"favicon.ico"}


def test_rebuilding_removes_deleted_pages(source_tree):
    _node("preparePublic", source_tree)
    (source_tree / "docs/powerbi/foo.md").unlink()
    _node("preparePublic", source_tree)
    assert not (source_tree / "frontend/.generated/public/docs/powerbi/foo.md").exists()


def test_publish_uses_the_built_snapshot_including_docs(source_tree):
    _node("preparePublic", source_tree)
    dist = source_tree / "frontend/dist"
    shutil.copytree(source_tree / "frontend/.generated/public", dist)
    (dist / "index.html").write_text("<html></html>")
    # Source edits after compilation must not change the published snapshot.
    (source_tree / "docs/index.md").write_text("edited after build")
    _node("publishFrontend", source_tree)
    published = source_tree / "frontend_static"
    assert _files(published) == _files(dist)
    for name in _files(dist):
        assert (published / name).read_bytes() == (dist / name).read_bytes()


def test_missing_build_preserves_previous_static_output(source_tree):
    with pytest.raises(subprocess.CalledProcessError):
        _node("publishFrontend", source_tree)
    assert (source_tree / "frontend_static/docs/deleted.md").is_file()


def test_missing_docs_fails_before_replacing_generated_assets(source_tree):
    (source_tree / "docs").rename(source_tree / "docs-missing")
    with pytest.raises(subprocess.CalledProcessError):
        _node("preparePublic", source_tree)
    assert (source_tree / "frontend/.generated/public/docs/deleted.md").is_file()


@pytest.mark.parametrize("platform,npm", [("win32", "npm.cmd"), ("linux", "npm")])
def test_python_entry_uses_root_npm_lifecycle(build_module, source_tree, platform, npm):
    builder = build_module.Builder()
    builder.root_dir = source_tree
    (source_tree / "frontend_static/index.html").write_text("built")
    original_cwd = os.getcwd()
    with (
        mock.patch.object(build_module.sys, "platform", platform),
        mock.patch.object(build_module.subprocess, "run") as run,
    ):
        assert builder.build_frontend() is True
    run.assert_called_once_with([npm, "run", "build"], cwd=source_tree, check=True)
    assert os.getcwd() == original_cwd


def test_python_entry_rejects_missing_output(build_module, source_tree):
    builder = build_module.Builder()
    builder.root_dir = source_tree
    with mock.patch.object(build_module.subprocess, "run"):
        assert builder.build_frontend() is False


@pytest.mark.parametrize(
    "error", [FileNotFoundError("npm"), subprocess.CalledProcessError(1, "npm")]
)
def test_python_entry_propagates_build_failure(build_module, source_tree, error):
    builder = build_module.Builder()
    builder.root_dir = source_tree
    with mock.patch.object(build_module.subprocess, "run", side_effect=error):
        assert builder.run() is False
