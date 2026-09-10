"""Exercise startup from a clean environment, without Databricks or network IO."""

import json
import os
import shutil
import subprocess
import sys
import types
import zipfile
from pathlib import Path

import pytest

APP_ROOT = Path(__file__).resolve().parents[3]
# Execute the entrypoint's actual stdlib bootstrap, stopping before backend imports.
BOOTSTRAP = (
    (APP_ROOT / "entrypoint.py")
    .read_text()
    .split("# Add backend directory to path FIRST", 1)[0]
)

launcher = types.ModuleType("entrypoint_bootstrap")
launcher.__file__ = str(APP_ROOT / "entrypoint.py")
exec(compile(BOOTSTRAP, launcher.__file__, "exec"), launcher.__dict__)


def test_missing_manifests_report_actionable_diagnostic(tmp_path):
    with pytest.raises(RuntimeError, match="DEP-v1: Dependency files are missing"):
        launcher._dependency_project(tmp_path)


def test_incomplete_root_pair_does_not_silently_select_another_project(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[project]\n")
    backend = tmp_path / "backend"
    backend.mkdir()
    (backend / "pyproject.toml").touch()
    (backend / "uv.lock").touch()
    with pytest.raises(RuntimeError, match="DEP-v1: Incomplete dependency files"):
        launcher._dependency_project(tmp_path)


def test_missing_uv_reports_diagnostic_before_importing_backend(monkeypatch, capsys):
    monkeypatch.setenv("DATABRICKS_APP_NAME", "test-app")
    monkeypatch.delenv("KASAL_LOCKED_ENTRYPOINT", raising=False)
    monkeypatch.setattr(launcher.shutil, "which", lambda _: None)
    with pytest.raises(SystemExit) as result:
        launcher._ensure_databricks_environment()
    assert result.value.code == 1
    assert "DEP-v1: uv is unavailable" in capsys.readouterr().err


def test_local_startup_does_not_change_the_environment(monkeypatch):
    monkeypatch.delenv("DATABRICKS_APP_NAME", raising=False)
    monkeypatch.setattr(
        launcher.shutil, "which", lambda _: pytest.fail("local bootstrap")
    )
    launcher._ensure_databricks_environment()


def test_reexecuted_entrypoint_does_not_sync_again(monkeypatch):
    monkeypatch.setenv("DATABRICKS_APP_NAME", "test-app")
    monkeypatch.setenv(
        "KASAL_LOCKED_ENTRYPOINT", str(Path(launcher.__file__).resolve())
    )
    monkeypatch.setattr(
        launcher.shutil, "which", lambda _: pytest.fail("bootstrap loop")
    )
    launcher._ensure_databricks_environment()


@pytest.fixture
def uv_environment(tmp_path):
    uv = shutil.which("uv")
    if not uv:
        pytest.skip("uv is required for the offline startup integration test")
    env = {
        k: v
        for k, v in os.environ.items()
        if not k.startswith("UV_") and k != "KASAL_LOCKED_ENTRYPOINT"
    }
    env.update(
        DATABRICKS_APP_NAME="test-app",
        UV_CACHE_DIR=str(tmp_path / "cache"),
        UV_PYTHON=sys.executable,
        UV_PYTHON_DOWNLOADS="never",
        UV_OFFLINE="1",
        # Reproduce a host interpreter/environment different from the app's.
        VIRTUAL_ENV=sys.prefix,
    )
    return uv, env


@pytest.mark.parametrize("layout", ["bundle", "source"])
def test_launcher_installs_locked_dependency_and_uses_its_environment(
    tmp_path, uv_environment, layout
):
    uv, env = uv_environment
    root = tmp_path / "app"
    root.mkdir()
    project = root if layout == "bundle" else root / "backend"
    project.mkdir(exist_ok=True)
    # A real, minimal wheel makes this test offline and independent of caches.
    # The probe does not exist in the host interpreter; uv must install it.
    wheel = project / "kasal_runtime_probe-1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("kasal_runtime_probe.py", "VALUE = 'installed-from-lock'\n")
        archive.writestr(
            "kasal_runtime_probe-1.0.dist-info/METADATA",
            "Metadata-Version: 2.1\nName: kasal-runtime-probe\nVersion: 1.0\n",
        )
        archive.writestr(
            "kasal_runtime_probe-1.0.dist-info/WHEEL",
            "Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        )
        archive.writestr("kasal_runtime_probe-1.0.dist-info/RECORD", "")
    manifest = project / "pyproject.toml"
    manifest.write_text(
        '[project]\nname = "runtime-test"\nversion = "1.0"\n'
        'requires-python = ">=3.11"\ndependencies = ["kasal-runtime-probe==1.0"]\n'
        "[tool.uv]\npackage = false\n[tool.uv.sources]\n"
        f'kasal-runtime-probe = {{ path = "{wheel.name}" }}\n'
    )
    subprocess.run(
        [uv, "lock", "--project", str(project)],
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    lock_before = (project / "uv.lock").read_bytes()
    assert not (project / ".venv").exists()
    result_file = tmp_path / "result.json"
    (root / "entrypoint.py").write_text(
        BOOTSTRAP + "import json, os, sys, kasal_runtime_probe\n"
        "with open(sys.argv[1], 'w') as output:\n"
        "    json.dump({'prefix': sys.prefix, 'cwd': os.getcwd(), "
        "'dependency': kasal_runtime_probe.VALUE}, output)\n"
    )
    command = [sys.executable, str(root / "entrypoint.py"), str(result_file)]
    completed = subprocess.run(
        command,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert completed.returncode == 0, completed.stderr
    assert "DEP-v1: Synchronizing locked dependencies" in completed.stdout
    result = json.loads(result_file.read_text())
    assert Path(result["prefix"]) == project / ".venv"
    assert Path(result["cwd"]) == root
    assert result["dependency"] == "installed-from-lock"
    assert (project / "uv.lock").read_bytes() == lock_before

    # A mismatched lock must fail before serving the application, not resolve
    # new versions implicitly on one instance of a horizontally scaled app.
    manifest.write_text(
        manifest.read_text().replace(
            'dependencies = ["kasal-runtime-probe==1.0"]',
            'dependencies = ["kasal-runtime-probe==1.0", "missing-runtime-dependency==1.0"]',
        )
    )
    result_file.unlink()
    failed = subprocess.run(
        command,
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert failed.returncode != 0
    assert not result_file.exists()
    assert (project / "uv.lock").read_bytes() == lock_before
