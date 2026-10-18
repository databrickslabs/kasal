"""Kasal has one version number, and every file that states it agrees.

``settings.VERSION`` is the one the app reports: it is the version on every
Databricks User-Agent and in the OpenAPI document. The release script refuses
to tag a number that differs from it.
"""

import json
import re
import tomllib
from pathlib import Path

from src.config.settings import settings

REPO_ROOT = Path(__file__).resolve().parents[5]


def _pyproject_version(path: Path) -> str:
    return str(tomllib.loads(path.read_text())["project"]["version"])


def test_the_version_is_plain_major_minor_patch():
    # The Databricks SDK raises on a version that is not semver when it builds
    # the User-Agent, and services call it at import time; X.Y.Z is stricter.
    assert re.fullmatch(r"\d+\.\d+\.\d+", settings.VERSION)


def test_every_file_states_the_same_version():
    frontend = REPO_ROOT / "src" / "frontend"
    lock = json.loads((frontend / "package-lock.json").read_text())
    stated = {
        "pyproject.toml": _pyproject_version(REPO_ROOT / "pyproject.toml"),
        "src/backend/pyproject.toml": _pyproject_version(
            REPO_ROOT / "src" / "backend" / "pyproject.toml"
        ),
        "src/frontend/package.json": json.loads(
            (frontend / "package.json").read_text()
        )["version"],
        "src/frontend/package-lock.json": lock["version"],
    }
    assert stated == dict.fromkeys(stated, settings.VERSION)
