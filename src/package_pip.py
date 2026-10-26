#!/usr/bin/env python3
"""Build the pip wheel for Kasal.

    python src/package_pip.py            # build frontend if missing or stale, then wheel
    python src/package_pip.py --skip-frontend
    python src/package_pip.py --sdist    # also build the sdist

Steps: ensure src/frontend_static exists and is newer than every file in
src/frontend/src (running the npm lifecycle when not),
strip __pycache__ from the backend tree (hatch force-include copies verbatim),
then `uv build` at the repository root. The wheel lands in dist/.
"""

import argparse
import shutil
import subprocess
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"


def _stale(index: Path) -> bool:
    """True when a UI source file changed after the last frontend build."""
    built = index.stat().st_mtime
    sources = (SRC / "frontend" / "src").rglob("*")
    return any(f.is_file() and f.stat().st_mtime > built for f in sources)


def ensure_frontend(skip: bool) -> None:
    index = SRC / "frontend_static" / "index.html"
    if index.exists() and not _stale(index):
        print(f"frontend_static present and current: {index.parent}")
        return
    if skip:
        if not index.exists():
            sys.exit("src/frontend_static missing and --skip-frontend given")
        print("WARNING: packaging a frontend_static older than src/frontend/src")
        return
    print("Building frontend (npm run build in src/)…")
    npm_cmd = "npm.cmd" if sys.platform == "win32" else "npm"
    subprocess.run([npm_cmd, "run", "build"], cwd=str(SRC), check=True)
    if not index.exists():
        sys.exit(
            "frontend build finished but src/frontend_static/index.html is missing"
        )


def strip_pycache() -> None:
    removed = 0
    for cache in (SRC / "backend" / "src").rglob("__pycache__"):
        shutil.rmtree(cache, ignore_errors=True)
        removed += 1
    print(f"Removed {removed} __pycache__ dirs from the backend tree")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-frontend", action="store_true")
    parser.add_argument("--sdist", action="store_true")
    args = parser.parse_args()

    ensure_frontend(args.skip_frontend)
    strip_pycache()
    build_args = ["uv", "build"] + ([] if args.sdist else ["--wheel"])
    subprocess.run(build_args, cwd=str(ROOT), check=True)
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    built = sorted((ROOT / "dist").glob(f"kasal-{version}[-.]*"))
    wheels = [path for path in built if path.suffix == ".whl"]
    if not wheels:
        sys.exit(f"no kasal {version} wheel produced")
    print(f"\nBuilt: {', '.join(str(path) for path in built)}")
    print(f"Install locally:  pip install {wheels[0]}")
    # Name the files: dist/ keeps older versions, which PyPI refuses to re-upload.
    print(f"Publish to PyPI:  uv publish dist/kasal-{version}*  (needs a PyPI token)")


if __name__ == "__main__":
    main()
