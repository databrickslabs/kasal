#!/usr/bin/env python3
"""Build the pip wheel for Kasal.

    python src/package_pip.py            # build frontend if missing, then wheel
    python src/package_pip.py --skip-frontend
    python src/package_pip.py --sdist    # also build the sdist

Steps: ensure src/frontend_static exists (running the npm lifecycle when not),
strip __pycache__ from the backend tree (hatch force-include copies verbatim),
then `uv build` at the repository root. The wheel lands in dist/.
"""

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"


def ensure_frontend(skip: bool) -> None:
    index = SRC / "frontend_static" / "index.html"
    if index.exists():
        print(f"frontend_static present: {index.parent}")
        return
    if skip:
        sys.exit("src/frontend_static missing and --skip-frontend given")
    print("Building frontend (npm run build in src/)…")
    subprocess.run(["npm", "run", "build"], cwd=str(SRC), check=True)
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
    wheels = sorted((ROOT / "dist").glob("kasal-*.whl"))
    if not wheels:
        sys.exit("no wheel produced")
    print(f"\nBuilt: {wheels[-1]}")
    print("Install locally:  pip install " + str(wheels[-1]))
    print("Publish to PyPI:  uv publish  (needs a PyPI token)")


if __name__ == "__main__":
    main()
