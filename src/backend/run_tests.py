#!/usr/bin/env python3
"""
Test runner script for the backend.

This script provides a convenient way to run different types of tests
with various configurations and options.
"""

import argparse
import os
import shlex
import subprocess
import sys
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {shlex.join(command)}")
    print(f"{'='*60}")

    try:
        subprocess.run(command, check=True)
        print(f"✅ {description} completed successfully")
        return True
    except OSError as e:
        print(f"{description} could not run: {e}")
        return False
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False


def run_tests(args, backend_dir):
    """Run pytest with the selected scope, parallelism and coverage options."""
    pytest_cmd = [sys.executable, "-m", "pytest"]

    # Add test directories based on type
    if args.type == "unit":
        pytest_cmd.append("tests/unit")
    elif args.type == "integration":
        pytest_cmd.append("tests/integration")
    else:  # all
        pytest_cmd.append("tests")

    # Add verbosity
    if args.verbose:
        pytest_cmd.append("-v")

    # Parallel by default. --dist loadfile keeps each FILE on one worker:
    # several suites stub sys.modules or set env at module scope, which is only
    # safe if their tests share a process.
    if str(args.parallel) not in ("0", "1", "", "none", "None"):
        pytest_cmd.extend(["-n", str(args.parallel), "--dist", "loadfile"])

    # Add markers
    if args.markers:
        pytest_cmd.extend(["-m", args.markers])

    # Add coverage options. Scope, branch mode and the (not yet set)
    # fail_under floor live in [tool.coverage.*] in pyproject.toml.
    # coverage.xml is what CI uploads as an artifact.
    if args.coverage or args.html_coverage:
        pytest_cmd.extend(
            ["--cov=src", "--cov-report=term-missing", "--cov-report=xml:coverage.xml"]
        )

        if args.html_coverage:
            pytest_cmd.append("--cov-report=html:tests/coverage_html")

    success = run_command(pytest_cmd, f"Running {args.type} tests")

    if args.html_coverage and success:
        coverage_path = backend_dir / "tests" / "coverage_html" / "index.html"
        if coverage_path.exists():
            print(f"\n📊 HTML coverage report available at: {coverage_path}")
    return success


def run_linting():
    """Run every static check and report all failures, not just the first."""
    python_module = [sys.executable, "-m"]
    targets = ["src", "tests", "run_tests.py", "check_types.py"]
    linting_commands = [
        (python_module + ["black", "--check", *targets], "Black formatting"),
        (python_module + ["isort", "--check-only", *targets], "Import sorting"),
        (python_module + ["ruff", "check", *targets], "Ruff linting"),
        (
            [sys.executable, "check_types.py"],
            "Mypy: no new errors against the documented baseline",
        ),
        (
            [str(Path(sys.executable).parent / "lint-imports")],
            "Architecture contracts",
        ),
    ]
    success = True
    for command, description in linting_commands:
        success &= run_command(command, description)
    return success


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="Run backend tests")
    parser.add_argument(
        "--type",
        choices=["unit", "integration", "all"],
        default="all",
        help="Type of tests to run",
    )
    parser.add_argument(
        "--coverage", action="store_true", help="Generate coverage report"
    )
    parser.add_argument(
        "--html-coverage", action="store_true", help="Generate HTML coverage report"
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    parser.add_argument(
        "--parallel",
        "-n",
        default="auto",
        help=(
            "Parallel workers: 'auto' (default, one per core), a number, or "
            "'0'/'1' to run serially. Serial is ~9x slower on this suite "
            "(129s vs 15s) with no benefit outside step-through debugging."
        ),
    )
    parser.add_argument(
        "--markers",
        "-m",
        type=str,
        help="Run tests with specific markers (e.g., 'not slow')",
    )
    lint_mode = parser.add_mutually_exclusive_group()
    lint_mode.add_argument(
        "--skip-lint",
        action="store_true",
        help="Run only the tests (CI runs lint as its own parallel job)",
    )
    lint_mode.add_argument(
        "--lint-only",
        action="store_true",
        help="Run only formatting, lint, typing baseline and architecture checks",
    )
    parser.add_argument(
        "--install-deps",
        action="store_true",
        help="Install test dependencies before running tests",
    )

    args = parser.parse_args()

    # Change to backend directory
    backend_dir = Path(__file__).parent
    os.chdir(backend_dir)

    success = True

    # Install dependencies if requested
    if args.install_deps:
        success &= run_command(
            ["uv", "sync", "--frozen"], "Installing test dependencies"
        )
        if not success:
            return 1

    if not args.lint_only:
        success &= run_tests(args, backend_dir)

    # Lint runs whether or not the tests passed, so a PR with a failing test
    # still gets formatting/lint/typing feedback in the same run.
    if args.lint_only or (args.type == "all" and not args.skip_lint):
        success &= run_linting()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
