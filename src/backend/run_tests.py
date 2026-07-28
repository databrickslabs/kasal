#!/usr/bin/env python3
"""
Test runner script for the backend.

This script provides a convenient way to run different types of tests
with various configurations and options.
"""
import argparse
import subprocess
import sys
import os
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors."""
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=False)
        print(f"✅ {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ {description} failed with exit code {e.returncode}")
        return False


def main():
    """Main test runner function."""
    parser = argparse.ArgumentParser(description="Run backend tests")
    parser.add_argument(
        "--type", 
        choices=["unit", "integration", "all"], 
        default="all",
        help="Type of tests to run"
    )
    parser.add_argument(
        "--coverage", 
        action="store_true",
        help="Generate coverage report"
    )
    parser.add_argument(
        "--html-coverage", 
        action="store_true",
        help="Generate HTML coverage report"
    )
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true",
        help="Verbose output"
    )
    parser.add_argument(
        "--parallel", "-n",
        default="auto",
        help=(
            "Parallel workers: 'auto' (default, one per core), a number, or "
            "'0'/'1' to run serially. Serial is ~9x slower on this suite "
            "(129s vs 15s) with no benefit outside step-through debugging."
        ),
    )
    parser.add_argument(
        "--markers", "-m",
        type=str,
        help="Run tests with specific markers (e.g., 'not slow')"
    )
    parser.add_argument(
        "--install-deps",
        action="store_true", 
        help="Install test dependencies before running tests"
    )
    
    args = parser.parse_args()
    
    # Change to backend directory
    backend_dir = Path(__file__).parent
    os.chdir(backend_dir)
    
    success = True
    
    # Install dependencies if requested
    if args.install_deps:
        success &= run_command(
            "pip install -r tests/requirements-test.txt",
            "Installing test dependencies"
        )
        if not success:
            return 1
    
    # Build pytest command
    pytest_cmd = ["python", "-m", "pytest"]
    
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
    
    # Add coverage options
    if args.coverage or args.html_coverage:
        pytest_cmd.extend([
            "--cov=src",
            "--cov-report=term-missing"
        ])
        
        if args.html_coverage:
            pytest_cmd.append("--cov-report=html:tests/coverage_html")
    
    # Run tests
    success &= run_command(
        " ".join(pytest_cmd),
        f"Running {args.type} tests"
    )
    
    if args.html_coverage and success:
        coverage_path = backend_dir / "tests" / "coverage_html" / "index.html"
        if coverage_path.exists():
            print(f"\n📊 HTML coverage report available at: {coverage_path}")
    
    # Run linting if running all tests
    if args.type == "all" and success:
        # (probe, command, description). The probe is how we decide the tool is
        # installed, and it must name THE TOOL BEING RUN. It used to probe
        # `black --version` before every command, so with black present the loop
        # happily ran `python -m flake8`, which is neither installed nor listed
        # in pyproject.toml — while ruff, which IS both, was never invoked at all.
        linting_commands = [
            ("python -m black --version",
             "python -m black --check src tests", "Black code formatting check"),
            ("python -m isort --version",
             "python -m isort --check-only src tests", "Import sorting check"),
            # Ruff replaces flake8 here. Configured in [tool.ruff.lint] as
            # select = ["E", "F"] — errors and pyflakes, no opinionated extras.
            # Import order stays with isort above, which is why I001 is not selected.
            ("python -m ruff --version",
             "python -m ruff check src tests", "Ruff linting"),
            ("python -m mypy --version",
             "python -m mypy src", "Type checking with mypy"),
            # Architecture contracts: routers > services > repositories > models,
            # and nothing below services imports upward. See [tool.importlinter]
            # in pyproject.toml — the ignore lists there are known violations
            # kept passing on purpose, and they are meant to shrink.
            ("lint-imports --help", "lint-imports", "Architecture contracts (import-linter)"),
        ]

        for probe, cmd, desc in linting_commands:
            try:
                subprocess.run(probe, shell=True, capture_output=True, check=True)
            except subprocess.CalledProcessError:
                print(f"⚠️  Skipping {desc} - tool not installed")
                continue
            run_command(cmd, desc)
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())