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
        type=int,
        help="Number of parallel workers"
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
    
    # Add parallel execution
    if args.parallel:
        pytest_cmd.extend(["-n", str(args.parallel)])
    
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
        # Check if linting tools are available
        linting_commands = [
            ("python -m black --check src tests", "Black code formatting check"),
            ("python -m isort --check-only src tests", "Import sorting check"),
            ("python -m flake8 src tests", "Flake8 linting"),
            ("python -m mypy src", "Type checking with mypy")
        ]
        
        for cmd, desc in linting_commands:
            try:
                subprocess.run("python -m black --version", shell=True, capture_output=True, check=True)
                run_command(cmd, desc)
            except subprocess.CalledProcessError:
                print(f"⚠️  Skipping {desc} - tool not installed")
    
    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())