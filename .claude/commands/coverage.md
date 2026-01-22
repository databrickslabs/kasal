# coverage

Run backend tests with full coverage report.

## Command

```bash
cd ~/workspace/kasal/src/backend && source ~/workspace/venv/bin/activate && python run_tests.py --coverage
```

## Description

This command:
1. Changes to the backend directory
2. Activates the virtual environment
3. Runs the full test suite with coverage analysis
4. Generates HTML coverage report

## Usage

Simply type `/coverage` in Claude Code to run tests with coverage.

## Options

For specific test types:
- Unit tests only: `python run_tests.py --type unit`
- Integration tests: `python run_tests.py --type integration`
- E2E tests: `python run_tests.py --type e2e`
- Skip linting: `python run_tests.py --coverage --skip-lint`

## Output

- Terminal: Test results and coverage summary
- HTML report: `htmlcov/index.html`

## Coverage Requirements

- Minimum 80% coverage required for backend code
