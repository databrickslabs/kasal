# lint-backend

Run all backend linting and type checking tools.

## Command

```bash
cd ~/workspace/kasal/src/backend && source ~/workspace/venv/bin/activate && echo "Running black..." && black src tests --check && echo "Running isort..." && isort src tests --check-only && echo "Running ruff..." && ruff check src tests && echo "Running mypy..." && mypy src
```

## Description

This command runs all code quality tools:
1. **black** - Code formatting check
2. **isort** - Import sorting check
3. **ruff** - Fast linting (replaces flake8)
4. **mypy** - Static type checking

## Usage

Simply type `/lint-backend` in Claude Code to check code quality.

## Auto-fix Mode

To automatically fix issues:
- Format code: `black src tests`
- Sort imports: `isort src tests`
- Fix ruff issues: `ruff check src tests --fix`
