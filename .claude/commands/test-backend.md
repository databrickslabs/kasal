# test-backend

Run backend unit tests with coverage report.

## Command

```bash
cd ~/workspace/kasal/src/backend && source ~/workspace/venv/bin/activate && clear && python -m pytest tests/unit/ --cov=src --cov-report=html
```

## Description

This command:
1. Changes to the backend directory
2. Activates the virtual environment from `~/workspace/venv`
3. Clears the terminal
4. Runs unit tests with pytest
5. Generates coverage report in HTML format

## Usage

Simply type `/test-backend` in Claude Code to run the backend unit tests with coverage.

## Output

- Test results in terminal
- HTML coverage report in `htmlcov/` directory