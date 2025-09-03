# test-frontend

Run frontend tests with React Testing Library.

## Command

```bash
cd src/frontend && clear && npm test
```

## Description

This command:
1. Changes to the frontend directory
2. Clears the terminal
3. Runs React tests using react-scripts test

## Usage

Simply type `/test-frontend` in Claude Code to run the frontend tests.

## Options

For additional testing options:
- Coverage: `npm test -- --coverage`
- Watch mode off: `npm test -- --watchAll=false`
- Specific test: `npm test -- --testNamePattern="TestName"`