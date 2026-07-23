# test-frontend

Run frontend unit tests with Vitest.

## Command

```bash
cd src/frontend && npm test -- --run
```

## Description

This command:
1. Changes to the frontend directory
2. Runs all unit tests with Vitest (single run, no watch mode)

## Usage

Simply type `/test-frontend` in Claude Code to run the frontend tests.

## Options

For additional testing options:
- **Watch mode**: `npm test` (no `--run` flag)
- **Coverage**: `npm test -- --run --coverage`
- **Specific file**: `npm test -- --run src/path/to/file.test.tsx`
- **Pattern match**: `npm test -- --run --testNamePattern="pattern"`
- **Verbose output**: `npm test -- --run --reporter=verbose`

## Test Framework

- **Vitest** - Fast unit testing framework compatible with Jest API
- **React Testing Library** - Testing utilities for React components
- **JSDOM** - Browser environment simulation
