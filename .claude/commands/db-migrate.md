# db-migrate

Run Alembic database migrations.

## Command

```bash
cd ~/workspace/kasal/src/backend && source ~/workspace/venv/bin/activate && alembic upgrade head
```

## Description

This command:
1. Changes to the backend directory
2. Activates the virtual environment
3. Runs all pending database migrations

## Usage

Simply type `/db-migrate` in Claude Code to apply pending migrations.

## Options

For additional migration operations:
- Create new migration: `alembic revision --autogenerate -m "description"`
- Downgrade one step: `alembic downgrade -1`
- Show current revision: `alembic current`
- Show migration history: `alembic history`
