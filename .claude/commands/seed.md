# seed

Seed the database with initial data.

## Command

```bash
cd ~/workspace/kasal/src/backend && source ~/workspace/venv/bin/activate && python run_seeders.py
```

## Description

This command:
1. Changes to the backend directory
2. Activates the virtual environment
3. Runs all database seeders to populate initial data

## Usage

Simply type `/seed` in Claude Code to seed the database.

## What Gets Seeded

- Model configurations (LLM providers, models)
- Default tools and tool configurations
- Initial system settings
