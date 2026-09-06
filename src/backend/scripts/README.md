# Backend operational scripts

These scripts are run manually by an operator. They live outside `src/` because
they are not imported application code. Normal Databricks deployments and pip
wheels include the application package, so these scripts must be copied
explicitly when needed in a deployed environment.

Run maintenance commands from `src/backend`, using the app's configured
environment and virtualenv. Module invocation keeps the backend root on the
Python import path without script-specific `sys.path` changes.

| Module | Purpose | Effect |
| --- | --- | --- |
| `scripts.maintenance.create_powerbi_extraction_table` | Create a missing Power BI extraction table and indexes | Database DDL; supports `--dry-run` |
| `scripts.maintenance.fix_prompttemplate_sqlite_rebuild` | Remove an implicit unique constraint on prompt names | Rebuilds the SQLite table, preserving its rows |
| `scripts.maintenance.drop_prompttemplate_unique_sqlite` | Remove a unique prompt-name index | Changes SQLite indexes |
| `scripts.maintenance.migrate_roles_to_3tier` | Convert legacy role assignments and definitions | Updates database roles |
| `scripts.maintenance.clear_bi_specialist_memory` | Clear a workspace's default local memory store | Deletes local memory; supports `--dry-run` |
| `scripts.diagnostics.diagnose_prompt_registry` | Diagnose Unity Catalog prompt-registry access | Creates a probe prompt and attempts to delete it |

For example, inspect the proposed Power BI table DDL without executing it:

```bash
.venv/bin/python -m scripts.maintenance.create_powerbi_extraction_table --dry-run
```

Each script documents its arguments. Select the intended environment and review
the operation before running it. Diagnostics can perform writes: the prompt
registry probe requires create/delete privileges and may remain if cleanup
fails.

This directory does not replace Alembic revision history under `migrations/`
or deployed startup schema repair under `src/db/self_heal/`. Frontend build
utilities belong in `src/scripts/` at the repository level.
