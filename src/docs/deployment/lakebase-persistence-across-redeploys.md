# Lakebase connection does not survive redeploys — diagnosis & fix options

**Status: DIAGNOSIS + PROPOSAL — not yet decided, not yet built.** Deploy-path
changes are higher-risk than app code; this doc exists so the fix is chosen
deliberately, not patched blindly.

Date: 2026-07-16.

## Symptom

After every redeploy of Kasal (Databricks App) connected to a Lakebase, the app
comes up as if it has no data: saved crews, per-tool parameters (`workspace_id`,
`dataset_id`, `catalog`, `report_id`, …) are gone, and tools error until the user
re-enters the Lakebase connection in the UI — at which point the saved data
reappears. Re-entering **credentials** is acceptable (by design). Re-entering
**non-secret operational params** is not — that's the friction to remove.

## Root cause

The data was never lost. **The app forgets *where* the data lives.**

Which database Kasal uses is decided at runtime by
`db/database_router.is_lakebase_enabled()`, which reads a single `lakebase` row
from a **local SQLite file** (`./app.db`), via
`get_lakebase_config_from_db()`:

```
# database_router.py
"SELECT value FROM database_configs WHERE key = 'lakebase'"   # from ./app.db (SQLite)
```

and its own docstring states the design intent:

> `is_lakebase_enabled()` … Database is the single source of truth — **no
> environment variable overrides.**

The chain that produces the symptom:

1. `src/app.yaml` pins **no database resource** — there is no `resources:` /
   `valueFrom` block. The deployed app boots knowing nothing about which Lakebase
   to use.
2. The Lakebase pointer lives **only** in the `database_configs` row inside the
   local `./app.db` SQLite file (written when the user configures Lakebase in the
   UI, via `POST /lakebase/config` → `LakebaseService.save_config`).
3. On a Databricks App, that SQLite file sits in the **container filesystem, which
   is ephemeral** — a redeploy gives a fresh container, so `./app.db` is empty.
4. Empty `app.db` → `get_lakebase_config_from_db()` returns `None` →
   `is_lakebase_enabled()` returns `False` → the app silently falls back to a
   fresh local SQLite → every saved crew / `tool_configs` (which live in Lakebase)
   is invisible → tools error on the missing params.
5. The user re-enters the Lakebase connection → the config row is rewritten into
   the new `app.db` → the router flips to Lakebase → data reappears. Until the
   next redeploy.

**So `workspace_id` & co. reset because they are stored in crew/agent/task
`tool_configs` inside Lakebase (real DB columns — verified in `models/task.py`,
`agent.py`, and the crew/task/agent save schemas), and after a redeploy the app
is pointed at an empty local SQLite instead of Lakebase.** The params persist
correctly; the *connection to the store that holds them* does not.

This is the same fragility recorded earlier around Lakebase cross-app ownership —
the deploy-time Lakebase connection state is not durable.

## Why the env var doesn't already save us

`LAKEBASE_INSTANCE_NAME` **is** read (`database_router.py:130,180`;
`lakebase_session.py:632`) — but only to pick the *instance name* during
activation, which happens **after** `is_lakebase_enabled()` has already returned
`True`. The enable gate itself is SQLite-only by explicit design ("no environment
variable overrides"). So setting the env var alone does not turn Lakebase on; the
SQLite row is still required. That gate is the precise thing to change.

## Fix options

Ordered cleanest-first. All aim at the same goal: **the Lakebase pointer must
survive a fresh container.**

### Option A — Pin the Lakebase as a Databricks App resource (proper fix)

Declare the database as an app `resource` so the platform injects its coordinates
as env at startup, and let `is_lakebase_enabled()` treat that env as a valid
enable source.

- `deploy.py` / `app.yaml`: add a database `resources:` entry (Databricks Apps
  support database resources injected as env vars).
- `is_lakebase_enabled()`: accept an env-provided instance/endpoint as an enable
  path in addition to the SQLite row (relax the "SQLite is the only source of
  truth" rule to "SQLite **or** injected app-resource env").
- **Pro**: redeploy-proof by construction; no writable-file dependency; matches
  the platform's intended resource model.
- **Con**: requires confirming the deploy target's `resources:` support for
  databases and the exact injected env var names; touches the enable gate (the
  single most load-bearing line in the DB router) — needs careful testing incl.
  the subprocess Lakebase re-activation path.
- **Risk**: Medium. **Durability: Best.**

### Option B — Seed the SQLite config row from env at startup (small bridge)

Keep SQLite as the runtime source of truth, but if the `lakebase` row is missing
**and** Lakebase env vars are present, write the row from env before the router
first reads it.

- `deploy.py`: write `LAKEBASE_INSTANCE_NAME` (+ endpoint/catalog) into `app.yaml`
  `environment_vars`.
- Startup hook (in `session.py` init or app lifespan): if
  `get_lakebase_config_from_db()` is `None` and the env vars exist, call
  `save_config(...)` to seed the row, then proceed.
- **Pro**: minimal, localized; leaves the router's read path unchanged; easy to
  reason about and test.
- **Con**: still round-trips through the ephemeral SQLite file (re-seeded every
  boot); a bit of a workaround rather than the platform-native model.
- **Risk**: Low. **Durability: Good (re-seeds each boot).**

### Option C — Persist `app.db` on a durable volume

Mount `./app.db` on a persistent volume so the SQLite file (and its config row)
survives redeploys.

- **Pro**: no code change to the router; the existing design just becomes durable.
- **Con**: leaves the ephemeral-file assumption in place; volume mounting for a
  single-writer SQLite file on Databricks Apps is awkward and can introduce
  locking/concurrency issues; least aligned with the platform model.
- **Risk**: Medium (ops), and doesn't address the underlying "pointer in a local
  file" smell. **Durability: Depends on volume setup.**

## Recommendation

- **Ship Option B first** as the low-risk bridge — it removes the user-facing pain
  (params/crews reappear automatically after redeploy) with a small, well-scoped
  change and no rewrite of the enable gate.
- **Plan Option A as the real fix** once the deploy target's database-resource
  support and injected env-var contract are confirmed — it removes the
  ephemeral-file dependency entirely and is the platform-native design.
- **Avoid Option C** unless A/B are both blocked; it preserves the smell.

B and A are complementary: B can seed from the same env vars A would inject, so
doing B now does not throw away work toward A.

## Verification results (checked 2026-07-16)

All four questions answered — code-side from the repo, platform-side from the
Databricks Apps docs.

### Q1 — Does the App support a database resource, and what env does it inject? ✅ Yes (with a caveat)

Databricks Apps support a Lakebase resource: **`postgres`** (Lakebase
Autoscaling) and **`database`** (Lakebase Provisioned). It is attached via the
Apps UI or a bundle and referenced in `app.yaml` with `valueFrom`:

```yaml
env:
  - name: DB_HOST
    valueFrom: my_database_resource   # a database valueFrom resolves to the Lakebase Host (Provisioned)
```

**Caveat (a real input to Option A):** the docs confirm the mechanism and that a
database `valueFrom` resolves to the **Host**, but they do **not** enumerate the
full injected Postgres var set (PGHOST/PGPORT/PGDATABASE/PGUSER/…). So Option A's
exact env contract must be **discovered empirically** (attach a resource, dump the
runtime env) rather than taken from docs. Not a blocker — but A needs a
discovery step first.
Sources: Databricks Apps "Add resources", "Define environment variables",
"App development" docs.

### Q2 — What must the seeded config row contain? ✅ Known exactly

Confirmed against `lakebase_service.save_config`/`get_config`: the row holds
`enabled`, `instance_name`, `instance_status`, `endpoint`, `database_type`.
Cross-referenced with the gate (`database_router.py:84-92`), a valid seed is:

```json
{ "enabled": true, "endpoint": "<read_write_dns>",
  "database_type": "lakebase", "instance_status": "READY",
  "instance_name": "kasal-lakebase" }
```

(minimum: `enabled` + `endpoint` + one of `database_type=="lakebase"` /
`instance_status=="READY"` / `migration_completed==true`). **Option B's env seed
therefore needs only the endpoint DNS + instance name** — which maps directly onto
the app-resource Host from Q1.

### Q3 — Subprocess path ✅ Covered by B; A needs an env check

`activate_lakebase_in_subprocess()` calls the **same** `is_lakebase_enabled()` +
`get_lakebase_config_from_db()` (SQLite read), then falls back to
`LAKEBASE_INSTANCE_NAME`. Option B is automatically covered — the parent re-seeds
the shared `app.db` at startup and the subprocess reads the same file. Option A
must confirm the injected env reaches the spawned interpreter (env is inherited on
`multiprocessing.spawn`, so likely fine, but verify).

### Q4 — First-run / no-Lakebase fallback ✅ Intact under both options

When `is_lakebase_enabled()` is `False`, the session factory uses
`settings.DATABASE_URI` (local SQLite, StaticPool/WAL). Both options skip the seed
when no env + no row exist, so fresh dev installs boot on SQLite exactly as today.

### Net effect on the recommendation

- **Option B is fully de-risked**: env contract is just endpoint+instance,
  subprocess is covered, fallback intact. Ready to build.
- **Option A gains one prerequisite**: an empirical env-var discovery step, because
  the docs don't publish the full injected PG var set. Still the best long-term
  fix, just not doc-driven.

## Key code references

- `src/backend/src/db/database_router.py`
  - `get_lakebase_config_from_db()` — reads `database_configs` row from `./app.db`
  - `is_lakebase_enabled()` — the SQLite-only enable gate ("no env overrides")
  - `activate_lakebase_in_subprocess()` — subprocess re-activation
- `src/backend/src/db/session.py` — session factory / startup init
- `src/backend/src/api/database_management_router.py::save_lakebase_config` →
  `LakebaseService.save_config` — writes the config row
- `src/backend/src/models/database_config.py` — `database_configs` table
- `src/deploy.py` — builds the bundle; verifies `app.yaml`; sets OAuth scopes
  (already includes `postgres`)
- `src/app.yaml` — deployed env; currently pins **no** database resource
- `src/backend/src/models/task.py` / `agent.py` — `tool_configs` JSON columns
  (where `workspace_id` & co. actually live, inside whichever DB is active)

## See also
- `src/backend/CLAUDE.md` — DB type resolution, `USE_NULLPOOL`, subprocess Lakebase re-activation
- Memory: "Lakebase cross-app ownership" — related deploy-time Lakebase fragility
