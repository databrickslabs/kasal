# Deploying Kasal with Lakebase (Persistent PostgreSQL)

By default, Kasal uses SQLite stored at `./app.db` inside the Databricks Apps container. **This file is wiped on every restart or redeployment.** Lakebase provides managed PostgreSQL that persists independently — crews, agents, tasks, runs, and all configuration survive restarts.

---

## Quick start: deploy without Lakebase (SQLite)

If you just want to try Kasal and don't need data to survive restarts, use SQLite. No Lakebase instance, no secrets, no `libpq` — just deploy and run.

### 1. Set `src/app.yaml` to SQLite mode

Replace the contents of `src/app.yaml` with:

```yaml
command: ['python', 'entrypoint.py']

env:
  - name: PYTHONPATH
    value: './backend'
  - name: PYTHONUNBUFFERED
    value: '1'
  - name: FRONTEND_STATIC_DIR
    value: './frontend_static'
  - name: KASAL_LOG_LEVEL
    value: 'INFO'
  - name: KASAL_LOG_APP
    value: 'INFO'
  - name: KASAL_LOG_THIRD_PARTY
    value: 'WARNING'
  - name: KASAL_LOG_CONSOLE
    value: 'true'
  - name: KASAL_LOG_FILE
    value: 'true'
```

Key differences from the Lakebase config:
- `command` has no `--db-type postgres` — defaults to SQLite
- No `DATABASE_TYPE`, `POSTGRES_*` env vars
- No `resources` section (no secrets needed)
- No `apt_packages` (no `libpq-dev` required)

### 2. Deploy

```bash
DATABRICKS_HOST="https://e2-demo-field-eng.cloud.databricks.com" \
DATABRICKS_TOKEN="dapi-xxxxx" \
python3 src/deploy.py \
    --app-name kasal-dev \
    --user-name you@databricks.com
```

`deploy.py` reads `DATABRICKS_HOST` and `DATABRICKS_TOKEN` from the environment automatically. Alternatively, pass them as flags: `--host` and `--token`.

> **Data loss warning**: SQLite is stored inside the app container. Every restart and every redeployment wipes all crews, agents, tasks, and run history. Use Lakebase (see below) for anything you want to keep.

---

## Important: The current `src/app.yaml` is personal

The checked-in `src/app.yaml` is configured for a specific workspace and Databricks user. Before deploying to your own workspace, you must update it.

Fields that need to change:

| Field | What it contains | What to change it to |
|-------|-----------------|----------------------|
| `POSTGRES_USER` | The Databricks user email used to authenticate to Lakebase | Your own `user@databricks.com` |
| `LAKEBASE_INSTANCE_NAME` | The Lakebase instance name | Your instance name (default: `kasal-db`) |
| `POSTGRES_DB` | The PostgreSQL database name | Your database name (default: `kasal`) |
| `resources[].scope` | The Databricks secret scope holding credentials | Your secret scope name (default: `kasal`) |

---

## Two approaches for `app.yaml`

### Option A — Production (recommended): Databricks Secrets

This is what the current `src/app.yaml` uses. Credentials are **never stored in the file** — they are resolved from a Databricks secret scope at runtime. Safe to commit to git.

```yaml
command: ['python', 'entrypoint.py', '--db-type', 'postgres']

env:
  - name: DATABASE_TYPE
    value: 'postgres'
  - name: POSTGRES_SERVER
    valueFrom: lakebase-server        # resolved from secret scope at runtime
  - name: POSTGRES_USER
    value: 'you@databricks.com'       # <-- change this to your email
  - name: POSTGRES_PASSWORD
    valueFrom: lakebase-pat           # resolved from secret scope at runtime
  - name: POSTGRES_DB
    value: 'kasal'
  - name: POSTGRES_PORT
    value: '5432'
  - name: POSTGRES_SSL
    value: 'true'
  - name: LAKEBASE_INSTANCE_NAME
    value: 'kasal-db'                 # <-- change if using a different instance name

resources:
  - name: lakebase-server
    type: secret
    scope: kasal                      # <-- change to your secret scope name
    key: lakebase_server
  - name: lakebase-pat
    type: secret
    scope: kasal                      # <-- change to your secret scope name
    key: lakebase_pat

apt_packages:
  - libpq-dev
```

Required secrets in the scope:

| Key | Value |
|-----|-------|
| `lakebase_server` | The `read_write_dns` hostname from your Lakebase instance |
| `lakebase_pat` | A long-lived Databricks PAT token (`dapi...`) |

### Option B — Quick development: inline values

Use `app.yaml.template` from the `kasal-lakebase` repo as a starting point. Credentials are stored inline. **Do not commit this to git** — add `app.yaml` to `.gitignore` while using this approach.

```yaml
command: ['python', 'entrypoint.py', '--db-type', 'postgres']

env:
  - name: DATABASE_TYPE
    value: 'postgres'
  - name: POSTGRES_SERVER
    value: '<read_write_dns from setup-lakebase.sh>'
  - name: POSTGRES_USER
    value: 'you@databricks.com'
  - name: POSTGRES_PASSWORD
    value: 'dapi<your-pat>'
  - name: POSTGRES_DB
    value: 'kasal'
  - name: POSTGRES_PORT
    value: '5432'
  - name: POSTGRES_SSL
    value: 'true'
  - name: LAKEBASE_INSTANCE_NAME
    value: 'kasal-db'

apt_packages:
  - libpq-dev
```

---

## Setup steps

### Prerequisites

- Databricks CLI installed and on `$PATH`
- `psql` client installed (for database creation step)
- Access to a Databricks workspace with Lakebase enabled

### Step 1 — Provision the Lakebase instance

Use the scripts in [`/workspace/demos/kasal-lakebase`](../../demos/kasal-lakebase):

```bash
cd /path/to/demos/kasal-lakebase

./setup-lakebase.sh \
  --host https://<your-workspace>.cloud.databricks.com \
  --token dapi<your-pat>
```

This script:
1. Creates a `kasal-db` Lakebase instance (CU_1 capacity)
2. Waits until the instance is `AVAILABLE`
3. Creates the `kasal` PostgreSQL database
4. Prints the `read_write_dns` hostname you need for Step 2

Options: `--instance <name>`, `--database <name>`, `--capacity <CU_1|CU_2|...>`

### Step 2 — Create the Databricks secret scope and store credentials

```bash
# Create scope (skip if it already exists)
databricks secrets create-scope kasal

# Store the Lakebase hostname (from setup-lakebase.sh output)
databricks secrets put-secret kasal lakebase_server \
  --string-value "<read_write_dns>"

# Store a long-lived PAT token
databricks secrets put-secret kasal lakebase_pat \
  --string-value "dapi<your-pat>"

# Verify
databricks secrets list-secrets kasal
# Expected: lakebase_server, lakebase_pat
```

### Step 3 — Update `src/app.yaml`

Using Option A (secrets-based):
- Change `POSTGRES_USER` to your Databricks email
- Change `resources[].scope` to your secret scope name if different from `kasal`
- Change `LAKEBASE_INSTANCE_NAME` if you used a different instance name

Using Option B (inline):
- Copy `app.yaml.template` from the `kasal-lakebase` repo to `src/app.yaml`
- Fill in the values from Steps 1 and 2

### Step 4 — Deploy

```bash
cd /path/to/kasal
source src/backend/.venv/bin/activate
python src/build.py      # build frontend static assets
python src/deploy.py     # deploy to Databricks Apps
```

Or use the all-in-one script from `kasal-lakebase`:

```bash
./deploy-kasal.sh \
  --host https://<your-workspace>.cloud.databricks.com \
  --token dapi<your-pat>
```

---

## How authentication works

Lakebase requires a short-lived **OAuth JWT token** as the PostgreSQL password, not a PAT. The app handles the exchange automatically:

1. On startup, `entrypoint.py` exchanges the PAT from `POSTGRES_PASSWORD` for a fresh JWT via `POST /api/2.0/database/credentials`
2. `session.py` registers a SQLAlchemy `do_connect` event — on every new connection, it checks whether the token expires within 5 minutes and refreshes it
3. The original PAT is preserved as `LAKEBASE_PAT` (internal env var) so renewals always work

The 1-hour OAuth token expiry is fully transparent to the app.

---

## Verifying data is stored in Lakebase

```bash
databricks psql kasal-db -- -d kasal -c "
SELECT
  (SELECT COUNT(*) FROM crews)            AS crews,
  (SELECT COUNT(*) FROM agents)           AS agents,
  (SELECT COUNT(*) FROM tasks)            AS tasks,
  (SELECT COUNT(*) FROM executionhistory) AS executions;
"
```

---

## Rotating credentials

```bash
# Rotate PAT (new token takes effect on next connection)
databricks secrets put-secret kasal lakebase_pat --string-value "dapi<new-token>"

# Point to a different Lakebase instance
databricks secrets put-secret kasal lakebase_server --string-value "<new-host>"
```

No code or `app.yaml` changes needed when using the secrets-based approach.

---

## Reference: kasal-lakebase scripts

Located at `/workspace/demos/kasal-lakebase/`:

| Script | Purpose |
|--------|---------|
| `setup-lakebase.sh` | Provision Lakebase instance + create `kasal` database |
| `deploy-kasal.sh` | Full pipeline: setup → build frontend → inject config → deploy |
| `status-lakebase.sh` | Show current instance state and connection details |
| `teardown-lakebase.sh` | Delete instance — **destroys all data** |
| `app.yaml.template` | Starting point for Option B (inline credentials) |
