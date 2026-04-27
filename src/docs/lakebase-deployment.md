# Lakebase Setup for Kasal

By default, Kasal uses SQLite stored inside the Databricks Apps container. **SQLite is wiped on every restart or redeployment.** Lakebase provides managed PostgreSQL that lives outside the container — crews, agents, tasks, and run history all persist across restarts.

---

## Prerequisites

- Databricks CLI (`databricks`) installed and configured
- A Databricks workspace with Lakebase enabled
- A Databricks PAT token (`dapi...`)

---

## Step 1 — Provision a Lakebase instance

```bash
databricks lakebase create \
  --name kasal-db \
  --capacity CU_1
```

Wait until the instance is `AVAILABLE`:

```bash
databricks lakebase get --name kasal-db
```

Note the `read_write_dns` hostname from the output — you'll need it in Step 3.

---

## Step 2 — Create the `kasal` database

```bash
databricks psql kasal-db -- -c "CREATE DATABASE kasal;"
```

---

## Step 3 — Store credentials in Databricks Secrets

```bash
# Create a secret scope (skip if it already exists)
databricks secrets create-scope kasal

# Store the Lakebase hostname (read_write_dns from Step 1)
databricks secrets put-secret kasal lakebase_server \
  --string-value "<read_write_dns>"

# Store your PAT token
databricks secrets put-secret kasal lakebase_pat \
  --string-value "dapi<your-pat>"
```

Verify:

```bash
databricks secrets list-secrets kasal
# Should show: lakebase_server, lakebase_pat
```

---

## Step 4 — Configure `src/app.yaml`

Uncomment the Lakebase sections and fill in your values:

```yaml
command: ['python', 'entrypoint.py']

environment_vars:
  PYTHONPATH: './backend:${PYTHONPATH}'
  PYTHONUNBUFFERED: '1'
  FRONTEND_STATIC_DIR: './frontend_static'

  KASAL_LOG_LEVEL: 'INFO'
  KASAL_LOG_APP: 'INFO'
  KASAL_LOG_THIRD_PARTY: 'WARNING'
  KASAL_LOG_CONSOLE: 'true'
  KASAL_LOG_FILE: 'true'

  DATABASE_TYPE: 'postgres'
  POSTGRES_SERVER: <valueFrom lakebase-server>
  POSTGRES_USER: 'your-email@databricks.com'   # <-- your Databricks email
  POSTGRES_PASSWORD: <valueFrom lakebase-pat>
  POSTGRES_DB: 'kasal'
  POSTGRES_PORT: '5432'
  LAKEBASE_INSTANCE_NAME: 'kasal-db'           # <-- must match Step 1

resources:
  - name: lakebase-server
    type: secret
    scope: kasal                               # <-- your secret scope name
    key: lakebase_server
  - name: lakebase-pat
    type: secret
    scope: kasal
    key: lakebase_pat

apt_packages:
  - libpq-dev
```

---

## Step 5 — Deploy

```bash
python src/build.py   # build frontend static assets
python src/deploy.py  # deploy to Databricks Apps
```

---

## Verify data is persisting

```bash
databricks psql kasal-db -- -d kasal -c "
SELECT
  (SELECT COUNT(*) FROM crews)            AS crews,
  (SELECT COUNT(*) FROM agents)           AS agents,
  (SELECT COUNT(*) FROM tasks)            AS tasks,
  (SELECT COUNT(*) FROM executionhistory) AS executions;
"
```

Data should survive app restarts and redeployments.
