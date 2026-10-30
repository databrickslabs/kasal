# Local production-mode testing

How to reproduce, before a deploy, the Databricks Apps conditions that the hot-reload dev setup hides. For developers shipping Kasal to Databricks Apps.

> [!NOTE]
> **Status: internal / proposal.** The scripts below are proposals, not files in this repository. Save them under `/tmp` when you use them; the project keeps test scripts out of the tree. This page lives outside `src/docs/`, so the in-app docs viewer does not show it.

## Why this matters

The local dev setup (`run.sh` plus the Vite dev server) catches most bugs. The rest surface only in Databricks Apps, because of three container differences:

| Difference | Effect |
|------------|--------|
| **64 KiB pipe buffer** | Multiprocessing result queues can deadlock when a subprocess returns more than 64 KiB (for example a large LLM response) |
| **Deploy directory structure** | Imports and path lookups that depend on the checkout layout fail when the app root changes |
| **Production startup** | No hot-reload, a static frontend served by `src/entrypoint.py`, and identity from the platform proxy |

## Before you begin

- A backend virtualenv: `cd src/backend && uv sync --frozen`.
- A built frontend, if you want the static UI: `python src/build.py`.
- An identity for API calls. Outside Databricks Apps, a request with no identity header gets **401**. Either export `LOCAL_DEV_AUTH=true` (requests without an identity header then run as `LOCAL_DEV_USER_EMAIL`, default `dev@localhost`), or send `X-Forwarded-Email` yourself. `run.sh` sets `LOCAL_DEV_AUTH` for you; the recipes below do not use `run.sh`, so they set it explicitly.

## Reproduce the pipe-buffer limit

Linux pipes hold 64 KiB by default, the same as the container, so a crew run on a Linux machine already exercises the limit. macOS pipes behave differently, so test large results on Linux (or in the container recipe below) before trusting a macOS run.

`ulimit -p` does not help here: bash reports the pipe size but does not let you change it.

Run any crew that returns a large result, for example the UC Metric View Generator or the Pipeline Config Generator on a full dataset. If the status stays at `RUNNING` and never completes, you have a queue deadlock; fix it before deploying.

## Start the backend in production mode

`src/entrypoint.py` is what Databricks Apps runs (`src/app.yaml`). It serves the built frontend from `src/frontend_static/`, runs without reload, and defaults to SQLite at `src/kasal.db`.

Save this as `/tmp/run-prod-mode.sh`:

```bash
#!/usr/bin/env bash
# Start Kasal the way Databricks Apps does: entrypoint.py, static frontend, no reload.
set -euo pipefail
REPO="${1:?usage: run-prod-mode.sh <path-to-kasal-checkout>}"

cd "$REPO"
if [ ! -d src/frontend_static ] || [ -z "$(ls -A src/frontend_static 2>/dev/null)" ]; then
  python src/build.py
fi

# No platform proxy locally, so opt in to the development identity.
export LOCAL_DEV_AUTH=true

cd src
exec backend/.venv/bin/python entrypoint.py --port 8000
```

Run it with your checkout path:

```bash
bash /tmp/run-prod-mode.sh "$PWD"
```

> [!WARNING]
> Outside Databricks Apps `entrypoint.py` binds to `127.0.0.1` unless `KASAL_BIND_HOST` says otherwise. If you set `KASAL_BIND_HOST=0.0.0.0` with `LOCAL_DEV_AUTH` on, anyone who can reach port 8000 acts as the development user. Do that only on a machine or network you trust, or send an identity header and leave `LOCAL_DEV_AUTH` unset.

To run `uvicorn` directly instead, bind to loopback and set the identity:

```bash
cd src/backend
DATABASE_TYPE=sqlite LOCAL_DEV_AUTH=true \
  .venv/bin/uvicorn src.main:app --host 127.0.0.1 --port 8000 --workers 1 --log-level info
```

## Check the deploy directory structure

Databricks Apps copies the app under a different root. This check mirrors that layout and confirms the backend still imports from there, including the Power BI pipeline library the config generator loads.

Save this as `/tmp/test-deploy-structure.sh`:

```bash
#!/usr/bin/env bash
# Mirror the Databricks Apps directory layout and check that key imports resolve.
set -euo pipefail
REPO="${1:?usage: test-deploy-structure.sh <path-to-kasal-checkout>}"
DEPLOY_ROOT="$(mktemp -d)"
SIMULATED_BACKEND="$DEPLOY_ROOT/python/source_code/backend"

mkdir -p "$SIMULATED_BACKEND"
cp -r "$REPO/src/backend/src" "$SIMULATED_BACKEND/"

cd "$SIMULATED_BACKEND"
"$REPO/src/backend/.venv/bin/python" - <<'PY'
from src.services.tools.pipeline_config_generator_tool import PipelineConfigGeneratorTool
from src.services.powerbi import pipeline_config
print("pipeline_config_generator_tool:", PipelineConfigGeneratorTool.__name__)
print("powerbi.pipeline_config:", pipeline_config.__name__)
PY

rm -rf "$DEPLOY_ROOT"
echo "All import checks passed."
```

Run it with your checkout path:

```bash
bash /tmp/test-deploy-structure.sh "$PWD"
```

## Run in a container (optional)

For the highest fidelity, run the backend in a Linux container with the deployed layout. Save this as `/tmp/docker-compose.prod-test.yml`, replacing `<path-to-kasal-checkout>`:

```yaml
services:
  kasal-prod-test:
    image: python:3.11-slim
    working_dir: /app/python/source_code
    volumes:
      - <path-to-kasal-checkout>/src:/app/python/source_code
    environment:
      - LOCAL_DEV_AUTH=true            # no platform proxy in the container
      - KASAL_BIND_HOST=0.0.0.0        # reachable through the published port
      - UV_PROJECT_ENVIRONMENT=/opt/venv   # keep the Linux venv out of your checkout
      - DATABRICKS_HOST=${DATABRICKS_HOST}
      - DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
    entrypoint: >
      bash -c "
        pip install uv &&
        cd backend && uv sync --frozen && cd .. &&
        /opt/venv/bin/python entrypoint.py --port 8000
      "
    ports:
      - "127.0.0.1:8001:8000"          # loopback only; 8001 avoids the dev server
```

Start it:

```bash
docker compose -f /tmp/docker-compose.prod-test.yml up
```

## Pre-deploy checklist

Run these before every `python src/deploy.py`:

```bash
# 1. TypeScript compiles (catches frontend build failures)
cd src/frontend && npm run tsc

# 2. The frontend builds
npm run build

# 3. Imports resolve from the deployed layout
bash /tmp/test-deploy-structure.sh "$(git rev-parse --show-toplevel)"

# 4. Smoke test in production mode: run a large-result crew, then stop the server
bash /tmp/run-prod-mode.sh "$(git rev-parse --show-toplevel)"
```

## What each check catches

| Bug seen in past deploys | Caught by |
|---------------------------|-----------|
| Status stuck at `RUNNING` (queue deadlock on results over 64 KiB) | A large-result run on Linux or in the container |
| A module or data file not found from the deployed layout | `/tmp/test-deploy-structure.sh` |
| Frontend build fails (`Variable 'entries' used before declaration`) | `npm run tsc` |
| `expected str, bytes or os.PathLike, not dict` in tools | `/tmp/run-prod-mode.sh` with real data |
| UCMV Validator "No YAML content provided" | `/tmp/run-prod-mode.sh` with the full flow |

## Related

- [Quick start](../src/docs/QUICK_START.md)
- [Developer guide](../src/docs/DEVELOPER_GUIDE.md)
- [Configuration reference](../src/docs/CONFIGURATION.md)
- [Databricks Apps installation](../src/docs/databricks-app-installation.md)

Back to the [documentation hub](../src/docs/README.md).
