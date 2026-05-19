# Local Production-Mode Testing

## Why This Matters

The local dev setup (hot-reload backend + npm dev server) catches ~90% of bugs.
The remaining ~10% only surface in Databricks Apps because of three container-specific
differences:

| Difference | Effect |
|------------|--------|
| **65 KB pipe buffer limit** | Multiprocessing result queues deadlock when subprocess returns >64 KB (e.g. large LLM responses) |
| **Deploy directory structure** | `__file__`-relative path lookups fail when the app root changes |
| **Production startup** | No hot-reload, static frontend, no `--reload` flag |

This guide describes how to simulate all three locally before deploying.

---

## Quick Win — Catch the Queue Deadlock in 1 Line

The single most impactful thing you can do before any deploy:

```bash
# In the terminal where you start the backend:
ulimit -p 128   # restrict pipe buffer to 64 KB (matches container)
./run.sh
```

Then run any crew that returns a large result (e.g. UC Metric View Generator, Pipeline
Config Generator with a full SC Reporting dataset). If the status gets stuck at
`RUNNING` and never completes, you have a queue deadlock — fix it before deploying.

---

## Proposed Scripts

### 1. `run-prod-mode.sh` — Production-equivalent backend startup

Create this alongside the existing `run.sh`:

```bash
#!/usr/bin/env bash
# run-prod-mode.sh
#
# Start the backend in a mode that closely mirrors Databricks Apps:
#   - Pipe buffer limited to 64 KB (container limit)
#   - No hot-reload
#   - Static frontend served from frontend_static/
#   - NullPool DB connections
#   - Same PostgreSQL as the deployed app

set -e

# Simulate container pipe buffer limit (catches multiprocessing queue deadlocks)
ulimit -p 128

# Build the frontend if not already built
if [ ! -d "frontend_static" ] || [ -z "$(ls -A frontend_static 2>/dev/null)" ]; then
  echo "Building frontend..."
  python build.py
fi

# Production-equivalent environment variables
export USE_NULLPOOL=true
export SERVE_STATIC=true   # tells main.py to serve frontend_static/ directly

# Start without --reload (as in Databricks Apps)
echo "Starting backend in production mode (no hot-reload, pipe limit = 64 KB)..."
uvicorn src.main:app \
  --host 0.0.0.0 \
  --port 8000 \
  --workers 1 \
  --log-level info
```

Usage:
```bash
cd src/backend
source .venv/bin/activate
bash ../../run-prod-mode.sh
```

---

### 2. `test-deploy-structure.sh` — Verify file paths before deploying

Catches `__file__`-relative path bugs (like `generate_config.py not found`):

```bash
#!/usr/bin/env bash
# test-deploy-structure.sh
#
# Mirrors the Databricks Apps deploy directory structure and checks that all
# __file__-relative path lookups resolve correctly.
# Run this before every deploy.

set -e

DEPLOY_ROOT="/tmp/kasal-deploy-test"
SOURCE="$(cd "$(dirname "$0")" && pwd)/src/backend"

echo "=== Simulating Databricks Apps deploy structure ==="

# Mirror the exact path the deployed app sees
SIMULATED_ROOT="$DEPLOY_ROOT/python"
SIMULATED_BACKEND="$SIMULATED_ROOT/source_code/backend"

rm -rf "$DEPLOY_ROOT"
mkdir -p "$SIMULATED_BACKEND"
cp -r "$SOURCE/src" "$SIMULATED_BACKEND/"

echo "Checking __file__-relative paths..."

# Check that generate_config.py can be found from its tool
python3 - <<EOF
import sys
sys.path.insert(0, "$SIMULATED_BACKEND")

import os
tool_file = "$SIMULATED_BACKEND/src/engines/crewai/tools/custom/pipeline_config_generator_tool.py"
if os.path.exists(tool_file):
    print("  pipeline_config_generator_tool.py: found")
else:
    print("  ERROR: pipeline_config_generator_tool.py not found")
    sys.exit(1)

# Simulate the path resolution the tool does at runtime
this_dir = os.path.dirname(os.path.abspath(tool_file))
candidates = [
    this_dir,
    os.path.abspath(os.path.join(this_dir, *[".."] * 7, "examples", "uc_metric_view_migration"))
]
found = any(os.path.isfile(os.path.join(c, "generate_config.py")) for c in candidates)
print(f"  generate_config.py resolvable: {found}")
if not found:
    print(f"  ERROR: checked {candidates}")
    sys.exit(1)
EOF

echo "All path checks passed."
rm -rf "$DEPLOY_ROOT"
```

Usage:
```bash
bash test-deploy-structure.sh
```

---

### 3. Docker Compose (optional — highest fidelity)

For full container simulation:

```yaml
# docker-compose.prod-test.yml
version: "3.9"

services:
  kasal-prod-test:
    image: python:3.11-slim
    working_dir: /app/python/source_code/backend
    volumes:
      - ./src/backend:/app/python/source_code/backend
      - ./src/frontend_static:/app/python/source_code/frontend_static
    environment:
      - USE_NULLPOOL=true
      - DATABASE_URL=${DATABASE_URL}        # same PostgreSQL as deployed app
      - DATABRICKS_HOST=${DATABRICKS_HOST}
      - DATABRICKS_TOKEN=${DATABRICKS_TOKEN}
    ulimits:
      nofile: 1024
      # pipe buffer limit — set via ulimit in entrypoint
    entrypoint: >
      bash -c "
        ulimit -p 128 &&
        pip install -r requirements.txt -q &&
        uvicorn src.main:app --host 0.0.0.0 --port 8000 --workers 1
      "
    ports:
      - "8001:8000"   # use port 8001 locally to avoid conflict with dev server
```

Usage:
```bash
docker compose -f docker-compose.prod-test.yml up
```

---

## Pre-Deploy Checklist

Run these before every `python src/deploy.py`:

```bash
# 1. TypeScript compiles without errors (catches frontend build failures)
cd src/frontend && npm run tsc

# 2. Frontend builds successfully
cd src/frontend && npm run build

# 3. Verify file path resolution
bash test-deploy-structure.sh

# 4. Quick smoke test with pipe limit (catches queue deadlocks)
#    Run the UC Metric View Generator or Pipeline Config crew after:
ulimit -p 128 && cd src/backend && uvicorn src.main:app --port 8000
```

---

## What Each Check Catches

| Bug (from recent deploys) | Caught by |
|---------------------------|-----------|
| Status stuck at RUNNING (queue deadlock, large LLM responses >64 KB) | `ulimit -p 128` |
| `generate_config.py not found` | `test-deploy-structure.sh` |
| Frontend build fails (`Variable 'entries' used before declaration`) | `npm run tsc` |
| `expected str, bytes or os.PathLike, not dict` in tools | `run-prod-mode.sh` with real data |
| UCMV Validator "No YAML content provided" | `run-prod-mode.sh` full flow |

---

## Estimated Effort

| Task | Effort |
|------|--------|
| Add `ulimit -p 128` to dev workflow | 5 minutes |
| Create `run-prod-mode.sh` | ~1 hour |
| Create `test-deploy-structure.sh` | ~30 minutes |
| Docker Compose option | ~2–3 hours |

The **single highest-ROI change** is `ulimit -p 128` before starting the backend —
one line that would have caught the main bug responsible for ~8 deploys in a single day.
