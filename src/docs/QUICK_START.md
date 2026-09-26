# Quick Start

Install Kasal in minutes, from the Databricks Marketplace or a quick local install, and build your first agent workflow in under two minutes.

- [Install from the Databricks Marketplace](#install-from-the-databricks-marketplace)
- [Run locally](#run-locally)
- [Build your first workflow](#build-your-first-workflow)
- [Next steps](#next-steps)

## Install from the Databricks Marketplace

The recommended path is to install Kasal directly from the Databricks Apps Marketplace with one click. This is best for production use: Kasal deploys as a native Databricks App, inherits your workspace identity automatically (OAuth and on-behalf-of execution), and there is no separate infrastructure to run or connection to configure.

After installing, launch Kasal from your Databricks Apps. Because it runs inside your workspace, agents can query Unity Catalog, run SQL through your warehouses, and call your Foundation Model API or Model Serving endpoints with the calling user's permissions.

If you prefer a custom installation from source, you can deploy to your workspace with the included tooling:

```bash
python src/build.py
python src/deploy.py --app-name <app-name> --user-name <your-email>
```

`build.py` produces the frontend static assets, and `deploy.py` ships the app to your Databricks workspace (run `python src/deploy.py -h` for the profile, host and OAuth-scope options). For managed Lakebase PostgreSQL so that crews, agents, tasks, and run history survive app restarts, see [./lakebase-deployment.md](./lakebase-deployment.md).

## Run locally

For testing and development you can run Kasal on your own machine.

### Before you begin

- Python 3.11. The backend pins `>=3.11,<3.12`.
- [uv](https://docs.astral.sh/uv/), which installs the Python dependencies from `src/backend/uv.lock`. There is no `requirements.txt`.
- Node.js 22, the version CI uses.

### Start the backend and frontend

Start the backend from `src/backend`:

```bash
git clone https://github.com/nehmetohme/kasal.git
cd kasal/src/backend
./run.sh
```

`run.sh` runs `uv sync --frozen`, then serves the API at `http://127.0.0.1:8000` with auto-reload. By default it uses the SQLite file `src/backend/app.db`, whatever directory you start it from; `alembic` and the seeders use the same file.

In another terminal, start the frontend:

```bash
cd kasal/src/frontend
npm ci
npm start
```

The app is served at `http://localhost:3000`.

### What run.sh sets up for you

- **Loopback only.** The server binds to `127.0.0.1`. Set `KASAL_BIND_HOST=0.0.0.0` only if you mean to expose it, and read the warning `run.sh` prints first.
- **A development identity.** `run.sh` sets `LOCAL_DEV_AUTH=true`, so a request without an identity header runs as `LOCAL_DEV_USER_EMAIL` (default `dev@localhost`). If you start `uvicorn` yourself without it, every API call returns 401.
- **A safe port check.** If port 8000 is held by something other than a Kasal server from this checkout, `run.sh` refuses to start. Use `KASAL_PORT=<port>`, or set `KASAL_KILL_PORT_OWNER=true` to terminate the owner. The dev frontend reads the same `KASAL_PORT` (or `VITE_KASAL_PORT`), so start it with the same value, for example `KASAL_PORT=8001 npm start`.

Run `./run.sh -h` for the logging flags and `./run.sh postgres` to use PostgreSQL instead. For the full list of settings, see the [configuration reference](./CONFIGURATION.md); for day-to-day development, see the [developer guide](./DEVELOPER_GUIDE.md).

## Build your first workflow

Open the Workflow Designer (left sidebar: Workflows, then Designer, or the direct route `/workflow`). You can build a workflow in a few steps:

1. Start from a template or a blank canvas. Drag agents onto the canvas and give each one a role, a goal, and the tools it should use.
2. Add tasks and describe what each agent should do in plain language, for example "research 5 trending topics and summarize each in 2 to 3 sentences". You can use placeholders like `{industry}` to parameterize inputs at run time.
3. Point the agents at your data: Unity Catalog tables, SQL warehouses, Volumes for documents, and vector indexes for semantic search.
4. Connect the nodes to define the collaboration flow, then run the workflow and watch agents work in real time, with live logs and execution traces.

For a complete, screenshot-ready walkthrough that builds a multi-agent blog-production workflow end to end, follow the [end-user tutorial](./END_USER_TUTORIAL_CATALOG.md).

## Next steps

- [Why Kasal](./WHY_KASAL.md): the problems Kasal solves and who it is for on Databricks.
- [Solution architecture guide](./ARCHITECTURE_GUIDE.md): platform layers, request lifecycle, and the security model.
- [Developer guide](./DEVELOPER_GUIDE.md): local setup, configuration, and extension patterns.
- [Configuration reference](./CONFIGURATION.md): the environment variables operators and developers set.
- [End-user tutorial catalog](./END_USER_TUTORIAL_CATALOG.md): build and run your first workflow step by step.

Back to the [documentation hub](./README.md).
