# Installing Kasal with pip

Kasal ships as a single wheel: the FastAPI backend and the **built** React UI
in one package, with a `kasal` command that serves both on one port.

```bash
pip install kasal        # once published — or: pip install dist/kasal-*.whl
kasal                    # UI + API on http://127.0.0.1:8000
```

First boot creates `~/.kasal/kasal.db` (SQLite), seeds the model/tool catalog,
and writes logs to `~/.kasal/logs`. Options:

```
kasal --host 0.0.0.0 --port 9000 --data-dir /srv/kasal
```

Every environment variable the app normally reads still applies — the CLI only
sets defaults. PostgreSQL instead of SQLite:

```bash
DATABASE_TYPE=postgres POSTGRES_SERVER=… POSTGRES_USER=… POSTGRES_PASSWORD=… kasal
```

## How the wheel is put together

The backend's top-level import package is `src` (thousands of `from src.…`
imports), which must never be installed as a top-level module. The wheel
therefore ships the app **under** the `kasal` package:

```
kasal/
  cli.py                  # the `kasal` entry point
  _app/src/…              # the backend, verbatim
  _app/frontend_static/…  # the built UI (vite output)
```

`kasal.cli` prepends `kasal/_app` to `sys.path`, imports `src.main:app`,
mounts the UI as a `StaticFiles` fallback at `/` (real API routes win), and
runs uvicorn. `import src` works only inside the running process — site-packages
gains exactly one top-level name.

Dependencies are **not** duplicated: `hatch_build.py` (root) reads them from
`src/backend/pyproject.toml` at build time.

## Building and publishing

```bash
python src/package_pip.py            # builds frontend if missing → dist/kasal-*.whl
python src/package_pip.py --sdist    # wheel + sdist
uv publish                           # needs a PyPI token (UV_PUBLISH_TOKEN)
```

The name `kasal` was unclaimed on PyPI as of 2026-08-30. `dist/` and
`src/frontend_static/` are gitignored build artifacts; the root
`pyproject.toml` does not affect the Databricks App deploy (`src/deploy.py`
stages the backend's own pyproject into its bundle).
