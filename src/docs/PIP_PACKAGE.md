# Installing Kasal with pip

Kasal ships as a single wheel: the FastAPI backend and the **built** React UI
in one package, with a `kasal` command that serves both on one port.

```bash
pip install kasal        # from PyPI — or: pip install dist/kasal-<version>-py3-none-any.whl
export LOCAL_DEV_AUTH=true
kasal                    # UI + API on http://127.0.0.1:8000
```

Every API call needs an identity, and the `kasal` command does not set one.
Without a proxy in front, export `LOCAL_DEV_AUTH=true` first so requests run as
`dev@localhost`; otherwise every API call
returns 401. Only do that while the server is bound to loopback: with
`--host 0.0.0.0`, anyone who can reach the port acts as that user.

First boot creates `~/.kasal/kasal.db` (SQLite) and seeds the model/tool
catalog. Logs go to `~/.kasal/logs` (the command defaults `LOG_DIR` to
`<data dir>/logs`, and the app keeps a `LOG_DIR` that is already set). Options:

```bash
kasal --host 127.0.0.1 --port 9000 --data-dir /srv/kasal
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

```text
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
python src/package_pip.py --sdist    # wheel + sdist → dist/kasal-<version>*
uv publish dist/kasal-<version>*     # needs a PyPI token (UV_PUBLISH_TOKEN)
```

The wheel's version is the root `pyproject.toml` version, which matches
`settings.VERSION` (see "Releasing a version" in
[Continuous integration](./continuous-integration.md)). `kasal --version` reads it
from the installed package. The packager rebuilds the UI when
`src/frontend_static` is missing or older than `src/frontend/src`, so a wheel
never ships a stale UI; `--skip-frontend` packages what is there and warns.
Name the files when publishing: `dist/` keeps older builds, and PyPI refuses
to re-upload a version it already has.

Kasal is published on PyPI as [`kasal`](https://pypi.org/project/kasal/). `dist/` and
`src/frontend_static/` are gitignored build artifacts; the root
`pyproject.toml` does not affect the Databricks App deploy (`src/deploy.py`
stages the backend's own pyproject into its bundle).

## Related

- [Quick start](./QUICK_START.md)
- [Configuration reference](./CONFIGURATION.md)

Back to the [documentation hub](./README.md).
