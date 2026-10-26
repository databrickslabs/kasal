"""The `kasal` command: run the Kasal server with the bundled UI.

Defaults are chosen for someone who just ran `pip install kasal`:
SQLite in ~/.kasal (the directory Kasal's memory stores already use), logs
beside it, auto-seeded catalog on first boot, UI and API on one port. Every
environment variable the app normally reads still applies — the CLI only sets
defaults, never overrides — so pointing it at PostgreSQL is
`DATABASE_TYPE=postgres POSTGRES_SERVER=… kasal`.
"""

import argparse
import os
import sys
from pathlib import Path

_APP_DIR = Path(__file__).resolve().parent / "_app"


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="kasal",
        description="Run the Kasal server (API + bundled UI).",
    )
    parser.add_argument(
        "--host", default="127.0.0.1", help="Bind address (default: 127.0.0.1)"
    )
    parser.add_argument("--port", type=int, default=8000, help="Port (default: 8000)")
    parser.add_argument(
        "--data-dir",
        default=os.path.expanduser("~/.kasal"),
        help="Where the SQLite database and logs live (default: ~/.kasal)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"kasal {__import__('kasal').__version__}",
    )
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir).expanduser()
    (data_dir / "logs").mkdir(parents=True, exist_ok=True)

    # Defaults only — anything already in the environment wins.
    os.environ.setdefault("DATABASE_TYPE", "sqlite")
    os.environ.setdefault("SQLITE_DB_PATH", str(data_dir / "kasal.db"))
    os.environ.setdefault("LOG_DIR", str(data_dir / "logs"))

    # The backend's top-level import package is `src`; it ships inside this
    # package (kasal/_app) and becomes importable HERE, for this process only.
    sys.path.insert(0, str(_APP_DIR))

    import uvicorn  # noqa: PLC0415 — after env defaults, before app import

    from src.main import app  # noqa: PLC0415 — settings read env at import

    static_dir = _APP_DIR / "frontend_static"
    if (static_dir / "index.html").exists():
        from fastapi.staticfiles import StaticFiles  # noqa: PLC0415

        # Mounted last, at "/": FastAPI tries real routes first, so the API
        # keeps every path it owns and the UI gets the rest (html=True serves
        # index.html for the SPA).
        app.mount(
            "/", StaticFiles(directory=str(static_dir), html=True), name="kasal-ui"
        )

    print(f"Kasal:    http://{args.host}:{args.port}")
    print(f"API docs: http://{args.host}:{args.port}/api-docs")
    print(f"Data:     {data_dir}")
    uvicorn.run(app, host=args.host, port=args.port, log_level="info")


if __name__ == "__main__":
    main()
