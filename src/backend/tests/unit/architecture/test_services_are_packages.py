"""Every service lives in a package, never as a loose file under `services/`.

`services/` is a directory of DOMAINS, not of modules. When a domain is a
package, everything it owns has one obvious home and the package name says what
the code is about; when it is a loose `foo_service.py`, the next thing the domain
needs has nowhere to go, so it becomes `foo_service_helpers.py` beside it, and
the boundary that used to be a directory is now a naming convention nobody
enforces.

This tree learned it twice. `services/a2a_agent_service.py` sat beside the
`a2a/` package that held everything else A2A. `services/mcp/service.py` sat
beside a top-level `services/mcp_server/`, splitting one protocol across two
places so "which direction does this face?" was a question you answered by
reading the file rather than the path.

Unlike the other checks here this one has NO allowlist, because as of this commit
there is nothing to exempt. Adding an exemption is therefore a deliberate act
that shows up in review, which is the whole point.
"""

import pathlib

_SERVICES = pathlib.Path(__file__).resolve().parents[3] / "src" / "services"

#: `__init__.py` is the package itself, not a service living loose in it.
_ALLOWED = {"__init__.py"}


def _flat_modules() -> list[str]:
    return sorted(p.name for p in _SERVICES.glob("*.py") if p.name not in _ALLOWED)


def test_services_contains_no_loose_modules():
    offenders = _flat_modules()
    assert not offenders, (
        "These belong in a package under services/, not beside it: "
        + ", ".join(offenders)
        + ". Name the package for the DOMAIN (services/<domain>/<module>.py) and "
        "re-export from its __init__ if call sites depend on the old path."
    )


def test_every_service_package_declares_itself():
    """A package needs an ``__init__.py``.

    Without one it is an implicit namespace package, which imports fine right
    up until a stale ``__pycache__`` or a same-named directory elsewhere on the
    path shadows it — a failure that reads as "the module vanished" and costs an
    afternoon. It also leaves the package with nowhere to say what it is for.
    """
    missing = sorted(
        d.name
        for d in _SERVICES.iterdir()
        if d.is_dir() and d.name != "__pycache__" and not (d / "__init__.py").exists()
    )
    assert not missing, f"Service packages without __init__.py: {', '.join(missing)}"
