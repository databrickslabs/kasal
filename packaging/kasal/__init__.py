"""Kasal, installed from pip: the backend and built UI in one package.

The application itself lives in ``kasal/_app`` exactly as it exists in the
repository — ``src/`` (the backend, whose top-level import package is ``src``)
and ``frontend_static/`` (the built React UI). ``kasal.cli`` puts ``_app`` on
``sys.path`` and launches ``src.main:app``; nothing else imports ``src``, so
the name never leaks into site-packages.
"""

from importlib.metadata import PackageNotFoundError, version

# The installed distribution's version, which the build takes from the root
# pyproject.toml, so this never needs bumping by hand.
try:
    __version__ = version("kasal")
except PackageNotFoundError:  # imported from a source checkout, not installed
    __version__ = "0+unknown"
