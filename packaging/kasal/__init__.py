"""Kasal, installed from pip: the backend and built UI in one package.

The application itself lives in ``kasal/_app`` exactly as it exists in the
repository — ``src/`` (the backend, whose top-level import package is ``src``)
and ``frontend_static/`` (the built React UI). ``kasal.cli`` puts ``_app`` on
``sys.path`` and launches ``src.main:app``; nothing else imports ``src``, so
the name never leaks into site-packages.
"""

__version__ = "0.1.1"
