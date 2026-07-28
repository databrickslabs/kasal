"""
The OBO user token for LLM calls made on worker threads.

Contextvars do not propagate into the threads LLM calls run on inside a crew or
flow subprocess, so `UserContext.get_user_token()` returns None there. The
subprocess sets this module-level fallback once at startup and usage telemetry
reads it when the contextvar is empty.

It lives here rather than in the LLM manager because it is process state, not
manager behaviour: the manager writes it, telemetry reads it, and telemetry must
not have to import the manager to do so.
"""

from typing import Optional

_subprocess_user_token: Optional[str] = None


def set_subprocess_user_token(token: str) -> None:
    """Set the token usage telemetry falls back to in subprocess mode."""
    global _subprocess_user_token
    _subprocess_user_token = token


def get_subprocess_user_token() -> Optional[str]:
    """The fallback token, or None outside subprocess execution."""
    return _subprocess_user_token
