"""
Workspace settings: what the app is configured to use.

LLM model catalogue and per-group enablement (``models``), engine knobs
(``engine``), the A2UI/chrome config the frontend reads (``ui``), and provider
credentials (``api_keys``, encrypted at rest).

Named ``settings`` and not ``config`` on purpose — ``src/config/`` already
exists for process configuration, and two things called config is how you get a
bug report nobody can locate.
"""
