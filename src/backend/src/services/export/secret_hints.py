"""Shared secret-key hints for export sanitization.

Single source of truth used by both the in-app export sanitizer
(``crew_export_service``) and the Databricks App exporter so the two never
drift. Config keys whose lowercased name contains any of these substrings are
treated as secrets and are never baked into an export — the deployed app reads
them from env vars / OBO instead.
"""

SECRET_KEY_HINTS = (
    "api_key",
    "apikey",
    "secret",
    "password",
    "token",
    "pat",
    "credential",
)
