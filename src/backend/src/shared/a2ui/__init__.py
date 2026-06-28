"""Shared A2UI: one composer + catalog for the live app AND exported apps.

This package must import ONLY the stdlib (json/os/pathlib/typing) so it can be
bundled verbatim into a self-contained exported Databricks app. The LLM is always
injected by the caller as an ``llm_call`` lambda — never imported here.

Import from the submodule directly (``from src.shared.a2ui.compose import …``);
this package intentionally exposes no re-export barrel.
"""
