"""Schema-hygiene guard for Power BI tools (LLM-015).

Tool ``args_schema`` classes define the LLM-facing contract: every field is
serialized into EVERY LLM request that carries the tool, and the model is
invited to fill it. Auth/connection/LLM plumbing (client_secret, password,
access_token, llm_token, workspace/dataset IDs, ...) must therefore never
appear in an args_schema — it inflated each schema to 3-7 KB of prompt
tokens per call and advertised credentials as model-fillable parameters.
That plumbing is injected at tool-construction time from tool_configs.
"""

import importlib
import inspect
import pkgutil

import pytest
from pydantic import BaseModel

import src.engines.crewai.tools.custom as custom_tools_pkg

# Fields that must never be LLM-fillable.
FORBIDDEN_SCHEMA_FIELDS = {
    # credentials / secrets
    "client_secret",
    "password",
    "access_token",
    "llm_token",
    "api_key",
    "token",
    # auth plumbing
    "tenant_id",
    "client_id",
    "username",
    "auth_method",
    # connection / LLM plumbing (pre-configured via tool_configs)
    "workspace_id",
    "dataset_id",
    "llm_workspace_url",
    "llm_model",
}


def _powerbi_schema_classes():
    """Yield (module_name, schema_class) for every PBI tool args schema."""
    for modinfo in pkgutil.iter_modules(custom_tools_pkg.__path__):
        if not (modinfo.name.startswith("powerbi") or modinfo.name.startswith("pbi_")):
            continue
        try:
            module = importlib.import_module(
                f"{custom_tools_pkg.__name__}.{modinfo.name}"
            )
        except Exception:
            continue  # tools with unimportable optional deps are out of scope
        for _, cls in inspect.getmembers(module, inspect.isclass):
            if (
                issubclass(cls, BaseModel)
                and cls.__name__.endswith("Schema")
                and cls.__module__ == module.__name__
            ):
                yield modinfo.name, cls


def test_discovers_powerbi_schemas():
    """Sanity: the scan actually finds the PBI tool schemas."""
    found = list(_powerbi_schema_classes())
    assert len(found) >= 10, f"expected >=10 PBI schemas, found {len(found)}"


@pytest.mark.parametrize(
    "module_name,schema_cls",
    [(m, c) for m, c in _powerbi_schema_classes()],
    ids=lambda v: v if isinstance(v, str) else getattr(v, "__name__", str(v)),
)
def test_schema_has_no_auth_or_plumbing_fields(module_name, schema_cls):
    leaked = set(schema_cls.model_fields) & FORBIDDEN_SCHEMA_FIELDS
    assert not leaked, (
        f"{module_name}.{schema_cls.__name__} exposes plumbing/credential "
        f"fields to the LLM: {sorted(leaked)} — inject these via tool_configs "
        f"in the tool's __init__ instead."
    )
