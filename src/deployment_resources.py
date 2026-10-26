"""Read-only preflight for the resource bindings shipped in app.yaml."""

from pathlib import Path

import yaml


def _requires_lakebase(env: list) -> bool:
    """Lakebase is optional unless app.yaml sets KASAL_REQUIRE_LAKEBASE_RESOURCE."""
    return any(
        row.get("name") == "KASAL_REQUIRE_LAKEBASE_RESOURCE"
        and str(row.get("value", "")).lower() == "true"
        for row in env
    )


def validate_resources(client, app_name: str, app_yaml: Path) -> set[str]:
    """Validate assigned resources and return the environment names they bind.

    Only names whose resource is actually attached are returned, so a deploy
    without the optional ``lakebase`` resource does not report
    ``KASAL_LAKEBASE_RESOURCE`` as bound.
    """
    from databricks.sdk.errors import NotFound

    config = yaml.safe_load(app_yaml.read_text())
    env = config.get("env", [])
    required = {row["valueFrom"] for row in env if "valueFrom" in row}
    if not required:
        return set()
    try:
        app = client.apps.get(name=app_name)
    except NotFound as exc:
        raise ValueError(
            "Create the Databricks App and select its installation resources before "
            "deploying. See src/docs/databricks-app-installation.md."
        ) from exc
    resources = app.resources or []
    attached = {item.name for item in resources}
    optional = set() if _requires_lakebase(env) else {"lakebase"}
    missing = required - attached - optional
    if missing:
        raise ValueError(
            "Attach the required app resources before deploying: "
            + ", ".join(sorted(missing))
            + ". See src/docs/databricks-app-installation.md."
        )
    databases = [item for item in resources if item.database or item.postgres]
    if "lakebase" in required and (databases or "lakebase" not in optional):
        if len(databases) != 1 or databases[0].name != "lakebase":
            raise ValueError(
                "Attach exactly one Database resource, named 'lakebase'. "
                "Databricks supplies PG connection settings for the first database only."
            )
    return {row["name"] for row in env if row.get("valueFrom") in attached}
