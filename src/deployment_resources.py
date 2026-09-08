"""Read-only preflight for the resource bindings shipped in app.yaml."""

from pathlib import Path

import yaml


def validate_resources(client, app_name: str, app_yaml: Path) -> set[str]:
    """Validate assigned resources and return resource-backed environment names."""
    from databricks.sdk.errors import NotFound

    config = yaml.safe_load(app_yaml.read_text())
    required = {row["valueFrom"] for row in config.get("env", []) if "valueFrom" in row}
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
    missing = required - {item.name for item in resources}
    if missing:
        raise ValueError(
            "Attach the required app resources before deploying: "
            + ", ".join(sorted(missing))
            + ". See src/docs/databricks-app-installation.md."
        )
    if "lakebase" in required:
        databases = [item for item in resources if item.database or item.postgres]
        if len(databases) != 1 or databases[0].name != "lakebase":
            raise ValueError(
                "Attach exactly one Database resource, named 'lakebase'. "
                "Databricks supplies PG connection settings for the first database only."
            )
    return {row["name"] for row in config.get("env", []) if "valueFrom" in row}
