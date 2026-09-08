"""Databricks Apps installation defaults, independent of tenant configuration.

Ambient SDK credentials (and the dev entrypoint's synthetic app name) are not
evidence that Kasal is hosted. Resource bindings only apply inside the platform.
"""

import hashlib
import os
from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(frozen=True)
class DatabricksAppInstallation:
    hosted: bool
    host: str = ""
    app_name: str = ""
    workspace_id: str = ""
    warehouse_id: str = ""
    output_volume: str = ""
    default_model: str = ""

    @classmethod
    def from_env(cls, env: Optional[Mapping[str, str]] = None):
        env = os.environ if env is None else env

        def value(key: str) -> str:
            return (env.get(key) or "").strip()

        hosted = value("KASAL_DEPLOYMENT_MODE").lower() != "local" and all(
            value(key)
            for key in (
                "DATABRICKS_APP_NAME",
                "DATABRICKS_APP_PORT",
                "DATABRICKS_WORKSPACE_ID",
                "DATABRICKS_HOST",
            )
        )
        if not hosted:
            return cls(hosted=False)
        return cls(
            hosted=True,
            host=value("DATABRICKS_HOST").rstrip("/"),
            app_name=value("DATABRICKS_APP_NAME"),
            workspace_id=value("DATABRICKS_WORKSPACE_ID"),
            warehouse_id=value("KASAL_SQL_WAREHOUSE_ID"),
            output_volume=value("KASAL_OUTPUT_VOLUME"),
            default_model=value("KASAL_DEFAULT_MODEL"),
        )

    def experiment_name(self, group_id: str) -> str:
        """Stable personal/teamspace destination across installation resource changes."""
        if not self.hosted or not group_id:
            raise ValueError("A hosted installation and teamspace are required")
        installation = hashlib.sha256(
            f"{self.workspace_id}:{self.app_name}".encode()
        ).hexdigest()[:16]
        tenant = hashlib.sha256(group_id.encode()).hexdigest()[:24]
        return f"/Shared/kasal-{installation}-{tenant}-traces-uc"

    def output_path(self, group_id: str) -> str:
        if not group_id or not self.output_volume.startswith("/Volumes/"):
            raise ValueError("Output storage requires an assigned volume and teamspace")
        scope = hashlib.sha256(
            f"{self.workspace_id}:{self.app_name}:{group_id}".encode()
        ).hexdigest()[:32]
        return f"{self.output_volume.rstrip('/')}/kasal/{scope}"


def is_databricks_app() -> bool:
    return DatabricksAppInstallation.from_env().hosted


@dataclass(frozen=True)
class LakebaseAppResource:
    host: str
    database: str
    user: str
    port: int = 5432
    endpoint: str = ""

    @classmethod
    def from_env(cls):
        if not is_databricks_app():
            return None
        host, database, user = (
            os.getenv(key, "").strip() for key in ("PGHOST", "PGDATABASE", "PGUSER")
        )
        if not (host or database or user):
            return None
        if not all((host, database, user)):
            raise ValueError(
                "The Lakebase app resource must supply PGHOST, PGDATABASE and PGUSER"
            )
        port = int(os.getenv("PGPORT", "5432"))
        if not 1 <= port <= 65535:
            raise ValueError("Invalid PGPORT in the Lakebase app resource")
        return cls(
            host, database, user, port, os.getenv("KASAL_LAKEBASE_RESOURCE", "").strip()
        )

    def configuration(self) -> dict:
        return {
            "enabled": True,
            "database_type": "lakebase",
            "instance_status": "READY",
            "endpoint": self.host,
            "instance_name": self.endpoint or self.host,
            "database_name": self.database,
            "installation_managed": True,
        }
