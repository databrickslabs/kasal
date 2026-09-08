"""Connect to the exact Lakebase database selected in Databricks Apps resources."""

import asyncio
import re
import uuid

from sqlalchemy.engine import URL

from src.core.databricks_app import DatabricksAppInstallation, LakebaseAppResource

_ENDPOINT_PATH = re.compile(r"projects/[^/]+/branches/[^/]+/endpoints/[^/]+")
_BRANCH_PATH = re.compile(r"projects/[^/]+/branches/[^/]+")
_DATABASE_PATH = re.compile(r"(projects/[^/]+/branches/[^/]+)/databases/[^/]+")


def _database_resource(app):
    """PG* variables describe the first database; require an unambiguous binding."""
    databases = [
        item
        for item in app.resources or []
        if getattr(item, "database", None) or getattr(item, "postgres", None)
    ]
    if not databases:
        raise ValueError(
            "No Lakebase Database resource is attached to this Databricks App. "
            "Attach the database and redeploy so PG connection settings are refreshed."
        )
    if len(databases) != 1:
        raise ValueError(
            "Multiple Lakebase Database resources are attached. Kasal requires "
            "exactly one because Databricks injects PG settings for the first database only."
        )
    item = databases[0]
    if item.database and item.postgres:
        raise ValueError(
            "The Lakebase resource has both database and postgres metadata; configure one resource type"
        )
    return item


async def _autoscaling_endpoint(client, attached, resource: LakebaseAppResource) -> str:
    """Resolve only the attached branch, matching PGHOST rather than list order."""
    branch = (attached.branch or "").strip()
    database = (attached.database or "").strip()
    database_path = _DATABASE_PATH.fullmatch(database)
    if database_path:
        database_branch = database_path.group(1)
        if branch and branch != database_branch:
            raise ValueError(
                "The Lakebase resource's database belongs to a different branch than its branch setting"
            )
        branch = database_branch
        # The resource's database ID need not be its PostgreSQL database name.
        metadata = await asyncio.to_thread(client.postgres.get_database, name=database)
        database = (
            getattr(metadata.status, "postgres_database", None)
            or getattr(metadata.spec, "postgres_database", None)
            or ""
        )
    if not database:
        raise ValueError(
            "The Lakebase Autoscaling resource does not identify a PostgreSQL database"
        )
    if database != resource.database:
        raise ValueError(
            "PGDATABASE does not match the attached Lakebase Autoscaling database. "
            "Remove manual PG overrides and redeploy with the intended database resource."
        )
    if not _BRANCH_PATH.fullmatch(branch):
        raise ValueError(
            "The Lakebase Autoscaling resource must identify its full projects/.../branches/... path"
        )

    # SDK pagination performs I/O during iteration, so consume it in the worker.
    endpoints = await asyncio.to_thread(
        lambda: list(client.postgres.list_endpoints(parent=branch))
    )
    matches = [
        endpoint
        for endpoint in endpoints
        if (getattr(getattr(endpoint.status, "hosts", None), "host", "") or "")
        .lower()
        .rstrip(".")
        == resource.host.lower().rstrip(".")
        and _ENDPOINT_PATH.fullmatch(endpoint.name or "")
        and endpoint.name.rsplit("/endpoints/", 1)[0] == branch
    ]
    if len(matches) != 1:
        raise ValueError(
            "Could not uniquely match PGHOST to an endpoint in the attached Lakebase branch. "
            "Check the database resource and remove manual PGHOST overrides before redeploying."
        )
    return matches[0].name


async def resource_token(client, resource: LakebaseAppResource) -> str:
    """Mint for the assigned endpoint or resolve the attached database's actual type."""
    endpoint = resource.endpoint.strip().lstrip("/")
    if _ENDPOINT_PATH.fullmatch(endpoint):
        credential = await asyncio.to_thread(
            client.postgres.generate_database_credential, endpoint=endpoint
        )
        return credential.token

    # A hostname or absent valueFrom is not evidence of a Provisioned database.
    # Use the native resource metadata; its key is a label, not database identity.
    app = await asyncio.to_thread(
        client.apps.get, DatabricksAppInstallation.from_env().app_name
    )
    attached = _database_resource(app)
    if attached.postgres:
        endpoint = await _autoscaling_endpoint(client, attached.postgres, resource)
        credential = await asyncio.to_thread(
            client.postgres.generate_database_credential, endpoint=endpoint
        )
    else:
        if attached.database.database_name != resource.database:
            raise ValueError(
                "PGDATABASE does not match the attached Lakebase Provisioned database. "
                "Remove manual PG overrides and redeploy with the intended database resource."
            )
        if not attached.database.instance_name:
            raise ValueError(
                "The Lakebase Provisioned resource is missing its instance name"
            )
        credential = await asyncio.to_thread(
            client.database.generate_database_credential,
            request_id=str(uuid.uuid4()),
            instance_names=[attached.database.instance_name],
        )
    return credential.token


def resource_url(resource: LakebaseAppResource) -> str:
    return URL.create(
        "postgresql+asyncpg",
        username=resource.user,
        password="placeholder",
        host=resource.host,
        port=resource.port,
        database=resource.database,
    ).render_as_string(hide_password=False)


async def initialize_resource_database() -> None:
    """Initialize only the assigned database, before any seeding or requests."""
    from sqlalchemy import text

    from src.db.all_models import Base
    from src.db.lakebase_session import LakebaseSessionFactory
    from src.db.lakebase_state import mark_lakebase_activated
    from src.db.session import async_session_factory, run_schema_self_heal

    resource = LakebaseAppResource.from_env()
    if resource is None:
        raise ValueError("Attach a Lakebase database resource before deploying Kasal")
    factory = LakebaseSessionFactory(resource.endpoint or resource.host)
    try:
        await factory.create_engine()
        async with factory._engine.begin() as connection:
            vector = await connection.execute(
                text("SELECT 1 FROM pg_extension WHERE extname = 'vector'")
            )
            if vector.scalar() != 1:
                raise RuntimeError(
                    "Prepare the selected Lakebase database before deployment: its owner must run CREATE EXTENSION IF NOT EXISTS vector"
                )
            await connection.execute(text("CREATE SCHEMA IF NOT EXISTS kasal"))
            await connection.run_sync(Base.metadata.create_all)
            await run_schema_self_heal(connection)
        async_session_factory.activate_lakebase(factory._session_factory)
        mark_lakebase_activated()
        # Keep the engine and refresh task in the factory used by normal reads.
        from src.db import lakebase_session

        lakebase_session._lakebase_factory = factory
    except Exception:
        await factory.dispose()
        raise
