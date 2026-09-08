"""Connect to the exact Lakebase database selected in Databricks Apps resources."""

import asyncio
import uuid

from sqlalchemy.engine import URL

from src.core.databricks_app import DatabricksAppInstallation, LakebaseAppResource


async def resource_token(client, resource: LakebaseAppResource) -> str:
    """Mint for the assigned endpoint; never guess the production branch."""
    endpoint = resource.endpoint
    if endpoint.startswith("projects/"):
        credential = await asyncio.to_thread(
            client.postgres.generate_database_credential, endpoint=endpoint
        )
        return credential.token

    # Provisioned resources resolve valueFrom to a hostname. Read the matching
    # app resource to obtain its instance, rather than choosing a default one.
    app = await asyncio.to_thread(
        client.apps.get, DatabricksAppInstallation.from_env().app_name
    )
    databases = [item for item in app.resources or [] if item.database or item.postgres]
    if (
        len(databases) != 1
        or databases[0].name != "lakebase"
        or not databases[0].database
        or databases[0].database.database_name != resource.database
    ):
        raise ValueError(
            "Bind the Lakebase resource as 'lakebase' so its exact endpoint can be resolved"
        )
    credential = await asyncio.to_thread(
        client.database.generate_database_credential,
        request_id=str(uuid.uuid4()),
        instance_names=[databases[0].database.instance_name],
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
