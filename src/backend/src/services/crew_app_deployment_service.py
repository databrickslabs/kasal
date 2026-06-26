"""Service for deploying an exported CrewAI crew directly to Databricks Apps.

This deploys the same project produced by :class:`DatabricksAppExporter` to the
Databricks Apps platform from the running backend — no local CLI. It mirrors the
proven create -> upload -> deploy -> start -> poll sequence in ``src/deploy.py``
but uses the SDK only (the ``databricks`` CLI is not present in the Apps runtime).
Auth prefers the requesting user's OBO token and falls back to the workspace PAT
(then service principal) via ``get_workspace_client`` — so it also works when the
deploy is run locally without a forwarded OBO token.

The deploy takes several minutes, so it runs as a background task and progress is
tracked in an in-memory registry the API polls. (The registry is process-local —
status is lost on a backend restart, which is acceptable for this transient op.)
"""

import asyncio
import io
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from src.schemas.crew_export import (
    AppDeploymentConfig,
    AppDeploymentResponse,
    AppDeploymentStatusResponse,
    ExportFormat,
)
from src.services.crew_export_service import CrewExportService
from src.utils.databricks_auth import get_workspace_client
from src.utils.user_context import GroupContext

logger = logging.getLogger(__name__)

# Terminal + in-flight step labels surfaced to the UI.
STATUS_PENDING = "PENDING"
STATUS_RUNNING = "RUNNING"
STATUS_SUCCEEDED = "SUCCEEDED"
STATUS_FAILED = "FAILED"

# OAuth user-authorization scopes granted to the deployed app — the umbrella
# scope ids Databricks Apps uses on the consent screen. Kept in sync with
# get_desired_oauth_scopes() in src/deploy.py (Kasal's own app uses the same set).
DESIRED_OAUTH_SCOPES = [
    "sql",
    "genie",
    "postgres",
    "model-serving",
    "catalog.catalogs:read",
    "catalog.schemas:read",
    "catalog.tables:read",
    "catalog.connections",
    "files",
    "ai-gateway",
    "vector-search",
    "workspace.workspace",
    "mcp.external",
    "mcp.functions",
]


class CrewAppDeploymentService:
    """Deploy a crew to Databricks Apps under the requesting user's identity."""

    # Process-local registry of deployment status, keyed by deployment_id.
    _deployments: Dict[str, Dict[str, Any]] = {}

    def __init__(self, session: AsyncSession):
        self.session = session
        self.export_service = CrewExportService(session=session)

    async def start_deployment(
        self,
        crew_id: str,
        config: AppDeploymentConfig,
        group_context: Optional[GroupContext] = None,
    ) -> AppDeploymentResponse:
        """Generate the app project, then kick off the deploy in the background."""
        user_token = group_context.access_token if group_context else None
        group_id = group_context.primary_group_id if group_context else None

        # Build the workspace client up front so auth failures surface now AND so
        # we can create the MLflow experiment before generating the project.
        # Auth chain (handled by get_workspace_client): OBO user token first,
        # then the workspace PAT (from the DB, scoped to the group), then the
        # service principal. The PAT/SPN fallback is what makes this work when
        # running the deploy locally with no forwarded OBO token.
        from databricks.sdk.useragent import with_product

        from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

        with_product(f"{KASAL_BASE}_{KasalProduct.DEPLOYMENT}", VERSION)
        client = await get_workspace_client(user_token=user_token, group_id=group_id)
        if client is None:
            raise PermissionError(
                "Could not authenticate to Databricks. Provide a workspace PAT "
                "(or log in via Databricks OAuth) with access to the workspace."
            )

        # The deployed app creates its OWN MLflow experiment, bound to Unity
        # Catalog at creation — the ONLY way to get UC trace storage, since a UC
        # trace location cannot be added to an existing experiment (Databricks:
        # "an experiment can only be bound to a UC trace location at creation
        # time"). So we do NOT pre-create an experiment here (that would create a
        # plain, non-UC experiment and permanently poison the name); we just pass
        # the desired name and let the app create it UC-bound. Default to the app
        # name when the deploy screen leaves it blank.
        experiment_name = (
            config.experiment_name or self._normalize_app_name(config.app_name) or ""
        )

        # Thread the deploy-screen selections into the export options so they are
        # baked into the generated project (model, catalog/schema, experiment).
        opts = config.options
        if config.model:
            opts.model_override = config.model
        if config.catalog:
            opts.databricks_catalog = config.catalog
        if config.schema_name:
            opts.databricks_schema = config.schema_name
        if config.lakebase_instance:
            # Surfaced to the deployed app as LAKEBASE_INSTANCE_NAME so the crew
            # can connect to its database (the resource attaches the credential).
            opts.lakebase_instance = config.lakebase_instance
        if config.warehouse_id:
            # SQL warehouse the app uses to provision its UC trace tables. When
            # not chosen on the deploy screen, the exporter falls back to the
            # workspace's configured warehouse (from crew_data).
            opts.databricks_warehouse_id = config.warehouse_id
        if experiment_name:
            opts.mlflow_experiment_name = experiment_name

        # Effective UC target (deploy-screen selection first, else the workspace's
        # configured catalog/schema/warehouse). Used for the OTel telemetry tables
        # and to auto-grant the app's service principal the explicit UC privileges
        # it needs to provision/write trace tables (ALL_PRIVILEGES is NOT enough).
        ws_catalog, ws_schema, ws_warehouse = (
            await self.export_service._get_databricks_catalog_schema(group_context)
        )
        eff_catalog = config.catalog or ws_catalog or ""
        eff_schema = config.schema_name or ws_schema or ""
        eff_warehouse = config.warehouse_id or ws_warehouse or ""

        # Generate the deployable project (reuses the export pipeline).
        export = await self.export_service.export_crew(
            crew_id=crew_id,
            export_format=ExportFormat.DATABRICKS_APP,
            options=opts,
            group_context=group_context,
        )
        files: List[Dict[str, str]] = export["files"]
        app_name = (
            self._normalize_app_name(config.app_name) or export["metadata"]["app_name"]
        )

        deployment_id = str(uuid.uuid4())
        self._deployments[deployment_id] = {
            "deployment_id": deployment_id,
            "crew_id": str(crew_id),
            "app_name": app_name,
            "status": STATUS_PENDING,
            "step": "QUEUED",
            "message": "Deployment queued",
            "app_url": None,
            "error": None,
        }

        # Fire-and-forget; progress is polled via get_status().
        asyncio.create_task(
            self._run_deployment(
                deployment_id,
                client,
                app_name,
                files,
                eff_catalog,
                eff_schema,
                config.lakebase_instance or "",
                bool(config.create_lakebase),
                eff_warehouse,
            )
        )

        logger.info(
            "Started Databricks Apps deployment %s for crew %s -> %s",
            deployment_id,
            crew_id,
            app_name,
        )
        return AppDeploymentResponse(
            deployment_id=deployment_id,
            crew_id=str(crew_id),
            app_name=app_name,
            status=STATUS_PENDING,
            message="Deployment started",
        )

    def get_status(self, deployment_id: str) -> Optional[AppDeploymentStatusResponse]:
        record = self._deployments.get(deployment_id)
        if not record:
            return None
        return AppDeploymentStatusResponse(**record)

    async def list_lakebase_instances(
        self, group_context: Optional[GroupContext] = None
    ) -> List[Dict[str, Any]]:
        """List the workspace's Lakebase (database) instances for the deploy screen.

        Non-fatal: returns [] if Lakebase is unavailable or the lookup fails, so
        the deploy screen can still offer "create new".
        """
        user_token = group_context.access_token if group_context else None
        group_id = group_context.primary_group_id if group_context else None

        from databricks.sdk.useragent import with_product

        from src.utils.telemetry import KASAL_BASE, VERSION, KasalProduct

        with_product(f"{KASAL_BASE}_{KasalProduct.DEPLOYMENT}", VERSION)
        client = await get_workspace_client(user_token=user_token, group_id=group_id)
        if client is None:
            raise PermissionError(
                "Could not authenticate to Databricks. Provide a workspace PAT "
                "(or log in via Databricks OAuth) to list Lakebase instances."
            )

        def _list() -> List[Dict[str, Any]]:
            out: List[Dict[str, Any]] = []
            for inst in client.database.list_database_instances():
                name = getattr(inst, "name", None)
                if not name:
                    continue
                state = getattr(inst, "state", None)
                out.append(
                    {
                        "name": name,
                        "state": getattr(state, "value", None) or str(state or ""),
                        "capacity": getattr(inst, "capacity", None),
                    }
                )
            return out

        try:
            return await asyncio.to_thread(_list)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not list Lakebase instances: %s", exc)
            return []

    # ── Internals ──────────────────────────────────────────────────────

    @staticmethod
    def _ensure_lakebase_instance(
        client: Any, name: str, capacity: str = "CU_1"
    ) -> None:
        """Create a Lakebase (database) instance if it doesn't already exist.

        Blocks until the instance is available (``create_*_and_wait``). If an
        instance with this name already exists, it's reused.
        """
        from databricks.sdk.service.database import DatabaseInstance

        try:
            client.database.get_database_instance(name=name)
            logger.info("Lakebase instance %s already exists; reusing", name)
            return
        except Exception:  # noqa: BLE001
            pass
        logger.info("Creating Lakebase instance %s (capacity=%s)", name, capacity)
        client.database.create_database_instance_and_wait(
            DatabaseInstance(name=name, capacity=capacity)
        )

    def _grant_trace_permissions(
        self,
        deployment_id: str,
        client: Any,
        app_name: str,
        catalog: str,
        schema: str,
        warehouse_id: str,
    ) -> None:
        """Grant the app's service principal the explicit UC privileges needed to
        provision + write its MLflow trace tables.

        UC trace storage requires EXPLICIT ``CREATE TABLE``/``MODIFY``/``SELECT``
        (plus ``USE CATALOG``/``USE SCHEMA``) — ``ALL_PRIVILEGES`` does NOT satisfy
        it. Best-effort: runs as the deploying identity (which must own/manage the
        schema); logs a clear hint on failure rather than failing the deploy.
        """
        self._set(
            deployment_id,
            step="GRANTING_TRACE_PERMS",
            message=f"Granting trace permissions on {catalog}.{schema}",
        )
        try:
            sp = getattr(
                client.apps.get(name=app_name), "service_principal_client_id", None
            )
            if not sp:
                logger.warning("No app service principal id; skipping trace grants")
                return
            statements = [
                f"GRANT USE CATALOG ON CATALOG `{catalog}` TO `{sp}`",
                "GRANT USE SCHEMA, CREATE TABLE, MODIFY, SELECT ON SCHEMA "
                f"`{catalog}`.`{schema}` TO `{sp}`",
            ]
            for stmt in statements:
                client.statement_execution.execute_statement(
                    warehouse_id=warehouse_id, statement=stmt, wait_timeout="30s"
                )
            logger.info(
                "Granted UC trace privileges on %s.%s to app SP %s",
                catalog,
                schema,
                sp,
            )
        except Exception as exc:  # noqa: BLE001
            # The deploying user may not own the schema. Surface a clear hint; the
            # app still deploys (traces fall back to managed storage until granted).
            logger.warning("Could not grant trace permissions (continuing): %s", exc)
            self._set(
                deployment_id,
                message=(
                    f"Could not grant UC trace permissions on {catalog}.{schema} "
                    f"({exc}). Grant the app's service principal USE CATALOG/SCHEMA "
                    "+ CREATE TABLE + MODIFY + SELECT to enable Unity Catalog traces."
                ),
            )

    def _set(self, deployment_id: str, **fields: Any) -> None:
        record = self._deployments.get(deployment_id)
        if record:
            record.update(fields)

    @staticmethod
    def _normalize_app_name(name: Optional[str]) -> Optional[str]:
        """Coerce a user-supplied name into a valid Databricks App name."""
        if not name:
            return None
        import re

        slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
        if slug and not slug[0].isalpha():
            slug = f"agent-{slug}".strip("-")
        slug = slug[:30].strip("-")
        return slug or None

    def _configure_app(
        self,
        deployment_id: str,
        client: Any,
        app_name: str,
        catalog: str,
        schema: str,
        lakebase_instance: str = "",
    ) -> None:
        """Set the app's OAuth scopes, Lakebase resource, and OTel->UC telemetry
        destination in a single update.

        Note: the MLflow experiment is NOT attached as a resource — the app
        creates its own UC-bound experiment at startup (owned by the app's
        service principal), since a UC trace location can only be set at
        experiment creation time.

        Builds a CLEAN ``App`` (not the ``get()`` result, whose read-only fields make
        ``update`` fail). Non-fatal: logs/records the error and continues, since the
        deploy can still succeed without full configuration.
        """
        from databricks.sdk.service.apps import (
            App,
            AppResource,
            AppResourceDatabase,
            AppResourceDatabaseDatabasePermission,
            TelemetryExportDestination,
            UnityCatalog,
        )

        app = App(
            name=app_name,
            description="Deployed from Kasal",
            user_api_scopes=list(DESIRED_OAUTH_SCOPES),
        )
        resources = []
        if lakebase_instance:
            # Attaching the Lakebase instance grants the app's identity
            # connect+create on it (the platform injects the DB credential).
            resources.append(
                AppResource(
                    name="database",
                    database=AppResourceDatabase(
                        instance_name=lakebase_instance,
                        database_name="databricks_postgres",
                        permission=(
                            AppResourceDatabaseDatabasePermission.CAN_CONNECT_AND_CREATE
                        ),
                    ),
                )
            )
        if resources:
            app.resources = resources
        # OTel -> Unity Catalog: configuring this makes the platform inject
        # OTEL_EXPORTER_OTLP_ENDPOINT and route logs/metrics/spans to these tables.
        if catalog and schema:
            app.telemetry_export_destinations = [
                TelemetryExportDestination(
                    unity_catalog=UnityCatalog(
                        logs_table=f"{catalog}.{schema}.otel_logs",
                        metrics_table=f"{catalog}.{schema}.otel_metrics",
                        traces_table=f"{catalog}.{schema}.otel_spans",
                    )
                )
            ]
        try:
            client.apps.update(name=app_name, app=app)
            logger.info(
                "Configured app %s: %d scopes, telemetry=%s",
                app_name,
                len(DESIRED_OAUTH_SCOPES),
                f"{catalog}.{schema}" if (catalog and schema) else "none",
            )
            # Read back so the deploy confirms the resource actually landed.
            try:
                landed = client.apps.get(name=app_name)
                res_names = [
                    getattr(r, "name", "")
                    for r in (getattr(landed, "resources", None) or [])
                ]
                logger.info(
                    "App %s resources after update: %s", app_name, res_names or "none"
                )
                res_summary = ", ".join(res_names) or "none"
                self._set(
                    deployment_id,
                    message=f"App configured (resources: {res_summary})",
                )
            except Exception:  # noqa: BLE001
                pass
        except Exception as exc:  # noqa: BLE001
            logger.warning("App configuration failed (continuing): %s", exc)
            self._set(deployment_id, message=f"App configuration warning: {exc}")

    async def _run_deployment(
        self,
        deployment_id: str,
        client: Any,
        app_name: str,
        files: List[Dict[str, str]],
        catalog: str = "",
        schema: str = "",
        lakebase_instance: str = "",
        create_lakebase: bool = False,
        warehouse_id: str = "",
    ) -> None:
        """Run the blocking SDK deploy off the event loop and record the outcome."""
        loop = asyncio.get_event_loop()
        try:
            await loop.run_in_executor(
                None,
                self._deploy_blocking,
                deployment_id,
                client,
                app_name,
                files,
                catalog,
                schema,
                lakebase_instance,
                create_lakebase,
                warehouse_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception(f"Deployment {deployment_id} failed")
            self._set(
                deployment_id,
                status=STATUS_FAILED,
                step="FAILED",
                message="Deployment failed",
                error=str(exc),
            )

    def _deploy_blocking(
        self,
        deployment_id: str,
        client: Any,
        app_name: str,
        files: List[Dict[str, str]],
        catalog: str = "",
        schema: str = "",
        lakebase_instance: str = "",
        create_lakebase: bool = False,
        warehouse_id: str = "",
    ) -> None:
        from databricks.sdk.service.apps import (
            App,
            AppDeployment,
            AppDeploymentMode,
        )
        from databricks.sdk.service.workspace import ImportFormat

        self._set(
            deployment_id,
            status=STATUS_RUNNING,
            step="CREATING_APP",
            message=f"Creating app '{app_name}'",
        )

        # 1. Create the app if it does not already exist.
        try:
            client.apps.get(name=app_name)
            logger.info(f"App {app_name} already exists; redeploying")
        except Exception:
            logger.info(f"Creating app {app_name}")
            client.apps.create_and_wait(
                app=App(name=app_name, description="Deployed from Kasal")
            )

        # 1a. Create the Lakebase instance if the user asked for a new one. It
        # must exist before it can be attached as an app resource below.
        if lakebase_instance and create_lakebase:
            self._set(
                deployment_id,
                step="CREATING_LAKEBASE",
                message=f"Creating Lakebase instance '{lakebase_instance}'",
            )
            try:
                self._ensure_lakebase_instance(client, lakebase_instance)
            except Exception as exc:  # noqa: BLE001
                # Non-fatal: continue the deploy, but the resource attach may fail.
                logger.warning("Lakebase instance creation failed: %s", exc)
                self._set(deployment_id, message=f"Lakebase creation warning: {exc}")

        # 1b. Configure the app: OAuth scopes the agent needs at runtime, the
        # Lakebase instance as a resource (so the SP can connect to the DB), and
        # the OTel -> Unity Catalog telemetry destination (logs/metrics/spans
        # tables). The MLflow experiment is NOT attached — the app creates its own
        # UC-bound experiment at startup (see agent_server/agent.py).
        self._set(
            deployment_id,
            step="CONFIGURING_APP",
            message="Configuring app scopes/resources/telemetry",
        )
        self._configure_app(
            deployment_id,
            client,
            app_name,
            catalog,
            schema,
            lakebase_instance,
        )

        # 1c. Grant the app's service principal the EXPLICIT Unity Catalog
        # privileges it needs to provision + write its trace tables. ALL_PRIVILEGES
        # is NOT sufficient for UC trace tables (per the Databricks docs), so we
        # grant USE CATALOG / USE SCHEMA / CREATE TABLE / MODIFY / SELECT directly.
        if catalog and schema and warehouse_id:
            self._grant_trace_permissions(
                deployment_id, client, app_name, catalog, schema, warehouse_id
            )

        # 2. Upload the project tree to the user's workspace.
        try:
            user_name = client.current_user.me().user_name
        except Exception:
            user_name = "unknown"
        workspace_dir = f"/Workspace/Users/{user_name}/kasal-apps/{app_name}"
        self._set(
            deployment_id,
            step="UPLOADING",
            message=f"Uploading {len(files)} files to {workspace_dir}",
        )

        # Pre-create every directory depth-first (upload does not mkdirs).
        dirs = sorted(
            {
                f"{workspace_dir}/{file['path'].rsplit('/', 1)[0]}"
                for file in files
                if "/" in file["path"]
            }
            | {workspace_dir}
        )
        for directory in dirs:
            try:
                client.workspace.mkdirs(directory)
            except Exception as exc:  # noqa: BLE001
                logger.debug(f"mkdirs {directory}: {exc}")

        for file in files:
            remote_path = f"{workspace_dir}/{file['path']}"
            client.workspace.upload(
                remote_path,
                io.BytesIO(file["content"].encode("utf-8")),
                format=ImportFormat.AUTO,
                overwrite=True,
            )

        # 3. Deploy the uploaded snapshot.
        self._set(deployment_id, step="DEPLOYING", message="Deploying app")
        app_deployment = AppDeployment(
            source_code_path=workspace_dir, mode=AppDeploymentMode.SNAPSHOT
        )
        deadline = time.time() + 1800  # allow an in-flight deploy up to 30 min
        while True:
            try:
                waiter = client.apps.deploy(
                    app_name=app_name, app_deployment=app_deployment
                )
                dep_id = getattr(getattr(waiter, "bind", None), "deployment_id", None)
                try:
                    result = waiter.result()
                    dep_id = result.deployment_id
                except TimeoutError:
                    if not dep_id or not self._wait_for_deployment(
                        client, app_name, dep_id
                    ):
                        raise RuntimeError("App deployment did not succeed")
                break
            except Exception as exc:  # noqa: BLE001
                if (
                    "active deployment in progress" in str(exc)
                    and time.time() < deadline
                ):
                    logger.info("Another deployment in progress; retrying in 20s")
                    time.sleep(20)
                    continue
                raise

        # 4. Start (or restart) the app, then resolve its URL.
        self._set(deployment_id, step="STARTING", message="Starting app")
        try:
            client.apps.start(app_name)
        except Exception as exc:  # noqa: BLE001
            if "compute is in ACTIVE state" not in str(exc):
                raise

        app_url = None
        try:
            app_url = getattr(client.apps.get(name=app_name), "url", None)
        except Exception:
            pass
        if not app_url:
            host = (getattr(client.config, "host", "") or "").rstrip("/")
            app_url = f"{host}#apps/{app_name}" if host else None

        self._set(
            deployment_id,
            status=STATUS_SUCCEEDED,
            step="SUCCEEDED",
            message="App deployed and started",
            app_url=app_url,
        )
        logger.info(f"Deployment {deployment_id} succeeded: {app_url}")

    @staticmethod
    def _wait_for_deployment(
        client: Any,
        app_name: str,
        deployment_id: str,
        timeout_seconds: int = 3600,
        interval_seconds: int = 30,
    ) -> bool:
        """Poll an app deployment until it reaches a terminal state (from deploy.py)."""
        from databricks.sdk.service.apps import AppDeploymentState

        deadline = time.time() + timeout_seconds
        while time.time() < deadline:
            try:
                dep = client.apps.get_deployment(
                    app_name=app_name, deployment_id=deployment_id
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(f"Error polling deployment {deployment_id}: {exc}")
                time.sleep(interval_seconds)
                continue
            state = getattr(getattr(dep, "status", None), "state", None)
            if state == AppDeploymentState.SUCCEEDED:
                return True
            if state in (AppDeploymentState.FAILED, AppDeploymentState.CANCELLED):
                return False
            time.sleep(interval_seconds)
        return False
