# Databricks Apps installation

Kasal reads infrastructure from the Databricks App's assigned resources. In a
hosted installation, users do not need to enter a warehouse ID, configure
Lakebase, or enable MLflow in Kasal. Local development remains opt-in.

## Select resources before deployment

The supported Marketplace requirements are in `src/manifest.yaml` and their
runtime bindings are in `src/app.yaml`.

| Resource key | Select | Permission |
| --- | --- | --- |
| `sql-warehouse` | SQL warehouse for trace queries and SQL operations | Can use |
| `mlflow-experiment` | An installation experiment configured with Unity Catalog trace storage | Can read |
| `output-volume` | Existing UC volume for generated files | Can read and write |
| `default-model` | Ready, chat-capable serving endpoint supporting tool calls | Can query |
| `embedding-model` | 1024-dimensional embedding endpoint, such as `databricks-gte-large-en` | Can query |
| `encryption-key` | Secret containing a persistent Fernet key | Read |
| `lakebase` | Lakebase project, branch and database | Can connect and create |

**Lakebase Marketplace limitation:** the public AppManifest schema currently
has no database/postgres `resource_specs` field. Attach a native **Database**
resource named `lakebase` in Databricks App resources before deployment. Do not
add an unsupported manifest field. Attach exactly one database: Databricks
injects `PGHOST`, `PGDATABASE`, `PGUSER` and `PGPORT` for the first database only.
New installations should use Lakebase Autoscaling. Keep an existing
Provisioned resource's type when upgrading it.

The selected database and branch are used as supplied. Kasal does not guess a
production branch or create a replacement SQLite database if Lakebase fails.
Existing data is not automatically migrated when changing the assigned database.

## Installer preparation

Complete these once with an administrator, before starting Kasal:

1. In the selected Lakebase database, enable pgvector as its owner:

   ```sql
   CREATE EXTENSION IF NOT EXISTS vector;
   ```

   The app's resource grant permits schemas and tables, but does not grant the
   superuser capability required to install this extension. Kasal creates its
   schema, application tables and memory tables at startup. Knowledge embeddings
   use the same database; raw uploads are temporary and are deleted after ingestion.

2. Prepare a private Unity Catalog schema for traces. Configure the installation
   experiment's trace storage there. Grant the app principal `USE CATALOG`,
   `USE SCHEMA` and `CREATE TABLE`. Configure explicit `SELECT` and `MODIFY`
   privileges for the app principal on the trace schema, as required by MLflow
   UC trace storage. Avoid broad inherited read grants to other users.
   The experiment resource grants access to the experiment; it does not supply
   these additional UC permissions.

3. Create a Fernet key secret once and assign it as `encryption-key`. On upgrades,
   reuse the key that encrypted the existing data. Replacing it makes existing
   encrypted credentials unreadable. The legacy CLI installation normally uses
   `kasal/kasal_encryption_key`; select that existing secret when upgrading.

The CLI deploy command validates required resource keys before building or
uploading. For a new CLI installation, create the app and attach these resources
first. Marketplace declares the six supported resource requirements; the native
Lakebase attachment remains a separate step until that manifest schema supports it.

## Defaults and isolation

- `default-model` becomes the server default for Chat and builder prompts,
  including intent classification. Explicit model selections and saved crews
  keep their models. Assigned model endpoints use the app identity granted access
  by the resource; other endpoints retain the usual authentication path. Unknown custom endpoints are registered with conservative
  limits; adjust their model configuration to the endpoint's actual capabilities.
- Memory and knowledge use `embedding-model`. Keep its identity and vector
  dimension unchanged for existing data unless you re-embed that data.
- New personal spaces and teamspaces inherit Lakebase memory. Explicit memory
  configurations and disabled settings remain respected.
- MLflow tracing defaults on; paid evaluations remain opt-in. An explicit saved
  tracing opt-out stays off. Kasal creates a stable experiment and UC table
  prefix per personal space or teamspace, separate from the installation's
  resource experiment. Trace destinations are also bound per async request.
- Personal spaces are separate; members of one teamspace share that team's
  scope. UC privileges govern direct Databricks access, so experiment names and
  Kasal filters do not replace a private UC schema and correct grants.
- Generated files go beneath the assigned volume in installation/teamspace
  directories. Volume grants apply to the whole volume, not individual folders;
  avoid granting ordinary users direct access to the whole volume when outputs
  must remain isolated through Kasal.
- Installed connection and storage settings are displayed as managed in
  Configuration. Change assigned infrastructure in Databricks App resources
  and redeploy. Existing traces and files are not moved automatically.

Hosted detection requires the platform's app name, port, workspace ID and host.
SDK credentials alone do not enable these defaults. The local dev entrypoint
sets `KASAL_DEPLOYMENT_MODE=local`, which explicitly disables installation defaults.

## References

- [Databricks Apps resource environment variables](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/environment-variables)
- [Lakebase App resources and PostgreSQL settings](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase)
- [Serving endpoint resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/model-serving)
- [Unity Catalog volume resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/uc-volumes)
- [MLflow trace storage in Unity Catalog](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog)
- [Public AppManifest SDK schema](https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/apps.py)
