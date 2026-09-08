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
| `volume` | Existing UC volume for generated files; its parent catalog/schema is used for new trace tables | Can read and write |
| `serving-endpoint` | Ready, chat-capable serving endpoint supporting tool calls | Can query |
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

## Startup and fresh databases

The frontend build and backend startup are separate steps. For Git/Marketplace
source deployments, `src/package.json` builds the frontend; `src/app.yaml` then
starts `python entrypoint.py`. The deployment script also ships that entrypoint
and command. The entrypoint serves the backend's original FastAPI application,
including its startup lifecycle, middleware, and shutdown handlers.

Before accepting requests, Kasal creates the application tables in the assigned
Lakebase database, applies schema updates, initializes memory tables, and awaits
seeding when `AUTO_SEED_DATABASE` is enabled (the default). Startup verifies that
models, prompt templates, and tools have persisted defaults. A database setup
failure stops startup instead of serving an application with missing tables.
Redeploying runs the non-destructive initialization again.

Kasal attempts to enable pgvector if absent. If the app principal cannot install
it, complete the owner preparation below; Kasal does not grant itself additional
permissions. Missing `users` tables can prevent personal teamspace allocation:
fix database startup first rather than changing a user's permissions.

## Adding members before their first visit

In teamspace configuration, select existing Kasal users or enter email addresses
in **Add User**. Users do not need to open Kasal first. Membership is assigned to
the supplied email and becomes available when that identity signs in. This does
not send an invitation or grant access to the Databricks app itself.

The suggestions list contains Kasal users, not the entire Databricks directory.
A Databricks directory search would require a separate SCIM integration with
appropriate directory-read permissions.

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

2. Use a volume in a private Unity Catalog schema. Kasal derives the trace
   namespace from its path: `/Volumes/catalog/schema/volume` supplies `catalog`
   and `schema`. Grant the app principal `USE CATALOG`, `USE SCHEMA` and
   `CREATE TABLE`, plus explicit `SELECT` and `MODIFY` on that schema so its
   trace tables inherit the privileges required by MLflow UC trace storage.
   Avoid broad inherited read grants to other users. The volume resource's
   read/write grant does not supply these table privileges.

There is no experiment to create or attach during installation. Kasal provisions
a stable experiment and trace tables for each personal space/teamspace when it
first uses tracing. This runs as the app service principal and requires the
permissions above; the app does not grant itself privileges.

Encryption keys are managed automatically. On first startup Kasal creates a
private Databricks secret scope using the app service principal and persists
both its RSA keys and Fernet key there. Redeployment restores the same keys;
users do not create an encryption key or attach a Secret resource. Existing
explicit keys, old `encryption-key` resource bindings, and available local RSA
keys are retained when initializing the private scope. Keys that were already
lost before an upgrade cannot be recovered automatically.

Reuse applies to redeploying the same app with its existing service principal
and secret scope. Deleting and recreating the app changes that identity; migrate
the encryption material before reusing an existing database with a new app.

The app must be allowed to create/manage its own secret scope. If workspace
policy prevents this, startup reports that permission problem and does not
substitute an ephemeral key. Key material is never stored in the output volume
or application database.

The CLI deploy command validates required resource keys before building or
uploading. For a new CLI installation, create the app and attach these resources
first. Marketplace declares the three supported resource requirements; the native
Lakebase attachment remains a separate step until that manifest schema supports it.

## Defaults and isolation

- `serving-endpoint` becomes the server default for Chat and builder prompts,
  including intent classification. Explicit model selections and saved crews
  keep their models. Assigned model endpoints use the app identity granted access
  by the resource; other endpoints retain the usual authentication path. Unknown custom endpoints are registered with conservative
  limits; adjust their model configuration to the endpoint's actual capabilities.
- Memory and knowledge default to `databricks-gte-large-en` (1024 dimensions).
  They use the existing Databricks authentication path, with no embedding
  resource to configure during installation.
- New personal spaces and teamspaces inherit Lakebase memory. Explicit memory
  configurations and disabled settings remain respected.
- MLflow tracing defaults on; paid evaluations remain opt-in. An explicit saved
  tracing opt-out stays off. Kasal creates a stable experiment and UC table
  prefix per personal space or teamspace using the volume's parent catalog/schema.
  Trace destinations are also bound per async request.
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

On upgrades, existing per-space experiment names and their saved UC destinations
are reused, even if those tables are outside the volume's schema. Retain the app's
permissions on those existing tables. An old `experiment` resource may be detached;
it is no longer read or required by Kasal.

Hosted detection requires the platform's app name, port, workspace ID and host.
SDK credentials alone do not enable these defaults. The local dev entrypoint
sets `KASAL_DEPLOYMENT_MODE=local`, which explicitly disables installation defaults.

## Conversation tracing

Chat, Agent Builder, and Flow Builder attach the originating conversation ID to
MLflow traces. Builder planning calls and subsequent workload executions use that
same ID, so MLflow can group them as a session. Completed plans and run results
include an **MLflow** action when tracing is enabled, linking to the tracing
experiment and the recorded trace when available.

## References

- [Databricks Apps resource environment variables](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/environment-variables)
- [Lakebase App resources and PostgreSQL settings](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/lakebase)
- [Serving endpoint resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/model-serving)
- [Unity Catalog volume resources](https://docs.databricks.com/aws/en/dev-tools/databricks-apps/uc-volumes)
- [MLflow trace storage in Unity Catalog](https://docs.databricks.com/aws/en/mlflow3/genai/tracing/trace-unity-catalog)
- [Public AppManifest SDK schema](https://github.com/databricks/databricks-sdk-py/blob/main/databricks/sdk/service/apps.py)
