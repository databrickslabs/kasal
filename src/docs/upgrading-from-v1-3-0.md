# Upgrading from v1.3.0

Move an existing Kasal v1.3.0 Databricks App to this release and keep its data.

This release changes how a hosted Kasal finds its database. A v1.3.0 app will not
start on it until you attach the database it already uses as the app's `lakebase`
resource. The rest of the upgrade (new tables, new columns, data fixes) runs by
itself on the first start.

- [Before you begin](#before-you-begin)
- [What changes](#what-changes)
- [Find where your data lives](#find-where-your-data-lives)
- [Back up before upgrading](#back-up-before-upgrading)
- [Attach the database as the lakebase resource](#attach-the-database-as-the-lakebase-resource)
- [Deploy this release](#deploy-this-release)
- [What happens on the first start](#what-happens-on-the-first-start)
- [What to check after the upgrade](#what-to-check-after-the-upgrade)
- [Re-export apps that use Genie One](#re-export-apps-that-use-genie-one)
- [Backups after the upgrade](#backups-after-the-upgrade)

## Before you begin

- You can manage the Kasal app in Databricks Apps (add resources, deploy, read
  its logs).
- You can reach the Lakebase instance the app uses, or you know who owns it.
- Upgrade the **same** app. Do not delete and re-create it: its service principal
  owns the Kasal tables and the secret scope that holds the encryption keys. A
  re-created app has a new identity: it cannot alter those tables, and the
  encryption material would have to be migrated by hand (see
  [Databricks Apps installation](./databricks-app-installation.md)).

## What changes

- **Attach a `lakebase` Database resource to keep your data.** `src/app.yaml`
  binds `KASAL_LAKEBASE_RESOURCE` to a resource named `lakebase`. Without it,
  Kasal starts on the container's SQLite file and logs a warning, so an upgraded
  app with no resource comes up empty. If a Database resource is attached,
  `src/deploy.py` refuses to deploy unless it is the only one and is named
  `lakebase`. To make the resource mandatory, uncomment
  `KASAL_REQUIRE_LAKEBASE_RESOURCE` in `src/app.yaml`: the app then stops at
  startup with `Attach the 'lakebase' database resource before deploying Kasal`.
- **The installation owns the database.** Kasal reads the connection from the
  resource. The Lakebase connection form in **Configuration → Database
  Management** is hidden, saving a manual Lakebase configuration returns 400, and
  so do the connect-and-migrate endpoints: they could drop and replace the `kasal`
  schema the app runs on.
- **Schema updates apply at startup.** Deployed apps do not run Alembic. On every
  start Kasal creates missing tables and runs its schema self-heal, which adds
  missing columns and indexes and applies small data fixes.
- **Genie One moved to its GA endpoint.** See
  [Re-export apps that use Genie One](#re-export-apps-that-use-genie-one).

## Find where your data lives

A v1.3.0 app kept its data in one of two places:

- **A Lakebase instance connected from the app.** In v1.3.0, open
  **Configuration → Database Management**. If Lakebase shows as enabled, your
  data is in that instance, in database `databricks_postgres`, schema `kasal`.
  Note the instance name.
- **The container's SQLite file.** If Lakebase is not enabled, the data is in the
  app's local SQLite file. That file is wiped on every restart or redeploy, so
  the upgrade cannot keep it in place. While still on v1.3.0, connect a Lakebase
  instance in **Configuration → Database Management** and migrate the data into
  it, then continue with that instance.

## Back up before upgrading

Take the backup while the app still runs v1.3.0.

- **Lakebase.** Lakebase keeps point-in-time history for the instance's
  retention window (14 days for instances Kasal created). Write down the time
  just before you deploy the upgrade: that is the point to restore to if you need
  to roll back. Confirm the retention window covers it in the instance settings.
- **SQLite.** Use v1.3.0's **Export** in **Configuration → Database Management**
  to write the database file to a Unity Catalog volume before you migrate it to
  Lakebase as described in the previous section.

## Attach the database as the lakebase resource

In the app's settings in Databricks Apps, add a resource:

| Field | Value |
| --- | --- |
| Type | Database |
| Resource key | `lakebase` |
| Instance and database | The instance you found above, database `databricks_postgres` |
| Permission | Can connect and create |

Attach exactly one database: Databricks injects `PGHOST`, `PGDATABASE`, `PGUSER`
and `PGPORT` for the first one only. Keep the instance's type (an instance created
from v1.3.0 is a Provisioned instance). Kasal uses the database exactly as
attached and does not move data from another database.

The database owner must have enabled pgvector once:

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

If it is missing and the app cannot install it, startup stops and asks the owner
to run that statement.

## Deploy this release

Deploy with `python src/deploy.py` or update the Marketplace installation, as
you installed it. Redeploying also applies this release's OAuth scope list
(`src/manifest.yaml`, kept equal to `src/deploy.py`), which adds the `ai-gateway`
scope Genie One needs.

## What happens on the first start

Before it accepts requests, Kasal:

1. Connects to the attached database as the app's service principal and checks
   for pgvector.
2. Creates schema `kasal` if it is missing, then creates every table added since
   v1.3.0. Existing tables and rows are left as they are.
3. Runs the schema self-heal: it adds columns and indexes the models now map,
   and applies data fixes, for example renaming personal groups to "Personal
   Space" and moving Genie One registrations to the GA endpoint.
4. Creates or restores the app's encryption keys, then seeds defaults (models,
   prompt templates, tools).

Steps 2 and 3 run in one transaction, and every self-heal step runs in its own
savepoint. A step that cannot apply is rolled back on its own and logged; the
other steps and the new tables still apply. Startup does not stop for such a gap:
it is reported instead (see below). No table, column or row is dropped. The one
thing the pass removes is the foreign key from `decision_config.group_id` to
`groups`, which personal spaces cannot satisfy (on SQLite the table is rebuilt
without it and its rows are copied across). Redeploying runs the same pass
again, so a gap you fix is healed on the next start.

## What to check after the upgrade

In the app's logs, from the first start:

- `Schema self-heal applied every step on postgresql`: the schema is up to date.
- `Schema self-heal INCOMPLETE on postgresql: N step(s) did not apply: ...`, at
  ERROR level: those steps were skipped. Each one has an earlier warning,
  `Schema self-heal step <name> failed and was rolled back, continuing: <error>`,
  with the cause. Features that read the affected tables may fail until it is
  fixed.
- `Attach the 'lakebase' database resource before deploying Kasal`: the Database
  resource is missing or not named `lakebase`.

The most common cause of a skipped step is `must be owner of table`: the tables
were created by a different principal, typically after the app was deleted and
re-created. Ask the instance owner to give the tables back to the app's service
principal, then restart the app.

The same result is available without the logs. `GET /api/v1/health/db` includes
a `schema_heal` object:

```json
{
  "status": "degraded",
  "schema_heal": {
    "status": "incomplete",
    "failed_steps": ["_ensure_agent_columns"],
    "databases": ["postgresql"]
  }
}
```

`schema_heal.status` is `ok` when every step applied, `incomplete` when some did
not, and `not_run` when no pass ran or one stopped before it finished (the schema
was not checked). Unless it is `ok`, the top-level `status` is `degraded`. The
endpoint lists step names only; the causes stay in the logs.

Then open Kasal and check that your crews, flows, run history and teamspaces are
there.

## Re-export apps that use Genie One

Databricks retires the Beta Genie One endpoint on October 31, 2026. Registrations
inside Kasal move to the GA endpoint automatically on the first start (a
self-heal data step), but a Databricks App you **exported** from Kasal keeps the Beta URL in
its own code. Export those crews again after the upgrade and redeploy each
exported app, and grant its service principal `EXECUTE` on
`system.ai.genie_one_mcp`. The details are in
[Genie One endpoint move](./MCP.md#genie-one-endpoint-move).

## Backups after the upgrade

- **Use Lakebase point-in-time restore** to protect the attached database. In-app
  **Export** and **Import** cover SQLite databases and PostgreSQL databases whose
  tables are in schema `public`; Kasal on Lakebase keeps its tables in `kasal`, so
  both return 400 with that explanation instead of backing up another database.
- **Housekeeping works on Lakebase.** It purges old runs, traces and logs on the
  database the app is using.
- To roll back the upgrade, restore the instance to the time you noted before
  deploying.

## Related

- [Databricks Apps installation](./databricks-app-installation.md)
- [Genie One endpoint move](./MCP.md#genie-one-endpoint-move)
- [Continuous integration](./continuous-integration.md)
- [Documentation hub](./README.md)
