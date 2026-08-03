# Prompt optimization (GEPA): setup and requirements

Kasal's crew prompt optimization (the **Optimize crew** dialog, powered by GEPA)
searches for better agent and task prompts by running the crew for real and
scoring the deliverable. To do that it **registers the crew's baseline prompt in
the MLflow Prompt Registry** before the search begins.

On Databricks that registry is **Unity Catalog-governed**, so the app's service
principal needs UC privileges on the catalog and schema Kasal is configured to
use — otherwise the run fails immediately with:

```text
PERMISSION_DENIED: Permission denied to update prompt in schema <schema>.
```

This page covers that one-time grant. For MLflow **tracing** (a related but
separate feature) see [MLflow tracing setup](./mlflow-tracing-setup.md).

## What gets written, and where

The prompt is registered under a three-level Unity Catalog name:

```text
<catalog>.<schema>.kasal_crew_<id>_<group>
```

`<catalog>` and `<schema>` are the **catalog** and **schema** configured in the
Kasal **Configuration → Databricks** settings (the same ones used for the rest of
the workspace integration). A prompt is a registered-model-class UC entity, so
creating it needs the **`CREATE MODEL`** privilege on the schema — not just read
access.

## Who needs the grant

The identity that writes the prompt is the **Databricks App's service
principal** — the OAuth client the platform injects as `DATABRICKS_CLIENT_ID`
(visible in the failing run's error as `client_id=...`, `auth_type=oauth-m2m`).
Grants must target that SP's **application id** (the GUID), not your user and not
the app's display name.

> The registry write authenticates as the **app SP**, never on-behalf-of the
> signed-in user. Granting yourself access is not enough — the SP is what must
> hold the privileges.

## The grant (run once, as a catalog admin)

In a Databricks SQL editor or notebook, with `<catalog>`/`<schema>` matching your
Kasal Databricks config and `<app-sp-application-id>` the app's client id:

```sql
-- Traverse into the catalog (schema-level grants do NOT imply this)
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-application-id>`;

-- Use the schema and create the prompt (a registered-model entity)
GRANT USE SCHEMA, CREATE MODEL ON SCHEMA <catalog>.<schema>
  TO `<app-sp-application-id>`;
```

`USE CATALOG` on the **parent** is the step most often missed: `ALL PRIVILEGES`
on the *schema* does not include it, and without it UC cannot reach the schema at
all — so the SP is denied even with full schema rights.

If the schema is governed such that individual grants still resolve to a denial,
make the SP the schema **owner**, which bypasses the per-privilege lookup:

```sql
ALTER SCHEMA <catalog>.<schema> OWNER TO `<app-sp-application-id>`;
```

The schema must already exist — Kasal does not create it.

## Verifying the grant

```sql
SHOW GRANTS `<app-sp-application-id>` ON CATALOG <catalog>;      -- expect USE CATALOG
SHOW GRANTS `<app-sp-application-id>` ON SCHEMA <catalog>.<schema>;  -- expect USE SCHEMA + CREATE MODEL (or ALL PRIVILEGES)
```

After granting, start a new optimization run. UC grants take effect for new
authorization checks without redeploying Kasal.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `PERMISSION_DENIED: Permission denied to update prompt in schema <schema>` | The app SP lacks a required UC privilege | Run the grants above against the **app SP application id** |
| Denied even though the SP has `ALL PRIVILEGES` on the schema | Missing `USE CATALOG` on the parent catalog | `GRANT USE CATALOG ON CATALOG <catalog> TO \`<app-sp-application-id>\`` |
| Still denied after granting everything | Grant targeted the wrong principal (user / display name), or the wrong catalog's schema | Confirm with `SHOW GRANTS` that the rows are on the **applicationId** and the catalog Kasal is configured with |

## Related

- [MLflow tracing setup](./mlflow-tracing-setup.md): the sibling MLflow feature (traces, not prompts)
- [Solution architecture guide](./ARCHITECTURE_GUIDE.md): where optimization fits the platform

Back to the [documentation hub](./README.md).
