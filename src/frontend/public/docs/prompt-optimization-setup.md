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
the workspace integration). A prompt is stored as a Unity Catalog **function**,
so per the MLflow Prompt Registry docs the writer needs, on the schema:
**`CREATE FUNCTION`**, **`EXECUTE`**, and **`MANAGE`**.

> **The `MANAGE` trap (this is the one that bites).** `GRANT ALL PRIVILEGES`
> **does NOT include `MANAGE`** — Unity Catalog excludes it deliberately to
> prevent privilege escalation. So a service principal with `ALL PRIVILEGES` on
> the schema **still gets `PERMISSION_DENIED`** on `register_prompt`, and
> `SHOW GRANTS` shows only the single `ALL PRIVILEGES` line, hiding the gap.
> `MANAGE` must be granted **explicitly** (or come via ownership). This is the
> usual cause of "Permission denied to update prompt in schema …" on a workspace
> where an admin user can register prompts fine (admins bypass the check).

## Who needs the grant

The identity that writes the prompt is the **Databricks App's service
principal** — the OAuth client the platform injects as `DATABRICKS_CLIENT_ID`
(visible in the failing run's error as `client_id=...`, `auth_type=oauth-m2m`).
Grants must target that SP's **application id** (the GUID), not your user and not
the app's display name.

> The registry write authenticates as the **app SP**, never on-behalf-of the
> signed-in user. Granting yourself access is not enough — the SP is what must
> hold the privileges. (Prompt Registry is a Beta feature; a workspace admin may
> also need to enable it on the **Previews** page.)

## The grant (run once, as a catalog admin)

In a Databricks SQL editor or notebook, with `<catalog>`/`<schema>` matching your
Kasal Databricks config and `<app-sp-application-id>` the app's client id:

```sql
-- Traverse into the catalog (schema-level grants do NOT imply this)
GRANT USE CATALOG ON CATALOG <catalog> TO `<app-sp-application-id>`;

-- The prompt registry needs all three on the schema. MANAGE is separate from
-- ALL PRIVILEGES and MUST be granted explicitly.
GRANT USE SCHEMA, CREATE FUNCTION, EXECUTE, MANAGE ON SCHEMA <catalog>.<schema>
  TO `<app-sp-application-id>`;
```

Simplest guaranteed unblock — make the SP the schema **owner** (ownership
implies `MANAGE`, so this bypasses the per-privilege lookup entirely):

```sql
ALTER SCHEMA <catalog>.<schema> OWNER TO `<app-sp-application-id>`;
```

The schema must already exist. Kasal creates it (and the MLflow experiment) when
you save the MLflow settings — see [MLflow tracing setup](./mlflow-tracing-setup.md).

## Verifying the grant

```sql
SHOW GRANTS `<app-sp-application-id>` ON CATALOG <catalog>;         -- expect USE CATALOG
SHOW GRANTS `<app-sp-application-id>` ON SCHEMA <catalog>.<schema>; -- expect MANAGE listed EXPLICITLY (not just ALL PRIVILEGES)
```

If the row shows only `ALL PRIVILEGES` and not a separate `MANAGE`, that is the
bug — run the `MANAGE` grant above. After granting, start a new optimization
run; UC grants take effect for new authorization checks without redeploying Kasal.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `PERMISSION_DENIED: Permission denied to update prompt in schema <schema>`, and the SP shows `ALL PRIVILEGES` | **Missing `MANAGE`** — `ALL PRIVILEGES` excludes it | `GRANT MANAGE ON SCHEMA <catalog>.<schema> TO \`<app-sp-application-id>\`` (or make the SP the schema owner) |
| Denied and the SP has no catalog grant | Missing `USE CATALOG` on the parent catalog | `GRANT USE CATALOG ON CATALOG <catalog> TO \`<app-sp-application-id>\`` |
| An admin **user** can register prompts but the deployed app cannot | The user is a workspace/metastore admin (bypasses the check); the SP is not and lacks `MANAGE` | Grant the SP `MANAGE` explicitly — do not rely on the user's success as proof the SP is configured |
| Still denied after granting `MANAGE` | Grant targeted the wrong principal (user / display name / wrong catalog), or Prompt Registry preview is off | Confirm `SHOW GRANTS` rows are on the SP **applicationId** and the configured catalog; enable Prompt Registry on the **Previews** page |

## Related

- [MLflow tracing setup](./mlflow-tracing-setup.md): the sibling MLflow feature (traces, not prompts)
- [Solution architecture guide](./ARCHITECTURE_GUIDE.md): where optimization fits the platform

Back to the [documentation hub](./README.md).
