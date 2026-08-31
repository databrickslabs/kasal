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

## Judges

A judge is three things — a name, plain-language criteria that reference the
answer as `{{ outputs }}`, and the Kasal model that applies them. Kasal runs
judges **on demand**: GEPA grades every candidate with the crew's assigned
judges, and aligning a judge distils your grades with the judge's own model —
both through Kasal's LLM manager, never through MLflow's model client.

Judge definitions live in the **MLflow Prompt Registry** as versioned prompts:
`kasal_judge__<name>` for a library judge, `kasal_judge__crew_<id>__<name>`
for a crew's copy, each version tagged with its model and crew. On Databricks
that is Unity Catalog — the same catalog, schema and grants the crew prompts
above use, so there is nothing extra to set up. On a local MLflow server it is
the OSS registry.

They are deliberately **not** MLflow scheduled scorers (`make_judge().register()`).
On Databricks that registry is the experiment's *monitoring job*: every write
patches the job's scorer list, which needs job permissions the app's service
principal does not hold, and an existing name cannot be re-registered at all.
Kasal has no monitoring use for judges. If you ever want live-traffic scoring,
that is a separate, admin-gated step: grant the app SP `CAN MANAGE` on the
experiment's monitoring job and attach the judge there.

### Setup checklist for judges

Nothing beyond what crew optimization already needs:

- **Databricks:** the Prompt Registry preview is enabled on the workspace
  **Previews** page, and the app's service principal holds `USE CATALOG` on the
  configured catalog and `USE SCHEMA`, `CREATE FUNCTION`, `EXECUTE`, `MANAGE` on
  the configured schema (see the grants above). Judges then appear in the
  crew-traces experiment's **Prompts** tab as `kasal_judge__…`.
- **Local development:** the MLflow server the backend was launched against is
  running (for example `mlflow server --host 127.0.0.1 --port 5555 …`); judges
  appear on its Prompts page.
- Each judge chip links to the judge's page in MLflow (the Prompts page of a
  local server; the experiment's Prompts tab on Databricks).

Monitoring — scoring a published crew's live traffic on a schedule — is a
separate, later step and is not part of the Optimize dialog.

## Aligning judges to your grades (MemAlign)

Kasal's LLM judges score every candidate prompt set GEPA tries. A judge is
written in plain language, so it carries its author's assumptions: a judge asked
for "accurate listings" does not know that *your* team treats a listing outside
the German-speaking side as wrong. Aligning a judge teaches it that from the
grades you already give in the Optimize dialog, using MLflow's MemAlign
optimizer — the judge replays the graded answers, compares its verdicts with
yours, and distils the disagreements into short guidelines that become part of
its instructions.

In the crew catalog, open **Optimize**:

1. **Assign** a judge to the crew (or create one).
2. Under **Evaluation answers**, expand an answer, set **Grading for** to that
   judge, grade it, and say why. The grade is stored on the answer's trace as
   human feedback *in the judge's name*.
3. After grading a few answers, press the wand next to the judge chip
   (**Align**). Kasal saves the aligned criteria as a new version of the judge's
   prompt and lists what it learned.
4. Run **Optimize** — GEPA now scores candidates with the aligned judge.

Grades given under **Overall quality** feed the optimizer's own reflection but
do not align any judge: only grades given *for a judge* align that judge. Align
again whenever you have graded more answers — each alignment starts from the
grades, not from the previous alignment.

### Which models it uses

Nothing here is configured by environment variable. Alignment distils
guidelines with the **judge's own model** — the one chosen for it when the
judge was created or edited in the Optimize dialog — and embeds the graded
answers with the **embedder the crew's agents carry** (Agent form; Kasal's
default embedder when none is set). Both go through Kasal's LLM manager, so
the provider, endpoint and API key are the ones configured in the UI.

| Symptom | Cause | Fix |
|---|---|---|
| `No graded evaluation answers for this judge yet` | Grades were logged under **Overall quality** or under another judge | Grade a few answers with this judge selected, then align |
| `This judge has no model` | A judge registered without one | Edit the judge, pick a model, align again |
| `Embedding failed with the crew's embedder` | The embedder on the crew's agents (or the default) is not reachable | Fix the embedder on the Agent form, then align again |
| Alignment succeeds with no guidelines | The judge already agreed with every grade | Nothing to fix — grade more answers, especially ones you disagree with |

## Related

- [MLflow tracing setup](./mlflow-tracing-setup.md): the sibling MLflow feature (traces, not prompts)
- [Solution architecture guide](./ARCHITECTURE_GUIDE.md): where optimization fits the platform

Back to the [documentation hub](./README.md).
