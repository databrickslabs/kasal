# MLflow tracing in Kasal — setup and requirements

Kasal can export every crew and flow execution to **MLflow Tracing** so you get a
full, expandable span tree (`agent → task → tool / llm`) for each run, viewable in
the Databricks MLflow UI. This page explains what tracing requires, how it differs
between production and local development, and how to verify it is working.

> **Where traces live:** MLflow traces are stored in an **MLflow Experiment** — a
> workspace object (a path like `/Shared/kasal-crew-execution-traces`). They are
> **not** stored in a Unity Catalog table or schema. If you created a UC schema for
> Kasal, leave it; it has nothing to do with tracing.

---

## The two gates

Tracing only runs when **both** of the following are satisfied:

| # | Gate | What it is | Where it is set |
|---|------|-----------|-----------------|
| 1 | **Config toggle** | `mlflow_enabled` + the experiment name | Kasal's DB — set once per workspace by an admin in the **Configuration** UI. Persists; not per-user. Defaults to **OFF**. |
| 2 | **Service Principal (SPN) credentials** | `DATABRICKS_HOST`, `DATABRICKS_CLIENT_ID`, `DATABRICKS_CLIENT_SECRET` | Must be present in the **backend process environment**. |

If the toggle is off, the execution subprocess skips MLflow before it ever touches
credentials. If the toggle is on but any SPN variable is missing, tracing is
skipped with `SPN credentials required for MLflow — skipping`.

> **Note:** the tracing path uses **OAuth Service Principal** credentials only. A
> personal access token (PAT / `DATABRICKS_TOKEN`) is **not** used for tracing by
> design.

---

## Where the SPN credentials come from (differs by environment)

This is the key nuance: **gate #2 is owned by the deployment platform, not the user.**

| Environment | Config toggle (#1) | SPN env vars (#2) |
|-------------|--------------------|--------------------|
| **Databricks Apps (production)** | Admin flips it once in the UI | **Automatic** — injected by the platform |
| **Local development** | Flip in the UI (or seed in the DB) | **Manual** — you export them (dev only) |

### Production = Databricks Apps → fully automatic

When Kasal is deployed as a Databricks App, the app **runs as its own service
principal**, and the Apps platform **injects** `DATABRICKS_CLIENT_ID`,
`DATABRICKS_CLIENT_SECRET`, and `DATABRICKS_HOST` into the container's environment
at runtime — **zero manual steps, no shell script, nothing in `app.yaml`.**

So in production an end user does **nothing** for gate #2. An admin simply enables
the toggle (gate #1), and because the app's SP is already present in the
environment, tracing works.

**The one thing to ensure:** the app's service principal must have permission to
**write experiments** under the target path (e.g. `/Shared/`). That is a workspace
permission, not an environment variable.

> **Multi-app caveat:** every deployed Kasal app instance runs as a *different* app
> SP. If you standardize on one shared experiment path, each app's SP must be
> granted write access to it individually — otherwise you will see
> `host=yes, spn_id=yes` followed by a permission error on `set_experiment`.

### Local development → manual

There is no Apps platform locally to inject anything, so you supply the SP
credentials yourself. This is purely a **dev convenience** that mimics what the
platform does automatically in production — it is *not* how production works and is
never shipped to customers.

1. **Obtain a service principal with an OAuth secret** in the target workspace. If
   you are a workspace admin you can create one; otherwise reuse an SP you own and
   generate a secret for it:

   ```bash
   # Create an OAuth secret for an existing service principal (workspace-level)
   databricks service-principal-secrets-proxy create <SERVICE_PRINCIPAL_ID>
   ```

   You need the SP's **application ID** (the client ID) and the generated **secret**.

2. **Export the three variables** in the shell that launches the backend, then start it:

   ```bash
   export DATABRICKS_HOST="https://<your-workspace>.cloud.databricks.com"
   export DATABRICKS_CLIENT_ID="<service-principal-application-id>"
   export DATABRICKS_CLIENT_SECRET="<service-principal-oauth-secret>"
   cd src/backend && ./run.sh
   ```

   (You can keep these in a local, git-ignored file and `source` it — just make sure
   they end up in the backend process environment.)

3. **Enable the toggle** in the Kasal Configuration UI and set the experiment name
   (default `kasal-crew-execution-traces` → resolves to
   `/Shared/kasal-crew-execution-traces`).

4. **Run a crew** — on the first kickoff the experiment is created and traces appear.

### Non-Apps production (edge case)

If Kasal runs outside Databricks Apps (a plain VM/container), inject the SPN
credentials via that platform's secret/env mechanism (container env, Kubernetes
secret, systemd unit, etc.) — still environment/secret config, never a checked-in
shell script. The standard Kasal deployment target is Databricks Apps, so this
rarely applies.

---

## Finding your traces

Databricks UI → **Machine Learning → Experiments** → open the experiment matching
your configured name, e.g. `/Shared/kasal-crew-execution-traces`. Each run is a
trace you can expand into its `agent → task → tool / llm` span tree.

The numeric `?o=...` in a workspace URL is just the workspace org id — it is not the
experiment id, so a bare `/ml/experiments?o=...` URL only shows the experiment list.

---

## Verifying it works (backend log)

Watch the crew/subprocess log when you run a workflow. A healthy run shows, in order:

```text
[SUBPROCESS] MLflow enabled_for_workspace=True
[SUBPROCESS] MLflow auth env — host=yes, spn_id=yes, spn_cred=yes
[SUBPROCESS] MLflow experiment set: /Shared/kasal-crew-execution-traces (ID: <id>)
[SUBPROCESS] MLflow tracing destination set to experiment <id>
[OTel-MLflow][<job>] Flushed async trace logging (trace_id=tr-…)
[OTel-MLflow][<job>] VERIFY trace_id=tr-…: get_trace OK, retrievable spans=<N>
```

`retrievable spans=<N>` with `N > 0` is the definitive proof: it reads the trace
back from Databricks immediately after upload and confirms the span data persisted.

---

## Troubleshooting

| Symptom in log | Cause | Fix |
|----------------|-------|-----|
| `MLflow enabled_for_workspace=False` / `MLflow is disabled … skipping` | Gate #1 — toggle is off | Enable MLflow in the Configuration UI |
| `host=yes, spn_id=no, spn_cred=no` → `SPN credentials required … skipping` | Gate #2 — SPN env vars not in the process | Export them (local) / confirm the app SP is injected (prod) |
| Permission error on `set_experiment` | The SP cannot write to the experiment path | Grant the SP write access to the target experiment (e.g. under `/Shared/`) |
| Trace appears but has **no child spans** (only a summary) | Span-data (`traces.json`) upload did not complete before the subprocess exited | Ensure the build includes the async-flush fix; `VERIFY … retrievable spans=N` confirms persistence |

---

## Related

- [Lakebase setup for Kasal](./lakebase-deployment.md) — persistence for crews, agents, tasks, and run history
- [Crew export and deployment](./crew-export-deployment.md) — deployed apps that emit traces
- [Solution architecture guide](./ARCHITECTURE_GUIDE.md) — where tracing fits the platform

Back to the [documentation hub](./README.md).
