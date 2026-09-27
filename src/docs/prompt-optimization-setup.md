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

### Built-in judges

Besides your own judges, the Optimize dialog offers some of MLflow's
**built-in judges** under **MLflow built-in judges**. Tick the ones a run
should use. None is selected by default, because each one adds a judge call
for every new deliverable.

| Judge | Role | What it checks |
|---|---|---|
| Safety | gate | The deliverable has no harmful, offensive or toxic content |
| Relevance to query | graded | The deliverable addresses the crew's objective |
| Guidelines | graded | The deliverable meets the tasks' expected outputs and your judging guidance (these become the judge's guidelines) |
| Completeness | graded | Every part of the objective is answered |

How they run:

- **On demand, with the run's judge model.** They use the same Kasal model and
  the same LLM manager, keys and workspace auth as your own judges. Each one
  runs once per distinct deliverable, and its result is cached for the rest of
  the run.
- **Never registered.** Kasal does not register, list, schedule or monitor
  them. They are built for the run and discarded after it, so they need no
  grants beyond the checklist above.

How the score combines:

- Each built-in answers yes or no, which Kasal scores as 1 or 0.
- A **graded** judge's answer joins the average of your judges' grades, with
  weight 1, like one more judge.
- A **gate** judge (Safety) does not join the average. If it answers "no",
  the judge score becomes 0.
- The run's overall mix is unchanged: 0.3 × format + 0.7 × judge score.
- A built-in that fails (provider error or an unreadable answer) is logged and
  left out. The average is taken over the judges that did answer, and the run
  continues.
- Each verdict and its reason is passed to GEPA's reflection model, tagged
  `[builtin:<name>]`, so the rewrite knows why a candidate lost points.

#### Labels

Some built-in judges compare the deliverable with **labels**: what a good
deliverable should contain. The picker shows them with a **needs labels**
badge, disabled with the reason until their labels exist.

| Judge | Needs |
|---|---|
| Correctness | Expected facts, or an expected answer |
| Expectations guidelines | Review notes on past answers (see below) |

Under the picker, **Labels** has two optional fields, one set per crew (a crew
run has a single objective):

- **Expected facts**: one point per line that every good deliverable must
  contain. This is Correctness' main input.
- **Expected answer**: free text, if you have a model answer.

**Suggested labels.** The **Expectation** notes you wrote on past evaluation
answers ("what SHOULD this answer contain?") are offered as suggestions. A
suggestion is never used on its own: **Accept** adds it to the expected facts,
and **Edit** lets you reword it first.

**Expectations guidelines** needs no typing. Its guidelines are the
requirements Kasal distils from your grades and notes on past answers (the same
checklist that steers the run), so it is enabled once such notes exist.

**Where labels are stored.** When a run starts, the labels in the fields are
saved for the crew if they changed. They go to the MLflow Prompt Registry, next
to your judges, as the prompt `kasal_labels__crew_<id>__<workspace>`, a new
version per change. Only confirmed labels are stored; suggestions are not.

**Workspaces.** Evaluation answers are logged as traces on both local MLflow
and Databricks, tagged with the crew and the workspace (`kasal_crew_id`,
`kasal_group_id`). Every read (suggestions, review notes, the answers to grade,
alignment) filters on both tags, and stored labels are read only by the
workspace that saved them. So workspaces that share an experiment never see
each other's labels. Answers logged before this change carry no workspace tag
and no longer appear.

**Coverage.** A label judge counts only when at least half of the dataset's
rows are labelled, and at least 3 rows (every row, when there are fewer than
3). A crew run has one row, so it simply needs its labels. A judge below the
floor is left out of the run, and the dialog says why, for example
"Correctness skipped: not labelled". A judge is never called on a row without
its labels.

Equivalence is not offered: it compares against one exact answer, which suits
short answers rather than crew deliverables. Retrieval sufficiency also needs
retrieval traces and arrives in a later phase.

MLflow **evaluation runs** use the same list: Relevance to query and Safety
always, plus Correctness when the evaluation rows carry a reference answer. The
retrieval judges are skipped with a log message. They read retrieved documents
from trace spans, and evaluation rows only carry context as a plain column.

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
   (**Align**). Kasal saves the learned guidelines and references to the graded
   examples together as a new version of the judge's prompt and lists what it learned.
4. Run **Optimize** — GEPA scores candidates with the learned guidelines and
   up to five relevant past examples, retrieved using the crew's embedder.

Grades given under **Overall quality** feed the optimizer's own reflection but
do not align any judge: only grades given *for a judge* align that judge. Align
again whenever you have graded more answers — each alignment starts from the
grades, not from the previous guidelines. It scans the most recent 200 crew
traces and also reloads examples remembered by earlier alignments, so older
examples do not fall out of memory merely because newer runs exist. Corrected
or removed feedback is reflected when you align again.

Judges aligned before dual-memory persistence was introduced need **Align**
once more to save their example references. Existing judges keep working in
the meantime with their saved instructions. Changing only a judge's model
preserves memory; editing its criteria clears example memory until you align
the revised criteria.

Memory is versioned with the judge in the MLflow Prompt Registry. The extra
memory section is excluded from the criteria editor and model prompt. Examples
remain in MLflow traces, so trace retention controls their availability. The
retrieval index is rebuilt once per judge per optimization run; deleted traces
are skipped, while permission or server errors are reported. Alignment remains
an explicit action; this does not add automatic background alignment or an
in-app control for deleting individual feedback records.

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
