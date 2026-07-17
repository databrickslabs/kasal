# Stable ENCRYPTION_KEY across redeploys (Databricks secret scope)

**Status: IMPLEMENTED (2026-07-17).** Approach adjusted during build — see
"Implemented mechanism" below. Deploy + security change.

Date: 2026-07-17.

## Implemented mechanism (what actually shipped)

The final design reads the key **directly from the secret scope via the SDK at
startup** rather than relying on `app.yaml` `valueFrom` binding. Reason: the docs
confirmed the `secret` **resource** (which `valueFrom` references) is declared via
the Apps UI or a Declarative Bundle — **not** in `app.yaml` — and this deploy is
`python src/deploy.py` (no bundle). The SDK-read is self-contained, needs no
app.yaml/resource wiring, and can't break the deploy. Concretely:

1. **`deploy.py::ensure_encryption_key(client, app_name)`** — provisions a Fernet
   key **once** into `kasal/kasal_encryption_key` (generate-once, never overwrite;
   idempotent scope create; grants the app SP READ via `put_acl`). Best-effort:
   a failure logs a warning but never aborts the deploy.
2. **`EncryptionUtils.get_encryption_key()`** — resolves in order: `ENCRYPTION_KEY`
   env var → **secret scope** (`_read_key_from_secret_scope`, via `WorkspaceClient`
   using the app SP) → generate ephemeral (last resort, warns). Cached per process.
3. **`sensitive_data_utils.decrypt_value()`** — on decrypt failure, logs an
   actionable "encryption key changed — RE-ENTER this credential" warning (was a
   generic error), and the config-gen tool's credential-validation errors gained a
   redeploy hint. Fixes the misleading "workspace_id missing" symptom.

The `app.yaml` `resources:`/`valueFrom` path (below) remains a valid **alternative**
if the deploy ever moves to a Declarative Bundle — kept for reference.

## Problem

Kasal encrypts sensitive `tool_configs` fields (`client_secret`,
`databricks_token`, …) at rest with a Fernet key from `EncryptionUtils.
get_encryption_key()` (`src/backend/src/utils/encryption_utils.py:83`). When
`ENCRYPTION_KEY` is **not set**, it **generates a random key at every startup**
and logs `"Keys will not persist across restarts."` (line 88).

Consequence on a Databricks App **redeploy** (fresh container → new random key):

1. Secrets encrypted with the old key can no longer be decrypted.
2. `decrypt_sensitive_fields` catches the failure and **blanks the field to `""`**
   (`sensitive_data_utils.py:213`).
3. Non-sensitive fields like `workspace_id` are plaintext and survive — but the
   tool then **fails validation on the empty `client_secret`** and surfaces a
   **misleading "workspace_id missing"-style error** (the plaintext param is fine;
   the blanked secret is the real cause). Confirmed: key unset + `workspace_id`
   present-but-execute-errors.

Same failure for **crew upload** across instances (different random keys), and
any restart.

## Fix (Option 1): key in a Databricks secret scope, injected via `valueFrom`

Generate the Fernet key **once**, store it in a **Databricks secret scope**, and
have the app reference it by name in `app.yaml` — the key value never touches the
repo, git, or the bundle. Platform-native; mirrors the Lakebase-resource pattern.

### Why not write the key into `app.yaml` directly

Rejected — `app.yaml` ships in the repo/bundle. A plaintext master key beside the
code defeats the encryption (anyone with repo/bundle read can decrypt every stored
credential). The secret scope keeps the key in a store with its own ACL.

## Implementation

### 1. `deploy.py` — provision the key once (idempotent)

Before syncing the bundle, ensure the scope + key exist (the deploy already holds
a `WorkspaceClient` at `src/deploy.py:362`):

```python
from databricks.sdk.errors import ResourceAlreadyExists
SCOPE = "kasal"                       # shared, pre-existing scope
KEY = "kasal_encryption_key"          # distinct name — scope also holds lakebase_pat/lakebase_server
APP_SP = "fafb65bd-0d43-4c9f-993a-8cba10b6ef31"  # kasal app service_principal_client_id
try:
    client.secrets.create_scope(scope=SCOPE)          # no-op if exists (it does)
except ResourceAlreadyExists:
    pass
existing = {s.key for s in client.secrets.list_secrets(scope=SCOPE)}
if KEY not in existing:                                # generate ONCE, never overwrite
    from cryptography.fernet import Fernet
    client.secrets.put_secret(scope=SCOPE, key=KEY, string_value=Fernet.generate_key().decode())
    logger.info("Provisioned a new Kasal encryption key in scope '%s'", SCOPE)
else:
    logger.info("Reusing existing Kasal encryption key from scope '%s'", SCOPE)
```

- **Generate-once, never-overwrite** is critical: overwriting the key would strand
  every already-encrypted secret. The `if KEY not in existing` guard enforces this.
- **Distinct key name** (`kasal_encryption_key`): the `kasal` scope is shared and
  already contains `lakebase_pat` + `lakebase_server` — a generic `encryption_key`
  risks collision.
- **SP READ access is granted by the `secret` resource declaration in §2**
  (`permission: READ`), not a manual `put_acl` — so §1 only needs to *provision the
  value*. (An idempotent `client.secrets.put_acl(SCOPE, APP_SP, READ)` is optional
  belt-and-suspenders but redundant once the resource is declared.)

### 2. `app.yaml` — reference the secret (value never in the file)

**Confirmed mechanism** (from the installed `databricks-sdk` apps models): it's a
**two-part binding** — declare a named `secret` **resource**, then point the env
var's `value_from` at that **resource name** (NOT a `{scope,key}` object):

```yaml
resources:
  - name: encryption-key            # resource name (referenced below)
    secret:
      scope: kasal
      key: kasal_encryption_key
      permission: READ              # grants the app SP READ — no separate put_acl needed

env:
  - name: ENCRYPTION_KEY
    valueFrom: encryption-key       # ← the RESOURCE name, not scope/key
```

Two corrections vs. the first draft:
- `value_from` is a **string = the resource name**, not a nested secret object
  (`EnvVar(name, value, value_from)` — `value_from` is a plain `Optional[str]`).
- The **`secret` resource declaration with `permission: READ` IS the ACL grant**
  (`AppResourceSecret(scope, key, permission)`) — so the manual
  `client.secrets.put_acl(...)` in §1 is **redundant** when the resource is
  declared here. Keep §1's `put_secret` (provision the value); drop the `put_acl`
  in favor of the resource declaration (or keep it as belt-and-suspenders — it's
  idempotent, but the resource is the platform-native grant).

Verified via SDK introspection (no test-deploy needed): `apps.EnvVar` has a
`value_from` field, and `apps.AppResourceSecret(scope, key, permission)` +
`AppResource.secret` exist with a `READ` permission enum.

### 3. UX — stop the misleading error

`decrypt_sensitive_fields` (`sensitive_data_utils.py:210-214`): when a decrypt
fails, keep blanking the value **but** flag it so callers can tell secrets apart
from genuinely-missing config. Minimal change: log the field name at WARNING with
an explicit "encryption key changed — re-enter this credential" message, and
(optionally) surface a distinct error at the tool-validation boundary:
*"Stored credential '<field>' could not be decrypted (encryption key changed) —
please re-enter it"* instead of a generic missing-`workspace_id` error.

## Residual security risks (honest, with Option 1)

Option 1 removes the "key in git" risk but does **not** make the system
zero-risk. Stated plainly:

1. **Single symmetric master key.** Anyone who can read the `kasal` secret scope
   can decrypt every stored credential. Security reduces to *who has READ on the
   scope* — lock it to the app SP + admins; do **not** grant broadly. This is an
   auditable ACL boundary (unlike a key in a file), but it's still one key.
2. **Deploy identity can read/write the scope.** Whoever runs `deploy.py` (or its
   SP) can provision/read the key. Treat deploy credentials as sensitive.
3. **No automatic key rotation.** Rotating means: new key → re-encrypt all stored
   secrets under it. Not built here; document as a manual runbook. Until rotated,
   a leaked key stays valid.
4. **Cross-instance sharing is inherent.** Uploading a crew from instance A to B
   still can't decrypt A's secrets on B unless both use the same scope/key — a
   property of encryption, not a bug. Cross-instance secret sharing should be an
   explicit decision, not a default.
5. **Migration gap (one-time).** Secrets already encrypted with a random key are
   **unrecoverable** — after this ships, each affected crew needs its credentials
   **re-entered once**. Plaintext fields (`workspace_id` etc.) are unaffected.
6. **Fernet is symmetric + unauthenticated-per-tenant.** The key is not per-group;
   all groups share it. Per-tenant keys would be a larger design change — out of
   scope here, noted for the security backlog.

None of these are regressions — they are the baseline properties of "encrypt at
rest with a managed key," and every one is strictly better than today's
random-key-per-restart (which silently destroys data on redeploy).

## Verify before building

Status as of 2026-07-17.

1. **Does the target Databricks App `valueFrom` support secret-scope refs**, and
   the exact YAML schema? — ✅ **CONFIRMED via SDK introspection** (no test-deploy
   needed). `apps.EnvVar` exposes `value_from: Optional[str]`, and
   `apps.AppResourceSecret(scope, key, permission)` + `AppResource.secret` exist
   with a `READ` permission. Mechanism: declare a named `secret` **resource** +
   set the env var's `value_from` to that **resource name** (see §2). No Apps-API
   env-set fallback needed.
2. **Can the deploy identity create a scope + put/grant secrets?** — ✅
   **CONFIRMED.** SDK exposes `WorkspaceClient.secrets` with `create_scope`,
   `put_secret`, `put_acl`, `list_secrets`, `list_scopes` (deploy uses the SDK it
   already imports, `src/deploy.py:362`). `secrets list-scopes` succeeded on
   `kasal-target`, and the **`kasal` scope already exists** — so we `put_secret`
   into it (keep the idempotent create guard for fresh workspaces).
3. **Confirm the app SP principal id** to grant scope READ. — ✅ **CONFIRMED.**
   App SP: `service_principal_client_id = fafb65bd-0d43-4c9f-993a-8cba10b6ef31`,
   `service_principal_name = app-52gkvp kasal`. This principal needs READ on the
   `kasal` scope: `client.secrets.put_acl(scope="kasal", principal=<sp>,
   permission=READ)`.

   **Finding A — the app's `effective_user_api_scopes` do NOT include secrets.**
   The app has sql/genie/vectorsearch/serving/files only. Runtime secret access
   therefore depends on the **SP's scope ACL** (above) + the env-injection method,
   NOT an OAuth scope — so the `put_acl` grant is mandatory, and Verify #1 (does
   `valueFrom`/the Apps runtime actually surface the secret to the app) is now
   even more decisive.

   **Finding B — the `kasal` scope is SHARED** (pre-existing, alongside 12 other
   app scopes on this workspace). Use a **distinct key name** —
   `kasal_encryption_key` — not a generic `encryption_key`, to avoid colliding
   with anything already stored in the scope.

### Git sync (pre-build) — ✅ done
Merge target is `origin/performance-improvements` (`origin` =
`databrickslabs/kasal`). After a fresh fetch: **0 behind / 70 ahead**, and the
merge-base equals the target HEAD (`32e0b869`) — the target is fully contained in
our history, nothing to pull.

## Verification (after implementing)

- `deploy.py` run 1: creates scope + key; logs "Provisioned". Run 2: logs
  "Reusing" (never regenerates).
- Deployed app env: `ENCRYPTION_KEY` present at runtime (check the startup — the
  "Generated a temporary key" warning must be **gone**).
- Configure a crew with a `client_secret` → **redeploy** → execute without
  re-entering → succeeds (secret decrypts).
- Negative: point the app at a scope without the key → startup falls back to a
  generated key + logs the warning (proves the wiring is what supplies it).
- Unit: `get_encryption_key()` returns the env value when set (no generation).

## Key code references
- `src/backend/src/utils/encryption_utils.py:83` — `get_encryption_key()` (the random-key source)
- `src/backend/src/utils/sensitive_data_utils.py:185` — `decrypt_sensitive_fields` (blanks on failure)
- `src/backend/src/services/databricks_secrets_service.py:236` — `set_databricks_secret_value` (REST put + scope create; deploy can use the SDK directly instead)
- `src/deploy.py:362` — `WorkspaceClient` already available in the deploy flow
- `src/app.yaml` / `.../exporters/templates/databricks_app/app.yaml.template` — the `env:` block to extend

## See also
- `lakebase-persistence-across-redeploys.md` — same class of redeploy-durability issue
