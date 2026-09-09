# Plan: tag Kasal-created resources for real internal $DBU

## Goal

Get **real $DBU** (from `system.billing.usage`) for the Databricks resources Kasal
**creates**, by stamping a `custom_tag` at creation time. Billing can then be
filtered `WHERE custom_tags.app = 'kasal'` to attribute cost to Kasal precisely.

This is the **complement** to the HTTP-header approach (see the
`kasal_consumption_tracking` repo), not a replacement. Read the scope honestly:

| Resource | Kasal's relationship | This plan covers it? |
|----------|----------------------|----------------------|
| **Lakebase instance** | Kasal **creates** it (`lakebase_service.create_instance`) | ✅ yes — tag at create |
| **Vector Search endpoint** | Kasal **creates** it (`databricks_vector_endpoint_repository.create_endpoint`) | ✅ yes — tag at create |
| **Databricks App** (Kasal itself) | deployed via bundle | ✅ yes — tag in `app.yaml` / bundle |
| **Model serving / LLM** | Kasal only **connects** to shared `databricks-*` endpoints | ❌ **no** — not ours to tag → use the HTTP-header token→$ path instead |
| Connect-to-**existing** Lakebase/VS | Kasal didn't create it | ❌ no — can't tag another owner's resource |

**Known limits (state these up front):**
1. Tagging covers only Kasal-**created** resources. Anything Kasal *connects to*
   (shared serving endpoints — the largest cost line; user-supplied existing
   Lakebase/VS) is out of scope here; the token-fingerprint cost layer handles
   serving, and connect-to-existing is inherently not Kasal-attributable via tags.
2. `custom_tags` are **free-form and user-editable** — a tag is an attribution
   *convention*, not a trust boundary. Someone could tag a non-Kasal resource
   `app=kasal` or strip the tag. Treat tag-based $ as "resources Kasal provisioned
   under its own SP", not a guaranteed exhaustive total.
3. Reading `system.billing.usage` requires SELECT on it in the workspace where the
   resources live — an access grant, separate from this code change.

---

## Code changes

Use a single shared constant so the tag is consistent and greppable.

### 0. Shared tag constant (new)
Add to `src/utils/telemetry.py` (next to `KASAL_BASE`):
```python
# Resource tag stamped on Databricks resources Kasal CREATES, so their cost is
# attributable in system.billing.usage (WHERE custom_tags.app = 'kasal').
KASAL_RESOURCE_TAGS = {"app": "kasal", "created_by": "kasal"}
```

### 1. Lakebase instance — `services/lakebase_service.py::create_instance` (~L359)
`DatabaseInstance` accepts `custom_tags` (verify against the installed
`databricks-sdk` version; the field may be `custom_tags: Dict[str,str]`). Change:
```python
from src.utils.telemetry import KASAL_RESOURCE_TAGS
instance = w.database.create_database_instance(
    DatabaseInstance(
        name=instance_name,
        capacity=capacity,
        retention_window_in_days=retention_days,
        node_count=node_count if node_count > 1 else None,
        custom_tags=KASAL_RESOURCE_TAGS,     # NEW  (guard: only if the SDK field exists)
    )
)
```
If the SDK's `DatabaseInstance` has no `custom_tags`, tag via the update/patch API
after creation, or fall back to a budget policy (below).

### 2. Vector Search endpoint — `repositories/databricks_vector_endpoint_repository.py::create_endpoint` (~L36)
The endpoint is created by a raw REST `payload`. Add tags to the body (confirm the
`/api/2.0/vector-search/endpoints` create schema supports `custom_tags`):
```python
from src.utils.telemetry import KASAL_RESOURCE_TAGS
payload = {
    "name": endpoint_data.name,
    "endpoint_type": ...,
    "custom_tags": [                          # NEW  (list-of-{key,value} per the API)
        {"key": k, "value": v} for k, v in KASAL_RESOURCE_TAGS.items()
    ],
}
```
> Verify the exact shape — some VS endpoints don't accept tags at create; if so this
> line is a no-op and VS cost stays log-volume-only (document it).

### 3. Databricks App (Kasal itself)
Two paths, both real:
- **Budget policy (preferred for cost):** the export already wires
  `budget_policy_id` into `app.yaml` (`quickstart.py::update_databricks_yml_app_name`).
  A budget policy *is* the first-class cost-attribution mechanism for Apps — prefer
  a `kasal` budget policy over a free-form tag. Ensure the deploy attaches one.
- **Tag:** if using tags, add `custom_tags` to the app resource in the bundle
  (`src/deploy.py` / `app.yaml`) as `{"app": "kasal"}`.

### 4. (Optional) Serving endpoints Kasal *does* create
Kasal mostly connects, but `agentbricks`/deployment paths may create serving
endpoints. Where a `create_serving_endpoint`/deploy call exists, add the same tags.
Foundation `databricks-*` endpoints are NOT created by Kasal — do not attempt.

---

## Verification (once deployed + billing access granted)

In the workspace where Kasal's resources live:
```sql
SELECT billing_origin_product, sku_name,
       SUM(usage_quantity) AS dbus
FROM system.billing.usage
WHERE usage_date >= current_date() - 30
  AND custom_tags.app = 'kasal'          -- the tag from KASAL_RESOURCE_TAGS
GROUP BY billing_origin_product, sku_name
ORDER BY dbus DESC;
```
Non-empty rows = tagging works and cost is Kasal-attributable. This becomes the
"real $DBU" companion to the token-fingerprint serving $.

---

## Rollout order

1. **Ship the tag constant + Lakebase/VS/App create-time tags** (this doc). Low risk,
   additive — a create-time tag can't break creation if the SDK accepts it, and is a
   guarded no-op if it doesn't.
2. **New resources get tagged from then on** (existing untagged resources stay
   untagged — optionally backfill via ALTER/patch if the APIs allow).
3. **Request SELECT on `system.billing.usage`** for the Kasal workspace(s).
4. **Add a billing-based `$DBU` source** to `kasal_consumption_tracking` filtered by
   `custom_tags.app='kasal'` — the tagged-resource cost line, alongside the
   token-fingerprint serving $ that's already built.

## Why this is only half the picture (and that's OK)

- **Serving/LLM (biggest cost):** solved by the HTTP header — `Kasal tokens ×
  per-model price`, no tag possible or needed.
- **Kasal-created Lakebase/VS/App:** solved by this tagging plan → real billing $.
- **Connect-to-existing / shared:** not tag-attributable; log volume is the honest
  ceiling.

Together the header path + this tagging plan cover the two cost lines that CAN be
turned into real dollars. Neither alone is complete; both together are the honest
maximum for internal Kasal cost.
