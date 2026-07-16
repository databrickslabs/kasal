# Config-automation proposal — reducing HITL in the UCMV pipeline

**Status: PROPOSAL — not yet decided, not yet built.** This is a judgment aid, not
a plan of record. It defines the concepts, states honestly what could be
automated vs. what can't, and gives effort/risk so you can decide whether it's
worth doing at all.

Date: 2026-07-16.

---

## The question on the table

Today the UCMV pipeline auto-fills most of the config but leaves ~4 keys for a
human to complete in the Config Editor (`/config-editor`). See
[UCMV_PIPELINE_CONFIG_GUIDE.md](../UCMV_PIPELINE_CONFIG_GUIDE.md) for the current
state. The question: **can we remove that human step, and should we?**

Short answer: **most of it is automatable, none of it is free, and one part
shouldn't be fully automated even if it could be.** Details below so you can call it.

---

## Concepts (plain definitions)

Before judging the automation, the four manual keys in one paragraph each.

### `filter_set`
A **named list of category values** that measures reuse. Example:
`"CWC_FILTER": ["APET","CAN","PET","RGB", …]`. A measure references it by name
(`num_fs: "CWC_FILTER"`) to mean *"only sum rows where the category column is one
of these."* It emits as `SUM(x) FILTER (WHERE bic_cwc_type IN ('APET','CAN', …))`.
It exists so the same list isn't copy-pasted into 40 measures — define once,
reference many. It encodes a business decision ("these are the packaging types
that count as core supply chain").

### `join_key_map.source_table`
The **physical 3-level Databricks table name** (`catalog.schema.table`) for a
dimension used in joins. The rest of the join entry (alias, join key, columns) is
auto-derived; only the physical table name is missing, because a Power BI M-query
doesn't always spell out the UC name.

### `fact_join_map`
**Cross-fact wiring**: when two fact tables must be combined (UNION or JOIN), this
says how — the join/union strategy, which rows belong to which side
(`primary_exclude_filter`), the shared key expression, and — for a narrow/tall
KBI table — how to pivot codes into columns (`pivot_col`, grain). It encodes a
data-architecture decision.

### `manual_overrides`
**Hand-written SQL** for the small set of measures whose DAX is too complex for
automated translation even after the LLM-first path + correctness guards.

---

## What could be automated, per key

The enabling fact: **the warehouse SQL-execution capability already exists** in
the pipeline — `metric_view_deployer_tool` (Tool 88) runs arbitrary SQL via
`_execute_sql_sync` (`POST /api/2.0/sql/statements`) using the existing
OBO→PAT→SPN auth (`get_auth_context`). Config-gen just doesn't call it yet; it
takes `catalog`/`schema` but not `warehouse_id`. So "add warehouse introspection
to config-gen" = **reuse proven plumbing + thread one field**, not new scope.

### 1. `filter_sets` — flag columns  ·  deterministic, no LLM, no confirm

- **Today**: `derive_filter_sets` already extracts inline `IN {…}` value lists
  from DAX. It produces nothing when the DAX only gates on a flag
  (`Table[cwc_filter] = 1`) — the values live in DB rows, not the formula.
  `_detect_cwc_filter_column` already identifies *which* column is the flag.
- **Mechanism**: one query per flag —
  `SELECT DISTINCT {value_col} FROM {dim} WHERE {flag_col} = 1`. The predicate
  comes verbatim from the DAX we already parse (so no guessing `=1` vs `=true`).
- **Confidence**: High / deterministic. Right, or the query errors and we keep the
  `TODO` (never fabricate).
- **Depends on**: `source_table` being resolved first (can't query an unnamed table).

### 2. `join_key_map.source_table` — two tiers

- **Tier 1 — parse the M-query (no warehouse, no LLM, exact).** Databricks-connector
  dimensions carry the answer literally in a navigation chain:
  `Databricks.Catalogs(…){[Name="cat"]}[Data]{[Name="sch"]}[Data]{[Name="tbl"]}[Data]`
  → `cat.sch.tbl`. Same for `Sql.Database` and `Value.NativeQuery(… FROM cat.sch.tbl)`.
  `classify_mquery_source` already tags these as `extractable`. **Majority of a
  Databricks-backed model resolves here with zero ambiguity.**
- **Tier 2 — warehouse fuzzy-match (proposal, confirm).** For opaque
  dataflow/gateway M sources: query `information_schema.tables`, score candidates
  by name similarity **and column-set overlap** (we know the dim's columns from
  the scan). Top candidate → propose, human confirms (a wrong table silently
  joins garbage).
- **Confidence**: Tier 1 High/exact · Tier 2 Medium/confirm.

### 3. `fact_join_map` — full proposal from evidence, human confirms

The part I initially mislabeled "business logic in someone's head." Most of it is
recoverable from evidence we already have (warehouse + the routing DAX):

| Sub-field | Source of truth | Automatable? |
|-----------|-----------------|--------------|
| `grain` | Warehouse: minimal key where `GROUP BY … HAVING COUNT(*)>1` is empty | Yes — deterministic |
| `union_mode` (union vs join) | Compare the two resolved grains | Yes — deterministic |
| `union_key_expr` (e.g. `CONCAT(plant,'/',workcenter)`) | Sample the target's key column, infer separator + components | Yes — infer |
| `pivot_col` | Low-cardinality discriminator column not in the grain | Yes — heuristic |
| `primary_exclude_filter` (`comp_code NOT IN (…)`) | The geo-routing DAX literally lists the codes | Yes — from DAX |
| pivot code→measure map | Each scorecard measure filters `[bic_csubkbi]="52R…"` | Yes — from DAX |
| *the decision to merge two facts at all* | Not recorded anywhere; inferable weakly from shared-source + co-usage | Weak signal only |

- **Confidence**: Medium overall — full draft is achievable, but see the residual.

### 4. `manual_overrides` — shrinking, LLM-escalated

- Already smaller after the LLM-first path + guards. A higher-effort LLM pass with
  a verify-loop closes a few more. "Business-critical + human-verified SQL" is a
  legitimate reason to keep some here.
- **Confidence**: Partial. Not a target for full automation.

---

## The irreducible residual (what stays human even at the ceiling)

Being honest so this isn't oversold:

1. **Verification of `fact_join_map`.** A wrong union/pivot produces **silently
   wrong numbers** — no error, just bad data on a dashboard. Even a high-confidence
   proposal must be human-confirmed. This is a cost-of-error gate, not an
   impossibility.
2. **DAX-to-warehouse format stitching.** Real example from the corpus: the
   routing DAX says `{550, 403}` (2 codes) but the correct config filter is
   `('0403','0550','0307')` — **3 codes, zero-padded**. The auto-proposal would
   likely miss `0307` and get padding wrong. The info is spread across DAX + a
   column-format convention; stitching it perfectly is error-prone. Human review
   catches this delta.
3. **The "why merge these facts" intent** — a weak signal at best; a human may
   still need to say "yes, these two belong together."

**So the honest ceiling is "auto-propose everything, human confirms the risky
ones" — not "zero human."** The reductions are large (`filter_sets` flag columns
and connector `source_table` become truly hands-off), but `fact_join_map` moves
from "author from scratch" to "confirm a draft," not to "gone."

---

## What it would take to build

| Piece | Change | New risk |
|-------|--------|----------|
| Shared `uc_query` helper | Lift `_execute_sql_sync` + `_authenticate` from the deployer into `metric_view_utils/uc_query.py` | Low — refactor of proven code |
| Optional `warehouse_id` in config-gen | One field; **when absent, behaves exactly as today** (parse-only Tier 1 works, flag sets stay `TODO`) | Low — additive, opt-in |
| M-query `source_table` parser (Tier 1) | Walk connector navigation chains; **needs no warehouse at all** | Very low — pure parse + tests |
| `filter_sets` flag resolver | `SELECT DISTINCT … WHERE flag=1` per detected flag | Low — deterministic |
| `source_table` fuzzy-match (Tier 2) | `information_schema` query + scoring + **confirm UI** | Medium — must be propose-not-silent |
| `fact_join_map` proposer | grain/union introspection + DAX parse + LLM-assemble + **confirm UI** | Medium-High — silent-wrong failure mode |

**Blast radius**: config-gen tool + one new util + Config Editor confirm affordance
+ tests. The subprocess boundary and the generator (Tool 86) are untouched.

---

## Recommendation

If we do anything, do it **in this order**, and consider stopping after the first
box:

1. **M-query `source_table` Tier-1 parser** — zero warehouse dependency, exact,
   pure win. Removes the most common `source_table` chore for Databricks-backed
   models. **Do this regardless.**
2. **Shared `uc_query` helper + optional `warehouse_id`** — enables 3 & 4; low
   risk; keeps PBI-only path intact.
3. **`filter_sets` flag resolver** — deterministic, no LLM, no confirm. Clean win
   once (2) exists.
4. **`source_table` Tier-2 fuzzy-match** — only if opaque sources are common in
   real tenants; needs confirm UI.
5. **`fact_join_map` proposer** — highest effort, highest payoff, but **keep the
   human confirm**. Only worth it if cross-fact facts are frequent (they were rare
   in the sample — mostly one model).

**My honest take**: (1) is worth doing on its own merit. (2)+(3) are a tidy,
low-risk package that removes a genuinely annoying manual chore (querying
dimension tables by hand). (4) and especially (5) are real engineering with a
silent-wrong failure mode — only justify them if tenant data shows opaque sources
/ cross-fact merges are *common*, not one-offs. **Don't build (5) speculatively.**

The strategic framing: the goal isn't "zero human" — it's **"the human confirms a
correct draft instead of authoring from a blank field."** That's achievable and
valuable; "fully autonomous cross-fact modeling" is neither fully achievable nor
safe, because the failure mode is invisible.

---

## Open questions for the decision

- How common are **opaque (non-connector) dimension M-sources** in real tenants?
  (Decides whether Tier-2 fuzzy-match is worth building.)
- How common are **cross-fact merges** beyond the sample model? (Decides (5).)
- Is config-gen ever run **without** a target warehouse available? (Decides how
  hard the opt-in fallback matters — I believe it must stay optional.)
- Appetite for a **confirm UI** in the Config Editor (accept/reject a proposed
  value) vs. keeping proposals as pre-filled `TODO`s the human edits?

## See also
- [UCMV_PIPELINE_CONFIG_GUIDE.md](../UCMV_PIPELINE_CONFIG_GUIDE.md) — current manual keys + UI how-to
- [ucmv-pipeline-architecture.md](./ucmv-pipeline-architecture.md) — end-to-end stage map
