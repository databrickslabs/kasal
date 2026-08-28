"""A service reaches another DOMAIN's data through that domain's service.

Repositories are not a shared data-access pool. Each one belongs to a domain, and
that domain's service is where its invariants live — group scoping, field
encryption, cascade order, status transitions. A service that constructs another
domain's repository directly gets the table without the rules.

This is not hypothetical. ``SchedulerService`` built ``ExecutionHistoryRepository``
itself, and the reason was real: ``ExecutionService.get_execution()`` returns a
flow-shaped dict and applies NO tenant filter, so the scheduler could not have used
it — it needs the ORM row AND ``group_ids``, because without the filter anyone could
create a schedule from another tenant's run and read its config and prompts.

The fix was to make the SERVICE offer the guarantee (``get_execution_record``), not
to keep reaching past it. That is the general shape: when the owning service does
not expose what you need, add it there rather than bypassing it.

**Almost a gate.** This started at 42 cross-domain pairs and is down to **6**, all of
them the same thing: the ``repositories`` dict the flow runner injects into
``BackendFlow`` inside the flow SUBPROCESS (see ``_BASELINE``). Convert that and the
ratchet becomes a hard ban.

Fixing the other 36 surfaced four real bugs that the bypass had been hiding, each one
swallowed by an ``except`` so nothing failed loudly:

* GEPA's apply/revert wrote agent and task rows through their repositories, skipping
  ``get_with_group_check`` — with entity ids parsed out of a JSON blob on the run.
* ``TemplateService(template_repository)`` passed a repository where a session was
  expected, so ``generate_connections`` could never load its system prompt.
* ``tool_factory`` called ``get_databricks_config(group_id=...)`` on a method taking
  no arguments, so its auth check reported "no config" for every workspace.
* ``delete_all_traces_for_group`` called ``get_all_executions_for_groups``, which does
  not exist on any repository — bulk trace deletion always raised and deleted nothing.
  Its router test mocked the whole method, so nothing caught it.

The baseline was GENERATED from the tree, never hand-written: a guessed entry silently
exempts a real violation, which is worse than no list at all.

Two things are NOT violations:
- a service using its OWN domain's repository (that is the normal chain)
- a repository used for a pure read where the owning service adds nothing, PROVIDED
  the pair is in the baseline — the point of the list is that adding to it is a
  decision someone made on purpose.
"""

import pathlib
import re

import pytest

_SERVICES = pathlib.Path(__file__).resolve().parents[3] / "src" / "services"

#: Which repositories each service domain OWNS. A domain may always use these.
#: Derived from where the write path lives, not from name similarity — `knowledge`
#: owns `documentation_embedding` even though the names share nothing.
_OWNED = {
    "a2a": {"a2a_agent", "a2a_push_config"},
    "a2ui": {"ui_config"},
    "agent_builder": {"agent", "task", "crew"},
    "catalog": {"agent", "task", "crew", "template", "crew_feedback", "schema"},
    "chat": {"chat_history", "chat_session"},
    "databricks": {
        "databricks_config",
        "databricks_volume",
        "database_config",
        "database_backup",
        "genie",
        "agentbricks",
    },
    "deployment": {"crew", "agent", "task", "tool"},
    "execution": {
        "execution",
        "execution_history",
        "execution_logs",
        "execution_trace",
        "flow_state",
        "log",
    },
    "export": {"crew", "agent", "task", "tool"},
    "external": {"crew_publication"},
    "flow_builder": {"flow", "flow_state"},
    "generation": {"crew_generator", "log"},
    "groups": {"group", "group_tool", "user"},
    "guardrails": {"data_processing"},
    "hitl": {"hitl"},
    "knowledge": {"documentation_embedding", "databricks_volume"},
    "llm": {"model_config", "log"},
    "mcp": {"mcp"},
    "memory": {"memory_backend", "memory_maintenance"},
    "mlflow": {"mlflow"},
    "otel_tracing": {"execution_trace"},
    "powerbi": {
        "conversion",
        "powerbi_config",
        "powerbi_context_config",
        "powerbi_extraction",
        "powerbi_semantic_model_cache",
    },
    "prompt_optimization": {"prompt_optimization_run"},
    "publications": {"crew_publication"},
    "recipes": {"workflow_recipe", "workflow_recipe_trial"},
    "scheduling": {"schedule"},
    "security": {"api_key", "user"},
    "settings": {"api_key", "model_config", "ui_config", "engine_config", "schema"},
    "skills": {"skill"},
    "tools": {"tool", "group_tool", "schema"},
    "trace": {"execution_trace"},
    "triggers": {"trigger_queue", "event_subscription"},
}

#: The ONLY cross-domain pairs left, and all six are the same thing: the
#: ``repositories`` dict that ``flow_runner_service`` builds and injects into
#: ``BackendFlow`` for a DYNAMIC flow run.
#:
#: They are not converted because that dict is threaded through 17 read sites across
#: 6 modules INSIDE the flow subprocess (``backend_flow``, ``checkpoint_resume``,
#: ``flow_config``, ``flow_processors``, ``task_adapter``). Replacing it with services
#: cannot be proven safe from in-process tests: the spawned interpreter has its own
#: event loop and its own Lakebase activation, so a break shows up only in a real
#: flow execution. See services/execution/CLAUDE.md on subprocess-boundary changes.
#:
#: Everything else that was here is gone — 42 pairs down to these 6. Do not add to
#: this list; convert the dict in a change that can be exercised by a live flow run,
#: and this becomes a hard gate.
_BASELINE = {
    "flow_builder -> agent",
    "flow_builder -> crew",
    "flow_builder -> execution_history",
    "flow_builder -> execution_trace",
    "flow_builder -> task",
    "flow_builder -> tool",
}


#: Modules under repositories/ that are NOT database repositories, so the ownership
#: rule does not apply to them. Each is misnamed rather than misused.
_NOT_A_DB_REPOSITORY = {
    # An httpx client for the Databricks Lakeview REST API — takes a user_token, not
    # a session, and touches no table. It lives in repositories/ by historical
    # accident; "which domain owns it" is not a meaningful question.
    "dashboard",
    # Same: an httpx client for the Genie conversation API, constructed with a
    # GenieAuthConfig and no session.
    "genie",
}

#: Paths where the rule does not apply.
_EXEMPT = (
    # Lakebase management legitimately spans every table: it migrates, backs up and
    # truncates them. Going through each owning service would mean 20 services.
    "services/databricks/lakebase/",
    # Shipped verbatim into exported apps, which have no service layer at all.
    "services/export/templates/",
)


def _pairs() -> set[str]:
    """Every ``domain -> repository`` a service module reaches for."""
    found = set()
    for path in _SERVICES.rglob("*.py"):
        rel = path.relative_to(_SERVICES.parents[0]).as_posix()
        if any(part in rel for part in _EXEMPT):
            continue
        parts = rel.split("/")
        if len(parts) < 3:
            continue  # services/<file>.py — no domain
        domain = parts[1]
        source = path.read_text()
        for match in re.finditer(r"from src\.repositories\.(\w+)_repository", source):
            repo = match.group(1)
            if repo in _OWNED.get(domain, set()):
                continue
            if repo in _NOT_A_DB_REPOSITORY:
                continue
            found.add(f"{domain} -> {repo}")
    return found


def test_no_new_cross_domain_repository_use():
    new = sorted(_pairs() - _BASELINE)
    assert not new, (
        "These services reach into another domain's repository:\n  "
        + "\n  ".join(new)
        + "\n\nGo through that domain's SERVICE instead. If it does not expose what "
        "you need, ADD IT THERE — that is what ExecutionService.get_execution_record "
        "is: the scheduler needed the ORM row plus group_ids, so the guarantee moved "
        "into the service rather than the scheduler reaching past it. A repository "
        "gives you the table without the owning domain's rules (group scoping, "
        "encryption, cascade order).\n\nIf it is genuinely justified, add the pair to "
        "_BASELINE in this file."
    )


def test_baseline_has_no_stale_entries():
    """A pair fixed but left in the baseline hides the next regression in it."""
    stale = sorted(_BASELINE - _pairs())
    assert not stale, (
        "These no longer reach across domains — delete them from _BASELINE:\n  "
        + "\n  ".join(stale)
    )


@pytest.mark.parametrize("domain", sorted(_OWNED))
def test_every_owned_domain_exists(domain):
    """An ownership entry for a deleted domain silently widens the rule."""
    assert (_SERVICES / domain).is_dir(), (
        f"services/{domain}/ no longer exists — remove it from _OWNED, or the "
        "domain it was renamed to inherits nothing."
    )


def test_owned_repositories_exist():
    """Likewise for a repository that was renamed or removed."""
    repos = {
        p.stem.replace("_repository", "")
        for p in (_SERVICES.parents[0] / "repositories").glob("*_repository.py")
    }
    unknown = {
        f"{domain} -> {repo}"
        for domain, owned in _OWNED.items()
        for repo in owned
        if repo not in repos
    }
    assert not unknown, (
        f"_OWNED names repositories that do not exist: {sorted(unknown)}. "
        "A typo here silently exempts a real cross-domain use."
    )
