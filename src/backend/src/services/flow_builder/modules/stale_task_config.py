"""Re-sync a flow's STALE task tool_configs from the crew's CURRENT task.

A flow's startingPoint/listener task IDs are frozen when the flow is saved, but
editing a crew mints NEW task rows (re-checking a tool only updates the current
task). So a flow can run a stale task whose ``tool_configs`` still holds old
values — a previous ``warehouse_id`` / ``dataset_id`` / ``enable_reconciliation``,
or an MCP server that has since changed — while the crew itself (which uses the
current task) is correct. That drift is why a flow run reconciled against a stale
Power BI dataset and skipped reconciliation even though the crew's task config
was right.

These helpers live here (rather than in ``flow_processors``) as one cohesive
seam; ``flow_processors`` re-exports them so existing import paths stay stable.
"""


def recover_mcp_from_current_tasks(
    effective_tool_configs, flow_task_id, flow_task_name, current_tasks
):
    """Recover a missing MCP_SERVERS config from the crew's CURRENT task(s).

    When MCP_SERVERS is absent from effective_tool_configs, merge it from the
    crew's current task: an exact task-name match, or — if the crew has a single
    task — that task. Mutates and returns ``effective_tool_configs``.

    Args:
        effective_tool_configs: dict merged from crew-level + flow-task-level configs
        flow_task_id: the (possibly stale) task ID the flow references
        flow_task_name: name of the flow's task (for matching)
        current_tasks: list of (task_id, task_name, tool_configs) for the crew's
            CURRENT tasks (from crew.task_ids)
    """
    cfg = effective_tool_configs if isinstance(effective_tool_configs, dict) else {}
    if "MCP_SERVERS" in cfg:
        return cfg
    single = len(current_tasks) == 1
    for tid, tname, tcfg in current_tasks:
        if str(tid) == str(flow_task_id):
            continue
        if not isinstance(tcfg, dict) or "MCP_SERVERS" not in tcfg:
            continue
        if single or (flow_task_name and tname == flow_task_name):
            cfg.update(tcfg)
            break
    return cfg


async def load_current_crew_tasks(crew_data, task_repo):
    """``(task_id, name, tool_configs)`` for the crew's CURRENT tasks.

    The source of truth for re-syncing a stale flow task from live crew config.
    Best-effort: returns ``[]`` on any issue — recovery must never break a flow
    run — and loads each task defensively for the same reason.
    """
    out = []
    current_task_ids = getattr(crew_data, "task_ids", None) or []
    for cur_tid in current_task_ids:
        try:
            cur_task = await task_repo.get(str(cur_tid))
        except Exception:  # noqa: BLE001 — a missing/renamed task must not abort
            cur_task = None
        if cur_task is not None:
            out.append(
                (
                    cur_tid,
                    getattr(cur_task, "name", None),
                    getattr(cur_task, "tool_configs", None),
                )
            )
    return out


def resync_tool_configs_from_current_task(
    effective_tool_configs, flow_task_id, flow_task_name, current_tasks
):
    """Re-sync a STALE flow task's tool_configs from the crew's CURRENT task.

    This is the whole-config generalisation of the MCP-only drift
    ``recover_mcp_from_current_tasks`` fixes. When ``flow_task_id`` is NOT among
    the crew's current task IDs the reference is stale: overlay the current
    matching task's tool_configs so the flow runs the crew's LIVE config.
    ``update`` is non-destructive — keys the current task lacks (e.g. an
    MCP_SERVERS block merged from the canvas node) are kept, so live values win
    without dropping node-only config. Matching mirrors the MCP helper: exact
    task-name match, or — if the crew has a single task — that task. Mutates and
    returns ``effective_tool_configs``.
    """
    cfg = effective_tool_configs if isinstance(effective_tool_configs, dict) else {}
    current_ids = {str(tid) for tid, _, _ in current_tasks}
    if str(flow_task_id) in current_ids:
        return cfg  # flow points at a live task — nothing stale to re-sync
    single = len(current_tasks) == 1
    for _tid, tname, tcfg in current_tasks:
        if not isinstance(tcfg, dict) or not tcfg:
            continue
        if single or (flow_task_name and tname == flow_task_name):
            cfg.update(tcfg)  # live config wins over the frozen snapshot
            break
    return cfg
