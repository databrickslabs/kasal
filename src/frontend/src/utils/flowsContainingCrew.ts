/**
 * Which open flows use a given crew.
 *
 * The Flow Builder can already reach the Agent Builder — a crew node opens the
 * crew it names. This is the other direction, and it cannot be answered the same
 * way: a crew on the Agent Builder canvas is a dozen agent and task nodes, and
 * none of them knows about a flow. The tab does.
 *
 * Derived rather than stored. Remembering "this tab was opened from that flow"
 * would only work for crews opened THAT way, and would go stale the moment the
 * flow tab is closed. Matching a saved crew id against the crew nodes of the
 * open flow tabs answers the question actually being asked — "is this crew in a
 * flow I have open?" — and is right however the crew got here.
 */

interface CrewNodeLike {
  type?: string;
  data?: { crewId?: string | number };
}

export interface FlowTabLike {
  id: string;
  name: string;
  flowNodes?: CrewNodeLike[];
  savedFlowName?: string;
  group_id?: string;
  lastModified?: Date | string;
}

export interface FlowReference {
  tabId: string;
  name: string;
}

/**
 * The open flows containing `crewId`, most recently touched first.
 *
 * Reads `flowNodes`, not `viewMode`: a tab holds both canvases, so one showing
 * its crew side still HAS its flow, and hiding the link while you happen to be
 * looking the other way is exactly the moment you want it.
 */
export function flowsContainingCrew(
  tabs: FlowTabLike[],
  crewId: string | number | undefined | null,
): FlowReference[] {
  if (crewId === undefined || crewId === null || crewId === '') return [];
  const wanted = String(crewId);

  return tabs
    .filter((tab) =>
      (tab.flowNodes ?? []).some(
        (node) =>
          node?.type === 'crewNode' &&
          node?.data?.crewId !== undefined &&
          String(node.data.crewId) === wanted,
      ),
    )
    .sort(
      (a, b) =>
        new Date(b.lastModified ?? 0).getTime() -
        new Date(a.lastModified ?? 0).getTime(),
    )
    .map((tab) => ({ tabId: tab.id, name: tab.savedFlowName || tab.name }));
}
