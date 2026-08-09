import { Edge, Node } from 'reactflow';
import { FlowConfiguration, Listener, Action, StartingPoint, Router } from '../types/workflow/flow';
import { deriveFlowStateConfig, FlowStateConfig } from './flowStateSchema';

// Task type used in flow configuration
interface FlowTask {
  id: string;
  name: string;
  description?: string;
}

// State mapping type for router configuration
interface StateMapping {
  sourceTaskId: string;
  outputField: string;
  stateVariable: string;
}

/** The one route name the engine treats as "when nothing else matched". */
export const DEFAULT_ROUTE_NAME = 'default';

/**
 * The index the backend will have given this crew's generated method.
 *
 * Both `listener_N` and `starting_point_N` are named per CREW: the backend
 * groups its input array by `crewId` and numbers the groups in first-appearance
 * order (flow_processors.py:103-123 for starting points, :600-653 for
 * listeners). The arrays built here hold one entry per EDGE and per TASK
 * respectively, so a crew appearing twice pushes every later crew's raw index
 * up, and indexing the raw array names a method the backend never created.
 *
 * That produced a router wired to a `listener_2` in a flow whose backend made
 * only listener_0 and listener_1: it never fired, and the run reported
 * COMPLETED having done half its work. The starting-point side has the same
 * defect whenever a start crew contributes more than one task.
 *
 * One helper for both, because two copies of one rule is how the halves drifted
 * apart in the first place.
 *
 * Returns -1 when the crew is not in the array.
 */
export function crewGroupIndex(
  entries: Array<{ crewId?: string; conditionType?: string }>,
  crewId: string
): number {
  const crewsInOrder: string[] = [];
  entries.forEach((entry) => {
    if (!entry.crewId) return;
    // The backend skips ROUTER-typed listeners before grouping; starting points
    // carry no conditionType, so this is a no-op for them.
    if (entry.conditionType === 'ROUTER') return;
    if (!crewsInOrder.includes(entry.crewId)) crewsInOrder.push(entry.crewId);
  });
  return crewsInOrder.indexOf(crewId);
}

/**
 * Build FlowConfiguration from nodes and edges
 * This utility is used by both SaveFlow (when saving) and JobExecutionService (when executing without saving)
 */
export const buildFlowConfiguration = (
  nodes: Node[],
  edges: Edge[],
  flowName: string,
  /**
   * The flow's already-declared state block, when the caller has it. Names are
   * derived from the canvas, but reducers and the conversational flag are the
   * author's and cannot be inferred — passing them here is what keeps a rebuild
   * from silently turning an appending channel back into an overwriting one.
   */
  declaredState?: Partial<FlowStateConfig>,
): FlowConfiguration => {
  const listeners: Listener[] = [];
  const actions: Action[] = [];
  const startingPoints: StartingPoint[] = [];
  const routers: Router[] = [];

  // Build listeners from crew nodes with tasks
  nodes.forEach(node => {
    if (node.type === 'crewNode' && node.data?.allTasks && node.data.allTasks.length > 0) {
      // Find edges targeting this node to get listen configuration
      const incomingEdges = edges.filter(edge => edge.target === node.id);

      // Only create listeners for nodes that have configured incoming edges
      // An edge is considered configured if it has listenToTaskIds populated
      // Skip ROUTER edges - they create routers instead of listeners
      incomingEdges.forEach(edge => {
        if (edge.data?.listenToTaskIds && edge.data.listenToTaskIds.length > 0) {
          // Skip if this edge is a ROUTER - routers handle conditional execution themselves
          if (edge.data.logicType === 'ROUTER') {
            return;
          }

          // Filter tasks based on targetTaskIds from edge configuration
          const targetTasks = edge.data.targetTaskIds
            ? node.data.allTasks.filter((t: FlowTask) => edge.data.targetTaskIds.includes(t.id))
            : node.data.allTasks;

          const listener: Listener = {
            id: `listener-${node.id}`,
            name: node.data.crewName || node.data.label,
            crewId: node.data.crewId || node.id,
            crewName: node.data.crewName || node.data.label,
            listenToTaskIds: edge.data.listenToTaskIds || [],
            listenToTaskNames: [], // Could be populated from task details if needed
            tasks: targetTasks,
            state: {
              stateType: 'unstructured',
              stateDefinition: '',
              stateData: {}
            },
            conditionType: edge.data.logicType || 'NONE',
            routerConfig: edge.data.routerConfig
          };
          listeners.push(listener);
        }
      });
    }

    // Detect starting points (nodes with no incoming edges)
    // Only include tasks that are actually selected in outgoing edges' listenToTaskIds
    const hasIncomingEdges = edges.some(edge => edge.target === node.id);
    if (!hasIncomingEdges && node.type === 'crewNode' && node.data?.allTasks) {
      // Get the tasks that are selected in outgoing edges
      const outgoingEdges = edges.filter(edge => edge.source === node.id);
      const selectedTaskIds = new Set<string>();

      outgoingEdges.forEach(edge => {
        if (edge.data?.listenToTaskIds) {
          edge.data.listenToTaskIds.forEach((taskId: string) => selectedTaskIds.add(taskId));
        }
      });

      // If no tasks are explicitly selected, use all tasks (fallback for simple flows)
      const tasksToUse = selectedTaskIds.size > 0
        ? node.data.allTasks.filter((task: FlowTask) => selectedTaskIds.has(task.id))
        : node.data.allTasks;

      tasksToUse.forEach((task: FlowTask) => {
        startingPoints.push({
          crewId: node.data.crewId || node.id,
          crewName: node.data.crewName || node.data.label,
          taskId: task.id,
          taskName: task.name,
          isStartPoint: true
        });
      });
    }
  });

  // Build actions from configured edges
  // An edge is considered configured if it has targetTaskIds populated
  edges.forEach(edge => {
    if (edge.data?.targetTaskIds && edge.data.targetTaskIds.length > 0) {
      const targetNode = nodes.find(n => n.id === edge.target);
      if (targetNode) {
        edge.data.targetTaskIds.forEach((taskId: string) => {
          const task = targetNode.data?.allTasks?.find((t: FlowTask) => t.id === taskId);
          if (task) {
            actions.push({
              id: `action-${edge.id}-${taskId}`,
              crewId: targetNode.data.crewId || targetNode.id,
              crewName: targetNode.data.crewName || targetNode.data.label,
              taskId: task.id,
              taskName: task.name
            });
          }
        });
      }
    }
  });

  // Build routers from edges with logicType="ROUTER"
  // Group ROUTER edges by source node to create multi-route routers
  // Router conditions now evaluate state variables (populated by stateMappings)
  const routerEdgeGroups: Map<string, Edge[]> = new Map();

  edges.forEach(edge => {
    if (edge.data?.logicType === 'ROUTER' && edge.data?.listenToTaskIds && edge.data.listenToTaskIds.length > 0) {
      // Group by source node only - conditions now use state variables, not specific task outputs
      const groupKey = edge.source;

      if (!routerEdgeGroups.has(groupKey)) {
        routerEdgeGroups.set(groupKey, []);
      }
      routerEdgeGroups.get(groupKey)?.push(edge);
    }
  });

  // Create one router per source node, with multiple routes from grouped edges
  // Router conditions now evaluate state variables (populated by stateMappings)
  routerEdgeGroups.forEach((groupEdges, groupKey) => {
    const firstEdge = groupEdges[0];
    const sourceNode = nodes.find(n => n.id === firstEdge.source);

    if (!sourceNode) return;

    // Determine which method to listen to (first selected task from first edge)
    // IMPORTANT: Must match backend method names in flow_builder.py
    // The router's source node can be either:
    // 1. A starting point (no incoming edges) -> use 'starting_point_X'
    // 2. A listener (has incoming edges) -> use 'listener_X'
    const firstTaskId = firstEdge.data.listenToTaskIds[0];
    const sourceNodeId = sourceNode.id;

    // First check if source node is a starting point
    const startingPoint = startingPoints.find(sp => sp.taskId === firstTaskId);
    const sourceTaskIndex = startingPoint
      ? crewGroupIndex(startingPoints, startingPoint.crewId)
      : -1;

    let listenTo: string;
    if (sourceTaskIndex >= 0) {
      // Source is a starting point — indexed by CREW, as the backend names them
      listenTo = `starting_point_${sourceTaskIndex}`;
    } else {
      // Source is a listener - find which listener index it corresponds to
      // The listener's crewId should match the source node's crewId
      const sourceCrewId = sourceNode.data?.crewId || sourceNode.id;
      const listenerIndex = crewGroupIndex(listeners, sourceCrewId);

      if (listenerIndex >= 0) {
        listenTo = `listener_${listenerIndex}`;
      } else {
        // Fallback: check if any of the source node's tasks are in a listener's tasks
        // This handles cases where the crewId doesn't match exactly
        const sourceTaskIds = sourceNode.data?.allTasks?.map((t: FlowTask) => t.id) || [];
        const listenerByTask = listeners.find(l =>
          l.tasks?.some((t: FlowTask) => sourceTaskIds.includes(t.id))
        );
        const byTaskIndex = listenerByTask
          ? crewGroupIndex(listeners, listenerByTask.crewId)
          : -1;

        if (byTaskIndex >= 0) {
          listenTo = `listener_${byTaskIndex}`;
        } else {
          // Final fallback to starting_point_0 (legacy behavior)
          console.warn(`Router source node ${sourceNodeId} is neither a starting point nor a listener, defaulting to starting_point_0`);
          listenTo = 'starting_point_0';
        }
      }
    }

    // Collect all state mappings from all edges in this group
    // State mappings extract task outputs → state variables for condition evaluation
    const allStateMappings: Array<{ sourceTaskId: string; outputField: string; stateVariable: string }> = [];

    // Build routes from all edges in this group
    // Route name is auto-generated from target crew name for simplicity
    const routes: Record<string, Array<{ id: string; crewId: string; crewName: string }>> = {};
    const routeConditions: Record<string, string> = {};

    groupEdges.forEach(edge => {
      const targetNode = nodes.find(n => n.id === edge.target);
      if (!targetNode) return;

      // Collect state mappings from this edge
      if (edge.data?.stateMappings && Array.isArray(edge.data.stateMappings)) {
        edge.data.stateMappings.forEach((mapping: StateMapping) => {
          if (mapping.sourceTaskId && mapping.outputField && mapping.stateVariable) {
            // Avoid duplicates
            const exists = allStateMappings.some(
              m => m.sourceTaskId === mapping.sourceTaskId &&
                   m.outputField === mapping.outputField &&
                   m.stateVariable === mapping.stateVariable
            );
            if (!exists) {
              allStateMappings.push({
                sourceTaskId: mapping.sourceTaskId,
                outputField: mapping.outputField,
                stateVariable: mapping.stateVariable
              });
            }
          }
        });
      }

      // Auto-generate route name from target crew name (sanitized to valid identifier).
      //
      // An edge marked `isDefaultRoute` is named `default` instead. The engine
      // has always honoured a route with that exact name — it takes it when no
      // condition matches (flow_builder: `return "default" if "default" in
      // router_routes else None`) — but the name was unreachable from here, so
      // the fallback was dead and a batch matching nothing just ended the flow.
      const targetCrewName = targetNode.data?.crewName || targetNode.data?.label || 'target';
      const routeName = edge.data?.isDefaultRoute
        ? DEFAULT_ROUTE_NAME
        : `route_to_${targetCrewName.replace(/[^a-zA-Z0-9]/g, '_').toLowerCase()}`;

      // Get target tasks for this route
      const targetTasks = edge.data.targetTaskIds
        ? targetNode.data?.allTasks?.filter((t: FlowTask) => edge.data.targetTaskIds.includes(t.id)) || []
        : targetNode.data?.allTasks || [];

      // Convert to the format backend expects
      const routeTasks = targetTasks.map((task: FlowTask) => ({
        id: task.id,
        crewId: targetNode.data.crewId || targetNode.id,
        crewName: targetNode.data.crewName || targetNode.data.label
      }));

      // Add or merge tasks into the route
      if (routes[routeName]) {
        routes[routeName].push(...routeTasks);
      } else {
        routes[routeName] = routeTasks;
      }

      // Store condition for this route (first condition wins if duplicates)
      // Conditions now reference state variables (e.g., "state.confidence > 0.8").
      // The default route deliberately carries NO condition — it is what runs
      // when every other condition was false, so giving it one would put it in
      // the running alongside them.
      if (edge.data?.isDefaultRoute) {
        // nothing to store
      } else if (edge.data?.routerCondition && !routeConditions[routeName]) {
        routeConditions[routeName] = edge.data.routerCondition;
      }
    });

    // If no routes were created, skip this router
    if (Object.keys(routes).length === 0) return;

    const router: Router = {
      name: `router_${groupKey.replace(/[^a-zA-Z0-9_]/g, '_')}`,
      listenTo,
      stateMappings: allStateMappings,  // State mappings to extract task outputs → state variables
      routes,
      routeConditions  // Map of route name to its condition (evaluates state variables)
    };

    routers.push(router);
  });

  // The flow's state declaration, derived from the same canvas as everything
  // above. Without it the backend runs the flow on a bare dict, which accepts
  // any key — so a misspelled input lands under the typo, the condition reading
  // the correct name sees nothing, and the flow branches as though the value
  // were never supplied. Undefined when the flow names no state, which keeps
  // those flows on the dict exactly as before.
  const state = deriveFlowStateConfig(nodes, edges, declaredState);

  return {
    id: `flow-${Date.now()}`,
    name: flowName,
    type: 'default',
    listeners,
    actions,
    startingPoints,
    routers: routers.length > 0 ? routers : undefined,
    // Spread rather than `state`, so a flow that declares nothing omits the key
    // entirely. This config is spread OVER a saved one in the chat path, and an
    // explicit `state: undefined` would blank whatever was stored there.
    ...(state ? { state } : {})
  };
};
