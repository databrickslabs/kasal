import { Edge, Node } from 'reactflow';
import { FlowConfiguration, Listener, Action, StartingPoint, Router } from '../types/flow';

/**
 * Build FlowConfiguration from nodes and edges
 * This utility is used by both SaveFlow (when saving) and JobExecutionService (when executing without saving)
 */
export const buildFlowConfiguration = (nodes: Node[], edges: Edge[], flowName: string): FlowConfiguration => {
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
            ? node.data.allTasks.filter((t: any) => edge.data.targetTaskIds.includes(t.id))
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

    // Detect starting points (nodes with no incoming edges or marked as start)
    const hasIncomingEdges = edges.some(edge => edge.target === node.id);
    if (!hasIncomingEdges && node.type === 'crewNode' && node.data?.allTasks) {
      node.data.allTasks.forEach((task: any) => {
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
          const task = targetNode.data?.allTasks?.find((t: any) => t.id === taskId);
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
  edges.forEach((edge, index) => {
    if (edge.data?.logicType === 'ROUTER' && edge.data?.routerCondition) {
      const sourceNode = nodes.find(n => n.id === edge.source);
      const targetNode = nodes.find(n => n.id === edge.target);

      if (sourceNode && targetNode && edge.data.listenToTaskIds && edge.data.listenToTaskIds.length > 0) {
        // Determine which method to listen to (the start method for the source task)
        const sourceTaskId = edge.data.listenToTaskIds[0];
        const sourceTaskIndex = startingPoints.findIndex(sp => sp.taskId === sourceTaskId);
        const listenTo = sourceTaskIndex >= 0 ? `start_flow_${sourceTaskIndex}` : 'start_flow_0';

        // Get target tasks for this router
        const targetTasks = edge.data.targetTaskIds
          ? targetNode.data?.allTasks?.filter((t: any) => edge.data.targetTaskIds.includes(t.id)) || []
          : targetNode.data?.allTasks || [];

        // Backend expects routes as Dict[str, List[Dict]] where each dict has 'id' and 'crewId'
        // Convert CrewTask objects to the format backend expects
        const routeTasks = targetTasks.map((task: any) => ({
          id: task.id,
          crewId: targetNode.data.crewId || targetNode.id
        }));

        // Create routes based on router condition
        // Backend expects routes as a plain dict, not RouterRoute objects
        const routes: Record<string, any[]> = {
          'default': routeTasks
        };

        const router: any = {
          name: `router_${index}`,
          listenTo,
          conditionField: 'success',  // Default condition field
          routes,
          condition: edge.data.routerCondition  // Add condition at router level
        };

        routers.push(router);
      }
    }
  });

  return {
    id: `flow-${Date.now()}`,
    name: flowName,
    type: 'default',
    listeners,
    actions,
    startingPoints,
    routers: routers.length > 0 ? routers : undefined
  };
};
