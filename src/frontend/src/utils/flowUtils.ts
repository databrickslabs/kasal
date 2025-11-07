import { Node, Edge } from 'reactflow';

/**
 * Calculates a position for a new node that doesn't overlap with existing nodes
 */
export const calculateNonOverlappingPosition = (basePosition: { x: number; y: number }, existingNodes: Node[]): { x: number; y: number } => {
  const padding = 100; // Padding between nodes
  const nodeHeight = 80; // Default node height
  
  // If there are existing task nodes, align vertically with the first one
  const existingTaskNodes = existingNodes.filter(n => n.type === 'taskNode');
  if (existingTaskNodes.length > 0) {
    // Use the x position of the first task node
    const firstTaskX = existingTaskNodes[0].position.x;
    
    // Find the lowest task node
    const lowestTaskY = Math.max(...existingTaskNodes.map(n => n.position.y));
    
    // Position the new task below the lowest task
    return {
      x: firstTaskX, // Align with the first task's x position
      y: lowestTaskY + nodeHeight + padding // Place below the lowest task with padding
    };
  }
  
  // If no existing tasks, use the base position
  return basePosition;
};

/**
 * Logs edge details for debugging purposes
 */
export const logEdgeDetails = (edges: Edge[], message: string) => {
  
  // Group edges by type (agent-task, task-task)
  // const _agentToTaskEdges = edges.filter(edge => 
  //   edge.source.includes('agent') && edge.target.includes('task')
  // );
  
  const taskToTaskEdges = edges.filter(edge => 
    edge.source.includes('task') && edge.target.includes('task')
  );
  
  
  // Log a few example edges for debugging
  if (taskToTaskEdges.length > 0) {
    // Task-to-task edges exist
  } else {
    // No task-to-task edges
  }
}; 