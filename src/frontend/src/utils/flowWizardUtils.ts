import { CrewResponse } from '../types/crews';
import type { FlowEdgeFormData } from '../types/flow';
import { Node } from 'reactflow';

// Custom interfaces for the flow wizard
export interface CrewTask {
  id: string;
  name: string;
  description: string;
  expected_output: string;
  agent_id: string;
  tools: string[];
  async_execution?: boolean;
  context?: string[];
  config?: Record<string, unknown>;
}

export interface Listener {
  id: string;
  name: string;  // Name of the listener
  crewId: string;
  crewName: string;
  listenToTaskIds: string[];  // The task IDs this listener is listening to
  listenToTaskNames: string[];  // The names of the tasks being listened to
  tasks: CrewTask[];  // Tasks to execute when the listener is triggered
  state: FlowEdgeFormData;
  conditionType: 'NONE' | 'AND' | 'OR' | 'ROUTER'; // Type of condition to apply
  routerConfig?: {
    defaultRoute: string;
    routes: {
      name: string;
      condition: string;
      taskIds: string[];
    }[];
  };
}

export interface Action {
  id: string;
  crewId: string;
  crewName: string;
  taskId: string;
  taskName: string;
}

export interface FlowConfiguration {
  id: string;
  name: string;
  startingPoints: {
    crewId: string;
    crewName: string;
    taskId: string;
    taskName: string;
  }[];
  listeners: {
    id: string;
    crewId: string;
    crewName: string;
    tasks: {
      id: string;
      name: string;
    }[];
    state: FlowEdgeFormData;
  }[];
  actions: {
    id: string;
    crewId: string;
    crewName: string;
    taskId: string;
    taskName: string;
  }[];
}

export interface StartingPoint {
  crewId: string;  // The id of the crew that the task belongs to
  taskId: string;  // The specific task id that will be used as a starting point
  isStartPoint: boolean;  // Whether this task is selected as a starting point
}

// Utility functions

/**
 * Extracts tasks from crew data
 */
export const extractTasksFromCrews = (crews: CrewResponse[]): CrewTask[] => {
  const crewTasks: CrewTask[] = [];
  
  crews.forEach(crew => {
    // Get tasks from task_ids or nodes of type taskNode
    const taskNodes = crew.nodes?.filter(node => 
      node.type === 'taskNode' || 
      (node.data && typeof node.data === 'object' && 'taskId' in node.data)
    ) || [];
    
    taskNodes.forEach(node => {
      if (node.data && typeof node.data === 'object') {
        const taskData = node.data;
        const taskId = String(taskData.taskId || taskData.id || node.id);
        
        // Skip if this is an agent ID (starts with "agent-") rather than a task
        if (taskId.startsWith('agent-')) {
          return;
        }
        
        crewTasks.push({
          id: taskId,
          name: String(taskData.label || taskData.name || ''),
          description: String(taskData.description || ''),
          expected_output: String(taskData.expected_output || ''),
          agent_id: crew.id.toString(),
          tools: Array.isArray(taskData.tools) ? taskData.tools : []
        });
      }
    });
    
    // If crew has tasks array, add those too
    if (crew.tasks && Array.isArray(crew.tasks)) {
      crew.tasks.forEach(task => {
        // Skip if this is an agent ID (starts with "agent-") rather than a task
        if (task.id.toString().startsWith('agent-')) {
          return;
        }
        
        // TaskNode has data property containing the actual task info
        if (task.data) {
          crewTasks.push({
            id: String(task.id),
            name: String(task.data.name || task.data.label || ''),
            description: String(task.data.description || ''),
            expected_output: String(task.data.expected_output || ''),
            agent_id: crew.id.toString(),
            tools: Array.isArray(task.data.tools) ? task.data.tools : []
          });
        }
      });
    }
  });
  
  return crewTasks;
};

/**
 * Helper function to determine if a crew is a flow node or a crew node
 */
export const isFlowNode = (crew: CrewResponse): boolean => {
  // Check name first
  if (crew.name.toLowerCase().includes('flow')) {
    return true;
  }
  
  // Check node types
  if (crew.nodes && crew.nodes.length > 0) {
    // If any node has a flow type or contains flow data, consider it a flow
    const hasFlowNodes = crew.nodes.some(node => 
      node.type?.toLowerCase().includes('flow') || 
      (node.data && 
       typeof node.data === 'object' && 
       ('flowConfig' in node.data || 'isFlow' in node.data))
    );
    
    if (hasFlowNodes) {
      return true;
    }
  }
  
  // Check edges for flow-related edges
  if (crew.edges && crew.edges.length > 0) {
    const hasFlowEdges = crew.edges.some(edge => 
      edge.type?.toLowerCase().includes('flow') || 
      (edge.data && typeof edge.data === 'object' && 'isFlow' in edge.data)
    );
    
    if (hasFlowEdges) {
      return true;
    }
  }
  
  return false;
};

/**
 * Creates node positions for crews in a grid layout
 */
export const _generateCrewPositions = (
  selectedCrews: CrewResponse[]
): { [key: string]: { x: number; y: number } } => {
  const positions: { [key: string]: { x: number; y: number } } = {};
  
  // Separate crews based on type using the helper function
  const crews = selectedCrews.filter(crew => !isFlowNode(crew));
  const flows = selectedCrews.filter(crew => isFlowNode(crew));

  // Canvas dimensions
  const canvasWidth = 1200;
  const canvasHeight = 800;
  
  // Define side widths
  const leftSideWidth = canvasWidth / 2;
  const _rightSideWidth = canvasWidth / 2;
  
  // Spacing between nodes
  const xSpacing = 250;
  const ySpacing = 200;
  
  // Position crews on the left side
  if (crews.length > 0) {
    // Calculate grid dimensions
    const cols = Math.ceil(Math.sqrt(crews.length));
    const rows = Math.ceil(crews.length / cols);
    
    // Starting position for crews (left side)
    const startX = 200;
    const startY = (canvasHeight - (rows * ySpacing)) / 2 + 100;
    
    crews.forEach((crew, index) => {
      // Calculate position in the grid
      const col = index % cols;
      const row = Math.floor(index / cols);
      
      positions[crew.id.toString()] = {
        x: startX + (col * xSpacing),
        y: startY + (row * ySpacing)
      };
    });
  }
  
  // Position flows on the right side
  if (flows.length > 0) {
    // Calculate grid dimensions
    const cols = Math.ceil(Math.sqrt(flows.length));
    const rows = Math.ceil(flows.length / cols);
    
    // Starting position for flows (right side)
    const startX = leftSideWidth + 200;
    const startY = (canvasHeight - (rows * ySpacing)) / 2 + 100;
    
    flows.forEach((flow, index) => {
      // Calculate position in the grid
      const col = index % cols;
      const row = Math.floor(index / cols);
      
      positions[flow.id.toString()] = {
        x: startX + (col * xSpacing),
        y: startY + (row * ySpacing)
      };
    });
  }
  
  // If there are no flows but there are crews, still position them well
  if (flows.length === 0 && crews.length > 0) {
    // Keep the crew positions as they were calculated above
  }
  
  // If there are no crews but there are flows, position the flows in the center
  if (crews.length === 0 && flows.length > 0) {
    const cols = Math.ceil(Math.sqrt(flows.length));
    const rows = Math.ceil(flows.length / cols);
    
    const startX = (canvasWidth - (cols * xSpacing)) / 2 + 150;
    const startY = (canvasHeight - (rows * ySpacing)) / 2 + 100;
    
    flows.forEach((flow, index) => {
      const col = index % cols;
      const row = Math.floor(index / cols);
      
      positions[flow.id.toString()] = {
        x: startX + (col * xSpacing),
        y: startY + (row * ySpacing)
      };
    });
  }
  
  // If there's just one item, center it
  if (selectedCrews.length === 1) {
    positions[selectedCrews[0].id.toString()] = {
      x: canvasWidth / 2,
      y: canvasHeight / 2
    };
  }
  
  return positions;
};

/**
 * Creates initial listeners for selected crews
 */
export const createInitialListeners = (
  selectedCrewIds: string[],
  crews: CrewResponse[]
): Listener[] => {
  return selectedCrewIds.map((crewId, index) => {
    const crew = crews.find(c => c.id === crewId);
    return {
      id: `listener-${index}`,
      name: `Listener ${index + 1}`,
      crewId,
      crewName: crew?.name || `Crew ${crewId}`,
      listenToTaskIds: [],  // Initially empty, will be set by user
      listenToTaskNames: [],  // Initially empty, will be set by user
      tasks: [],
      state: {
        stateType: 'unstructured' as 'unstructured' | 'structured',
        stateDefinition: '',
        stateData: {}
      },
      conditionType: 'NONE'
    };
  });
};

/**
 * Creates flow configuration object from wizard data
 */
export function createFlowConfiguration(
  name: string,
  listeners: Listener[],
  actions: Action[],
  startingPoints: StartingPoint[],
  crews: CrewResponse[],
  tasks: CrewTask[]
): FlowConfiguration {
  // Extract selected starting points
  const selectedStartingPoints = startingPoints.filter(point => point.isStartPoint).map(point => {
    const crew = crews.find(c => c.id === point.crewId);
    const task = tasks.find(t => t.id.toString() === point.taskId);
    
    if (!task) {
      console.warn(`Unable to find task with ID ${point.taskId} for starting point`);
    }
    
    return {
      crewId: point.crewId,
      crewName: crew?.name || 'Unknown Crew',
      taskId: point.taskId,
      taskName: task?.name || 'Unknown Task'
    };
  });
  
  return {
    id: `flow-${Date.now()}`,
    name,
    startingPoints: selectedStartingPoints,
    listeners: listeners.map(listener => ({
      id: listener.id,
      crewId: listener.crewId,
      crewName: listener.crewName,
      tasks: listener.tasks.map(task => ({
        id: task.id,
        name: task.name
      })),
      state: listener.state
    })),
    actions: actions.map(action => ({
      id: action.id,
      crewId: action.crewId,
      crewName: action.crewName,
      taskId: action.taskId,
      taskName: action.taskName
    }))
  };
}

/**
 * Ensures all nodes have valid position data
 */
export const validateNodePositions = (nodes: Node[]): Node[] => {
  return nodes.map(node => {
    // Check if position exists and has valid x and y coordinates
    if (!node.position || 
        typeof node.position.x !== 'number' || 
        !isFinite(node.position.x) ||
        typeof node.position.y !== 'number' || 
        !isFinite(node.position.y)) {
      
      // Generate a random position within a reasonable range
      return {
        ...node,
        position: {
          x: 100 + Math.random() * 300,
          y: 100 + Math.random() * 200
        }
      };
    }
    
    // Position exists but may have extreme values
    if (Math.abs(node.position.x) > 10000 || Math.abs(node.position.y) > 10000) {
      return {
        ...node,
        position: {
          x: 100 + Math.random() * 300,
          y: 100 + Math.random() * 200
        }
      };
    }
    
    // Position is valid, return node as is
    return node;
  });
}; 