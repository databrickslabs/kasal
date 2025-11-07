import { useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { Task } from '../../../types/task';
import { useFlowNodesManager } from '../../workflow/useFlowNodesManager';
import { ValidationResult } from '../../../types/validation';

interface TaskNodeManagerProps {
  nodes: Node[];
  setNodes: React.Dispatch<React.SetStateAction<Node[]>>;
  viewport?: { x: number; y: number };
}

export const useTaskNodeManager = ({ nodes, setNodes, viewport }: TaskNodeManagerProps) => {
  const flowManager = useFlowNodesManager({ nodes, setNodes });

  const validateConnections = useCallback((nodes: Node[], edges: Edge[]): ValidationResult => {
    const errors: string[] = [];
    
    // Find all task nodes
    const taskNodes = nodes.filter(node => node.type === 'taskNode');
    
    // Check each task node
    taskNodes.forEach(taskNode => {
      // Find incoming edges to this task
      const incomingEdges = edges.filter(edge => edge.target === taskNode.id);
      
      // Find incoming edges from agent nodes
      const agentEdges = incomingEdges.filter(edge => {
        const sourceNode = nodes.find(n => n.id === edge.source);
        return sourceNode?.type === 'agentNode';
      });

      // Check if task has no assigned agent
      if (agentEdges.length === 0) {
        errors.push(`Task "${taskNode.data.label}" has no assigned agent`);
      }
      
      // Check if task has multiple assigned agents
      if (agentEdges.length > 1) {
        errors.push(`Task "${taskNode.data.label}" cannot be assigned to multiple agents`);
      }
    });

    return {
      isValid: errors.length === 0,
      errors
    };
  }, []);

  const addTaskNode = useCallback((task: Task, position?: { x: number, y: number }) => {
    const newNode = {
      id: `task-${task.id}`,
      type: 'taskNode',
      position: position || {
        x: viewport ? (viewport.x * -1) + 350 : 350,
        y: viewport ? (viewport.y * -1) + 50 : 50
      },
      data: { 
        label: task.name,
        description: task.description,
        expected_output: task.expected_output,
        tools: task.tools || [],
        icon: 'assignment',
        taskId: task.id,
        async_execution: task.async_execution || false,
        context: task.context || [],
        config: {
          cache_response: task.config?.cache_response || false,
          cache_ttl: task.config?.cache_ttl || 3600,
          retry_on_fail: task.config?.retry_on_fail || false,
          max_retries: task.config?.max_retries || 3,
          timeout: task.config?.timeout || null,
          priority: task.config?.priority || 1,
          error_handling: task.config?.error_handling || 'default',
          output_file: task.config?.output_file || null,
          output_json: task.config?.output_json || false,
          output_pydantic: task.config?.output_pydantic || null,
          callback: task.config?.callback || null,
          human_input: task.config?.human_input || false,
          markdown: task.config?.markdown || false
        }
      }
    };
    
    setNodes((nds: Node[]) => [...nds, newNode]);
  }, [setNodes, viewport]);

  return {
    ...flowManager,
    addTaskNode,
    validateConnections
  };
}; 