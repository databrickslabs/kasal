import { Node, Edge } from 'reactflow';

export interface ValidationResult {
  isValid: boolean;
  errors: string[];
}

export class ValidationService {
  static validateFlow(nodes: Node[], edges: Edge[]): ValidationResult {
    const errors: string[] = [];

    // Validate tasks have agents assigned
    const taskNodes = nodes.filter(node => node.type === 'taskNode');
    taskNodes.forEach(taskNode => {
      const incomingEdges = edges.filter(edge => edge.target === taskNode.id);
      const agentEdges = incomingEdges.filter(edge => {
        const sourceNode = nodes.find(n => n.id === edge.source);
        return sourceNode?.type === 'agentNode';
      });

      if (agentEdges.length === 0) {
        errors.push(`Task "${taskNode.data.label}" has no assigned agent`);
      }
      if (agentEdges.length > 1) {
        errors.push(`Task "${taskNode.data.label}" has multiple agents assigned`);
      }
    });

    // Validate no agent-to-agent connections
    edges.forEach(edge => {
      const sourceNode = nodes.find(n => n.id === edge.source);
      const targetNode = nodes.find(n => n.id === edge.target);

      if (sourceNode?.type === 'agentNode' && targetNode?.type === 'agentNode') {
        errors.push(`Invalid connection: Agent "${sourceNode.data.label}" connected to agent "${targetNode.data.label}"`);
      }

      if (sourceNode?.type === 'taskNode' && targetNode?.type === 'agentNode') {
        errors.push(`Invalid connection: Task "${sourceNode.data.label}" connected to agent "${targetNode.data.label}"`);
      }
    });

    return {
      isValid: errors.length === 0,
      errors
    };
  }
} 