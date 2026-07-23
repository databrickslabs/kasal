import { useCallback } from 'react';
import { Node, Edge } from 'reactflow';
import { Agent } from '../../../types/agent';
import { useFlowNodesManager } from '../../workflow/useFlowNodesManager';
import { ValidationResult } from '../../../types/validation';
import { FlowNodeManagerProps } from '../../../types/crew';

interface Position {
  x: number;
  y: number;
}

export const useAgentNodeManager = ({ nodes, setNodes, viewport }: FlowNodeManagerProps) => {
  const flowManager = useFlowNodesManager({ nodes, setNodes });

  const validateConnections = useCallback((nodes: Node[], edges: Edge[]): ValidationResult => {
    const errors: string[] = [];
    
    // Check each edge for valid connections
    edges.forEach(edge => {
      const sourceNode = nodes.find(n => n.id === edge.source);
      const targetNode = nodes.find(n => n.id === edge.target);
      
      if (!sourceNode || !targetNode) {
        errors.push('Invalid connection: Node not found');
        return;
      }

      // Case 1: Agent to Agent connection (not allowed)
      if (sourceNode.type === 'agentNode' && targetNode.type === 'agentNode') {
        errors.push(`Invalid connection: Cannot connect agent "${sourceNode.data.label}" to agent "${targetNode.data.label}"`);
      }
    });

    return {
      isValid: errors.length === 0,
      errors
    };
  }, []);

  const addAgentNode = useCallback((agent: Agent, offset?: Position) => {
    const position = {
      x: offset?.x || 100,
      y: offset?.y || 100
    };

    const newNode = {
      id: `agent-${agent.id}`,
      type: 'agentNode',
      position,
      data: {
        label: agent.name,
        role: agent.role,
        goal: agent.goal,
        backstory: agent.backstory,
        tools: agent.tools,
        agentId: agent.id,
        llm: agent.llm,
        function_calling_llm: agent.function_calling_llm,
        max_iter: agent.max_iter,
        max_rpm: agent.max_rpm,
        max_execution_time: agent.max_execution_time,
        memory: agent.memory,
        verbose: agent.verbose,
        allow_delegation: agent.allow_delegation,
        cache: agent.cache,
        system_template: agent.system_template,
        prompt_template: agent.prompt_template,
        response_template: agent.response_template,
        allow_code_execution: agent.allow_code_execution,
        code_execution_mode: agent.code_execution_mode,
        max_retry_limit: agent.max_retry_limit,
        use_system_prompt: agent.use_system_prompt,
        respect_context_window: agent.respect_context_window,
        embedder_config: agent.embedder_config,
        knowledge_sources: agent.knowledge_sources,
        type: 'agent',
      },
    };

    setNodes((nds) => [...nds, newNode]);
  }, [setNodes]);

  return {
    ...flowManager,
    addAgentNode,
    validateConnections
  };
}; 