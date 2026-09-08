import { useCallback } from 'react';
import { Node as ReactFlowNode } from 'reactflow';
import type { Agent } from '../../types/workflow/agent';

interface UseAgentManagerProps {
  setNodes: (updater: (nodes: ReactFlowNode[]) => ReactFlowNode[]) => void;
}

export const useAgentManager = ({ setNodes }: UseAgentManagerProps) => {
  const addAgentNode = useCallback((agent: Agent, offset?: { x: number, y: number }) => {
    const position = offset || {
      x: 100,
      y: Math.random() * 400
    };

    // Generate a unique ID if the agent doesn't have one
    const agentId = agent.id || `temp-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
    
    const newNode: ReactFlowNode = {
      id: `agent-${agentId}`,
      type: 'agentNode',
      position,
      data: {
        ...agent,
        agentId: agent.id, // Use the actual agent.id (which could be undefined for new agents)
        id: agent.id, // Also set id in data for consistency
        label: agent.name,
        type: 'agent',
      }
    };

    setNodes(nds => [...nds, newNode]);
  }, [setNodes]);

  const handleAgentSelect = useCallback((selectedAgents: Agent[]) => {
    // Add each selected agent to the canvas vertically
    selectedAgents.forEach((agent, index) => {
      // Use a fixed X position and increment Y position for each agent
      const position = {
        x: 100,
        y: 200 + (index * 150)
      };
      addAgentNode(agent, position);
    });
  }, [addAgentNode]);

  return { handleAgentSelect };
};
