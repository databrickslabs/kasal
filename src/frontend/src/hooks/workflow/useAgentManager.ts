import { useState, useCallback, useEffect } from 'react';
import { Node as ReactFlowNode } from 'reactflow';
import { Agent, AgentService } from '../../api/AgentService';

interface UseAgentManagerProps {
  nodes: ReactFlowNode[];
  setNodes: (updater: (nodes: ReactFlowNode[]) => ReactFlowNode[]) => void;
}

export const useAgentManager = ({ nodes, setNodes }: UseAgentManagerProps) => {
  const [agents, setAgents] = useState<Agent[]>([]);
  const [isAgentDialogOpen, setIsAgentDialogOpen] = useState(false);

  const fetchAgents = useCallback(async () => {
    try {
      const fetchedAgents = await AgentService.listAgents();
      setAgents(fetchedAgents);
    } catch (error) {
      console.error('Error fetching agents:', error);
    }
  }, []);

  // Load agents on initial mount
  useEffect(() => {
    fetchAgents();
  }, [fetchAgents]);

  const addAgentNode = useCallback((agent: Agent, offset?: { x: number, y: number }) => {
    const position = offset || {
      x: 100,
      y: Math.random() * 400
    };

    const newNode: ReactFlowNode = {
      id: `agent-${agent.id}`,
      type: 'agentNode',
      position,
      data: {
        ...agent,
        agentId: agent.id,
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
      // Starting at Y=50 with 100px vertical spacing
      const position = {
        x: 100,
        y: 200 + (index * 150)
      };
      addAgentNode(agent, position);
    });
    setIsAgentDialogOpen(false);
  }, [addAgentNode]);

  const handleShowAgentForm = useCallback(() => {
    // TODO: Implement agent form display logic
    console.log('Show agent form');
  }, []);

  return {
    agents,
    addAgentNode,
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    handleAgentSelect,
    handleShowAgentForm,
    fetchAgents
  };
}; 