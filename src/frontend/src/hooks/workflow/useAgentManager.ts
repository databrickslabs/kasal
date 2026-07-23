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
  const [openInCreateMode, setOpenInCreateMode] = useState(false);

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

  const openAgentDialog = useCallback((createMode = false) => {
    setOpenInCreateMode(createMode);
    setIsAgentDialogOpen(true);
  }, []);

  return {
    agents,
    addAgentNode,
    isAgentDialogOpen,
    setIsAgentDialogOpen,
    handleAgentSelect,
    handleShowAgentForm,
    fetchAgents,
    openInCreateMode,
    openAgentDialog
  };
}; 