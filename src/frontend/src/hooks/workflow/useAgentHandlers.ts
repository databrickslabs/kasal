import { useCallback } from 'react';
import { Node, NodeChange, ReactFlowInstance } from 'reactflow';
import { Agent } from '../../types/agent';
import { AgentService } from '../../api/AgentService';
import { useErrorStore } from '../../store/error';

interface UseAgentHandlersProps {
  nodes: Node[];
  onNodesChange: (changes: NodeChange[]) => void;
  reactFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
  fetchAgents?: () => Promise<void>;
}

export const useAgentHandlers = ({
  nodes,
  onNodesChange,
  reactFlowInstanceRef,
  setSuccessMessage,
  setShowSuccess,
  fetchAgents
}: UseAgentHandlersProps) => {
  const errorStore = useErrorStore();

  const handleAgentGenerated = useCallback(async (agent: Agent) => {
    try {
      const createdAgent = await AgentService.createAgent(agent);
      
      if (!createdAgent) {
        throw new Error('Failed to create agent in backend');
      }
      
      await new Promise(resolve => setTimeout(resolve, 100));
      
      const existingNodes = nodes;
      
      const position = { x: 100, y: 100 };
      
      if (existingNodes.length > 0) {
        const maxY = Math.max(...existingNodes.map(n => n.position.y + (n.height || 80)));
        position.y = maxY + 100;
        
        position.x = Math.min(Math.max(position.x, 50), 500);
        position.y = Math.min(Math.max(position.y, 50), 800);
      }
      
      const nodeId = `agent-${createdAgent.id}`;
      
      const newNode: Node = {
        id: nodeId,
        type: 'agentNode',
        position,
        width: 180,
        height: 180,
        data: {
          label: createdAgent.name,
          role: createdAgent.role,
          goal: createdAgent.goal,
          backstory: createdAgent.backstory,
          tools: createdAgent.tools || [],
          llm: createdAgent.llm,
          agentId: createdAgent.id,
          isActive: false,
          isCompleted: false
        },
        selected: true
      };
      
      onNodesChange([{
        type: 'add',
        item: newNode
      }]);
      
      setTimeout(() => {
        reactFlowInstanceRef.current?.fitView({ duration: 800, padding: 0.2 });
      }, 100);
      
      setSuccessMessage(`Agent "${createdAgent.name}" generated successfully and added to canvas`);
      setShowSuccess(true);
      
      if (fetchAgents) {
        await fetchAgents();
      }
    } catch (error) {
      errorStore.setErrorMessage(`Failed to create agent: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [nodes, onNodesChange, reactFlowInstanceRef, setSuccessMessage, setShowSuccess, fetchAgents, errorStore]);

  const handleUpdateAllAgentsLLM = useCallback(async (llmModel: string) => {
    if (!reactFlowInstanceRef.current) return;
    
    try {
      const allNodes = reactFlowInstanceRef.current.getNodes();
      const agentNodes = allNodes.filter((node: Node) => node.type === 'agentNode');
      
      if (agentNodes.length === 0) {
        errorStore.setErrorMessage('No agent nodes found on the canvas');
        errorStore.setShowError(true);
        return;
      }
      
      const updatePromises = agentNodes.map(async (node: Node) => {
        const agentId = node.data?.agentId;
        
        if (!agentId) {
          console.warn('Agent node missing agentId:', node);
          return;
        }
        
        try {
          const agent = await AgentService.getAgent(agentId);
          
          if (!agent) {
            return null;
          }
          
          const updatedAgent = {
            ...agent,
            llm: llmModel
          };
          
          return await AgentService.updateAgentFull(agentId, updatedAgent);
        } catch (error) {
          return null;
        }
      });
      
      const results = await Promise.all(updatePromises);
      const successfulUpdates = results.filter(Boolean).length;
      
      if (reactFlowInstanceRef.current) {
        const currentNodes = reactFlowInstanceRef.current.getNodes();
        const updatedNodes = currentNodes.map((node: Node) => {
          if (node.type === 'agentNode') {
            return {
              ...node,
              data: {
                ...node.data,
                llm: llmModel
              }
            };
          }
          return node;
        });
        
        reactFlowInstanceRef.current.setNodes(updatedNodes);
        onNodesChange(updatedNodes.map((node: Node) => ({ type: 'reset', item: node })));
      }
      
      setSuccessMessage(`Updated ${successfulUpdates} out of ${agentNodes.length} agents to use ${llmModel}`);
      setShowSuccess(true);
    } catch (error) {
      errorStore.setErrorMessage(`Failed to update agents: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [reactFlowInstanceRef, setSuccessMessage, setShowSuccess, errorStore, onNodesChange]);

  const handleUpdateAllAgentsMaxRPM = useCallback(async (maxRPM: number) => {
    if (!reactFlowInstanceRef.current) return;
    
    try {
      const allNodes = reactFlowInstanceRef.current.getNodes();
      const agentNodes = allNodes.filter((node: Node) => node.type === 'agentNode');
      
      if (agentNodes.length === 0) {
        errorStore.setErrorMessage('No agent nodes found on the canvas');
        errorStore.setShowError(true);
        return;
      }
      
      const updatePromises = agentNodes.map(async (node: Node) => {
        const agentId = node.data?.agentId;
        
        if (!agentId) {
          console.warn('Agent node missing agentId:', node);
          return;
        }
        
        try {
          const agent = await AgentService.getAgent(agentId);
          
          if (!agent) {
            return null;
          }
          
          const updatedAgent = {
            ...agent,
            max_rpm: maxRPM
          };
          
          return await AgentService.updateAgentFull(agentId, updatedAgent);
        } catch (error) {
          return null;
        }
      });
      
      const results = await Promise.all(updatePromises);
      const successfulUpdates = results.filter(Boolean).length;
      
      if (reactFlowInstanceRef.current) {
        const currentNodes = reactFlowInstanceRef.current.getNodes();
        const updatedNodes = currentNodes.map((node: Node) => {
          if (node.type === 'agentNode') {
            return {
              ...node,
              data: {
                ...node.data,
                max_rpm: maxRPM
              }
            };
          }
          return node;
        });
        
        reactFlowInstanceRef.current.setNodes(updatedNodes);
        onNodesChange(updatedNodes.map((node: Node) => ({ type: 'reset', item: node })));
      }
      
      setSuccessMessage(`Updated ${successfulUpdates} out of ${agentNodes.length} agents to use ${maxRPM} RPM`);
      setShowSuccess(true);
    } catch (error) {
      errorStore.setErrorMessage(`Failed to update agents: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [reactFlowInstanceRef, setSuccessMessage, setShowSuccess, errorStore, onNodesChange]);

  return {
    handleAgentGenerated,
    handleUpdateAllAgentsLLM,
    handleUpdateAllAgentsMaxRPM
  };
}; 