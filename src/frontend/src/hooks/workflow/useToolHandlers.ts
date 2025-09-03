import { useCallback } from 'react';
import { Node, ReactFlowInstance } from 'reactflow';
import { AgentService } from '../../api/AgentService';
import { useErrorStore } from '../../store/error';

interface UseToolHandlersProps {
  reactFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
}

export const useToolHandlers = ({
  reactFlowInstanceRef,
  setSuccessMessage,
  setShowSuccess
}: UseToolHandlersProps) => {
  const errorStore = useErrorStore();

  const handleChangeToolsForAllAgents = useCallback(async (selectedTools: string[]) => {
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
            tools: selectedTools
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
                tools: selectedTools
              }
            };
          }
          return node;
        });
        
        reactFlowInstanceRef.current.setNodes(updatedNodes);
      }
      
      setSuccessMessage(`Updated ${successfulUpdates} out of ${agentNodes.length} agents with ${selectedTools.length} tools`);
      setShowSuccess(true);
    } catch (error) {
      errorStore.setErrorMessage(`Failed to update agents: ${error instanceof Error ? error.message : 'Unknown error'}`);
    }
  }, [reactFlowInstanceRef, errorStore, setSuccessMessage, setShowSuccess]);

  return {
    handleChangeToolsForAllAgents
  };
}; 