import { useCallback, useState } from 'react';
import { Connection, ReactFlowInstance } from 'reactflow';

interface UseConnectionGeneratorProps {
  reactFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  onConnect: (connection: Connection) => void;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
}

export const useConnectionGenerator = ({
  reactFlowInstanceRef,
  onConnect,
  setSuccessMessage,
  setShowSuccess
}: UseConnectionGeneratorProps) => {
  const [isGeneratingConnections, setIsGeneratingConnections] = useState(false);

  const handleGenerateConnections = useCallback(async () => {
    if (!reactFlowInstanceRef.current) return false;
    
    setIsGeneratingConnections(true);
    try {
      const nodes = reactFlowInstanceRef.current.getNodes();
      const edges = reactFlowInstanceRef.current.getEdges();
      
      // Check if there are existing edges on the canvas
      if (edges.length > 0) {
        setSuccessMessage('Cannot generate connections: existing edges found on canvas. Please clear existing connections first.');
        setShowSuccess(true);
        return false;
      }
      
      const agentNodes = nodes.filter(node => node.type === 'agentNode');
      const taskNodes = nodes.filter(node => node.type === 'taskNode');
      
      if (agentNodes.length === 0 || taskNodes.length === 0) {
        setSuccessMessage('Cannot generate connections: need both agent and task nodes on canvas');
        setShowSuccess(true);
        return false;
      }
      
      let connectionsCreated = 0;
      for (const agentNode of agentNodes) {
        for (const taskNode of taskNodes) {
          const connection: Connection = {
            source: agentNode.id,
            target: taskNode.id,
            sourceHandle: null,
            targetHandle: null
          };
          onConnect(connection);
          connectionsCreated++;
        }
      }
      
      if (connectionsCreated > 0) {
        setSuccessMessage(`Successfully generated ${connectionsCreated} connections`);
        setShowSuccess(true);
        return true;
      } else {
        setSuccessMessage('No new connections were generated');
        setShowSuccess(true);
        return false;
      }
    } catch (error) {
      console.error('Error generating connections:', error);
      setSuccessMessage('Failed to generate connections');
      setShowSuccess(true);
      return false;
    } finally {
      setIsGeneratingConnections(false);
    }
  }, [reactFlowInstanceRef, onConnect, setSuccessMessage, setShowSuccess]);

  return {
    isGeneratingConnections,
    handleGenerateConnections
  };
}; 