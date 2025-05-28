import { useCallback } from 'react';
import { Node, NodeChange, ReactFlowInstance } from 'reactflow';
import { Task } from '../../types/task';
import { TaskService } from '../../api/TaskService';
import { useErrorStore } from '../../store/error';
import { calculateNonOverlappingPosition } from '../../utils/flowUtils';

interface UseTaskHandlersProps {
  nodes: Node[];
  onNodesChange: (changes: NodeChange[]) => void;
  reactFlowInstanceRef: React.MutableRefObject<ReactFlowInstance | null>;
  setSuccessMessage: (message: string) => void;
  setShowSuccess: (show: boolean) => void;
}

export const useTaskHandlers = ({
  nodes,
  onNodesChange,
  reactFlowInstanceRef,
  setSuccessMessage,
  setShowSuccess
}: UseTaskHandlersProps) => {
  const errorStore = useErrorStore();

  const handleTaskGenerated = useCallback(async (task: Task) => {
    if (!task) return;
    
    try {
      const createdTask = await TaskService.createTask(task);
      
      const timestamp = Date.now();
      const taskId = createdTask.id || timestamp;
      
      const existingNodes = nodes;
      
      const agentNodes = existingNodes.filter(n => n.type === 'agentNode');
      const maxAgentX = agentNodes.length > 0 
        ? Math.max(...agentNodes.map(n => n.position.x)) 
        : 0;
      
      const taskNodes = existingNodes.filter(n => n.type === 'taskNode');
      
      const basePosition = { 
        x: Math.max(maxAgentX + 300, 400),
        y: 100 
      };
      
      const newPosition = calculateNonOverlappingPosition(basePosition, taskNodes);
      
      const nodeId = `task-${taskId}`;
      
      const newNode: Node = {
        id: nodeId,
        type: 'taskNode',
        position: newPosition,
        width: 280,
        height: 140,
        data: {
          label: task.name,
          taskId: taskId,
          description: task.description || task.name,
          expected_output: task.expected_output || '',
          human_input: task.config?.human_input || false,
          tools: task.tools || [],
          config: {
            markdown: task.config?.markdown || false
          },
          task: {
            ...task,
            description: task.description || task.name
          }
        }
      };
      
      onNodesChange([{
        type: 'add',
        item: newNode
      }]);
      
      setTimeout(() => {
        reactFlowInstanceRef.current?.fitView({ duration: 800, padding: 0.2 });
      }, 100);
      
      setSuccessMessage(`Task "${task.name}" generated successfully and added to canvas`);
      setShowSuccess(true);
    } catch (error) {
      errorStore.showErrorMessage('Failed to create task');
    }
  }, [nodes, onNodesChange, reactFlowInstanceRef, setSuccessMessage, setShowSuccess, errorStore]);

  return {
    handleTaskGenerated
  };
}; 