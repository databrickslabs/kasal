import { useCallback } from 'react';
import { Node as ReactFlowNode } from 'reactflow';
import type { Task } from '../../types/workflow/task';

interface UseTaskManagerProps {
  setNodes: (updater: (nodes: ReactFlowNode[]) => ReactFlowNode[]) => void;
}

export const useTaskManager = ({ setNodes }: UseTaskManagerProps) => {
  const addTaskNode = useCallback((task: Task, offset?: { x: number, y: number }) => {
    
    const position = offset || {
      x: 100,
      y: Math.random() * 400
    };

    const newNode: ReactFlowNode = {
      id: `task-${task.id}`,
      type: 'taskNode',
      position,
      data: {
        ...task,
        taskId: task.id,
        label: task.name,
        type: 'task',
        config: {
          ...task.config,
          markdown: task.config?.markdown || false
        }
      }
    };

    setNodes(nds => [...nds, newNode]);
  }, [setNodes]);

  const handleTaskSelect = useCallback((selectedTasks: Task[]) => {
    
    // Add each selected task to the canvas vertically
    selectedTasks.forEach((task, index) => {
      // Use a fixed X position and increment Y position for each task
      const position = {
        x: 400,
        y: 200 + (index * 150)
      };
      addTaskNode(task, position);
    });
  }, [addTaskNode]);

  return { handleTaskSelect };
};
