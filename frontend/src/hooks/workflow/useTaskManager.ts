import { useState, useCallback, useEffect } from 'react';
import { Node as ReactFlowNode } from 'reactflow';
import { Task, TaskService } from '../../api/TaskService';
// import { useTaskStatus } from '../../context/TaskStatusContext';
// import { convertToInterfaceTask } from '../../utils/taskUtils';

interface UseTaskManagerProps {
  nodes: ReactFlowNode[];
  setNodes: (updater: (nodes: ReactFlowNode[]) => ReactFlowNode[]) => void;
}

// This should match the return type of convertToInterfaceTask
/* interface InterfaceTask {
  id: string | number;
  name: string;
  description: string;
  expected_output: string;
  tools: string[];
  agent_id: number;
  async_execution: boolean;
  context: string[];
  config: {
    cache_response: boolean;
    cache_ttl: number;
    retry_on_fail: boolean;
    max_retries: number;
    timeout: number | null;
    priority: number;
    error_handling: 'default' | 'retry' | 'ignore' | 'fail';
    output_file: string | null;
    output_json: string | null;
    output_pydantic: string | null;
    callback: string | null;
    human_input: boolean;
    condition?: string;
  };
  created_at?: string;
  updated_at?: string;
  output?: string;
  callback?: string;
  converter_cls?: string;
} */

export const useTaskManager = ({ nodes, setNodes }: UseTaskManagerProps) => {
  const [tasks, setTasks] = useState<Task[]>([]);
  const [isTaskDialogOpen, setIsTaskDialogOpen] = useState(false);
  // Get the resetTaskStatuses function from the TaskStatusContext
  // const { resetTaskStatuses } = useTaskStatus();

  const fetchTasks = useCallback(async () => {
    try {
      const fetchedTasks = await TaskService.listTasks();
      setTasks(fetchedTasks);
    } catch (error) {
      console.error('Error fetching tasks:', error);
    }
  }, []);

  // Load tasks on initial mount
  useEffect(() => {
    fetchTasks();
  }, [fetchTasks]);

  const addTaskNode = useCallback((task: Task, offset?: { x: number, y: number }) => {
    // Reset task statuses when adding new task nodes to ensure clean state
    // resetTaskStatuses();
    
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
    // Reset task statuses before adding new tasks
    // resetTaskStatuses();
    
    // Add each selected task to the canvas vertically
    selectedTasks.forEach((task, index) => {
      // Use a fixed X position and increment Y position for each task
      // Starting at Y=50 with 100px vertical spacing
      const position = {
        x: 400,
        y: 200 + (index * 150)
      };
      addTaskNode(task, position);
    });
    setIsTaskDialogOpen(false);
  }, [addTaskNode]);

  const handleShowTaskForm = useCallback(() => {
    // TODO: Implement task form display logic
    console.log('Show task form');
  }, []);

  return {
    tasks,
    addTaskNode,
    isTaskDialogOpen,
    setIsTaskDialogOpen,
    handleTaskSelect,
    handleShowTaskForm,
    fetchTasks
  };
}; 