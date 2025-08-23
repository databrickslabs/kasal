import { AxiosError } from 'axios';
import { Task } from '../types/task';
import { apiClient } from '../config/api/ApiConfig';

// Define specific types for error response
interface ErrorResponse {
  detail?: string;
}

// Define extended task interface with assigned_agent field
interface TaskWithAssignedAgent extends Partial<Task> {
  assigned_agent?: string;
}

export type { Task };

export class TaskService {
  static async getTask(id: string): Promise<Task | null> {
    try {
      const response = await apiClient.get<Task>(`/tasks/${id}`);
      console.log('Fetched task:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error fetching task:', error);
      return null;
    }
  }

  static async findOrCreateTask(task: Partial<Task>): Promise<Task> {
    try {
      // Use the same logic as createTask but with find-or-create endpoint
      const taskData = { ...task };
      
      // Ensure required fields have defaults
      if (!taskData.tools) taskData.tools = [];
      if (!taskData.context) taskData.context = [];
      if (!taskData.async_execution) taskData.async_execution = false;
      
      console.log('TaskService - find-or-create task data:', {
        name: taskData.name,
        config: taskData.config
      });

      // Use find-or-create endpoint to prevent duplicates
      const response = await apiClient.post<Task>('/tasks/find-or-create', taskData);
      
      console.log('Task find-or-create successful:', response.data);
      return response.data;
    } catch (error) {
      const axiosError = error as AxiosError<ErrorResponse>;
      console.error('Error in find-or-create task:', axiosError.response?.data?.detail || axiosError.message);
      throw axiosError;
    }
  }

  static async createTask(task: Partial<Task>): Promise<Task> {
    try {
      // Extract assigned_agent if it exists in the incoming task object
      const taskWithAgent = task as TaskWithAssignedAgent;
      const assignedAgent = taskWithAgent.assigned_agent;
      
      console.log('TaskService - Initial task data received:', {
        name: task.name,
        config: task.config,
        markdown: task.markdown,
        configMarkdown: task.config?.markdown
      });
      console.log('TaskService - Initial output_pydantic value:', task.config?.output_pydantic);
      
      // Validate and format the task data
      const taskData = {
        ...task,
        name: task.name?.trim(),
        description: task.description?.trim(),
        expected_output: task.expected_output?.trim(),
        tools: task.tools || [],
        agent_id: task.agent_id || "",
        async_execution: task.async_execution !== undefined ? Boolean(task.async_execution) : false,
        markdown: task.markdown !== undefined ? Boolean(task.markdown) : Boolean(task.config?.markdown),
        context: task.context || [],
        config: {
          ...task.config,
          cache_response: Boolean(task.config?.cache_response),
          cache_ttl: Number(task.config?.cache_ttl || 3600),
          retry_on_fail: Boolean(task.config?.retry_on_fail),
          max_retries: Number(task.config?.max_retries || 3),
          timeout: task.config?.timeout ? Number(task.config.timeout) : null,
          priority: Number(task.config?.priority || 1),
          error_handling: task.config?.error_handling || 'default',
          output_file: task.config?.output_file || null,
          output_json: task.config?.output_json || null,
          output_pydantic: task.config?.output_pydantic || null,
          callback: task.config?.callback || null,
          human_input: Boolean(task.config?.human_input),
          condition: task.config?.condition || undefined,
          guardrail: task.config?.guardrail || undefined,
          markdown: Boolean(task.config?.markdown)
        }
      };

      console.log('TaskService - Formatted task data:', {
        name: taskData.name,
        markdown: taskData.markdown,
        config: JSON.stringify(taskData.config)
      });
      console.log('TaskService - Formatted output_pydantic value:', taskData.config.output_pydantic);

      // Add assigned_agent to the final taskData if it exists
      const finalTaskData: TaskWithAssignedAgent = assignedAgent ? { 
        ...taskData, 
        assigned_agent: assignedAgent 
      } : taskData;

      console.log('TaskService - Creating task with data:', {
        name: finalTaskData.name,
        agent_id: finalTaskData.agent_id,
        assigned_agent: finalTaskData.assigned_agent,
        tools: finalTaskData.tools,
        config: JSON.stringify(finalTaskData.config)
      });

      const response = await apiClient.post<Task>('/tasks', finalTaskData);
      
      // Ensure task response has agent_id set
      const responseData = response.data;
      
      // If agent_id isn't in the response but was in our request, add it back
      if ((!responseData.agent_id || responseData.agent_id === '') && finalTaskData.agent_id) {
        console.log('TaskService - Adding missing agent_id to response:', finalTaskData.agent_id);
        responseData.agent_id = finalTaskData.agent_id;
      }
      
      console.log('TaskService - Created task response:', {
        id: responseData.id,
        name: responseData.name,
        agent_id: responseData.agent_id,
        config: JSON.stringify(responseData.config)
      });
      console.log('TaskService - Response output_pydantic value:', responseData.config?.output_pydantic);
      
      return responseData;
    } catch (error) {
      console.error('Error creating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error creating task');
    }
  }

  static async updateTask(id: string, task: Partial<Task>): Promise<Task> {
    try {
      console.log('TaskService - Initial update data received:', {
        id,
        name: task.name,
        config: task.config
      });
      console.log('TaskService - Initial output_pydantic value for update:', task.config?.output_pydantic);
      
      // Validate and format the task data
      const taskData = {
        ...task,
        name: task.name?.trim(),
        description: task.description?.trim(),
        expected_output: task.expected_output?.trim(),
        tools: task.tools || [],
        agent_id: task.agent_id || "",
        async_execution: task.async_execution !== undefined ? Boolean(task.async_execution) : false,
        markdown: task.markdown !== undefined ? Boolean(task.markdown) : Boolean(task.config?.markdown),
        context: task.context || [],
        config: {
          ...task.config,
          cache_response: Boolean(task.config?.cache_response),
          cache_ttl: Number(task.config?.cache_ttl || 3600),
          retry_on_fail: Boolean(task.config?.retry_on_fail),
          max_retries: Number(task.config?.max_retries || 3),
          timeout: task.config?.timeout ? Number(task.config.timeout) : null,
          priority: Number(task.config?.priority || 1),
          error_handling: task.config?.error_handling || 'default',
          output_file: task.config?.output_file || null,
          output_json: task.config?.output_json || null,
          output_pydantic: task.config?.output_pydantic || null,
          callback: task.config?.callback || null,
          human_input: Boolean(task.config?.human_input),
          condition: task.config?.condition || undefined,
          guardrail: task.config?.guardrail || undefined,
          markdown: Boolean(task.config?.markdown)
        }
      };

      console.log('TaskService - Formatted update data:', {
        id,
        name: taskData.name,
        config: JSON.stringify(taskData.config)
      });
      console.log('TaskService - Formatted output_pydantic value for update:', taskData.config.output_pydantic);

      const response = await apiClient.put<Task>(`/tasks/${id}`, taskData);
      console.log('Updated task:', response.data);
      console.log('TaskService - Response output_pydantic value after update:', response.data.config?.output_pydantic);
      return response.data;
    } catch (error) {
      console.error('Error updating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating task');
    }
  }

  static async updateTaskFull(id: string, task: Partial<Task>): Promise<Task> {
    try {
      // Format the task data to match the server's expected structure
      const taskData = {
        ...task,
        config: {
          ...task.config,
          condition: task.config?.condition || undefined,
          guardrail: task.config?.guardrail || undefined
        }
      };

      const response = await apiClient.put<Task>(`/tasks/${id}/full`, taskData);
      console.log('Updated task (full):', response.data);
      return response.data;
    } catch (error) {
      console.error('Error updating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating task');
    }
  }

  static async deleteTask(id: string): Promise<void> {
    try {
      await apiClient.delete(`/tasks/${id}`);
      console.log('Deleted task:', id);
    } catch (error) {
      console.error('Error deleting task:', error);
    }
  }

  static async deleteAllTasks(): Promise<void> {
    try {
      await apiClient.delete('/tasks');
      console.log('Deleted all tasks');
    } catch (error) {
      console.error('Error deleting all tasks:', error);
    }
  }

  static async listTasks(): Promise<Task[]> {
    try {
      const response = await apiClient.get<Task[]>('/tasks');
      return response.data;
    } catch (error) {
      console.error('Error fetching tasks:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error fetching tasks');
    }
  }

  static async generateTask(prompt: string): Promise<Task> {
    try {
      // Log the request payload for debugging
      const payload = { text: prompt.trim() };
      console.log('Sending generate task request with payload:', payload);

      const response = await apiClient.post<Task>(
        '/generate/generate-task', 
        payload,
        {
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
      
      console.log('Generated task response:', response.data);
      return response.data;
    } catch (error) {
      console.error('Error generating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      if (axiosError.response) {
        console.error('Server response:', axiosError.response.data);
        console.error('Status code:', axiosError.response.status);
      }
      throw new Error(axiosError.response?.data?.detail || 'Error generating task');
    }
  }
} 