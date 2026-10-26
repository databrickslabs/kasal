import { AxiosError } from 'axios';
import { Task } from '../../types/workflow/task';
import { apiClient } from '../../shared/api/client';

// Define specific types for error response
interface ErrorResponse {
  detail?: string;
}

// Define extended task interface with assigned_agent field
interface TaskWithAssignedAgent extends Partial<Task> {
  assigned_agent?: string;
}

export type { Task };

/**
 * The body of a task update: the keys the caller passed, coerced, and nothing
 * else. The backend leaves an absent key untouched and REPLACES a config that
 * is sent, so a partial update (the Tools/MCP chip, the knowledge upload) must
 * not synthesize a config, agent_id "", context [], tools [] or a root
 * llm_guardrail: null — doing so switched off the task's approval gate, LLM
 * guardrail and output schema and unassigned its agent.
 */
export function toTaskUpdatePayload(task: Partial<Task>): Partial<Task> {
  const payload: Partial<Task> = {};
  for (const [key, value] of Object.entries(task)) {
    if (value !== undefined) (payload as Record<string, unknown>)[key] = value;
  }
  if (task.name !== undefined) payload.name = task.name?.trim();
  if (task.description !== undefined) payload.description = task.description?.trim();
  if (task.expected_output !== undefined) payload.expected_output = task.expected_output?.trim();
  if (task.tools !== undefined) payload.tools = task.tools || [];
  if (task.context !== undefined) payload.context = task.context || [];
  if (task.async_execution !== undefined) payload.async_execution = Boolean(task.async_execution);
  if (task.markdown !== undefined) payload.markdown = Boolean(task.markdown);
  // "" / null is "no agent chosen here", not "unassign"; the assignment lives
  // on the canvas edge, and the row keeps its agent_id when none is sent.
  if (!task.agent_id) delete payload.agent_id;

  if (task.config) {
    const config = task.config;
    payload.config = {
      ...config,
      cache_response: Boolean(config.cache_response),
      cache_ttl: Number(config.cache_ttl || 3600),
      retry_on_fail: Boolean(config.retry_on_fail),
      max_retries: Number(config.max_retries || 3),
      timeout: config.timeout ? Number(config.timeout) : null,
      priority: Number(config.priority || 1),
      error_handling: config.error_handling || 'default',
      output_file: config.output_file || null,
      output_json: config.output_json || null,
      output_pydantic: config.output_pydantic || null,
      callback: config.callback || null,
      human_input: Boolean(config.human_input),
      condition: config.condition || undefined,
      guardrail: config.guardrail || undefined,
      llm_guardrail: config.llm_guardrail ?? null,
      markdown: Boolean(config.markdown),
    };
    if (task.markdown === undefined) payload.markdown = Boolean(config.markdown);
    // Mirrored at the top level for the database sync, as before.
    payload.llm_guardrail = config.llm_guardrail ?? null;
  }
  return payload;
}

export class TaskService {
  static async getTask(id: string): Promise<Task | null> {
    try {
      const response = await apiClient.get<Task>(`/tasks/${id}`);
      console.log('Fetched task:', response.data);
      console.log('Task tool_configs:', response.data?.tool_configs);
      if (response.data?.tool_configs?.GenieTool) {
        console.log('GenieTool config in fetched task:', response.data.tool_configs.GenieTool);
      }
      return response.data;
    } catch (error) {
      console.error('Error fetching task:', error);
      return null;
    }
  }


  static async createTask(task: Partial<Task>): Promise<Task> {
    try {
      // Extract assigned_agent if it exists in the incoming task object
      const taskWithAgent = task as TaskWithAssignedAgent;
      const assignedAgent = taskWithAgent.assigned_agent;


      // Validate and format the task data
      const taskData = {
        ...task,
        name: task.name?.trim(),
        description: task.description?.trim(),
        expected_output: task.expected_output?.trim(),
        tools: task.tools || [],
        ...(task.tool_configs ? { tool_configs: task.tool_configs } : {}),  // Include tool_configs only if present
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
          llm_guardrail: task.config?.llm_guardrail ?? null,
          markdown: Boolean(task.config?.markdown)
        },
        // Also include llm_guardrail at top level for database sync
        llm_guardrail: task.config?.llm_guardrail ?? null
      };

      // Add assigned_agent to the final taskData if it exists
      const finalTaskData: TaskWithAssignedAgent = assignedAgent ? { 
        ...taskData, 
        assigned_agent: assignedAgent 
      } : taskData;


      const response = await apiClient.post<Task>('/tasks', finalTaskData);
      
      // Ensure task response has agent_id set
      const responseData = response.data;
      
      // If agent_id isn't in the response but was in our request, add it back
      if ((!responseData.agent_id || responseData.agent_id === '') && finalTaskData.agent_id) {
        console.log('TaskService - Adding missing agent_id to response:', finalTaskData.agent_id);
        responseData.agent_id = finalTaskData.agent_id;
      }


      return responseData;
    } catch (error) {
      console.error('Error creating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error creating task');
    }
  }

  /**
   * PUT only the keys the caller passed (see `toTaskUpdatePayload`). Pass the
   * whole task, as TaskForm does, to replace its config.
   */
  static async updateTask(id: string, task: Partial<Task>): Promise<Task> {
    try {
      const response = await apiClient.put<Task>(`/tasks/${id}`, toTaskUpdatePayload(task));
      return response.data;
    } catch (error) {
      console.error('Error updating task:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error updating task');
    }
  }

  static async patchTaskToolConfigs(
    id: string,
    toolConfigs: Record<string, Record<string, unknown>>,
  ): Promise<Task> {
    try {
      const response = await apiClient.put<Task>(`/tasks/${id}`, { tool_configs: toolConfigs });
      return response.data;
    } catch (error) {
      console.error('Error patching task tool_configs:', error);
      const axiosError = error as AxiosError<ErrorResponse>;
      throw new Error(axiosError.response?.data?.detail || 'Error patching task tool_configs');
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

}
