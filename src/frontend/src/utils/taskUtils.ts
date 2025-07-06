import { Task as ServiceTask } from '../api/TaskService';
import { Task as InterfaceTask } from '../types/task';

export const convertToServiceTask = (task: InterfaceTask): ServiceTask => ({
  id: String(task.id),
  name: task.name,
  description: task.description,
  expected_output: task.expected_output,
  tools: task.tools ?? [],
  agent_id: task.agent_id ?? null,
  async_execution: false,
  context: [],
  markdown: false,
  config: {
    cache_response: false,
    cache_ttl: 3600,
    retry_on_fail: true,
    max_retries: 3,
    timeout: null,
    priority: 1,
    error_handling: 'default',
    output_file: null,
    output_json: null,
    output_pydantic: null,
    callback: null,
    human_input: false,
    guardrail: null,
    markdown: false
  }
});

export const convertToInterfaceTask = (task: ServiceTask): InterfaceTask => ({
  id: String(task.id),
  name: task.name,
  description: task.description,
  expected_output: task.expected_output,
  tools: task.tools,
  agent_id: task.agent_id,
  async_execution: Boolean(task.async_execution),
  context: (task.context || []).map(String),
  markdown: Boolean(task.markdown),
  config: {
    cache_response: Boolean(task.config?.cache_response),
    cache_ttl: Number(task.config?.cache_ttl || 3600),
    retry_on_fail: Boolean(task.config?.retry_on_fail),
    max_retries: Number(task.config?.max_retries || 3),
    timeout: task.config?.timeout ? Number(task.config.timeout) : null,
    priority: Number(task.config?.priority || 1),
    error_handling: task.config?.error_handling as 'default' | 'ignore' | 'retry' | 'fail' || 'default',
    output_file: task.config?.output_file ? String(task.config.output_file) : null,
    output_json: task.config?.output_json ? String(task.config.output_json) : null,
    output_pydantic: task.config?.output_pydantic ? String(task.config.output_pydantic) : null,
    callback: task.config?.callback ? String(task.config.callback) : null,
    human_input: Boolean(task.config?.human_input),
    guardrail: task.config?.guardrail ? String(task.config.guardrail) : null,
    markdown: Boolean(task.config?.markdown)
  }
}); 