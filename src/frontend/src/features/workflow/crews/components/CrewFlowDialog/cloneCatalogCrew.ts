/**
 * "Open as copy" for a catalog crew: re-create its agent and task rows and
 * return the canvas re-pointed at the copies.
 *
 * This is "start from this crew", not "open this crew" — the catalog card
 * opens a crew in place, on its own rows. The caller must NOT bind the
 * original crew id to the new tab: Save then creates a new crew, where a bound
 * id would repoint the original crew at the copies.
 *
 * The copy is built from the canvas nodes, which carry only part of a row
 * (per-agent LLM overrides, skills and task tool_configs are not on them), so
 * it is a starting point rather than a duplicate.
 */
import type { Edge, Node } from 'reactflow';
import { getDefaultModel } from '../../../../../config/defaultModel';
import { AgentService } from '../../../../../api/workflow/AgentService';
import { TaskService } from '../../../../../api/workflow/TaskService';
import type { CrewResponse } from '../../../../../types/workflow/crew';

type IdMapping = Record<string, string>;

async function cloneAgents(nodes: Node[], idMapping: IdMapping): Promise<void> {
  for (const node of nodes) {
    if (node.type !== 'agentNode') continue;
    try {
      const agentData = node.data;
      // Format agent data following AgentForm pattern
      const formattedAgentData = {
        name: agentData.name || agentData.label || '',
        role: agentData.role || '',
        goal: agentData.goal || '',
        backstory: agentData.backstory || '',
        llm: agentData.llm || getDefaultModel(),
        tools: agentData.tools || [],
        function_calling_llm: agentData.function_calling_llm,
        max_iter: agentData.max_iter || 25,
        max_rpm: agentData.max_rpm,
        max_execution_time: agentData.max_execution_time || 300,
        memory: agentData.memory ?? true,
        verbose: agentData.verbose || false,
        allow_delegation: agentData.allow_delegation || false,
        cache: agentData.cache ?? true,
        system_template: agentData.system_template,
        prompt_template: agentData.prompt_template,
        response_template: agentData.response_template,
        allow_code_execution: agentData.allow_code_execution || false,
        code_execution_mode: agentData.code_execution_mode || 'safe',
        max_retry_limit: agentData.max_retry_limit || 3,
        use_system_prompt: agentData.use_system_prompt ?? true,
        respect_context_window: agentData.respect_context_window ?? true,
        embedder_config: agentData.embedder_config,
        knowledge_sources: agentData.knowledge_sources || []
      };

      const newAgent = await AgentService.createAgent(formattedAgentData);
      if (newAgent && newAgent.id) {
        idMapping[node.id] = newAgent.id.toString();
      } else {
        throw new Error(`Failed to create agent: ${formattedAgentData.name}`);
      }
    } catch (err) {
      const error = err as Error;
      console.error('Error creating agent:', error);
      throw new Error(`Failed to create agent: ${error.message}`);
    }
  }
}

async function cloneTasks(nodes: Node[], edges: Edge[], idMapping: IdMapping): Promise<void> {
  for (const node of nodes) {
    if (node.type !== 'taskNode') continue;
    try {
      const taskData = node.data;
      // The copied agent the canvas connects this task to. Node data carries
      // no agent_id, so the agent->task edge is the only place it lives; with
      // several, the LAST wins, as in a canvas run (JobExecutionService).
      const agentEdge = edges.filter(edge => edge.target === node.id && idMapping[edge.source]
        && nodes.some(n => n.id === edge.source && n.type === 'agentNode')).pop();

      // Format task data following TaskForm pattern
      const formattedTaskData = {
        name: String(taskData.label || ''),
        description: String(taskData.description || ''),
        expected_output: String(taskData.expected_output || ''),
        tools: (taskData.tools || []).map((tool: unknown) => String(tool)),
        agent_id: agentEdge ? idMapping[agentEdge.source] : null,
        async_execution: Boolean(taskData.async_execution),
        context: (taskData.context || []).map((item: unknown) => String(item)),
        config: {
          cache_response: Boolean(taskData.config?.cache_response),
          cache_ttl: Number(taskData.config?.cache_ttl || 3600),
          retry_on_fail: Boolean(taskData.config?.retry_on_fail),
          max_retries: Number(taskData.config?.max_retries || 3),
          timeout: taskData.config?.timeout ? Number(taskData.config.timeout) : null,
          priority: Number(taskData.config?.priority || 1),
          error_handling: (taskData.config?.error_handling || 'default') as 'default' | 'retry' | 'ignore' | 'fail',
          output_file: taskData.config?.output_file || null,
          output_json: taskData.config?.output_json || null,
          output_pydantic: taskData.config?.output_pydantic || null,
          callback: taskData.config?.callback || null,
          human_input: Boolean(taskData.config?.human_input),
          condition: taskData.config?.condition === 'is_data_missing' ? 'is_data_missing' : undefined,
          guardrail: taskData.config?.guardrail || null,
          // Config is source of truth for user's choice (null = disabled)
          // Only fall back to top-level if config is undefined (never set)
          llm_guardrail: taskData.config?.llm_guardrail !== undefined
            ? taskData.config.llm_guardrail
            : (taskData.task?.config?.llm_guardrail ?? null),
          markdown: taskData.config?.markdown === true || taskData.config?.markdown === 'true' || taskData.markdown === true || taskData.markdown === 'true'
        }
      };

      const newTask = await TaskService.createTask(formattedTaskData);
      if (newTask && newTask.id) {
        idMapping[node.id] = newTask.id.toString();
      } else {
        throw new Error(`Failed to create task: ${formattedTaskData.name}`);
      }
    } catch (err) {
      const error = err as Error;
      console.error('Error creating task:', error);
      throw new Error(`Failed to create task: ${error.message}`);
    }
  }
}

/** A node id on the copied canvas: agents and tasks follow their new row id. */
function copiedNodeId(node: Node | undefined, id: string, idMapping: IdMapping): string {
  if (node?.type === 'managerNode') return id; // Manager keeps its original ID
  const prefix = node?.type === 'agentNode' ? 'agent' : 'task';
  return `${prefix}-${idMapping[id] || id}`;
}

export async function cloneCatalogCrew(crew: CrewResponse): Promise<{ nodes: Node[]; edges: Edge[] }> {
  const sourceNodes = crew.nodes || [];
  const sourceEdges = crew.edges || [];
  // Old node id -> new row id. Agents first, so a task can find its agent's copy.
  const idMapping: IdMapping = {};
  await cloneAgents(sourceNodes, idMapping);
  await cloneTasks(sourceNodes, sourceEdges, idMapping);

  const nodes = sourceNodes.map(node => ({
    ...node,
    id: copiedNodeId(node, node.id, idMapping),
    type: node.type, // Ensure type is preserved
    data: {
      ...node.data,
      id: node.type === 'managerNode' ? node.data.id : (idMapping[node.id] || node.data.id),
      agentId: node.type === 'agentNode' ? idMapping[node.id] || node.data.agentId : node.data.agentId,
      taskId: node.type === 'taskNode' ? idMapping[node.id] || node.data.taskId : node.data.taskId,
      type: node.type === 'agentNode' ? 'agent' : (node.type === 'managerNode' ? 'manager' : 'task') // Set the internal type field
    }
  }));

  // Update edge source and target IDs to match the new node IDs
  const edges = sourceEdges.map(edge => ({
    ...edge,
    source: copiedNodeId(sourceNodes.find(n => n.id === edge.source), edge.source, idMapping),
    target: copiedNodeId(sourceNodes.find(n => n.id === edge.target), edge.target, idMapping),
  }));

  return { nodes, edges };
}
