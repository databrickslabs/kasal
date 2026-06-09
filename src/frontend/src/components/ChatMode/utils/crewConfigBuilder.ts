/**
 * Converts plan/flow nodes + edges into the CrewConfig format
 * expected by the Kasal backend POST /executions endpoint.
 *
 * This mirrors the logic in the main Kasal frontend's
 * JobExecutionService.executeJob().
 */

interface NodeData {
  [key: string]: unknown;
}

interface FlowNode {
  id: string;
  type: string;
  data: NodeData;
  position?: { x: number; y: number };
}

interface FlowEdge {
  id: string;
  source: string;
  target: string;
  sourceHandle?: string;
  targetHandle?: string;
  data?: Record<string, unknown>;
}

export interface CrewExecutionConfig {
  agents_yaml: Record<string, Record<string, unknown>>;
  tasks_yaml: Record<string, Record<string, unknown>>;
  inputs: Record<string, unknown>;
  planning: boolean;
  reasoning: boolean;
  model?: string;
  execution_type: string;
  schema_detection_enabled: boolean;
  /**
   * Stable chat-session id that scopes session-only memory recall. The backend
   * keeps its own deterministic `crew_id` for tracing/identity; this is purely
   * the memory partition for the conversation, stable across messages.
   */
  session_id?: string;
  /** Memory READ scope: true = workspace-wide (default), false = this chat session only. */
  memory_workspace_scope?: boolean;
}

export interface FlowExecutionConfig {
  agents_yaml: Record<string, Record<string, unknown>>;
  tasks_yaml: Record<string, Record<string, unknown>>;
  inputs: Record<string, unknown>;
  planning: boolean;
  reasoning: boolean;
  model?: string;
  execution_type: string;
  schema_detection_enabled: boolean;
  nodes: { id: string; type: string; position: { x: number; y: number }; data: NodeData }[];
  edges: { id: string; source: string; target: string; sourceHandle?: string; targetHandle?: string; data?: Record<string, unknown> }[];
  flow_id?: string;
  flow_config?: Record<string, unknown>;
}

function normalizeTaskName(nodeId: string): string {
  if (nodeId.startsWith('task-') || nodeId.startsWith('task_')) {
    return nodeId.replace('task-', 'task_');
  }
  return `task_${nodeId}`;
}

function isAgentNode(node: FlowNode): boolean {
  return node.type === 'agentNode' || node.type === 'agent';
}

function isTaskNode(node: FlowNode): boolean {
  return node.type === 'taskNode' || node.type === 'task';
}

export function buildCrewConfig(plan: {
  name?: string;
  nodes: unknown[];
  edges: unknown[];
  process?: string;
  planning?: boolean;
}, model?: string, inputs?: Record<string, string>): CrewExecutionConfig {
  const nodes = plan.nodes as FlowNode[];
  const edges = plan.edges as FlowEdge[];

  const agents_yaml: Record<string, Record<string, unknown>> = {};
  const tasks_yaml: Record<string, Record<string, unknown>> = {};

  // First pass: create agent and task configurations from nodes
  nodes.forEach((node) => {
    if (isAgentNode(node)) {
      const d = node.data;
      const agentName = `agent_${node.id}`;
      const agentConfig: Record<string, unknown> = {
        role: d.role || '',
        goal: d.goal || '',
        backstory: d.backstory || '',
        tools: Array.isArray(d.tools) ? d.tools : [],
      };

      // Copy optional agent fields if present
      const optionalFields = [
        'llm', 'function_calling_llm', 'max_iter', 'max_rpm',
        'max_execution_time', 'memory', 'verbose', 'allow_delegation',
        'cache', 'system_template', 'prompt_template', 'response_template',
        'allow_code_execution', 'code_execution_mode', 'max_retry_limit',
        'use_system_prompt', 'respect_context_window', 'reasoning',
        'max_reasoning_attempts', 'embedder_config', 'knowledge_sources',
        'tool_configs', 'inject_date', 'date_format',
      ];
      optionalFields.forEach((field) => {
        if (d[field] !== undefined && d[field] !== null) {
          agentConfig[field] = d[field];
        }
      });

      agents_yaml[agentName] = agentConfig;
    } else if (isTaskNode(node)) {
      const d = node.data;
      const taskName = normalizeTaskName(node.id);
      tasks_yaml[taskName] = {
        id: node.id.startsWith('task-') ? node.id.substring(5) : node.id,
        description: d.description || '',
        expected_output: d.expected_output || '',
        tools: Array.isArray(d.tools) ? d.tools : [],
        context: [],
        agent: null,
        async_execution: Boolean(d.async_execution),
        output_file: d.config
          ? (d.config as Record<string, unknown>).output_file || `output/${node.id}.md`
          : `output/${node.id}.md`,
      };

      // Copy optional task fields
      if (d.tool_configs) tasks_yaml[taskName].tool_configs = d.tool_configs;
      if ((d as Record<string, unknown>).config) {
        const cfg = (d as Record<string, unknown>).config as Record<string, unknown>;
        if (cfg.output_json) tasks_yaml[taskName].output_json = String(cfg.output_json);
        if (cfg.output_pydantic) tasks_yaml[taskName].output_pydantic = String(cfg.output_pydantic);
        if (cfg.human_input !== undefined) tasks_yaml[taskName].human_input = Boolean(cfg.human_input);
        if (cfg.guardrail) {
          try {
            tasks_yaml[taskName].guardrail = JSON.parse(String(cfg.guardrail));
          } catch { /* ignore */ }
        }
        if (cfg.llm_guardrail) {
          tasks_yaml[taskName].guardrail = JSON.stringify(cfg.llm_guardrail);
        }
      }
    }
  });

  // Second pass: edges -> connect agents to tasks and set task dependencies
  edges.forEach((edge) => {
    const sourceNode = nodes.find((n) => n.id === edge.source);
    const targetNode = nodes.find((n) => n.id === edge.target);

    if (sourceNode && isAgentNode(sourceNode) && targetNode && isTaskNode(targetNode)) {
      const agentName = `agent_${edge.source}`;
      const taskName = normalizeTaskName(edge.target);
      // A task-typed target always has an entry created in the first pass.
      tasks_yaml[taskName].agent = agentName;
    } else if (sourceNode && isTaskNode(sourceNode) && targetNode && isTaskNode(targetNode)) {
      const depTaskName = normalizeTaskName(edge.source);
      const targetTaskName = normalizeTaskName(edge.target);
      // A task-typed target always has an entry created in the first pass.
      (tasks_yaml[targetTaskName].context as string[]).push(depTaskName);
    }
  });

  // Third pass: handle agent_id on task nodes for missing connections
  nodes.forEach((node) => {
    if (isTaskNode(node) && node.data.agent_id) {
      const taskName = normalizeTaskName(node.id);
      if (tasks_yaml[taskName] && !tasks_yaml[taskName].agent) {
        const agentNode = nodes.find(
          (n) =>
            isAgentNode(n) &&
            (n.data.id === node.data.agent_id || n.id === node.data.agent_id)
        );
        if (agentNode) {
          tasks_yaml[taskName].agent = `agent_${agentNode.id}`;
        }
      }
    }
  });

  return {
    agents_yaml,
    tasks_yaml,
    inputs: inputs || {},
    planning: plan.planning || false,
    reasoning: false,
    model: model || undefined,
    execution_type: 'crew',
    schema_detection_enabled: true,
  };
}

/**
 * Builds a CrewExecutionConfig directly from generated agent/task arrays
 * (as returned by the generation_complete SSE event), without needing
 * ReactFlow nodes/edges.
 */
export function buildCrewConfigFromGenerated(
  agents: Record<string, unknown>[],
  tasks: Record<string, unknown>[],
  model?: string,
  toolConfigs?: Record<string, Record<string, unknown>>,
  inputs?: Record<string, string>,
  toolNameMap: Record<string, string> = {},
  sessionId?: string | null,
  workspaceMemory: boolean = true,
  memoryEnabled: boolean = true,
): CrewExecutionConfig {
  const agents_yaml: Record<string, Record<string, unknown>> = {};
  const tasks_yaml: Record<string, Record<string, unknown>> = {};

  // toolConfigs are keyed by canonical tool NAME (e.g. "GenieTool"), but a
  // generated agent/task references tools by ID. Resolve each tool entry through
  // toolNameMap so the override attaches under the name the backend looks up
  // (task_tool_configs.get(tool_name)). Without this, a Genie space picked in
  // the chat never reaches the tool ("Genie space ID is not configured").
  const applicableToolConfigs = (
    tools: string[],
  ): Record<string, Record<string, unknown>> => {
    const applicable: Record<string, Record<string, unknown>> = {};
    if (!toolConfigs) return applicable;
    tools.forEach((t) => {
      // toolConfigs is keyed by canonical tool name; the agent/task may list the
      // tool by id, so resolve through toolNameMap before matching.
      const name = toolNameMap[String(t)] || String(t);
      if (toolConfigs[name]) applicable[name] = toolConfigs[name];
    });
    return applicable;
  };

  // Build agent configs keyed by agent_<id>
  const agentIdToKey: Record<string, string> = {};
  agents.forEach((agent) => {
    const id = String(agent.id || '');
    const key = `agent_${id}`;
    agentIdToKey[id] = key;

    const agentTools: string[] = Array.isArray(agent.tools) ? agent.tools as string[] : [];
    const agentConfig: Record<string, unknown> = {
      role: agent.role || '',
      goal: agent.goal || '',
      backstory: agent.backstory || '',
      tools: agentTools,
    };

    // Inject tool_configs for tools that have overrides (e.g. GenieTool spaceId)
    const agentApplicable = applicableToolConfigs(agentTools);
    if (Object.keys(agentApplicable).length > 0) {
      agentConfig.tool_configs = agentApplicable;
    }

    const optionalFields = [
      'llm', 'function_calling_llm', 'max_iter', 'max_rpm',
      'max_execution_time', 'memory', 'verbose', 'allow_delegation',
      'cache', 'system_template', 'prompt_template', 'response_template',
      'allow_code_execution', 'code_execution_mode', 'max_retry_limit',
      'use_system_prompt', 'respect_context_window',
    ];
    optionalFields.forEach((field) => {
      if (agent[field] !== undefined && agent[field] !== null) {
        agentConfig[field] = agent[field];
      }
    });

    // "No memory" mode: force every agent to be created without memory. The
    // backend's determine_crew_memory() disables crew memory entirely when ALL
    // agents have memory:false, so nothing is recalled or persisted.
    if (!memoryEnabled) {
      agentConfig.memory = false;
    }

    agents_yaml[key] = agentConfig;
  });

  // Build task configs
  tasks.forEach((task) => {
    const id = String(task.id || '');
    const key = `task_${id}`;

    const agentId = String(task.agent_id || task.agent || '');
    const agentKey = agentIdToKey[agentId] || null;

    // Resolve context (task dependencies)
    let context: string[] = [];
    if (Array.isArray(task.context)) {
      context = task.context.map((dep: unknown) => {
        if (typeof dep === 'string') return `task_${dep}`;
        if (typeof dep === 'object' && dep !== null && 'id' in dep) return `task_${(dep as Record<string, unknown>).id}`;
        return '';
      }).filter(Boolean);
    }

    const taskTools: string[] = Array.isArray(task.tools) ? task.tools as string[] : [];
    const taskEntry: Record<string, unknown> = {
      id,
      description: task.description || '',
      expected_output: task.expected_output || '',
      tools: taskTools,
      context,
      agent: agentKey,
      async_execution: Boolean(task.async_execution),
      output_file: (task.output_file as string) || `output/${id}.md`,
    };

    // Inject tool_configs for tasks that have matching tools
    const taskApplicable = applicableToolConfigs(taskTools);
    if (Object.keys(taskApplicable).length > 0) {
      taskEntry.tool_configs = taskApplicable;
    }

    tasks_yaml[key] = taskEntry;
  });

  return {
    agents_yaml,
    tasks_yaml,
    inputs: inputs || {},
    planning: false,
    reasoning: false,
    model: model || undefined,
    execution_type: 'crew',
    schema_detection_enabled: true,
    // Memory scoping for chat. The backend generates its own deterministic
    // crew_id for tracing; we pass the chat session id purely as the memory
    // partition. The READ scope is controlled by `memory_workspace_scope`:
    //   - true (default): backend recalls workspace-wide (group_id) — all context;
    //   - false: backend recalls only this chat session (session_id).
    session_id: sessionId || undefined,
    memory_workspace_scope: workspaceMemory,
  };
}

export function buildFlowConfig(flow: {
  id?: string;
  name?: string;
  nodes: unknown[];
  edges: unknown[];
  flow_config?: Record<string, unknown>;
}, model?: string): FlowExecutionConfig {
  const nodes = flow.nodes as FlowNode[];
  const edges = flow.edges as FlowEdge[];

  const mappedNodes = nodes.map((node) => ({
    id: node.id,
    type: node.type || 'unknown',
    position: node.position || { x: 0, y: 0 },
    data: node.data || {},
  }));

  const mappedEdges = edges.map((edge) => ({
    id: edge.id,
    source: edge.source,
    target: edge.target,
    sourceHandle: edge.sourceHandle || undefined,
    targetHandle: edge.targetHandle || undefined,
    data: edge.data || {},
  }));

  return {
    agents_yaml: {},
    tasks_yaml: {},
    inputs: {},
    planning: false,
    reasoning: false,
    model: model || undefined,
    execution_type: 'flow',
    schema_detection_enabled: true,
    nodes: mappedNodes,
    edges: mappedEdges,
    flow_id: flow.id,
    flow_config: {
      ...(flow.flow_config || {}),
      nodes: mappedNodes,
      edges: mappedEdges,
    },
  };
}
