import type { Node as FlowNode } from 'reactflow';

// Extract variables from nodes
export const extractVariablesFromNodes = (workflowNodes: FlowNode[]): string[] => {
  const variablePattern = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;
  const foundVariables = new Set<string>();

  const scanString = (value: unknown) => {
    if (value && typeof value === 'string') {
      let match;
      variablePattern.lastIndex = 0;
      while ((match = variablePattern.exec(value)) !== null) {
        foundVariables.add(match[1]);
      }
    }
  };

  workflowNodes.forEach(node => {
    if (node.type === 'agentNode' || node.type === 'taskNode') {
      const data = node.data as Record<string, unknown>;

      // Scan standard agent/task fields
      [data.role, data.goal, data.backstory, data.description, data.expected_output, data.label]
        .forEach(scanString);

      // Scan tool_configs values (e.g. {user_question} in Reducer config)
      // Check both data.tool_configs (progressive SSE path) and data.task.tool_configs (all-at-once/LoadCrew path)
      const toolConfigs = (data.tool_configs || (data.task as Record<string, unknown>)?.tool_configs) as Record<string, Record<string, unknown>> | undefined;
      if (toolConfigs && typeof toolConfigs === 'object') {
        Object.values(toolConfigs).forEach(toolCfg => {
          if (toolCfg && typeof toolCfg === 'object') {
            Object.values(toolCfg).forEach(scanString);
          }
        });
      }
    }
  });

  return Array.from(foundVariables);
};

