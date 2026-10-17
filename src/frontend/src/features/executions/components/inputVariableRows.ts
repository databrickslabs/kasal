import type { Node } from 'reactflow';
import { placeholdersInFlowNodes } from '../../../utils/flowInputs';

export interface InputVariable {
  name: string;
  value: string;
  description?: string;
  required: boolean;
}

const VARIABLE_PATTERN = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

/** The `{placeholder}` names used by the crew or flow on these nodes. */
export function detectInputVariables(nodes: Node[]): string[] {
  const found = new Set<string>();
  const scan = (value: unknown) => {
    if (!value || typeof value !== 'string') return;
    VARIABLE_PATTERN.lastIndex = 0;
    let match;
    while ((match = VARIABLE_PATTERN.exec(value)) !== null) found.add(match[1]);
  };

  // A flow's crew nodes carry the referenced crew's text nested inside them,
  // so the field-by-field scan below cannot reach it. Same deep walk the run
  // gate uses, so the dialog offers exactly what that gate detected.
  const flowNodes = nodes.filter(node => node.type === 'crewNode');
  if (flowNodes.length > 0) placeholdersInFlowNodes(flowNodes).forEach(name => found.add(name));

  nodes.forEach(node => {
    if (node.type !== 'agentNode' && node.type !== 'taskNode') return;
    const data = node.data as Record<string, unknown>;
    [data.role, data.goal, data.backstory, data.description, data.expected_output, data.label].forEach(scan);

    // tool_configs values too (e.g. {user_question} in Reducer config), on
    // data.tool_configs (progressive SSE path) or data.task.tool_configs (LoadCrew path).
    const toolConfigs = (data.tool_configs || (data.task as Record<string, unknown>)?.tool_configs) as
      Record<string, Record<string, unknown>> | undefined;
    if (toolConfigs && typeof toolConfigs === 'object') {
      Object.values(toolConfigs).forEach(toolCfg => {
        if (toolCfg && typeof toolCfg === 'object') Object.values(toolCfg).forEach(scan);
      });
    }
  });
  return Array.from(found);
}

const emptyRow = (name: string): InputVariable => ({ name, value: '', required: true });

/**
 * The rows for a fresh opening: exactly the variables of the run being started.
 * A value typed for the same name last time is kept; rows of an earlier run
 * (another crew, or a placeholder since removed) are not.
 */
export function rowsForRun(previous: InputVariable[], detected: string[]): InputVariable[] {
  return detected.map(name => previous.find(row => row.name === name) ?? emptyRow(name));
}

/** While open, add any newly detected variable; keep what the user typed or added. */
export function withNewlyDetected(rows: InputVariable[], detected: string[]): InputVariable[] {
  const missing = detected.filter(name => !rows.some(row => row.name === name));
  return missing.length > 0 ? [...rows, ...missing.map(emptyRow)] : rows;
}
