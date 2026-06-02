/**
 * Detects {variable_name} placeholders in crew/plan data so the UI
 * can prompt the user for values before execution.
 *
 * Uses the same regex as the main Kasal frontend InputVariablesDialog.
 */

export interface DetectedVariable {
  name: string;
  required: boolean;
}

const VARIABLE_PATTERN = /\{([a-zA-Z_][a-zA-Z0-9_-]*)\}/g;

const FIELDS_TO_SCAN = [
  'role',
  'goal',
  'backstory',
  'description',
  'expected_output',
  'label',
] as const;

function extractVariables(text: string, into: Set<string>): void {
  let match: RegExpExecArray | null;
  while ((match = VARIABLE_PATTERN.exec(text)) !== null) {
    into.add(match[1]);
  }
}

function scanObject(obj: Record<string, unknown>, into: Set<string>): void {
  for (const field of FIELDS_TO_SCAN) {
    const val = obj[field];
    if (typeof val === 'string') {
      extractVariables(val, into);
    }
  }
}

/**
 * Detect variables from plan/catalog nodes (the shape used by catalog_load).
 * Each node has { type, data: { role, goal, backstory, description, ... } }.
 */
export function detectVariablesFromNodes(
  nodes: unknown[],
): DetectedVariable[] {
  const found = new Set<string>();

  for (const raw of nodes) {
    if (!raw || typeof raw !== 'object') continue;
    const node = raw as { type?: string; data?: Record<string, unknown> };
    if (
      node.type === 'agentNode' ||
      node.type === 'agent' ||
      node.type === 'taskNode' ||
      node.type === 'task'
    ) {
      if (node.data && typeof node.data === 'object') {
        scanObject(node.data, found);
      }
    }
  }

  return Array.from(found).map((name) => ({ name, required: true }));
}

/**
 * Detect variables from generated agent/task arrays
 * (the shape returned by generation_complete SSE events).
 */
export function detectVariablesFromGenerated(
  agents: Record<string, unknown>[],
  tasks: Record<string, unknown>[],
): DetectedVariable[] {
  const found = new Set<string>();

  for (const agent of agents) {
    scanObject(agent, found);
  }
  for (const task of tasks) {
    scanObject(task, found);
  }

  return Array.from(found).map((name) => ({ name, required: true }));
}
