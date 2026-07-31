/**
 * What a flow actually reads from its state — i.e. what a caller must supply.
 *
 * Two sources, and neither alone is enough:
 *
 * - **Router conditions**, on EDGE data. `state.region == "DACH"` never appears
 *   as a `{placeholder}` anywhere, so a placeholder scan finds nothing and the
 *   caller is asked for nothing — and the flow then branches on a value it was
 *   never given. Since an unevaluable condition now fails the run rather than
 *   quietly reading as false, not asking is the difference between a prompt and
 *   a crash.
 * - **`{placeholders}`** in the flow's crew nodes, which is where a value gets
 *   used once a branch has been taken. Static condition analysis cannot see
 *   those at all.
 *
 * So the answer is the union. Anything less asks for half of what the flow
 * needs, which is indistinguishable from asking for the wrong things.
 */

import { DetectedVariable, detectVariablesFromNodes } from './variableDetector';

/**
 * State reads inside a condition expression.
 *
 * Deliberately only the three forms that NAME a key —`state.x`, `state.get('x')`
 * and `state['x']`. A bare identifier is not treated as an input: since
 * `evaluate_condition` started raising on unknown names, a bare name in a
 * condition is an authoring error, and turning it into a prompt would ask the
 * user to supply a value for someone's typo.
 */
const STATE_READS = [
  /\bstate\.get\(\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]/g,
  /\bstate\[\s*['"]([a-zA-Z_][a-zA-Z0-9_]*)['"]\s*\]/g,
  /\bstate\.([a-zA-Z_][a-zA-Z0-9_]*)/g,
];

/** Methods on `state` itself — reads of the container, not of a key. */
const NOT_KEYS = new Set(['get', 'keys', 'values', 'items', 'update']);

function readsInCondition(expression: string, into: Set<string>): void {
  for (const pattern of STATE_READS) {
    pattern.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = pattern.exec(expression)) !== null) {
      if (!NOT_KEYS.has(match[1])) into.add(match[1]);
    }
  }
}

/** Every condition string a flow carries, from edges and from node config. */
function conditionsOf(nodes: unknown[], edges: unknown[]): string[] {
  const found: string[] = [];
  const collect = (value: unknown): void => {
    if (typeof value === 'string' && value.trim()) found.push(value);
  };

  for (const raw of edges) {
    const edge = raw as { data?: Record<string, unknown> };
    collect(edge?.data?.routerCondition);
    collect(edge?.data?.condition);
  }
  for (const raw of nodes) {
    const node = raw as { data?: Record<string, unknown> };
    collect(node?.data?.routerCondition);
    collect(node?.data?.condition);
    // StateOperations carry their own conditions.
    const ops = node?.data?.stateOperations;
    if (Array.isArray(ops)) {
      for (const op of ops) collect((op as { condition?: unknown })?.condition);
    }
  }
  return found;
}

/**
 * The inputs a flow declares, from its routing logic and its crew text.
 *
 * Everything comes back `required: true` — same limitation as the crew path:
 * neither a condition nor a placeholder can say a value is optional. That is
 * what the publish dialog's input schema is for.
 */
export function deriveFlowInputs(
  nodes: unknown[],
  edges: unknown[] = [],
): DetectedVariable[] {
  const names = new Set<string>();
  for (const condition of conditionsOf(nodes, edges)) {
    readsInCondition(condition, names);
  }
  // Union, not fallback: a flow can read `region` in a router and `{quarter}` in
  // a crew task, and it needs both.
  for (const variable of detectVariablesFromNodes(nodes)) names.add(variable.name);

  return Array.from(names).map((name) => ({ name, required: true }));
}
