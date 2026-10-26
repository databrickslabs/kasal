/**
 * The state a flow declares — derived from the flow itself.
 *
 * Backend `Flow` has always supported a typed state; the builder read
 * `flow_config.state.model` and passed it nowhere, because nothing ever wrote
 * it. An untyped dict state accepts any key, which is how an input named
 * `topci` lands in state, the router reading `topic` sees nothing, the flow
 * takes the wrong branch, and the run reports success.
 *
 * The declaration is derived from the CANVAS rather than kept somewhere
 * separate, and that is the whole design decision. Everything else in
 * `flow_config` — listeners, routers, conditions — comes out of
 * `buildFlowConfiguration(nodes, edges)`, which is called on save, on update
 * AND before execution. A state schema stored anywhere else would be a second
 * source of truth those three paths could disagree about; that class of
 * divergence is exactly what produced "runs fine in Flow Builder, one crew
 * missing in ChatMode".
 *
 * Two rules make this safe to apply to every flow:
 *
 * - **Names, not types.** Each property is emitted with no `type`, so the
 *   backend compiles it to an untyped field. Guessing that `has_results` is a
 *   string would make the boolean the flow later writes a validation error —
 *   a derived type is a guess, and a wrong guess breaks a working flow.
 * - **A superset of what the flow mentions.** Reads, writes, mappings and
 *   placeholders all count. An extra name costs nothing (it defaults to null
 *   and accepts a value); a MISSING name would reject a legitimate input. So
 *   derivation errs wide — and the typo still raises, because a misspelling
 *   appears nowhere in the flow.
 */

import { deriveFlowInputs } from './flowInputs';

/** How writes to a channel merge. Omitted means `replace`, the default. */
export type FlowStateReducer = 'replace' | 'append' | 'merge' | 'add';

export interface FlowStateProperty {
  type?: string;
  reducer?: FlowStateReducer;
}

export interface FlowStateSchema {
  type: 'object';
  properties: Record<string, FlowStateProperty>;
}

export interface FlowStateConfig {
  enabled: true;
  type: 'structured';
  model: FlowStateSchema;
  initialValues: Record<string, unknown>;
  /**
   * Whether the flow holds a conversation. When true the backend builds its
   * state on `ConversationState`, which brings `messages` (append),
   * `last_user_message`, `last_intent` and `session_ready` — the turn contract
   * a multi-turn thread needs and no author should have to redeclare.
   */
  conversational?: boolean;
}

/** Variables a router writes into state from a task's output. */
function stateMappingVariables(edges: unknown[], into: Set<string>): void {
  for (const raw of edges) {
    const edge = raw as { data?: { routerConfig?: { stateMappings?: unknown } } };
    const mappings = edge?.data?.routerConfig?.stateMappings;
    if (!Array.isArray(mappings)) continue;
    for (const mapping of mappings) {
      const name = (mapping as { stateVariable?: unknown })?.stateVariable;
      if (typeof name === 'string' && name.trim()) into.add(name.trim());
    }
  }
}

/** Variables a node's state operations write. */
function stateOperationVariables(nodes: unknown[], into: Set<string>): void {
  for (const raw of nodes) {
    const node = raw as { data?: { stateOperations?: unknown } };
    const operations = node?.data?.stateOperations;
    if (!Array.isArray(operations)) continue;
    for (const operation of operations) {
      const name = (operation as { variable?: unknown })?.variable;
      if (typeof name === 'string' && name.trim()) into.add(name.trim());
    }
  }
}

/**
 * Every state name this flow mentions, from any direction.
 *
 * Reads (`deriveFlowInputs`: conditions + `{placeholders}`) plus writes
 * (router state mappings, node state operations). The read side alone would be
 * too narrow: a router that writes `confidence` for a LATER condition to read
 * is covered, but a write nothing reads yet is not — and dropping it would
 * reject an initial value for it.
 */
export function flowStateNames(nodes: unknown[], edges: unknown[] = []): string[] {
  const names = new Set<string>();
  for (const input of deriveFlowInputs(nodes, edges)) names.add(input.name);
  stateMappingVariables(edges, names);
  stateOperationVariables(nodes, names);
  // `id` is the checkpoint handle, never a declared field — the backend adds it.
  names.delete('id');
  return Array.from(names).sort();
}

/**
 * The `state` block of a flow's config, or undefined when there is nothing
 * to declare.
 *
 * Undefined matters: a flow that names no state keeps running on a dict,
 * exactly as before. Emitting an empty schema would type it to `{id}` alone
 * and reject every input it has ever been given.
 */
export function deriveFlowStateConfig(
  nodes: unknown[],
  edges: unknown[] = [],
  declared?: Partial<FlowStateConfig>,
): FlowStateConfig | undefined {
  const names = flowStateNames(nodes, edges);
  const declaredProps = declared?.model?.properties ?? {};
  const declaredNames = Object.keys(declaredProps);
  const conversational = !!declared?.conversational;

  // Typed state is OPT-IN: a flow gets one only once its author has declared
  // something about it — a reducer, or the conversational flag.
  //
  // Deriving names alone is not enough to justify it. Typing a state closes its
  // field set, so an input the flow does not read starts raising at kickoff;
  // turning that on for every existing flow the next time it happened to be
  // saved would be a behaviour change nobody asked for, applied to flows nobody
  // touched. The panel is the opt-in, and until it is used the flow keeps
  // running on a dict exactly as before.
  if (declaredNames.length === 0 && !conversational) {
    return undefined;
  }

  // Names come from the canvas; POLICY comes from the author. A reducer cannot
  // be derived — nothing in a condition or a placeholder says whether a channel
  // accumulates — so a rebuild that dropped declared reducers would silently
  // turn an appending channel back into an overwriting one, and the flow would
  // forget everything but its newest turn.
  const properties: Record<string, FlowStateProperty> = {};
  for (const name of [...names, ...declaredNames]) {
    properties[name] = { ...(declaredProps[name] ?? {}) };
  }

  return {
    enabled: true,
    type: 'structured',
    model: { type: 'object', properties },
    initialValues: declared?.initialValues ?? {},
    ...(conversational ? { conversational: true } : {}),
  };
}
