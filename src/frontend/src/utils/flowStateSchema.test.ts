/**
 * The flow's state declaration, derived from the flow.
 *
 * Cases are the real ones: the condition form the UI generates
 * (`state.get("has_results", "") == True`), a `{topic}` placeholder nested
 * under a crew node's `allTasks`, and a router state mapping. The shape comes
 * from a real exported flow — deriving `{has_results, topic}` from it is the
 * baseline these tests hold.
 */

import { describe, it, expect } from 'vitest';
import { deriveFlowStateConfig, flowStateNames } from './flowStateSchema';

const crewNode = (placeholder?: string) => ({
  id: 'crew-1',
  type: 'crewNode',
  data: {
    label: 'Researcher',
    allTasks: [
      {
        id: 't1',
        name: 'search',
        description: placeholder
          ? `Search the web for ${placeholder} and report back`
          : 'Search the web',
      },
    ],
  },
});

const routerEdge = (condition: string) => ({
  id: 'e1',
  source: 'crew-1',
  target: 'crew-2',
  data: { logicType: 'ROUTER', routerCondition: condition },
});

describe('flowStateNames', () => {
  it('takes the names a real flow mentions, from both directions', () => {
    const names = flowStateNames(
      [crewNode('{topic}')],
      [routerEdge('state.get("has_results", "") == True')],
    );

    expect(names).toEqual(['has_results', 'topic']);
  });

  it('includes a variable a router writes but nothing reads yet', () => {
    // Write-only today, read by a condition someone adds tomorrow — and an
    // initial value for it would be rejected if it were not declared.
    const names = flowStateNames(
      [crewNode()],
      [
        {
          id: 'e1',
          source: 'crew-1',
          target: 'crew-2',
          data: {
            logicType: 'ROUTER',
            routerConfig: {
              stateMappings: [
                { sourceTaskId: 't1', outputField: 'score', stateVariable: 'confidence' },
              ],
            },
          },
        },
      ],
    );

    expect(names).toContain('confidence');
  });

  it('includes a variable a node state operation writes', () => {
    const names = flowStateNames(
      [
        {
          ...crewNode(),
          data: {
            ...crewNode().data,
            stateOperations: [{ variable: 'attempts', value: 0 }],
          },
        },
      ],
      [],
    );

    expect(names).toContain('attempts');
  });

  it('never declares the checkpoint handle', () => {
    // `id` addresses a persisted run; the backend always adds it, and a flow
    // must not be able to redirect a restore by declaring its own.
    const names = flowStateNames([crewNode()], [routerEdge('state.id == "x"')]);

    expect(names).not.toContain('id');
  });

  it('does not mistake a method on state for a field', () => {
    const names = flowStateNames([crewNode()], [routerEdge('len(state.keys()) > 0')]);

    expect(names).toEqual([]);
  });
});

describe('deriveFlowStateConfig', () => {
  it('declares nothing until the author declares something', () => {
    // Typed state closes the field set, so an input the flow does not read
    // starts raising. Turning that on for every flow the next time it happened
    // to be saved would change flows nobody touched.
    const config = deriveFlowStateConfig(
      [crewNode('{topic}')],
      [routerEdge('state.get("has_results", "") == True')],
    );

    expect(config).toBeUndefined();
  });

  it('declares names without types once it is opted in', () => {
    // A derived type is a guess. Calling `has_results` a string would make the
    // boolean the flow later writes a validation error, breaking a flow that
    // works today — so the property is emitted with only what was declared and
    // the backend compiles the rest to untyped fields.
    const config = deriveFlowStateConfig(
      [crewNode('{topic}')],
      [routerEdge('state.get("has_results", "") == True')],
      { model: { type: 'object', properties: { has_results: { reducer: 'append' } } } },
    );

    expect(config).toEqual({
      enabled: true,
      type: 'structured',
      model: {
        type: 'object',
        properties: { has_results: { reducer: 'append' }, topic: {} },
      },
      initialValues: {},
    });
  });

  it('declares nothing for a flow that names no state', () => {
    // Undefined, not an empty schema: an empty schema would type the state to
    // `{id}` alone and reject every input the flow has ever been given.
    expect(deriveFlowStateConfig([crewNode()], [])).toBeUndefined();
  });

  it('is stable across rebuilds', () => {
    // The config is rebuilt on save, on update AND before execution. An
    // order-dependent schema would make those three disagree, which is the
    // failure mode this derivation exists to avoid.
    const nodes = [crewNode('{topic}')];
    const edges = [routerEdge('state.region == "DACH" and state.count > 1')];
    const declared = { conversational: true } as const;

    expect(deriveFlowStateConfig(nodes, edges, declared)).toEqual(
      deriveFlowStateConfig([...nodes], [...edges], declared),
    );
  });
});
