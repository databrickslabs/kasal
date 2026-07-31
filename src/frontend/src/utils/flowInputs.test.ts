import { describe, expect, it } from 'vitest';

import { deriveFlowInputs } from './flowInputs';

describe('what a flow reads from its state', () => {
  // A flow's inputs mostly live in router conditions on EDGES. They are not
  // {placeholders}, so the placeholder scan the crew path uses finds nothing —
  // the caller is asked for nothing and the flow then branches on a value it
  // was never given. Now that an unevaluable condition fails the run instead of
  // quietly reading as false, not asking is the difference between a prompt and
  // a crash.
  const edge = (condition: string) => ({ data: { routerCondition: condition } });

  it('finds attribute reads', () => {
    expect(deriveFlowInputs([], [edge('state.region == "DACH"')])).toEqual([
      { name: 'region', required: true },
    ]);
  });

  it('finds .get() and subscript reads', () => {
    const names = deriveFlowInputs(
      [],
      [edge("state.get('quarter') == 'Q3'"), edge('state["score"] > 5')],
    ).map((v) => v.name);
    expect(names.sort()).toEqual(['quarter', 'score']);
  });

  it('does not mistake a state method for a key', () => {
    expect(deriveFlowInputs([], [edge('len(state.keys()) > 0')])).toEqual([]);
  });

  it('ignores a bare identifier', () => {
    // Since evaluate_condition raises on unknown names, a bare name is an
    // authoring error. Prompting for it would ask the user to supply a value
    // for someone else's typo.
    expect(deriveFlowInputs([], [edge('regoin == "DACH"')])).toEqual([]);
  });

  it('UNIONS condition reads with crew {placeholders}', () => {
    // A flow can route on `region` and use `{quarter}` inside a task. Asking
    // for either alone is indistinguishable from asking for the wrong things.
    const nodes = [
      { type: 'taskNode', data: { description: 'Review {quarter}' } },
    ];
    const names = deriveFlowInputs(nodes, [edge('state.region == "DACH"')]).map(
      (v) => v.name,
    );
    expect(names.sort()).toEqual(['quarter', 'region']);
  });

  it('reads conditions on nodes and state operations too', () => {
    const nodes = [
      { data: { stateOperations: [{ condition: 'state.status == "ready"' }] } },
    ];
    expect(deriveFlowInputs(nodes, []).map((v) => v.name)).toEqual(['status']);
  });

  it('does not repeat a name read in several places', () => {
    const names = deriveFlowInputs(
      [],
      [edge('state.region == "DACH"'), edge('state.region == "EMEA"')],
    );
    expect(names).toHaveLength(1);
  });

  it('a flow with no conditions and no placeholders declares nothing', () => {
    expect(deriveFlowInputs([], [])).toEqual([]);
  });
});

describe('a real flow made of crewNodes', () => {
  // The shape a saved flow actually has, taken from an exported two-crew flow.
  // The placeholder its starting crew needs is NOT on the node — it is nested
  // inside the referenced crew's task text under allTasks[].description, on a
  // node whose type is 'crewNode'. The crew-path scan skips it twice over: once
  // on the type filter, once because it reads only top-level fields.
  const crewNode = {
    id: 'crew-fc1239e9-1785535044228',
    type: 'crewNode',
    data: {
      label: 'Gather News',
      crewName: 'Gather News',
      routerCondition: null,
      stateType: null,
      allTasks: [
        {
          id: '7ecdecd6',
          name: 'Gather News by Topic',
          description:
            'Research and collect current news related to a specified {topic} ' +
            'using specialized search capabilities.',
        },
      ],
      selectedTasks: [],
    },
  };

  it('finds the placeholder nested in the referenced crew task', () => {
    expect(deriveFlowInputs([crewNode], []).map((v) => v.name)).toEqual(['topic']);
  });

  it('derives nothing surprising when the flow has no conditions', () => {
    // logicType: 'ROUTER' on an edge without a condition is not an input.
    const edge = { data: { logicType: 'ROUTER', checkpoint: true } };
    expect(deriveFlowInputs([crewNode], [edge]).map((v) => v.name)).toEqual([
      'topic',
    ]);
  });
});
