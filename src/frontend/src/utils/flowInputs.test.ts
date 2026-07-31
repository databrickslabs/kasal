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
