import { describe, it, expect } from 'vitest';
import { flowsContainingCrew, FlowTabLike } from './flowsContainingCrew';

const flowTab = (
  id: string,
  crewIds: Array<string | number>,
  extra: Partial<FlowTabLike> = {},
): FlowTabLike => ({
  id,
  name: id,
  lastModified: new Date(0),
  flowNodes: crewIds.map((crewId, i) => ({
    type: 'crewNode',
    data: { crewId },
    id: `n${i}`,
  })),
  ...extra,
});

describe('flowsContainingCrew', () => {
  it('finds the flow whose crew node names this crew', () => {
    expect(
      flowsContainingCrew([flowTab('t1', ['7', '9'])], '7')
    ).toEqual([{ tabId: 't1', name: 't1' }]);
  });

  it('compares ids as strings, since a crew id arrives as either', () => {
    expect(flowsContainingCrew([flowTab('t1', [7])], '7')).toHaveLength(1);
    expect(flowsContainingCrew([flowTab('t1', ['7'])], 7)).toHaveLength(1);
  });

  it('prefers the saved flow name over the tab name', () => {
    expect(
      flowsContainingCrew([flowTab('t1', ['7'], { savedFlowName: 'Top News' })], '7')
    ).toEqual([{ tabId: 't1', name: 'Top News' }]);
  });

  it('returns every flow using the crew, most recently touched first', () => {
    const tabs = [
      flowTab('older', ['7'], { lastModified: new Date(1000) }),
      flowTab('newer', ['7'], { lastModified: new Date(9000) }),
    ];

    expect(flowsContainingCrew(tabs, '7').map((f) => f.tabId)).toEqual([
      'newer',
      'older',
    ]);
  });

  it('ignores flows that do not use the crew', () => {
    expect(flowsContainingCrew([flowTab('t1', ['8'])], '7')).toEqual([]);
  });

  it('ignores nodes that are not crew nodes', () => {
    const tab: FlowTabLike = {
      id: 't1',
      name: 't1',
      flowNodes: [{ type: 'agentNode', data: { crewId: '7' } }],
    };

    expect(flowsContainingCrew([tab], '7')).toEqual([]);
  });

  it('says nothing for a crew that was never saved', () => {
    const tabs = [flowTab('t1', ['7'])];

    expect(flowsContainingCrew(tabs, undefined)).toEqual([]);
    expect(flowsContainingCrew(tabs, null)).toEqual([]);
    expect(flowsContainingCrew(tabs, '')).toEqual([]);
  });

  it('does not need the tab to be showing its flow canvas', () => {
    // A tab holds both canvases. Hiding the link while the user looks at the
    // crew side is exactly when they want it.
    const tab = { ...flowTab('t1', ['7']), viewMode: 'crew' } as FlowTabLike;

    expect(flowsContainingCrew([tab], '7')).toHaveLength(1);
  });

  it('tolerates a tab with no flow nodes', () => {
    expect(flowsContainingCrew([{ id: 't1', name: 't1' }], '7')).toEqual([]);
  });
});
