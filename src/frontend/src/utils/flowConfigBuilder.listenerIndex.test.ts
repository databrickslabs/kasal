import { describe, it, expect } from 'vitest';
import { crewGroupIndex, buildFlowConfiguration } from './flowConfigBuilder';
import { Listener } from '../types/workflow/flow';

const listener = (crewId: string, conditionType = 'NONE'): Listener =>
  ({
    id: `listener-${crewId}`,
    name: crewId,
    crewId,
    crewName: crewId,
    listenToTaskIds: [],
    listenToTaskNames: [],
    tasks: [],
    state: { stateType: 'unstructured', stateDefinition: '', stateData: {} },
    conditionType,
  }) as unknown as Listener;

describe('crewGroupIndex', () => {
  it('mirrors the backend: one index per CREW, not per edge', () => {
    // "Send an Email" has two incoming edges (a merge group), so it appears
    // twice. The backend still creates only listener_0 and listener_1.
    const listeners = [listener('email'), listener('email'), listener('classify')];

    expect(crewGroupIndex(listeners, 'email')).toBe(0);
    expect(crewGroupIndex(listeners, 'classify')).toBe(1);
  });

  it('is the raw index only when no crew repeats', () => {
    const listeners = [listener('a'), listener('b'), listener('c')];

    expect(crewGroupIndex(listeners, 'c')).toBe(2);
  });

  it('skips ROUTER-typed listeners, as the backend does', () => {
    const listeners = [listener('a'), listener('r', 'ROUTER'), listener('b')];

    expect(crewGroupIndex(listeners, 'b')).toBe(1);
  });

  it('skips entries with no crewId', () => {
    const listeners = [listener(''), listener('a')];

    expect(crewGroupIndex(listeners, 'a')).toBe(0);
  });

  it('returns -1 for a crew that has no listener', () => {
    expect(crewGroupIndex([listener('a')], 'nope')).toBe(-1);
  });

  it('takes the FIRST appearance when a crew repeats late', () => {
    const listeners = [listener('a'), listener('b'), listener('a')];

    expect(crewGroupIndex(listeners, 'b')).toBe(1);
  });
});

describe('the router points at a listener that will exist', () => {
  it('regression: a merge group must not shift the router off the end', () => {
    // Reproduces execution afc87a6e. Two presentation crews both feed "Send an
    // Email", so the email node has two incoming edges. Classify sits at raw
    // index 2 but is the backend's listener_1.
    const node = (id: string, crewId: string, taskId: string) => ({
      id,
      type: 'crewNode',
      position: { x: 0, y: 0 },
      data: {
        crewId,
        crewName: crewId,
        label: crewId,
        allTasks: [{ id: taskId, name: taskId }],
      },
    });

    // Node order matters: listeners are built per node, so the email crew —
    // which has two incoming edges — lands at raw indices 0 and 1, pushing
    // classify to raw index 2. This is the order the real canvas produced.
    const nodes = [
      node('n-email', 'email', 't-email'),
      node('n-news', 'news', 't-news'),
      node('n-classify', 'classify', 't-classify'),
      node('n-politics', 'politics', 't-politics'),
      node('n-sports', 'sports', 't-sports'),
    ] as never[];

    const edge = (id: string, source: string, target: string, listenTo: string[], targets: string[], logicType: string) => ({
      id,
      source,
      target,
      data: { listenToTaskIds: listenTo, targetTaskIds: targets, logicType },
    });

    const edges = [
      edge('e1', 'n-news', 'n-classify', ['t-news'], ['t-classify'], 'NONE'),
      edge('e2', 'n-classify', 'n-politics', ['t-classify'], ['t-politics'], 'ROUTER'),
      edge('e3', 'n-classify', 'n-sports', ['t-classify'], ['t-sports'], 'ROUTER'),
      // the merge group: two edges into the same crew
      edge('e4', 'n-politics', 'n-email', ['t-politics'], ['t-email'], 'AND'),
      edge('e5', 'n-sports', 'n-email', ['t-sports'], ['t-email'], 'AND'),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    // The raw array still has the email crew twice — that is the backend's input.
    expect(config.listeners.map((l) => l.crewId)).toEqual([
      'email',
      'email',
      'classify',
    ]);

    // ...but the router must name the GROUPED index, which is what exists.
    expect(config.routers[0].listenTo).toBe('listener_1');
  });
});

describe('the router points at a starting point that will exist', () => {
  it('regression: a start crew with two tasks must not shift the index', () => {
    // The backend names ONE starting_point per CREW (flow_processors.py:103-123),
    // but the frontend builds one entry per TASK. A start crew contributing two
    // tasks used to push the next crew to starting_point_2 for a flow whose
    // backend created only starting_point_0 and starting_point_1.
    const startNode = (id: string, crewId: string, taskIds: string[]) => ({
      id,
      type: 'crewNode',
      position: { x: 0, y: 0 },
      data: {
        crewId,
        crewName: crewId,
        label: crewId,
        allTasks: taskIds.map((t) => ({ id: t, name: t })),
      },
    });

    const nodes = [
      startNode('n-a', 'crew-a', ['t-a1', 't-a2']),
      startNode('n-b', 'crew-b', ['t-b1']),
      startNode('n-target', 'target', ['t-target']),
    ] as never[];

    const edges = [
      // crew-a is a start point contributing TWO selected tasks
      { id: 'e1', source: 'n-a', target: 'n-target',
        data: { listenToTaskIds: ['t-a1', 't-a2'], targetTaskIds: ['t-target'], logicType: 'NONE' } },
      // crew-b is the second start crew, and the router listens to it
      { id: 'e2', source: 'n-b', target: 'n-target',
        data: { listenToTaskIds: ['t-b1'], targetTaskIds: ['t-target'], logicType: 'ROUTER',
                routerCondition: 'state.get("x", "") == "y"' } },
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    // Three raw starting points (two from crew-a), but only two backend methods.
    expect(config.startingPoints.map((sp) => sp.crewId)).toEqual([
      'crew-a', 'crew-a', 'crew-b',
    ]);
    // crew-b is the backend's starting_point_1, not _2.
    expect(config.routers[0].listenTo).toBe('starting_point_1');
  });
});
