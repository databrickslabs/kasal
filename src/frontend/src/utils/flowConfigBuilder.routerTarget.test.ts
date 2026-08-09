import { describe, it, expect } from 'vitest';
import { buildFlowConfiguration } from './flowConfigBuilder';

/**
 * A router says which crew it waits for by IDENTITY.
 *
 * It used to say it by generated method name — `listener_2` — which meant the
 * frontend had to predict a name only the backend knows. It predicted by
 * indexing its own arrays, and those hold one entry per EDGE (listeners) and
 * per TASK (starting points) while the backend names one method per CREW. So
 * any crew appearing twice shifted every later index, the router named a method
 * that was never created, it never fired, and the run reported COMPLETED having
 * done half its work.
 *
 * These are the two shapes that broke it. Neither can break now, because
 * nothing here derives a name.
 */
const node = (id: string, crewId: string, taskIds: string[]) => ({
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

const edge = (
  id: string,
  source: string,
  target: string,
  listenTo: string[],
  targets: string[],
  logicType: string,
  extra: Record<string, unknown> = {}
) => ({
  id,
  source,
  target,
  data: { listenToTaskIds: listenTo, targetTaskIds: targets, logicType, ...extra },
});

describe('a router names the crew it waits for', () => {
  it('is unaffected by a crew having several incoming edges', () => {
    // Reproduces execution afc87a6e: Politics and Sports both feed the email
    // crew, so it occupies two slots and pushed the classify crew to index 2.
    const nodes = [
      node('n-email', 'email', ['t-email']),
      node('n-news', 'news', ['t-news']),
      node('n-classify', 'classify', ['t-classify']),
      node('n-politics', 'politics', ['t-politics']),
      node('n-sports', 'sports', ['t-sports']),
    ] as never[];

    const edges = [
      edge('e1', 'n-news', 'n-classify', ['t-news'], ['t-classify'], 'NONE'),
      edge('e2', 'n-classify', 'n-politics', ['t-classify'], ['t-politics'], 'ROUTER', {
        routerCondition: 'state.get("category", "") == "politics"',
      }),
      edge('e3', 'n-classify', 'n-sports', ['t-classify'], ['t-sports'], 'ROUTER', {
        routerCondition: 'state.get("category", "") == "sports"',
      }),
      // the merge: two edges into one crew
      edge('e4', 'n-politics', 'n-email', ['t-politics'], ['t-email'], 'AND'),
      edge('e5', 'n-sports', 'n-email', ['t-sports'], ['t-email'], 'AND'),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    // The raw array still holds the email crew twice — that is the backend's
    // input, and it is what used to shift the index.
    expect(config.listeners.map((l) => l.crewId)).toEqual([
      'email',
      'email',
      'classify',
    ]);
    expect(config.routers[0].listenToCrewId).toBe('classify');
  });

  it('is unaffected by a start crew contributing several tasks', () => {
    // The other half of the same defect: startingPoints holds one entry per
    // task, so a start crew with two tasks shifted the next crew's index.
    const nodes = [
      node('n-a', 'crew-a', ['t-a1', 't-a2']),
      node('n-b', 'crew-b', ['t-b1']),
      node('n-target', 'target', ['t-target']),
    ] as never[];

    const edges = [
      edge('e1', 'n-a', 'n-target', ['t-a1', 't-a2'], ['t-target'], 'NONE'),
      edge('e2', 'n-b', 'n-target', ['t-b1'], ['t-target'], 'ROUTER', {
        routerCondition: 'state.get("x", "") == "y"',
      }),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    expect(config.startingPoints.map((sp) => sp.crewId)).toEqual([
      'crew-a',
      'crew-a',
      'crew-b',
    ]);
    expect(config.routers[0].listenToCrewId).toBe('crew-b');
  });

  it('emits no generated method name at all', () => {
    const nodes = [
      node('n-a', 'crew-a', ['t-a']),
      node('n-b', 'crew-b', ['t-b']),
    ] as never[];
    const edges = [
      edge('e1', 'n-a', 'n-b', ['t-a'], ['t-b'], 'ROUTER', {
        routerCondition: 'state.get("x", "") == "y"',
      }),
    ] as never[];

    const router = buildFlowConfiguration(nodes, edges, 'Dynamic Flow').routers[0];

    expect(JSON.stringify(router)).not.toMatch(/listener_\d|starting_point_\d/);
  });
});
