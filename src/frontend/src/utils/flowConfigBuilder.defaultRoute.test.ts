import { describe, it, expect } from 'vitest';
import { buildFlowConfiguration, DEFAULT_ROUTE_NAME } from './flowConfigBuilder';

/**
 * The engine has always honoured a route named exactly `default` — it takes it
 * when no condition matched, instead of stopping the flow. But every route name
 * was auto-generated as `route_to_<crew>`, so that name was unreachable and the
 * fallback was dead code: a batch matching nothing just ended the run.
 */
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

const routerEdge = (
  id: string,
  source: string,
  target: string,
  listenTo: string[],
  targets: string[],
  extra: Record<string, unknown> = {}
) => ({
  id,
  source,
  target,
  data: { listenToTaskIds: listenTo, targetTaskIds: targets, logicType: 'ROUTER', ...extra },
});

const nodes = [
  node('n-classify', 'classify', 't-classify'),
  node('n-politics', 'politics', 't-politics'),
  node('n-other', 'other', 't-other'),
] as never[];

describe('default route', () => {
  it('names a marked edge `default` so the engine recognises it', () => {
    const edges = [
      routerEdge('e1', 'n-classify', 'n-politics', ['t-classify'], ['t-politics'], {
        routerCondition: 'state.get("category", "") == "politics"',
      }),
      routerEdge('e2', 'n-classify', 'n-other', ['t-classify'], ['t-other'], {
        isDefaultRoute: true,
      }),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');
    const router = config.routers[0];

    expect(Object.keys(router.routes).sort()).toEqual([
      DEFAULT_ROUTE_NAME,
      'route_to_politics',
    ]);
  });

  it('gives the default route no condition of its own', () => {
    const edges = [
      routerEdge('e1', 'n-classify', 'n-politics', ['t-classify'], ['t-politics'], {
        routerCondition: 'state.get("category", "") == "politics"',
      }),
      routerEdge('e2', 'n-classify', 'n-other', ['t-classify'], ['t-other'], {
        isDefaultRoute: true,
        // Even if one is stored, it must not be emitted: the fallback runs
        // precisely when every other condition was false.
        routerCondition: 'state.get("category", "") == "anything"',
      }),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');
    const router = config.routers[0];

    expect(router.routeConditions[DEFAULT_ROUTE_NAME]).toBeUndefined();
    expect(router.routeConditions['route_to_politics']).toBe(
      'state.get("category", "") == "politics"'
    );
  });

  it('still points the default route at its target crew tasks', () => {
    const edges = [
      routerEdge('e2', 'n-classify', 'n-other', ['t-classify'], ['t-other'], {
        isDefaultRoute: true,
      }),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    expect(config.routers[0].routes[DEFAULT_ROUTE_NAME]).toEqual([
      { id: 't-other', crewId: 'other', crewName: 'other' },
    ]);
  });

  it('is unchanged when no edge is marked', () => {
    const edges = [
      routerEdge('e1', 'n-classify', 'n-politics', ['t-classify'], ['t-politics'], {
        routerCondition: 'state.get("category", "") == "politics"',
      }),
    ] as never[];

    const config = buildFlowConfiguration(nodes, edges, 'Dynamic Flow');

    expect(Object.keys(config.routers[0].routes)).toEqual(['route_to_politics']);
    expect(config.routers[0].routes[DEFAULT_ROUTE_NAME]).toBeUndefined();
  });
});
