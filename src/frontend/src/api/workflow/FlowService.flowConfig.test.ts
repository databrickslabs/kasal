/**
 * A saved flow must keep the whole config it was built with.
 *
 * Both save paths used to rebuild `flow_config` from an allow-list — id, name,
 * type, listeners, actions, startingPoints — and everything else was dropped in
 * transit. That silently discarded `routers` (a saved flow lost its routing),
 * `persistence`, and the `state` declaration.
 *
 * It went unnoticed for a long time because the chat path and JobExecutionService
 * REBUILD flow_config from nodes and edges before every run, so the flow still
 * behaved correctly at run time and only what was STORED was wrong. The symptom
 * that finally exposed it: turning on "hold a conversation", saving, and finding
 * `flow_config.state` still null.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import { FlowService } from './FlowService';
import { apiClient } from '../../config/api/ApiConfig';

vi.mock('../../config/api/ApiConfig', () => ({
  apiClient: { post: vi.fn(), put: vi.fn() },
}));

const CONFIG = {
  id: 'flow-1',
  name: 'Swiss News',
  type: 'default',
  listeners: [],
  actions: [],
  startingPoints: [],
  // The three that were being dropped:
  routers: [
    {
      name: 'router_0',
      listenTo: 'starting_point_0',
      routes: { found: [] },
      routeConditions: { found: 'state.get("has_results") == True' },
    },
  ],
  persistence: { enabled: true, level: 'flow' },
  state: {
    enabled: true,
    type: 'structured',
    conversational: true,
    model: { type: 'object', properties: { topic: { reducer: 'append' } } },
    initialValues: {},
  },
};

const payload = { name: 'Swiss News', crew_id: 'crew-1', nodes: [], edges: [], flowConfig: CONFIG };

beforeEach(() => {
  vi.clearAllMocks();
  (apiClient.post as ReturnType<typeof vi.fn>).mockResolvedValue({ data: { id: 'f1' } });
  (apiClient.put as ReturnType<typeof vi.fn>).mockResolvedValue({ data: { id: 'f1' } });
});

const sentConfig = (mock: ReturnType<typeof vi.fn>) =>
  (mock.mock.calls[0][1] as { flow_config: Record<string, unknown> }).flow_config;

describe('saveFlow keeps the whole config', () => {
  it('sends the routers', async () => {
    // Without these a saved flow has no routing at all, and only re-runs from a
    // path that rebuilds the config behave correctly.
    await FlowService.saveFlow(payload as never);

    expect(sentConfig(apiClient.post as ReturnType<typeof vi.fn>).routers).toEqual(
      CONFIG.routers,
    );
  });

  it('sends the state declaration', async () => {
    await FlowService.saveFlow(payload as never);

    expect(sentConfig(apiClient.post as ReturnType<typeof vi.fn>).state).toEqual(
      CONFIG.state,
    );
  });

  it('sends the persistence block', async () => {
    await FlowService.saveFlow(payload as never);

    expect(sentConfig(apiClient.post as ReturnType<typeof vi.fn>).persistence).toEqual(
      CONFIG.persistence,
    );
  });
});

describe('updateFlow keeps the whole config', () => {
  it('sends the routers', async () => {
    await FlowService.updateFlow('f1', payload as never);

    expect(sentConfig(apiClient.put as ReturnType<typeof vi.fn>).routers).toEqual(
      CONFIG.routers,
    );
  });

  it('sends the state declaration', async () => {
    // The exact failure seen in the product: toggle "hold a conversation",
    // save, and `flow_config.state` is still null in the database.
    await FlowService.updateFlow('f1', payload as never);

    expect(sentConfig(apiClient.put as ReturnType<typeof vi.fn>).state).toEqual(
      CONFIG.state,
    );
  });

  it('still normalises the fields it always normalised', async () => {
    // The spread must not have replaced the normalisation — it runs after it.
    await FlowService.updateFlow('f1', payload as never);

    const sent = sentConfig(apiClient.put as ReturnType<typeof vi.fn>);
    expect(sent.name).toBe('Swiss News');
    expect(sent.type).toBe('default');
    expect(Array.isArray(sent.listeners)).toBe(true);
  });
});
