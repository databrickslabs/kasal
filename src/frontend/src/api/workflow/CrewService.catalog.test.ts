import { beforeEach, describe, expect, it, vi } from 'vitest';
import { CrewService } from './CrewService';
import type { CrewSaveData } from '../../types/workflow/crew';

const api = vi.hoisted(() => ({ post: vi.fn(), put: vi.fn() }));
vi.mock('../../shared/api/client', () => ({ apiClient: api }));

beforeEach(() => {
  vi.clearAllMocks();
  Object.values(api).forEach(method => method.mockResolvedValue({ data: { id: 'catalog-42', name: 'News' } }));
});

describe('Catalog request normalization', () => {
  it.each([false, true])('does not mutate the canvas while saving (update=%s)', async update => {
    const data = Object.freeze({ label: 'Researcher', goal: 'Edited instructions', tools: [] });
    const crew: CrewSaveData = {
      name: 'News', agent_ids: ['42'], task_ids: [], edges: [],
      nodes: [{ id: 'agent-42', type: 'agentNode', position: { x: 0, y: 0 }, data }],
    };
    const before = JSON.stringify(crew);
    if (update) await CrewService.updateCrew('catalog-42', crew);
    else await CrewService.saveCrew(crew);
    expect(JSON.stringify(crew)).toBe(before);
    expect(update ? api.put : api.post).toHaveBeenCalledWith(update ? '/crews/catalog-42' : '/crews', expect.objectContaining({
      nodes: expect.arrayContaining([expect.objectContaining({ data: expect.objectContaining({
        goal: 'Edited instructions', context: [], async_execution: 'false',
      }) })]),
    }));
  });
});

describe('Clone (Save as new crew)', () => {
  it('cloneCrew POSTs to the deep-clone endpoint with the new name', async () => {
    await CrewService.cloneCrew('catalog-42', 'My Copy');
    expect(api.post).toHaveBeenCalledWith('/crews/catalog-42/clone', { name: 'My Copy' });
  });

  it('cloneCrew sends null when no name is given (backend defaults to "… (copy)")', async () => {
    await CrewService.cloneCrew('catalog-42');
    expect(api.post).toHaveBeenCalledWith('/crews/catalog-42/clone', { name: null });
  });

  it('duplicateCrew routes through the clone endpoint (independent copy, not a shared-row re-POST)', async () => {
    await CrewService.duplicateCrew('catalog-42', 'Dup');
    expect(api.post).toHaveBeenCalledWith('/crews/catalog-42/clone', { name: 'Dup' });
    expect(api.post).not.toHaveBeenCalledWith('/crews', expect.anything());
  });
});
