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
