import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  fetchEnabledSkills,
  invalidateSkillsCache,
  reconcileSelectedSkills,
} from './skillSelection';
import { useExecutionStore } from '../store/executionStore';

const list = vi.fn();
vi.mock('../../../api/tools/SkillService', () => ({
  SkillService: { list: (...a: unknown[]) => list(...a) },
}));

const SKILLS = [
  { id: 1, name: 'picked', description: '', enabled: true, global_enabled: false },
  { id: 2, name: 'global', description: '', enabled: true, global_enabled: true },
  { id: 3, name: 'off', description: '', enabled: false, global_enabled: false },
];

beforeEach(() => {
  vi.clearAllMocks();
  invalidateSkillsCache();
  useExecutionStore.setState({ selectedSkills: [] });
  list.mockResolvedValue(SKILLS);
});

describe('reconcileSelectedSkills', () => {
  it('makes no request when nothing is selected', async () => {
    expect(await reconcileSelectedSkills()).toEqual([]);
    expect(list).not.toHaveBeenCalled();
  });

  it('prunes gone, disabled and always-on names from the selection', async () => {
    useExecutionStore.setState({ selectedSkills: ['picked', 'gone', 'off', 'global'] });
    expect(await reconcileSelectedSkills()).toEqual(['picked']);
    expect(useExecutionStore.getState().selectedSkills).toEqual(['picked']);
  });

  it('keeps the selection untouched when the fetch fails', async () => {
    list.mockRejectedValue(new Error('down'));
    useExecutionStore.setState({ selectedSkills: ['picked', 'gone'] });
    expect(await reconcileSelectedSkills()).toEqual(['picked', 'gone']);
    expect(useExecutionStore.getState().selectedSkills).toEqual(['picked', 'gone']);
  });
});

describe('fetchEnabledSkills', () => {
  it('filters to enabled and caches within the TTL', async () => {
    const first = await fetchEnabledSkills();
    expect(first.map((s) => s.name)).toEqual(['picked', 'global']);
    await fetchEnabledSkills();
    expect(list).toHaveBeenCalledTimes(1);
    await fetchEnabledSkills(true); // force busts the cache
    expect(list).toHaveBeenCalledTimes(2);
  });
});
