import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  fetchEnabledSkills,
  pickSelectedSkills,
  invalidateSkillsCache,
} from './skillSelection';

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
  list.mockResolvedValue(SKILLS);
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

describe('pickSelectedSkills', () => {
  it('removes unavailable choices without reordering selected names', () => {
    const skills = [...SKILLS, { name: 'second', enabled: true, global_enabled: false }];
    expect(pickSelectedSkills(['second', 'off', 'picked', 'global', 'gone'], skills))
      .toEqual(['second', 'picked']);
  });

  it('returns an empty selection when no skills are available', () => {
    expect(pickSelectedSkills(['gone'], [])).toEqual([]);
  });
});
