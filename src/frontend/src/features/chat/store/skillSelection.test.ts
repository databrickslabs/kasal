import { reconcileSelectedSkills } from './skillSelection';
import { describe, it, expect, vi, beforeEach } from 'vitest';
import {
  invalidateSkillsCache,
} from '../utils/skillSelection';
import { useExecutionStore } from './executionStore';

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

  it('does not write state when every selected skill is still pickable', async () => {
    const setSelectedSkills = vi.spyOn(useExecutionStore.getState(), 'setSelectedSkills');
    useExecutionStore.setState({ selectedSkills: ['picked'] });
    expect(await reconcileSelectedSkills()).toEqual(['picked']);
    expect(setSelectedSkills).not.toHaveBeenCalled();
    setSelectedSkills.mockRestore();
  });

  it('keeps the selection untouched when the fetch fails', async () => {
    list.mockRejectedValue(new Error('down'));
    useExecutionStore.setState({ selectedSkills: ['picked', 'gone'] });
    expect(await reconcileSelectedSkills()).toEqual(['picked', 'gone']);
    expect(useExecutionStore.getState().selectedSkills).toEqual(['picked', 'gone']);
  });
});
