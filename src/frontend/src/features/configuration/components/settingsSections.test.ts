import { describe, expect, it } from 'vitest';
import { getSettingsSections, settingsSections } from './settingsSections';
const operator = { isSystemAdmin: false, isWorkspaceAdmin: false, isEditor: false };
describe('settings navigation permissions', () => {
  it('gives operators personal preferences only', () => {
    expect(getSettingsSections('personal', operator).map(item => item.id)).toEqual(['general']);
    expect(getSettingsSections('workspace', operator)).toEqual([]);
    expect(getSettingsSections('system', operator)).toEqual([]);
  });
  it('retains editor access to prompts, keys and saved objects', () => {
    expect(getSettingsSections('workspace', { ...operator, isEditor: true }).map(item => item.id)).toEqual(['prompts', 'event-triggers', 'api-keys', 'objects']);
  });
  it('keeps teamspace administration separate from system administration', () => {
    const admin = { ...operator, isWorkspaceAdmin: true };
    expect(getSettingsSections('workspace', admin).map(item => item.id)).toContain('models');
    expect(getSettingsSections('system', admin)).toEqual([]);
    const system = getSettingsSections('system', { ...admin, isSystemAdmin: true }).map(item => item.id);
    expect(system).toEqual(['models', 'tools', 'mcp', 'remote-agents', 'access', 'engines', 'database']);
    expect(system).not.toContain('api-keys');
  });
  it('uses one unique stable ID per section across scopes', () => {
    expect(new Set(settingsSections.map(item => item.id)).size).toBe(settingsSections.length);
  });
});
