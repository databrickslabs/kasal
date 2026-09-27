import { describe, expect, it } from 'vitest';
import { fallbackModelKey, resolveBuilderModel } from './modelFallback';

const models = [{ key: 'first' }, { key: 'server-default' }];

describe('fallbackModelKey', () => {
  it('prefers the server default when it is enabled', () => {
    expect(fallbackModelKey(models, 'server-default')).toBe('server-default');
  });
  it('else the first enabled model, else nothing', () => {
    expect(fallbackModelKey(models, 'not-enabled')).toBe('first');
    expect(fallbackModelKey([], 'server-default')).toBe('');
  });
});

describe('resolveBuilderModel', () => {
  it('keeps a model that is still enabled, even when it is not the default', () => {
    expect(resolveBuilderModel('first', models, 'server-default')).toBe('first');
  });
  it('falls back from a model that is no longer enabled', () => {
    expect(resolveBuilderModel('gone', models, 'server-default')).toBe('server-default');
    expect(resolveBuilderModel('gone', [{ key: 'only' }], 'server-default')).toBe('only');
  });
  it('fills an empty selection', () => {
    expect(resolveBuilderModel('', models, 'server-default')).toBe('server-default');
  });
  it('changes nothing while the list is unknown', () => {
    expect(resolveBuilderModel('gone', [], 'server-default')).toBe('gone');
  });
});
