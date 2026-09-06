import { describe, expect, it } from 'vitest';
import { applyResultTransform, dropResultTransform, registerResultTransform } from './resultTransforms';

describe('resultTransforms', () => {
  it('applies a registered transform exactly once', () => {
    registerResultTransform('j1', (t) => `[${t}]`);
    expect(applyResultTransform('j1', 'x')).toBe('[x]');
    expect(applyResultTransform('j1', 'x')).toBe('x');
  });

  it('leaves unknown jobs, no job, and dropped transforms alone', () => {
    expect(applyResultTransform('nope', 'x')).toBe('x');
    expect(applyResultTransform(undefined, 'x')).toBe('x');
    registerResultTransform('j2', () => 'never');
    dropResultTransform('j2');
    expect(applyResultTransform('j2', 'x')).toBe('x');
  });

  it('a throwing transform costs nothing', () => {
    registerResultTransform('j3', () => {
      throw new Error('boom');
    });
    expect(applyResultTransform('j3', 'x')).toBe('x');
  });
});
