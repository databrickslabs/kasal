import { describe, it, expect } from 'vitest';
import { isChunkLoadError } from './chunkErrors';

describe('isChunkLoadError', () => {
  it.each([
    new TypeError('Failed to fetch dynamically imported module: https://example.com/assets/Foo-abc123.js'),
    new TypeError('error loading dynamically imported module'),
    new TypeError('Importing a module script failed.'),
    new Error('Unable to preload CSS for /assets/index-abc.css'),
    new Error('Loading chunk 42 failed.'),
    new Error('Loading CSS chunk vendor-x failed'),
    Object.assign(new Error('boom'), { name: 'ChunkLoadError' }),
    'Failed to fetch dynamically imported module',
  ])('recognises a failed chunk load: %s', (error) => {
    expect(isChunkLoadError(error)).toBe(true);
  });

  it.each([null, undefined, 0, new Error('Cannot read properties of undefined'), { message: 42 }, {}])(
    'does not treat other errors as chunk failures: %s',
    (error) => {
      expect(isChunkLoadError(error)).toBe(false);
    },
  );
});
