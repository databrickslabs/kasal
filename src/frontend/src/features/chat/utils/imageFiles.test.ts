import { describe, expect, it } from 'vitest';
import { isImageFile, measureImage } from './imageFiles';

describe('imageFiles', () => {
  it('knows an image from a document, and never an SVG', () => {
    expect(isImageFile(new File([''], 'a.png', { type: 'image/png' }))).toBe(true);
    expect(isImageFile(new File([''], 'a.svg', { type: 'image/svg+xml' }))).toBe(false);
    expect(isImageFile(new File([''], 'a.pdf', { type: 'application/pdf' }))).toBe(false);
  });

  it('measures, or settles on 0×0 where the browser cannot decode', async () => {
    const size = await measureImage(new File(['x'], 'a.png', { type: 'image/png' }));
    expect(size).toEqual({ width: expect.any(Number), height: expect.any(Number) });
  }, 3000);
});
