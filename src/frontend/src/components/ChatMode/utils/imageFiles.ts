/**
 * Images in the composer: which files count as one, and how big they are.
 *
 * Mirrors the backend's allow-list (services/assets): SVG is deliberately not
 * an image here — it is a script vector, and the only thing between it and
 * the app would be the renderer's sandbox.
 */
export const IMAGE_MIME = new Set(['image/png', 'image/jpeg', 'image/webp', 'image/gif']);

const MEASURE_TIMEOUT_MS = 1200;

export function isImageFile(file: File): boolean {
  return IMAGE_MIME.has((file.type || '').toLowerCase());
}

/**
 * Pixel size of an image file, measured in the browser (the model is told
 * the size so it can lay the image out). `{0, 0}` when it cannot be measured —
 * the upload still goes through.
 */
export async function measureImage(file: File): Promise<{ width: number; height: number }> {
  try {
    if (typeof createImageBitmap === 'function') {
      const bmp = await createImageBitmap(file);
      const size = { width: bmp.width, height: bmp.height };
      bmp.close?.();
      return size;
    }
  } catch {
    // fall through to the <img> route
  }
  try {
    const url = URL.createObjectURL(file);
    try {
      return await new Promise((resolve, reject) => {
        const img = new Image();
        // A decoder that never answers (a corrupt file; jsdom) must not hold
        // the upload: settle on "unknown size" and let it through.
        const timer = setTimeout(() => resolve({ width: 0, height: 0 }), MEASURE_TIMEOUT_MS);
        img.onload = () => {
          clearTimeout(timer);
          resolve({ width: img.naturalWidth, height: img.naturalHeight });
        };
        img.onerror = () => {
          clearTimeout(timer);
          reject(new Error('decode failed'));
        };
        img.src = url;
      });
    } finally {
      URL.revokeObjectURL(url);
    }
  } catch {
    return { width: 0, height: 0 };
  }
}
