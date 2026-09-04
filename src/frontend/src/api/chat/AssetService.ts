import { apiClient } from '../../config/api/ApiConfig';

const BASE = '/chat/assets';

/** An image attached in the chat, as the backend describes it (no bytes). */
export interface ChatAsset {
  id: string;
  name: string;
  mime: string;
  size: number;
  width?: number | null;
  height?: number | null;
  session_id?: string | null;
  /** How HTML refers to it: `<img src="asset:<id>">`. */
  ref: string;
}

/** The reference the model writes into HTML; resolved when rendering. */
export const assetRef = (id: string): string => `asset:${id}`;

// One fetch per asset per page: the bytes are immutable by id, and a deck
// re-renders its frames many times.
const dataUrls = new Map<string, Promise<string>>();

export const AssetService = {
  /** Store an image; `width`/`height` are what the browser measured. */
  async upload(
    file: File,
    opts: { sessionId?: string | null; width?: number; height?: number } = {},
  ): Promise<ChatAsset> {
    const form = new FormData();
    form.append('file', file, file.name);
    form.append('session_id', opts.sessionId || '');
    form.append('width', String(opts.width || 0));
    form.append('height', String(opts.height || 0));
    const { data } = await apiClient.post<ChatAsset>(BASE, form, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return data;
  },

  async delete(id: string): Promise<void> {
    await apiClient.delete(`${BASE}/${id}`);
  },

  /**
   * The image as a data URL, fetched through the API client (so the request
   * carries the app's auth like any other) and cached. A data URL is what a
   * sandboxed frame can show: it has no origin to send cookies from, and its
   * CSP allows `img-src data:`.
   */
  dataUrl(id: string): Promise<string> {
    let pending = dataUrls.get(id);
    if (!pending) {
      pending = apiClient
        .get<Blob>(`${BASE}/${id}`, { responseType: 'blob' })
        .then(
          ({ data }) =>
            new Promise<string>((resolve, reject) => {
              const reader = new FileReader();
              reader.onload = () => resolve(String(reader.result));
              reader.onerror = () => reject(reader.error);
              reader.readAsDataURL(data);
            }),
        );
      // A failed fetch must not poison the cache: the next render retries.
      pending.catch(() => dataUrls.delete(id));
      dataUrls.set(id, pending);
    }
    return pending;
  },

  /** Test hook: forget cached bytes. */
  _resetCache(): void {
    dataUrls.clear();
  },
};

export default AssetService;
