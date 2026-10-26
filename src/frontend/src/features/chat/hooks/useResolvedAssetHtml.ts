import { useEffect, useMemo, useState } from 'react';
import { AssetService } from '../../../api/chat/AssetService';
import { findAssetIds, substituteAssetRefs } from '../utils/assetRefs';

/**
 * `html` with its `asset:<id>` references resolved to data URLs, for a frame
 * to render. Returns the input unchanged when it carries no references (the
 * common case, at no cost); otherwise the loading pixel stands in for each
 * image until its bytes arrive, then the html re-renders with them.
 */
export function useResolvedAssetHtml(html: string): string {
  const ids = useMemo(() => findAssetIds(html), [html]);
  const [urls, setUrls] = useState<Record<string, string>>({});
  useEffect(() => {
    let cancelled = false;
    const missing = ids.filter((id) => !urls[id]);
    if (missing.length === 0) return;
    for (const id of missing) {
      AssetService.dataUrl(id).then(
        (url) => {
          if (!cancelled) setUrls((u) => (u[id] ? u : { ...u, [id]: url }));
        },
        () => {
          // Left unresolved: the loading pixel stays, the deck still pages.
        },
      );
    }
    return () => {
      cancelled = true;
    };
    // `urls` is read for the diff but must not retrigger the effect.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [ids]);
  return useMemo(() => (ids.length ? substituteAssetRefs(html, urls) : html), [html, ids, urls]);
}

/** One asset's data URL, or null until it arrives. */
export function useAssetDataUrl(id: string | undefined): string | null {
  const [url, setUrl] = useState<string | null>(null);
  useEffect(() => {
    if (!id) return;
    let cancelled = false;
    AssetService.dataUrl(id).then(
      (u) => {
        if (!cancelled) setUrl(u);
      },
      () => {},
    );
    return () => {
      cancelled = true;
    };
  }, [id]);
  return url;
}
