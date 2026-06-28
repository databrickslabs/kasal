import { useEffect, useState } from 'react';
import { UIConfigService } from '../../../api/UIConfigService';
import type { Theme } from '../../Configuration/uiConfigShared';

/**
 * Per-workspace branding palettes (UIConfigurator style_json.themes), keyed by
 * deliverable type. These drive the shared A2UI renderer's deck theme + --a2-*
 * tokens when a surface renders inline in chat.
 *
 * Fetched ONCE per session (module-level cache) so the many inline surfaces don't
 * each hit /ui-config. Returns null when the workspace is unconfigured/disabled or
 * the fetch fails — the renderer then uses its built-in defaults.
 */
type ThemesMap = Record<string, Theme>;

let _cache: ThemesMap | null | undefined; // undefined = not fetched yet
let _inflight: Promise<ThemesMap | null> | null = null;

function fetchThemes(): Promise<ThemesMap | null> {
  if (!_inflight) {
    _inflight = UIConfigService.getConfig()
      .then((cfg) => {
        if (!cfg.enabled || !cfg.style_json) {
          _cache = null;
          return null;
        }
        try {
          const style = JSON.parse(cfg.style_json) as { themes?: unknown };
          if (style && typeof style.themes === 'object' && style.themes) {
            _cache = style.themes as ThemesMap;
            return _cache;
          }
        } catch {
          /* malformed style_json — fall through to no themes */
        }
        _cache = null;
        return null;
      })
      .catch(() => {
        _cache = null;
        return null;
      });
  }
  return _inflight;
}

export function useA2uiThemes(): ThemesMap | null {
  const [themes, setThemes] = useState<ThemesMap | null>(_cache ?? null);
  useEffect(() => {
    let active = true;
    if (_cache !== undefined) {
      setThemes(_cache);
      return;
    }
    fetchThemes().then((t) => {
      if (active) setThemes(t);
    });
    return () => {
      active = false;
    };
  }, []);
  return themes;
}
