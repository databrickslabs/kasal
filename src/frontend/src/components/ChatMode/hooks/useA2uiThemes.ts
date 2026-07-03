import { useEffect, useState } from 'react';
import { UIConfigService, UIConfig } from '../../../api/UIConfigService';
import type { Theme } from '../../Configuration/uiConfigShared';

/**
 * Per-workspace branding palettes (UIConfigurator style_json.themes), keyed by
 * deliverable type. These drive the shared A2UI renderer's deck theme + --a2-*
 * tokens when a surface renders inline in chat.
 *
 * Backed by the shared {@link UIConfigService} cache (one fetch per session across
 * all the inline surfaces) AND its change subscription: when an admin saves in the
 * UI Configurator, `updateConfig` refreshes the cache and notifies here, so open
 * chat surfaces re-theme immediately — no page reload. Returns null when the
 * workspace is unconfigured/disabled or the fetch fails (the renderer then uses
 * its built-in defaults).
 */
type ThemesMap = Record<string, Theme>;

/** Parse the palettes map out of a config, or null when disabled/unconfigured. */
function themesOf(cfg: UIConfig | null | undefined): ThemesMap | null {
  if (!cfg || !cfg.enabled || !cfg.style_json) return null;
  try {
    const style = JSON.parse(cfg.style_json) as { themes?: unknown };
    if (style && typeof style.themes === 'object' && style.themes) {
      return style.themes as ThemesMap;
    }
  } catch {
    /* malformed style_json — fall through to no themes */
  }
  return null;
}

export function useA2uiThemes(): ThemesMap | null {
  // Seed from the cache synchronously so a cache hit doesn't flash the defaults.
  const [themes, setThemes] = useState<ThemesMap | null>(() => themesOf(UIConfigService.peek()));
  useEffect(() => {
    let active = true;
    const apply = (cfg: UIConfig | null) => {
      if (active) setThemes(themesOf(cfg));
    };
    UIConfigService.getConfig()
      .then(apply)
      .catch(() => apply(null));
    // Re-apply whenever the config changes (admin saved in the Configurator).
    const unsubscribe = UIConfigService.subscribe(apply);
    return () => {
      active = false;
      unsubscribe();
    };
  }, []);
  return themes;
}
