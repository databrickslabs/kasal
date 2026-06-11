import { useEffect, useState } from 'react';
import { UIConfigService } from '../../../api/UIConfigService';
import { WorkspaceThemes } from '../utils/uiDocument';

/**
 * The workspace UI-Configurator palettes (style_json.themes), fetched on mount.
 * A surface theme is re-resolved against these via applyConfiguredTheme — the
 * configurator is the source of truth; the agent-embedded theme is only a
 * fallback (models routinely stamp the wrong palette). Stays null when the
 * config is disabled, has no themes, or the fetch fails — then the embedded
 * theme is used as before.
 */
export function useWorkspaceThemes(): WorkspaceThemes | null {
  const [themes, setThemes] = useState<WorkspaceThemes | null>(null);
  useEffect(() => {
    let cancelled = false;
    UIConfigService.getConfig()
      .then((cfg) => {
        if (cancelled || !cfg.enabled || !cfg.style_json) return;
        try {
          const style = JSON.parse(cfg.style_json) as { themes?: unknown };
          if (style && typeof style.themes === 'object' && style.themes) {
            setThemes(style.themes as WorkspaceThemes);
          }
        } catch {
          /* malformed style_json — keep the embedded theme */
        }
      })
      .catch(() => {
        /* config unavailable — keep the embedded theme */
      });
    return () => {
      cancelled = true;
    };
  }, []);
  return themes;
}
