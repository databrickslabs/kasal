import { useA2uiThemes } from './useA2uiThemes';
import type { WorkspaceThemes } from '../../Configuration/uiConfigShared';

/**
 * The workspace UI-Configurator palettes (style_json.themes), keyed by deliverable
 * type. A thin alias over {@link useA2uiThemes} so the run-activity log console and
 * the chat surfaces read from ONE cached, change-subscribed source — they can never
 * drift, and both re-theme instantly when an admin saves (no page reload). Stays
 * null when the config is disabled, has no themes, or the fetch fails — then the
 * built-in fallback theme is used as before.
 */
export function useWorkspaceThemes(): WorkspaceThemes | null {
  return useA2uiThemes() as WorkspaceThemes | null;
}
