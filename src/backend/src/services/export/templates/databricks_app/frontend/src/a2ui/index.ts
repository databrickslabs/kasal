/**
 * Shared A2UI renderer — the SAME renderer used by the exported Databricks app
 * (the exporter copies this whole `shared/a2ui` tree verbatim). It draws a
 * declarative A2UI Surface (`{surfaceKind, root, components[], dataModel}`) the
 * backend composer produces, both inline in the chat and in the preview pane.
 *
 * The module is self-contained (its own `lib/` + `ui/`, relative imports only) so
 * it relocates into an export without an `@/` alias. The Tailwind design tokens
 * the shadcn `ui/` primitives consume are provided by each host app's build
 * (live: `tailwind.config.js` + `chat.css` `--a2-*` vars; export: its v4 CSS).
 */
export { A2UIRenderer } from './A2UIRenderer';
export { SurfaceChromeContext } from './lib/surfaceContext';
export {
  DeckThemeContext,
  getDeckTheme,
  DECK_THEMES,
  DEFAULT_DECK_THEME_ID,
  themeToDeck,
  themeToTokens,
} from './lib/deckThemes';
export type { Surface, ComponentNode, NodeProps } from './types';
export type { DeckTheme, Palette } from './lib/deckThemes';
