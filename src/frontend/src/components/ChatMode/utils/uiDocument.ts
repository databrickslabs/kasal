/**
 * A2UI (Agent-to-UI) parsing for the chat preview.
 *
 * Conforms to the A2UI v0.10 message protocol + "minimal" catalog
 * (Text, Row, Column, Button, TextField) from the Apache-2.0 project
 * google/A2UI (https://github.com/google/A2UI). See src/docs/THIRD_PARTY_NOTICES
 * for attribution. We render A2UI documents with Kasal's own React renderer +
 * design tokens so agent-produced UIs are brand-consistent instead of arbitrary
 * HTML.
 *
 * An A2UI document is a stream of messages; we only need two:
 *   - createSurface:   declares a surface + which catalog it uses
 *   - updateComponents: a FLAT list of components (id + type + props); children
 *                       are referenced by id, so the tree is reconstructed here.
 */

/**
 * Components Kasal's renderer supports. A superset of the A2UI "minimal"
 * catalog plus common "basic"-catalog members (Card, List, Image, Divider,
 * Icon, CheckBox, Slider) and a Kasal "Badge" for KPI/status pills — enough to
 * compose real dashboards, cards and forms.
 */
export type UiComponentType =
  | 'Text'
  | 'Row'
  | 'Column'
  | 'Card'
  | 'List'
  | 'Divider'
  | 'Image'
  | 'Icon'
  | 'Badge'
  | 'Button'
  | 'TextField'
  | 'CheckBox'
  | 'Slider'
  | 'ChoicePicker'
  | 'Dashboard'
  | 'Stat'
  | 'Chart'
  | 'Table'
  | 'Quiz'
  | 'Slides'
  | 'Slide'
  | 'Album'
  | 'Mindmap';

/** The component names available in each catalog (for the configurator UI). */
export const CATALOG_COMPONENTS: Record<string, UiComponentType[]> = {
  minimal: ['Text', 'Row', 'Column', 'Button', 'TextField'],
  basic: [
    'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
    'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
    'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide', 'Album', 'Mindmap',
  ],
};

export interface UiComponent {
  id: string;
  component: UiComponentType;
  // Minimal-catalog props (loosely typed; the renderer reads what it needs).
  text?: unknown;
  variant?: string;
  children?: string[];
  child?: string;
  label?: string;
  value?: unknown;
  justify?: string;
  align?: string;
  weight?: number;
  [key: string]: unknown;
}

/** Branding tokens carried on the surface, set by the agent from the workspace
 *  UI-Configurator palette. The renderer derives its stage / accent / text /
 *  surface colors and font from these; any omitted token falls back to the
 *  built-in premium dark theme, so an un-themed doc renders exactly as before. */
export interface UiTheme {
  accent?: string;
  background?: string; // stage background
  surface?: string; // card / panel background
  text?: string;
  heading?: string;
  muted?: string;
  font?: 'sans' | 'serif' | 'rounded' | 'mono';
  density?: 'comfortable' | 'compact';
}

export interface UiSurface {
  rootId: string;
  /** Components keyed by id for O(1) child resolution. */
  components: Record<string, UiComponent>;
  /** Data model for `{ path: "/x" }` bindings (best-effort; may be empty). */
  data: Record<string, unknown>;
  /** Branding tokens (accent/background/text/font…); undefined = built-in theme. */
  theme?: UiTheme;
}

const VALID_TYPES: ReadonlySet<string> = new Set<UiComponentType>([
  'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
  'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
  'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide', 'Album', 'Mindmap',
]);

/** Scan from the first `open` char to its balanced `close` (string-aware) and
 *  return the enclosed substring, or null. Used to pull a JSON object/array out
 *  of surrounding prose. */
function balancedBlock(text: string, open: string, close: string): string | null {
  // Callers only invoke this once they've found `open` in the text, so `start`
  // is always >= 0 here.
  const start = text.indexOf(open);
  let depth = 0;
  let inStr = false;
  let esc = false;
  for (let i = start; i < text.length; i++) {
    const ch = text[i];
    if (inStr) {
      if (esc) esc = false;
      else if (ch === '\\') esc = true;
      else if (ch === '"') inStr = false;
      continue;
    }
    if (ch === '"') inStr = true;
    else if (ch === open) depth++;
    else if (ch === close) {
      depth--;
      if (depth === 0) return text.slice(start, i + 1);
    }
  }
  return null;
}

/**
 * Read a JSON value that may be a raw object, a JSON string, a ```json fenced
 * block, or JSON EMBEDDED in surrounding prose. Agents frequently add a
 * preamble ("Here is the UI document: { … }") despite a "JSON only" instruction;
 * we still want to render the document instead of dumping raw text into the chat.
 */
function coerceJson(
  raw: string | Record<string, unknown>,
  depth = 0,
): Record<string, unknown> | null {
  if (raw && typeof raw === 'object') return raw as Record<string, unknown>;
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();

  // The value may be a JSON-ENCODED string rather than raw JSON — e.g. an
  // execution result is stored stringified ("\"{\\\"messages\\\": …}\""), so it
  // arrives as a quoted, backslash-escaped blob. Decode the outer string
  // layer(s) and re-attempt, so an over-encoded document still renders instead
  // of dumping escaped JSON into the chat. Depth-capped to avoid any loop.
  if (depth < 4 && trimmed.startsWith('"') && trimmed.endsWith('"')) {
    try {
      // A valid JSON document delimited by quotes is necessarily a string.
      return coerceJson(JSON.parse(trimmed) as string, depth + 1);
    } catch {
      /* not a JSON-encoded string; fall through to normal handling */
    }
  }

  // Tolerate a ```json fence around the document.
  const fence = trimmed.match(/^```(?:json|ui)?\s*\n([\s\S]*?)\n\s*```$/);
  const fenced = fence ? fence[1].trim() : null;

  // Pull the first balanced {…} AND the first balanced […] out of the text. A
  // prose preamble OR a bracketed prefix (e.g. a "[STEP] {…}" log marker) must
  // not hide the document: whichever candidate parses into JSON wins. Trying
  // only the earliest bracket would let "[STEP]" shadow the real {…} object.
  const objBlock = trimmed.includes('{') ? balancedBlock(trimmed, '{', '}') : null;
  const arrBlock = trimmed.includes('[') ? balancedBlock(trimmed, '[', ']') : null;

  // Order: fenced body, whole string, the {…} object, then the […] array.
  for (const cand of [fenced, trimmed, objBlock, arrBlock]) {
    if (!cand) continue;
    const c = cand.trim();
    if (!c.startsWith('{') && !c.startsWith('[')) continue;
    try {
      return JSON.parse(c) as Record<string, unknown>;
    } catch {
      /* try the next candidate */
    }
  }
  return null;
}

/** Extract the message array from any of the shapes a document may arrive in.
 * A bare array of messages is wrapped into `{ messages }` by the caller. */
function extractMessages(obj: Record<string, unknown>): Record<string, unknown>[] | null {
  if (Array.isArray(obj.messages)) return obj.messages as Record<string, unknown>[];
  if ('createSurface' in obj || 'updateComponents' in obj) return [obj];
  return null;
}

/**
 * Parse an A2UI document into a renderable surface, or null if `raw` is not a
 * recognizable A2UI document (so the caller can fall back to HTML/markdown).
 */
export function parseUiDocument(raw: string | Record<string, unknown>): UiSurface | null {
  const obj = coerceJson(raw);
  if (!obj) return null;
  const messages = extractMessages(Array.isArray(obj) ? ({ messages: obj } as never) : obj);
  if (!messages || messages.length === 0) return null;

  const components: Record<string, UiComponent> = {};
  let data: Record<string, unknown> = {};
  let theme: UiTheme | undefined;

  for (const msg of messages) {
    if (!msg || typeof msg !== 'object') continue;

    // Optional branding carried on the surface declaration (createSurface.theme)
    // or as a bare { theme: {...} } message. Tokens accumulate across messages.
    const surf = msg.createSurface as { theme?: unknown } | undefined;
    const themeBlock = (surf && typeof surf === 'object' && surf.theme) || msg.theme;
    if (themeBlock && typeof themeBlock === 'object') {
      theme = { ...(theme || {}), ...(themeBlock as UiTheme) };
    }

    const update = msg.updateComponents as { components?: unknown } | undefined;
    if (update && Array.isArray(update.components)) {
      for (const c of update.components as Record<string, unknown>[]) {
        if (!c || typeof c !== 'object' || typeof c.id !== 'string') continue;
        // LLMs vary on the discriminator key — accept "component" or "type".
        const compName = (c.component ?? c.type) as string;
        if (VALID_TYPES.has(compName)) {
          components[c.id] = { ...c, component: compName } as UiComponent;
        }
      }
    }

    // Optional data-model message (supports value bindings).
    const dm = (msg.dataModelUpdate || msg.updateDataModel) as { contents?: unknown; data?: unknown } | undefined;
    if (dm && typeof dm === 'object') {
      const contents = (dm.contents ?? dm.data) as Record<string, unknown> | undefined;
      if (contents && typeof contents === 'object') data = { ...data, ...contents };
    }
  }

  // A valid doc has at least one recognized catalog component. (sawSurface is
  // tracked for spec-completeness but not required to render.)
  const ids = Object.keys(components);
  if (ids.length === 0) return null;

  const rootId = components.root ? 'root' : ids[0];
  return { rootId, components, data, theme };
}

/* NOTE: the crew "emit a UI document" instruction is built BACKEND-side
 * (src/backend/src/engines/crewai/helpers/ui_emission.py) so every execution
 * channel behaves the same. This module only parses + renders UI documents. */

/** Workspace UI-Configurator palettes keyed by deliverable type
 *  ('default', 'presentation', 'dashboard', …) — the parsed `themes` map of
 *  the workspace ui_config's style_json. */
export type WorkspaceThemes = Record<string, UiTheme>;

// Component → deliverable, ordered by specificity (mirrors the backend keyword
// table in ui_emission.py). The first component type present anywhere in the
// surface decides the deliverable; surfaces with none of these are 'default'.
const DELIVERABLE_BY_COMPONENT: [UiComponentType, string][] = [
  ['Slides', 'presentation'],
  ['Quiz', 'quiz'],
  ['Album', 'album'],
  ['Mindmap', 'mindmap'],
  ['Dashboard', 'dashboard'],
  ['Table', 'genie'],
];

/** Which deliverable type a surface materializes, judged by its components. */
export function inferSurfaceDeliverable(surface: UiSurface): string {
  const present = new Set(Object.values(surface.components).map((c) => c.component));
  for (const [component, deliverable] of DELIVERABLE_BY_COMPONENT) {
    if (present.has(component)) return deliverable;
  }
  return 'default';
}

/** True when every token the embedded theme defines equals that palette's —
 *  i.e. the theme is a (possibly partial) copy of the palette rather than a
 *  deliberate deviation. Color tokens compare case-insensitively. */
function matchesPalette(theme: UiTheme, palette: UiTheme | null | undefined): boolean {
  if (!palette) return false;
  return (Object.keys(theme) as (keyof UiTheme)[]).every((key) => {
    const own = theme[key];
    const configured = palette[key];
    if (typeof own === 'string' && typeof configured === 'string') {
      return own.trim().toLowerCase() === configured.trim().toLowerCase();
    }
    return own === configured;
  });
}

/**
 * Re-resolve a surface's theme from the workspace UI-Configurator palettes —
 * the source of truth. The agent is instructed to stamp the matching palette
 * onto createSurface.theme, but in practice models routinely copy the Default
 * palette onto every surface (turning e.g. a themed deck white), so the
 * renderer must not trust the embedded theme when the configured palettes are
 * known. Resolving at render time also retroactively fixes persisted documents.
 *
 * EXCEPTION: an embedded theme that deviates from EVERY configured palette was
 * changed on purpose — a refine like "make the background black" edits the
 * embedded theme — and is kept as-is. The wrong-palette failure mode this
 * function exists for is always a (possibly partial) COPY of a configured
 * palette, so copies re-resolve and deviations survive.
 *
 * - `themes` unavailable (config disabled / fetch failed) → surface unchanged.
 * - Presentation with no own palette → theme cleared, so the built-in
 *   Databricks deck identity (DECK_THEME_VARS) applies.
 * - Other deliverables fall back to the Default palette, then to whatever the
 *   agent embedded.
 */
export function applyConfiguredTheme(
  surface: UiSurface,
  themes: WorkspaceThemes | null | undefined,
): UiSurface {
  if (!themes) return surface;
  const embedded = surface.theme;
  if (
    embedded &&
    Object.keys(embedded).length > 0 &&
    !Object.values(themes).some((palette) => matchesPalette(embedded, palette))
  ) {
    return surface;
  }
  const deliverable = inferSurfaceDeliverable(surface);
  if (deliverable === 'presentation') {
    return { ...surface, theme: themes.presentation };
  }
  return { ...surface, theme: themes[deliverable] || themes.default || surface.theme };
}

/** Resolve a `{ path: "/a/b" }` binding (or literal) against the data model. */
export function resolveValue(value: unknown, data: Record<string, unknown>): unknown {
  if (value && typeof value === 'object' && 'path' in (value as Record<string, unknown>)) {
    const path = String((value as Record<string, unknown>).path || '');
    const segments = path.split('/').filter(Boolean);
    let cur: unknown = data;
    for (const seg of segments) {
      if (cur && typeof cur === 'object' && seg in (cur as Record<string, unknown>)) {
        cur = (cur as Record<string, unknown>)[seg];
      } else {
        return undefined;
      }
    }
    return cur;
  }
  return value;
}
