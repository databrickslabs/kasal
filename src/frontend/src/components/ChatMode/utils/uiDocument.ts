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
  | 'Mindmap'
  | 'Flashcards';

/** The component names available in each catalog (for the configurator UI). */
export const CATALOG_COMPONENTS: Record<string, UiComponentType[]> = {
  minimal: ['Text', 'Row', 'Column', 'Button', 'TextField'],
  basic: [
    'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
    'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
    'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide', 'Album', 'Mindmap', 'Flashcards',
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
  /** Set when a user has deterministically restyled the surface from the in-preview
   *  "Customize" panel. Marks the theme as an intentional, user-pinned choice so
   *  applyConfiguredTheme never re-resolves it away from the workspace palettes.
   *  Inert at render/PDF (the renderer only reads color/font/density tokens). */
  _pinned?: boolean;
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
  'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide', 'Album', 'Mindmap', 'Flashcards',
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

/**
 * Recursively locate an A2UI document anywhere inside an arbitrary value and
 * return the first renderable surface, or null.
 *
 * `parseUiDocument` only inspects the value it is handed: it parses a string
 * (incl. prose/fence/double-encoded) and reads a top-level message object/array.
 * But a stored execution result wraps the document in wildly varying shapes —
 * `{messages:[…]}`, `{result:"<json>"}`, `{someKey:{messages:[…]}}`, a multi-key
 * envelope `{output:"…", meta:"<json>"}`, a prose-prefixed string value, a
 * flow's per-crew aggregate, … — so a top-level-only check renders the surface
 * only "sometimes". This walks objects (values), arrays (elements) and
 * JSON/prose string values, applying `parseUiDocument` (the SAME strict
 * predicate — it returns null unless ≥1 recognized component exists) at every
 * node, so non-A2UI content can never false-positive.
 *
 * Outermost-first (pre-order): the node itself is tried before its children, so
 * a clean top-level document wins and surface selection is unchanged for results
 * that already worked. When only nested surfaces exist (e.g. a multi-crew flow),
 * the first one in traversal order is returned. Depth-capped against pathological
 * nesting; string parsing delegates to the depth-capped `coerceJson`.
 */
export function findUiSurface(raw: unknown, depth = 0): UiSurface | null {
  if (raw == null || depth > 6) return null;
  // Strings reuse parseUiDocument -> coerceJson (prose/fence/double-encode).
  if (typeof raw === 'string') return parseUiDocument(raw);
  if (typeof raw !== 'object') return null;
  // Try this node as-is first so the OUTERMOST valid document wins.
  const direct = parseUiDocument(raw as Record<string, unknown>);
  if (direct) return direct;
  // Otherwise descend: array elements in order, then object values in order.
  const children = Array.isArray(raw) ? raw : Object.values(raw as Record<string, unknown>);
  for (const child of children) {
    const found = findUiSurface(child, depth + 1);
    if (found) return found;
  }
  return null;
}

/**
 * Like {@link findUiSurface}, but returns the RAW A2UI document node (the
 * string or object that {@link parseUiDocument} accepts) rather than the parsed
 * surface. Use this to extract a clean, TOP-LEVEL document out of a wrapped
 * execution result so it can be stored/handed to the preview pane exactly like a
 * native deliverable — `parseUiDocument(node)` then renders it and the pane's
 * Customize / Look / refine / download all operate on a normal document. Returns
 * the matched node (outermost-first), or null when there is no surface.
 */
export function findUiDocument(
  raw: unknown,
  depth = 0,
): string | Record<string, unknown> | null {
  if (raw == null || depth > 6) return null;
  if (typeof raw === 'string') return parseUiDocument(raw) ? raw : null;
  if (typeof raw !== 'object') return null;
  if (parseUiDocument(raw as Record<string, unknown>)) {
    return raw as Record<string, unknown>;
  }
  const children = Array.isArray(raw) ? raw : Object.values(raw as Record<string, unknown>);
  for (const child of children) {
    const found = findUiDocument(child, depth + 1);
    if (found) return found;
  }
  return null;
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
  ['Flashcards', 'flashcards'],
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

/** Friendly, non-technical nouns for each deliverable — used as the "Customize
 *  this {label}" title in the preview's refine panel so a business user reads
 *  "Photo album" / "Data view" instead of internal keys like 'album' / 'genie'. */
export const DELIVERABLE_LABELS: Record<string, string> = {
  presentation: 'Presentation',
  dashboard: 'Dashboard',
  genie: 'Data view',
  mindmap: 'Mind map',
  album: 'Photo album',
  quiz: 'Quiz',
  flashcards: 'Flashcard deck',
  report: 'Report',
  default: 'Document',
};

/**
 * Deterministically restyle a stored A2UI document: merge `theme` into the
 * surface's branding and return the re-serialized document. This is how the
 * in-preview "Customize" panel applies a Look change WITHOUT an AI/crew run —
 * the renderer reads `surface.theme` at render time, so the preview restyles
 * instantly, and persisting the rewritten doc makes it survive reload + PDF.
 *
 * The theme is stamped `_pinned: true` so applyConfiguredTheme keeps it verbatim
 * (see there). Merges onto any existing theme so partial edits (one color) work.
 * The original message wrapper is preserved; only the theme is touched. Returns
 * `rawDoc` unchanged if it can't be parsed as JSON (caller falls back gracefully).
 */
export function setSurfaceTheme(rawDoc: string, theme: UiTheme): string {
  // Extract the document with the SAME tolerant coercion the renderer uses
  // (coerceJson) rather than a strict JSON.parse. A task output frequently
  // arrives wrapped — a prose preamble ("Here is your dashboard: { … }"), a
  // ```json fence, or a double-encoded string — and parseUiDocument renders
  // those fine, so the in-preview "Customize" panel must restyle them too. A
  // strict parse threw on any such wrapper and silently returned the doc
  // unchanged, so the instant "Look" did NOTHING for any deliverable an agent
  // prefaced with prose (while pure-JSON deliverables restyled normally — the
  // mismatch looked deliverable-specific). Re-serializing the coerced object
  // also canonicalizes the stored doc (the decorative wrapper is dropped).
  const parsed: unknown = coerceJson(rawDoc);
  if (!parsed) {
    return rawDoc;
  }

  // Normalize to a mutable message array, remembering how to put it back so the
  // document's original shape ({messages}, bare array, or single message) survives.
  let messages: Record<string, unknown>[];
  let rewrap: (msgs: Record<string, unknown>[]) => unknown;
  if (parsed && typeof parsed === 'object' && Array.isArray((parsed as { messages?: unknown }).messages)) {
    messages = (parsed as { messages: Record<string, unknown>[] }).messages;
    rewrap = (msgs) => ({ ...(parsed as object), messages: msgs });
  } else if (Array.isArray(parsed)) {
    messages = parsed as Record<string, unknown>[];
    rewrap = (msgs) => msgs;
  } else if (parsed && typeof parsed === 'object') {
    messages = [parsed as Record<string, unknown>];
    // A single message stays a single message — unless we had to prepend a
    // theme message, in which case it becomes a {messages:[…]} document.
    rewrap = (msgs) => (msgs.length === 1 ? msgs[0] : { messages: msgs });
  } else {
    return rawDoc;
  }

  const pinned: UiTheme = { ...theme, _pinned: true };
  const merge = (existing: unknown): UiTheme => ({
    ...(existing && typeof existing === 'object' ? (existing as UiTheme) : {}),
    ...pinned,
  });

  // Prefer the surface declaration's theme (where the agent stamps branding);
  // else a bare { theme } message; else prepend one (parseUiDocument reads both).
  const surfMsg = messages.find(
    (m) => m && typeof m === 'object' && (m as { createSurface?: unknown }).createSurface,
  );
  if (surfMsg) {
    // surfMsg was found by having a truthy createSurface, so it's safe to read.
    const cs = surfMsg.createSurface as Record<string, unknown>;
    surfMsg.createSurface = { ...cs, theme: merge(cs.theme) };
  } else {
    const themeMsg = messages.find((m) => m && typeof m === 'object' && (m as { theme?: unknown }).theme);
    if (themeMsg) {
      themeMsg.theme = merge(themeMsg.theme);
    } else {
      messages = [{ theme: pinned }, ...messages];
    }
  }

  return JSON.stringify(rewrap(messages));
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
  // A user-pinned theme (set via the in-preview "Customize" panel) is an explicit
  // choice — keep it verbatim, never re-resolve from the workspace palettes.
  if (embedded?._pinned) return surface;
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

/** Strip the markdown emphasis markers the A2UI Text renderer shows verbatim. */
function stripEmphasis(text: string): string {
  return text.replace(/\*\*/g, '').replace(/__/g, '').replace(/`/g, '').trim();
}

/** A short line that reads as a section header (an `=== … ===` banner, an
 *  ALL-CAPS banner, or a brief "Label:" lead-in) — returns the cleaned heading
 *  text, or null for ordinary prose. */
function headingText(line: string): string | null {
  const banner = line.match(/^=+\s*(.+?)\s*=+$/);
  if (banner) return banner[1].trim();
  const hasLetters = /[A-Za-z]/.test(line);
  if (hasLetters && line.length <= 70 && line === line.toUpperCase()) {
    return line.replace(/[:：]\s*$/, '').trim();
  }
  if (line.length <= 48 && /[:：]$/.test(line) && !/[.!?]/.test(line)) {
    return line.replace(/[:：]\s*$/, '').trim();
  }
  return null;
}

/**
 * Turn a free-text body — which may be a structured outline (headings, bullet
 * lists, key/value lines) — into a sequence of A2UI components: headings become
 * Text h4, runs of bullet/numbered lines become a List, everything else a body
 * paragraph. This is what makes verbose tool/memory output read as a clean
 * document instead of a wall of "===" banners and "•"/"-" markers. Returns the
 * new components plus the child ids to attach, all keyed under `prefix`.
 */
// Compact, elegant type scale for the preview's result cards — smaller than the
// renderer's deck-grade defaults. fontSize / lineHeight / fontWeight /
// letterSpacing all pass through extractNodeStyle's whitelist, so these refine
// the look without touching the shared renderer.
const TITLE_STYLE = { fontSize: '1.05rem', fontWeight: 700, letterSpacing: '0.01em' };
const HEAD_STYLE = { fontSize: '0.9rem', fontWeight: 600, letterSpacing: '0.03em' };
const BODY_STYLE = { fontSize: '0.85rem', lineHeight: 1.5 };

/** Drop a metadata line whose value is explicitly empty — "entities: []",
 *  "dates: {}", "result: none". A bare "Label:" is NOT noise (it's a section
 *  heading like "Suggested questions:" / "TITLE BLOCK:"), so the empty-value
 *  marker is required, not optional. */
function isNoiseLine(line: string): boolean {
  return /^[A-Za-z][\w &/()'-]*:\s*(\[\s*\]|\{\s*\}|none|n\/?a|null)\s*$/i.test(line);
}

// Provenance / framing the user does NOT want in the preview — only the
// retrieved CONTEXT itself. Drops "categories:/entities:/dates:/topics:/tags:/
// source(s):/score:" metadata lines and "Relevant/Found memories", "Search
// memory" framing headers.
const DROP_LABEL = /^(categories|category|entities|entity|dates|date|topics|topic|tags|tag|source|sources|score)\s*:/i;
const DROP_HEADER = /^(relevant memories|found memories|search memory|no memories|memories found)\b/i;

/** A "Label: • a • b • c" (or bare "• a • b • c") inline run → label + items. */
function splitInlineBullets(line: string): { label: string; items: string[] } | null {
  if (!line.includes('•')) return null;
  const idx = line.indexOf('•');
  const label = line.slice(0, idx).trim().replace(/[:：]\s*$/, '');
  const items = line.slice(idx).split('•').map((s) => s.trim()).filter(Boolean);
  if (items.length < 2) return null;
  return { label, items };
}

function bodyToComponents(
  body: string,
  prefix: string,
): { components: Record<string, UiComponent>; childIds: string[] } {
  const components: Record<string, UiComponent> = {};
  const childIds: string[] = [];
  let n = 0;
  let bullets: string[] = [];

  const pushText = (text: string, variant: string, style: Record<string, unknown>) => {
    const id = `${prefix}_${n++}`;
    components[id] = { id, component: 'Text', text, variant, style };
    childIds.push(id);
  };

  const flushBullets = () => {
    if (bullets.length === 0) return;
    const listId = `${prefix}_l${n++}`;
    const itemIds = bullets.map((t, k) => {
      const id = `${listId}_i${k}`;
      components[id] = { id, component: 'Text', text: t, variant: 'body', style: BODY_STYLE };
      return id;
    });
    components[listId] = { id: listId, component: 'List', children: itemIds };
    childIds.push(listId);
    bullets = [];
  };

  for (const raw of stripEmphasis(body).split('\n')) {
    let line = raw.trim();
    // Show ONLY the retrieved context — strip empty noise, provenance metadata
    // and the memory framing headers.
    if (!line || isNoiseLine(line) || DROP_LABEL.test(line) || DROP_HEADER.test(line)) {
      flushBullets();
      continue;
    }

    // A new memory/result starts with a "(score=…)" marker — drop the marker and
    // separate consecutive entries with a hairline divider for an elegant feed.
    const isEntryStart = /^\(score=[\d.]+\)/i.test(line);
    line = line.replace(/^\(score=[\d.]+\)\s*/i, '').trim();
    if (isEntryStart && childIds.length > 0) {
      flushBullets();
      const did = `${prefix}_d${n++}`;
      components[did] = { id: did, component: 'Divider' };
      childIds.push(did);
    }
    if (!line) continue;

    // "Label: • a • b • c" → a labelled bullet list. This is the big readability
    // win: dense outline lines become a heading over clean list items.
    const inline = splitInlineBullets(line);
    if (inline) {
      flushBullets();
      if (inline.label) pushText(inline.label, 'h4', HEAD_STYLE);
      bullets.push(...inline.items);
      flushBullets();
      continue;
    }

    // A leading bullet / number marker → list item.
    const bullet = line.match(/^[-•*]\s+(.*)$/) || line.match(/^\d+[.)]\s+(.*)$/);
    if (bullet) {
      bullets.push(bullet[1].trim());
      continue;
    }

    flushBullets();
    const heading = headingText(line);
    if (heading) pushText(heading, 'h4', HEAD_STYLE);
    else pushText(line, 'body', BODY_STYLE);
  }
  flushBullets();
  return { components, childIds };
}

/** Title-case a JSON key: "suggestedQuestions" / "text_attachments" → "Suggested questions". */
function humanizeKey(k: string): string {
  const s = k.replace(/[_-]+/g, ' ').replace(/([a-z])([A-Z])/g, '$1 $2').trim();
  return s ? s.charAt(0).toUpperCase() + s.slice(1) : k;
}

// JSON keys that are plumbing, not content — dropped from a humanized tool result.
const JSON_DROP_KEYS = new Set([
  'conversationid', 'messageid', 'conversation_id', 'message_id', 'id', 'ids',
  'status', 'queryattachments', 'query_attachments', 'metadata', 'usage', 'role',
]);

/**
 * Humanize a JSON tool result (e.g. a Genie response envelope) into readable
 * text: pull out the prose attachments and suggested questions, render
 * string-array fields as labelled bullet lists, and drop ids / status / empty
 * arrays. Returns null when `raw` isn't a JSON object/array, so prose bodies
 * pass straight through. The text it returns is then formatted by
 * {@link bodyToComponents} (headings / lists / paragraphs) like any other body.
 */
function humanizeToolJson(raw: string): string | null {
  const t = raw.trim();
  if (!(t.startsWith('{') || t.startsWith('['))) return null;
  let parsed: unknown;
  try {
    parsed = JSON.parse(t);
  } catch {
    return null;
  }
  if (typeof parsed === 'string') {
    try { parsed = JSON.parse(parsed); } catch { /* keep as-is */ }
  }
  if (!parsed || typeof parsed !== 'object') return null;

  const out: string[] = [];
  const collect = (o: Record<string, unknown>) => {
    for (const [k, v] of Object.entries(o)) {
      if (JSON_DROP_KEYS.has(k.toLowerCase())) continue;
      if (typeof v === 'string') {
        if (v.trim()) out.push(v.trim());
      } else if (Array.isArray(v)) {
        const strs = v.filter((x) => typeof x === 'string' && String(x).trim()).map((x) => String(x).trim());
        if (strs.length === 0) continue;
        if (/attachment|answer|result|content|text/i.test(k)) {
          strs.forEach((s) => out.push(s)); // prose → paragraphs (keeps own newlines/bullets)
        } else {
          out.push(`${/question/i.test(k) ? 'Suggested questions' : humanizeKey(k)}:`);
          strs.forEach((s) => out.push(`- ${s}`)); // → labelled bullet list
        }
      } else if (v && typeof v === 'object') {
        collect(v as Record<string, unknown>);
      }
    }
  };

  if (Array.isArray(parsed)) {
    parsed.forEach((el) => { if (el && typeof el === 'object') collect(el as Record<string, unknown>); });
  } else {
    collect(parsed as Record<string, unknown>);
  }
  return out.length ? out.join('\n') : null;
}

/**
 * Compose a list of `{ title, body }` results into an A2UI surface — a Column of
 * Cards, each a heading (Text h3) over the structured body (headings / lists /
 * paragraphs via {@link bodyToComponents}) — so transient / intermediate results
 * render through the SAME renderer pipeline as final deliverables, instead of
 * ad-hoc markup. Pure; built on demand, nothing here is persisted. Used by
 * ChatMode's live preview feed.
 */
export function buildResultsSurface(items: { title: string; body?: string }[]): UiSurface {
  const components: Record<string, UiComponent> = {};
  const cardIds: string[] = [];

  items.forEach((it, i) => {
    const childIds: string[] = [];
    const titleId = `r_title_${i}`;
    components[titleId] = { id: titleId, component: 'Text', text: stripEmphasis(it.title), variant: 'h3', style: TITLE_STYLE };
    childIds.push(titleId);

    // A JSON tool envelope (e.g. a Genie response) is humanized into readable
    // text first; prose bodies pass through untouched.
    const bodyText = humanizeToolJson(it.body || '') ?? (it.body || '');
    const blocks = bodyToComponents(bodyText, `r_b${i}`);
    Object.assign(components, blocks.components);
    childIds.push(...blocks.childIds);

    const colId = `r_col_${i}`;
    components[colId] = { id: colId, component: 'Column', children: childIds };
    const cardId = `r_card_${i}`;
    components[cardId] = { id: cardId, component: 'Card', children: [colId] };
    cardIds.push(cardId);
  });

  components.root = { id: 'root', component: 'Column', children: cardIds };
  return { rootId: 'root', components, data: {} };
}
