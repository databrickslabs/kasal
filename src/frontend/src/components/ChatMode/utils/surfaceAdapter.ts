/**
 * The single boundary that turns ANY stored execution result into the new shared
 * A2UI {@link Surface} shape, so every host (chat thread, preview pane, workflow
 * chat, Jobs result viewer) renders through ONE implementation — the shared
 * `A2UIRenderer` — exactly like the exported Databricks App.
 *
 * It accepts, in priority order:
 *   1. a new Surface already (`{surfaceKind, root, components[]}`) — passed through;
 *   2. a `{text, a2ui: Surface}` envelope (what the backend composer now persists);
 *   3. a JSON string of either of the above;
 *   4. a LEGACY `{summary, messages}` / `{rootId, components:Record}` document found
 *      anywhere in the tree (old Jobs-history runs) — adapted via
 *      {@link legacyToNewSurface}.
 *
 * Returns null when there is no renderable surface (the caller then shows text).
 *
 * This module also OWNS the legacy A2UI document IR + its tolerant parser
 * ({@link parseUiDocument}, {@link findUiSurface}, {@link extractDocSummary},
 * {@link inferSurfaceDeliverable}), which the boundary above and the chat
 * transcript consume directly.
 */
import type { ComponentNode, Surface } from '../../../shared/a2ui';
import type { UiTheme } from '../../Configuration/uiConfigShared';

/* ------------------------------------------------------------------ */
/*  Legacy A2UI document IR + tolerant parsing                          */
/* ------------------------------------------------------------------ */

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
 * Repair the bracket structure of a JSON-ish string so a slightly-malformed
 * document still parses. Weak models (e.g. gpt-5-nano) routinely emit A2UI with
 * MISMATCHED or EXTRA brackets — e.g. a tail of `}]}]}}]}` where `}]}}]}` was
 * meant — which `JSON.parse` rejects outright, leaking the raw document into the
 * chat instead of rendering it. This is string-aware (brackets inside string
 * values are never touched): it drops any closing bracket that doesn't match the
 * top of the open-bracket stack, and auto-closes anything still open at EOF.
 *
 * Conservative by construction — it only rebalances brackets, never invents keys
 * or values — and it is invoked ONLY after strict parsing has already failed, so
 * well-formed documents are returned untouched by the caller.
 */
function repairJsonBrackets(src: string): string {
  const out: string[] = [];
  const stack: string[] = [];
  const opener: Record<string, string> = { '}': '{', ']': '[' };
  let inStr = false;
  let esc = false;
  for (let i = 0; i < src.length; i++) {
    const ch = src[i];
    if (inStr) {
      out.push(ch);
      if (esc) esc = false;
      else if (ch === '\\') esc = true;
      else if (ch === '"') inStr = false;
      continue;
    }
    if (ch === '"') { inStr = true; out.push(ch); continue; }
    if (ch === '{' || ch === '[') { stack.push(ch); out.push(ch); continue; }
    if (ch === '}' || ch === ']') {
      if (stack.length && stack[stack.length - 1] === opener[ch]) {
        stack.pop();
        out.push(ch);
      } // else: spurious/mismatched closer — drop it.
      continue;
    }
    out.push(ch);
  }
  // Close anything left open (a model that truncated mid-document).
  while (stack.length) out.push(stack.pop() === '{' ? '}' : ']');
  return out.join('');
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
  const candidates = [fenced, trimmed, objBlock, arrBlock];
  for (const cand of candidates) {
    if (!cand) continue;
    const c = cand.trim();
    if (!c.startsWith('{') && !c.startsWith('[')) continue;
    try {
      return JSON.parse(c) as Record<string, unknown>;
    } catch {
      /* try the next candidate */
    }
  }

  // Every candidate failed STRICT parsing. A weak model likely emitted
  // mismatched/extra brackets; rebalance (string-aware) and retry so the
  // document still renders instead of leaking raw JSON into the chat.
  for (const cand of candidates) {
    if (!cand) continue;
    const c = cand.trim();
    if (!c.startsWith('{') && !c.startsWith('[')) continue;
    try {
      return JSON.parse(repairJsonBrackets(c)) as Record<string, unknown>;
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
 * Pull the model-authored chat one-liner out of an A2UI document, if it carries
 * one. The emission prompt (ui_emission.py) asks the agent for a top-level
 * `summary` sibling of `messages`; we also accept it on `createSurface` or as a
 * bare `{ summary }` message so minor model variance still surfaces it. Returns
 * the trimmed sentence, or null when there is none — the caller then falls back
 * to the generic "Generated an app…" line. Never affects rendering (the renderer
 * ignores `summary`); this is purely the text shown in the chat transcript.
 */
export function extractDocSummary(raw: string | Record<string, unknown>): string | null {
  const obj = coerceJson(raw);
  if (!obj || typeof obj !== 'object') return null;
  const pick = (v: unknown): string | null =>
    typeof v === 'string' && v.trim() ? v.trim() : null;
  // Canonical location: a top-level `summary` next to `messages`.
  if (!Array.isArray(obj)) {
    const top = pick((obj as Record<string, unknown>).summary);
    if (top) return top;
  }
  // Liberal fallbacks: createSurface.summary, or a bare { summary } message.
  const messages = extractMessages(Array.isArray(obj) ? ({ messages: obj } as never) : obj);
  if (messages) {
    for (const msg of messages) {
      if (!msg || typeof msg !== 'object') continue;
      const surf = (msg as { createSurface?: Record<string, unknown> }).createSurface;
      const fromSurf = surf && typeof surf === 'object' ? pick(surf.summary) : null;
      if (fromSurf) return fromSurf;
      const bare = pick((msg as Record<string, unknown>).summary);
      if (bare) return bare;
    }
  }
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

/* NOTE: the crew "emit a UI document" instruction is built BACKEND-side
 * (src/backend/src/engines/crewai/helpers/ui_emission.py) so every execution
 * channel behaves the same. This module only parses + renders UI documents. */

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

/* ------------------------------------------------------------------ */
/*  New Surface adapter / boundary                                      */
/* ------------------------------------------------------------------ */

// Legacy deliverable key → new surfaceKind. The new catalog has 6 surfaceKinds;
// deliverables the new renderer has no dedicated container for fall to 'document'.
const DELIVERABLE_TO_SURFACE_KIND: Record<string, string> = {
  presentation: 'presentation',
  dashboard: 'dashboard',
  mindmap: 'mindmap',
  quiz: 'quiz',
  report: 'document',
  album: 'document',
  flashcards: 'document',
  genie: 'document',
  default: 'document',
};

/**
 * Adapt a legacy {@link UiSurface} (`rootId` + id-keyed `components` Record) to the
 * new flat {@link Surface} (`root` + `components` array). Used only for OLD stored
 * runs — new runs already arrive as a Surface. Legacy-only component types (Album,
 * Flashcards, Badge, Icon, Button, Stat, Dashboard, Slides…) are no longer emitted
 * by the composer; the renderer shows them as `Unsupported`, which is acceptable
 * degradation for history. The legacy `theme` is dropped (workspace branding is
 * re-resolved at render time from the UIConfigurator).
 */
export function legacyToNewSurface(legacy: UiSurface): Surface {
  const deliverable = inferSurfaceDeliverable(legacy);
  return {
    surfaceKind: DELIVERABLE_TO_SURFACE_KIND[deliverable] ?? 'document',
    root: legacy.rootId,
    components: Object.values(legacy.components).map((c) => adaptLegacyNode(c)),
    dataModel: legacy.data,
  };
}

/** Adapt a single legacy component node to the new renderer's prop expectations.
 *  The shapes are largely identical; the one real divergence is Table rows — the
 *  legacy renderer read row OBJECTS keyed by column, the new one reads row ARRAYS
 *  — so convert (column-ordered) when needed. */
function adaptLegacyNode(c: UiSurface['components'][string]): ComponentNode {
  const node = { ...c } as unknown as ComponentNode;
  if (node.component === 'Table' && Array.isArray(node.rows) && Array.isArray(node.columns)) {
    const cols = node.columns as unknown[];
    const rows = node.rows as unknown[];
    const first = rows[0];
    if (first && typeof first === 'object' && !Array.isArray(first)) {
      node.rows = rows.map((r) =>
        cols.map((col) => (r as Record<string, unknown>)[String(col)] ?? ''),
      );
    }
  }
  return node;
}

function isNewSurface(value: unknown): value is Surface {
  return (
    !!value &&
    typeof value === 'object' &&
    'surfaceKind' in value &&
    'root' in value &&
    Array.isArray((value as { components?: unknown }).components)
  );
}

/** Recursively locate a NEW {@link Surface} anywhere in a raw result: a bare
 *  surface, a `{text, a2ui}` envelope, a JSON string of either, or nested inside
 *  a `{result:{…}}` / `{output:"<json>"}` wrapper (so a wrapped surface never
 *  leaks to the chat as raw JSON). */
function findNewSurface(raw: unknown, depth = 0): Surface | null {
  if (raw == null || depth > 6) return null;
  if (typeof raw === 'string') {
    const trimmed = raw.trim();
    if (!trimmed) return null;
    try {
      return findNewSurface(JSON.parse(trimmed), depth + 1);
    } catch {
      return null;
    }
  }
  if (isNewSurface(raw)) return raw;
  if (typeof raw === 'object') {
    const children = Array.isArray(raw)
      ? raw
      : Object.values(raw as Record<string, unknown>);
    for (const child of children) {
      const found = findNewSurface(child, depth + 1);
      if (found) return found;
    }
  }
  return null;
}

/**
 * Coerce a raw execution result (object, `{text,a2ui}` envelope, JSON string, or
 * legacy document) into a renderable {@link Surface}, or null. New surfaces win;
 * an older legacy document is adapted as a fallback (old Jobs-history runs).
 */
export function toSurface(raw: unknown): Surface | null {
  if (raw == null) return null;
  const fresh = findNewSurface(raw);
  if (fresh) return fresh;
  const legacy = findUiSurface(raw);
  return legacy ? legacyToNewSurface(legacy) : null;
}
