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
  | 'Slide';

/** The component names available in each catalog (for the configurator UI). */
export const CATALOG_COMPONENTS: Record<string, UiComponentType[]> = {
  minimal: ['Text', 'Row', 'Column', 'Button', 'TextField'],
  basic: [
    'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
    'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
    'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide',
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

export interface UiSurface {
  rootId: string;
  /** Components keyed by id for O(1) child resolution. */
  components: Record<string, UiComponent>;
  /** Data model for `{ path: "/x" }` bindings (best-effort; may be empty). */
  data: Record<string, unknown>;
}

const VALID_TYPES: ReadonlySet<string> = new Set<UiComponentType>([
  'Text', 'Row', 'Column', 'Card', 'List', 'Divider', 'Image', 'Icon',
  'Badge', 'Button', 'TextField', 'CheckBox', 'Slider', 'ChoicePicker',
  'Dashboard', 'Stat', 'Chart', 'Table', 'Quiz', 'Slides', 'Slide',
]);

/** Read a JSON value that may be a raw object or a JSON string. */
function coerceJson(raw: string | Record<string, unknown>): Record<string, unknown> | null {
  if (raw && typeof raw === 'object') return raw as Record<string, unknown>;
  if (typeof raw !== 'string') return null;
  const trimmed = raw.trim();
  // Tolerate a ```json fence around the document.
  const fence = trimmed.match(/^```(?:json|ui)?\s*\n([\s\S]*?)\n\s*```$/);
  const body = fence ? fence[1] : trimmed;
  if (!body.startsWith('{') && !body.startsWith('[')) return null;
  try {
    // body starts with { or [, so a successful parse is always an object/array.
    return JSON.parse(body) as Record<string, unknown>;
  } catch {
    return null;
  }
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

  for (const msg of messages) {
    if (!msg || typeof msg !== 'object') continue;

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
  return { rootId, components, data };
}

/* NOTE: the crew "emit a UI document" instruction is built BACKEND-side
 * (src/backend/src/engines/crewai/helpers/ui_emission.py) so every execution
 * channel behaves the same. This module only parses + renders UI documents. */

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
