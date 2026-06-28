/**
 * Shared UI-Configurator specs — the single source of truth for branding
 * palettes and per-deliverable settings.
 *
 * These were originally defined inline in UIConfigurator.tsx. They are now
 * extracted here so BOTH the workspace configurator (UIConfigurator.tsx) and
 * the in-preview "Customize" panel (ChatMode/components/Preview/RefinePanel.tsx)
 * read the same definitions — no duplication, no drift. Keep this file free of
 * React/MUI imports so it can be consumed from either surface.
 */

/* ------------------------------------------------------------------ */
/*  Branding palette                                                   */
/* ------------------------------------------------------------------ */

/** A full branding palette. The renderer (UiRenderer.tsx) maps these onto the
 *  stage background, accent, text colors and font; the agent embeds the matching
 *  palette as the surface `theme` (palette embedded by the A2UI composer). */
export interface Theme {
  accent: string;
  background: string;
  surface: string;
  text: string;
  heading: string;
  muted: string;
  font: 'sans' | 'serif' | 'rounded' | 'mono';
  density: 'comfortable' | 'compact';
}

// Deliverable types that can be branded/configured independently. Keys mirror the
// backend (_THEME_LABELS in ui_emission.py) and the chat format options.
// "default" is the base palette applied to anything without its own.
export const DELIVERABLE_TYPES = [
  { key: 'default', label: 'Default' },
  { key: 'dashboard', label: 'Dashboard' },
  { key: 'presentation', label: 'Presentation' },
  { key: 'genie', label: 'Genie' },
  { key: 'mindmap', label: 'Mindmap' },
  { key: 'album', label: 'Album' },
  { key: 'quiz', label: 'Quiz' },
  { key: 'flashcards', label: 'Flashcards' },
  { key: 'report', label: 'Report' },
  // The run-activity "context" / logs view (per-step retrieved context shown in
  // the preview pane). Styled distinctly from deliverables — see the Logs preset
  // and LOGS_THEME (the built-in fallback when this isn't customized).
  { key: 'logs', label: 'Run activity (logs)' },
] as const;
export type DeliverableKey = (typeof DELIVERABLE_TYPES)[number]['key'];

export const FONT_OPTIONS: { value: Theme['font']; label: string }[] = [
  { value: 'sans', label: 'Sans-serif (modern)' },
  { value: 'serif', label: 'Serif (editorial)' },
  { value: 'rounded', label: 'Rounded (friendly)' },
  { value: 'mono', label: 'Monospace (technical)' },
];

// CSS stacks for the live preview — kept in sync with FONT_STACK in UiRenderer.tsx.
export const FONT_CSS: Record<Theme['font'], string> = {
  sans: 'Inter, system-ui, sans-serif',
  serif: 'Georgia, "Times New Roman", serif',
  rounded: '"Nunito", "Quicksand", system-ui, sans-serif',
  mono: '"JetBrains Mono", Menlo, monospace',
};

// One-click starting palettes.
export const THEME_PRESETS: { key: string; label: string; theme: Theme }[] = [
  { key: 'default', label: 'Default', theme: { accent: '#2272B4', background: '#FFFFFF', surface: '#F8FAFC', text: '#0F172A', heading: '#0F172A', muted: '#64748B', font: 'sans', density: 'comfortable' } },
  { key: 'dark', label: 'Dark', theme: { accent: '#38BDF8', background: '#0F172A', surface: '#1E293B', text: '#E2E8F0', heading: '#F8FAFC', muted: '#94A3B8', font: 'sans', density: 'comfortable' } },
  { key: 'vibrant', label: 'Vibrant', theme: { accent: '#7C3AED', background: '#FFFFFF', surface: '#F5F3FF', text: '#1E1B4B', heading: '#6D28D9', muted: '#6B7280', font: 'rounded', density: 'comfortable' } },
  { key: 'minimal', label: 'Minimal', theme: { accent: '#111827', background: '#FFFFFF', surface: '#FFFFFF', text: '#111827', heading: '#000000', muted: '#9CA3AF', font: 'sans', density: 'compact' } },
  { key: 'corporate', label: 'Corporate', theme: { accent: '#1E3A5F', background: '#FFFFFF', surface: '#F1F5F9', text: '#1E293B', heading: '#0F2540', muted: '#64748B', font: 'serif', density: 'comfortable' } },
  { key: 'amber', label: 'Amber', theme: { accent: '#F97316', background: '#FFFBEB', surface: '#FEF3C7', text: '#7C2D12', heading: '#C2410C', muted: '#B45309', font: 'rounded', density: 'comfortable' } },
  // Mirrors the renderer's built-in presentation deck identity (UiRenderer
  // DECK_THEME_VARS) so users can pin it as an explicit palette and tweak it.
  { key: 'studio', label: 'Studio', theme: { accent: '#FF3621', background: '#0E1B21', surface: '#16272F', text: '#E8EEF2', heading: '#FFFFFF', muted: '#8FA3AD', font: 'sans', density: 'comfortable' } },
  // Elegant light identity for the run-activity "logs"/context view — clean sans,
  // compact, dark ink on a soft light surface. Mirrors LOGS_THEME below.
  { key: 'logs', label: 'Run activity (context)', theme: { accent: '#2563EB', background: '#F6F8FA', surface: '#FFFFFF', text: '#1F2937', heading: '#0F172A', muted: '#64748B', font: 'sans', density: 'compact' } },
];

export const DEFAULT_THEME: Theme = THEME_PRESETS[0].theme;

export const normalizeTheme = (t: Partial<Theme> | undefined): Theme => ({ ...DEFAULT_THEME, ...(t || {}) });

/** Branding tokens carried on the surface, set by the agent from the workspace
 *  UI-Configurator palette. The renderer derives its stage / accent / text /
 *  surface colors and font from these; any omitted token falls back to the
 *  built-in premium dark theme, so an un-themed doc renders exactly as before.
 *
 *  A partial {@link Theme} (de-dups the palette shape) plus `_pinned`:
 *  Set when a user has deterministically restyled the surface from the in-preview
 *  "Customize" panel. Marks the theme as an intentional, user-pinned choice so
 *  applyConfiguredTheme never re-resolves it away from the workspace palettes.
 *  Inert at render/PDF (the renderer only reads color/font/density tokens). */
export type UiTheme = Partial<Theme> & { _pinned?: boolean };

/** Workspace UI-Configurator palettes keyed by deliverable type
 *  ('default', 'presentation', 'dashboard', …) — the parsed `themes` map of
 *  the workspace ui_config's style_json. */
export type WorkspaceThemes = Record<string, UiTheme>;

/**
 * Built-in style for the run-activity "logs" view (per-step retrieved context):
 * a light console — monospace, compact, dark ink on a soft light surface,
 * distinct from any deliverable palette. Used as the fallback when the workspace
 * hasn't configured a 'logs' palette in the UI Configurator; mirrors the 'logs'
 * preset in uiConfigShared.ts.
 */
export const LOGS_THEME: UiTheme = {
  accent: '#2563EB',
  background: '#F6F8FA',
  surface: '#FFFFFF',
  text: '#1F2937',
  heading: '#0F172A',
  muted: '#64748B',
  font: 'sans',
  density: 'compact',
};

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

/* ------------------------------------------------------------------ */
/*  Per-deliverable settings (type-specific, beyond the palette)       */
/* ------------------------------------------------------------------ */

export type OptionValue = string | number | boolean;
export type OptionSpec =
  | { kind: 'select'; key: string; label: string; choices: { value: string; label: string }[]; default: string; phrase: (v: string) => string }
  | { kind: 'number'; key: string; label: string; min: number; max: number; step?: number; default: number; phrase: (v: number) => string }
  | { kind: 'switch'; key: string; label: string; default: boolean; phrase: (v: boolean) => string };

// Each option carries a `phrase()` so the directive text sent to the crew lives
// next to its control (single source of truth — the backend just appends it).
export const TYPE_OPTIONS: Record<string, OptionSpec[]> = {
  dashboard: [
    { kind: 'number', key: 'tilesPerRow', label: 'KPI tiles per row', min: 2, max: 4, default: 3, phrase: (v) => `lay out KPI Stat tiles ${v} per row` },
    { kind: 'select', key: 'chart', label: 'Preferred chart', default: 'auto', choices: [{ value: 'auto', label: 'Auto' }, { value: 'bar', label: 'Bar' }, { value: 'line', label: 'Line' }, { value: 'pie', label: 'Pie' }], phrase: (v) => (v === 'auto' ? 'pick the chart type that best fits each metric' : `prefer ${v} charts`) },
    { kind: 'switch', key: 'deltas', label: 'Show deltas / trends on tiles', default: true, phrase: (v) => (v ? 'show a delta/trend on each Stat tile' : 'omit deltas on Stat tiles') },
  ],
  presentation: [
    { kind: 'number', key: 'slides', label: 'Target slide count', min: 3, max: 20, default: 20, phrase: (v) => `aim for about ${v} slides` },
    { kind: 'number', key: 'bullets', label: 'Max bullets per slide', min: 2, max: 6, default: 4, phrase: (v) => `at most ${v} bullet points per slide` },
    { kind: 'switch', key: 'titleSlide', label: 'Open with a title slide', default: true, phrase: (v) => (v ? 'open with a dedicated title slide' : 'skip the title slide') },
    { kind: 'switch', key: 'summarySlide', label: 'End with a summary slide', default: true, phrase: (v) => (v ? 'end with a summary / takeaways slide' : 'do not add a summary slide') },
  ],
  genie: [
    { kind: 'select', key: 'chart', label: 'Default chart', default: 'auto', choices: [{ value: 'auto', label: 'Auto' }, { value: 'bar', label: 'Bar' }, { value: 'line', label: 'Line' }, { value: 'pie', label: 'Pie' }, { value: 'none', label: 'Table only' }], phrase: (v) => (v === 'none' ? 'show the result as a Table only (no chart)' : v === 'auto' ? 'add a chart when it aids understanding' : `visualize the result with a ${v} chart`) },
    { kind: 'number', key: 'maxRows', label: 'Max table rows', min: 5, max: 100, step: 5, default: 20, phrase: (v) => `show at most ${v} rows in the Table` },
    { kind: 'switch', key: 'showSql', label: 'Show the SQL query', default: false, phrase: (v) => (v ? 'include the SQL query used (as a caption)' : 'do not surface the SQL query') },
  ],
  mindmap: [
    { kind: 'number', key: 'depth', label: 'Max depth', min: 2, max: 5, default: 3, phrase: (v) => `nest the tree up to ${v} levels deep` },
    { kind: 'number', key: 'branches', label: 'Main branches', min: 3, max: 8, default: 5, phrase: (v) => `give the central topic about ${v} main branches` },
    { kind: 'switch', key: 'icons', label: 'Use icons / emoji on nodes', default: false, phrase: (v) => (v ? 'prefix node labels with a relevant emoji' : 'keep node labels plain text') },
  ],
  album: [
    { kind: 'select', key: 'layout', label: 'Layout', default: 'grid', choices: [{ value: 'grid', label: 'Grid' }, { value: 'carousel', label: 'One per screen (carousel)' }], phrase: (v) => (v === 'carousel' ? 'lay the album out as a full-width carousel showing ONE image per screen that scrolls left→right — set the Album component’s "layout" to "carousel"' : 'lay the album out as a responsive grid (Album "layout":"grid")') },
    { kind: 'switch', key: 'captions', label: 'Show captions', default: true, phrase: (v) => (v ? 'give every image a short caption' : 'omit image captions') },
    { kind: 'number', key: 'maxImages', label: 'Max images', min: 4, max: 30, step: 2, default: 12, phrase: (v) => `include at most ${v} images` },
  ],
  quiz: [
    { kind: 'number', key: 'questions', label: 'Number of questions', min: 3, max: 100, default: 20, phrase: (v) => `use the exact number of questions the request asks for and never cap it; if it names none, write about ${v}` },
    { kind: 'select', key: 'difficulty', label: 'Difficulty', default: 'mixed', choices: [{ value: 'easy', label: 'Easy' }, { value: 'medium', label: 'Medium' }, { value: 'hard', label: 'Hard' }, { value: 'mixed', label: 'Mixed' }], phrase: (v) => (v === 'mixed' ? 'mix easy, medium and hard questions' : `pitch questions at a ${v} difficulty`) },
    { kind: 'number', key: 'choices', label: 'Options per question', min: 2, max: 5, default: 4, phrase: (v) => `give each question ${v} answer options` },
  ],
  flashcards: [
    { kind: 'select', key: 'layout', label: 'Layout', default: 'carousel', choices: [{ value: 'carousel', label: 'One per screen (carousel)' }, { value: 'grid', label: 'Grid' }], phrase: (v) => (v === 'carousel' ? 'show one flashcard per screen that scrolls left→right — set the Flashcards component\'s "layout" to "carousel"' : 'lay the flashcards out as a responsive grid (Flashcards "layout":"grid")') },
    { kind: 'number', key: 'count', label: 'Number of cards', min: 4, max: 40, step: 2, default: 12, phrase: (v) => `make about ${v} flashcards` },
    { kind: 'select', key: 'style', label: 'Card style', default: 'qa', choices: [{ value: 'qa', label: 'Question → Answer' }, { value: 'term', label: 'Term → Definition' }, { value: 'cloze', label: 'Fill in the blank' }], phrase: (v) => (v === 'term' ? 'use term → definition cards' : v === 'cloze' ? 'use fill-in-the-blank (cloze) cards' : 'use question → answer cards') },
    { kind: 'switch', key: 'examples', label: 'Add a short example to each answer', default: false, phrase: (v) => (v ? 'add a brief example to each answer' : 'keep answers concise without examples') },
  ],
  report: [
    { kind: 'select', key: 'length', label: 'Length', default: 'standard', choices: [{ value: 'brief', label: 'Brief' }, { value: 'standard', label: 'Standard' }, { value: 'detailed', label: 'Detailed' }], phrase: (v) => `keep the report ${v} in length` },
    { kind: 'select', key: 'tone', label: 'Tone', default: 'neutral', choices: [{ value: 'neutral', label: 'Neutral' }, { value: 'executive', label: 'Executive' }, { value: 'technical', label: 'Technical' }, { value: 'casual', label: 'Casual' }], phrase: (v) => `write in a ${v} tone` },
    { kind: 'switch', key: 'execSummary', label: 'Include an executive summary', default: true, phrase: (v) => (v ? 'lead with an executive summary card' : 'no executive summary') },
    { kind: 'switch', key: 'sources', label: 'Include sources / citations', default: true, phrase: (v) => (v ? 'list sources / citations at the end' : 'do not include a sources list') },
  ],
};

export const optionSpecs = (type: string): OptionSpec[] => TYPE_OPTIONS[type] || [];

export const optionVal = (opts: Record<string, OptionValue> | undefined, s: OptionSpec): OptionValue => {
  const v = opts?.[s.key];
  return v === undefined ? s.default : v;
};

/** Join phrased clauses into a single capitalized directive sentence. */
const sentence = (clauses: string[]): string => {
  if (clauses.length === 0) return '';
  const joined = clauses.join('; ');
  return joined.charAt(0).toUpperCase() + joined.slice(1) + '.';
};

/** Turn a type's option values into the human directive appended to the crew.
 *  Phrases EVERY spec (filling defaults) — used when saving the workspace config. */
export const buildDirective = (type: string, opts: Record<string, OptionValue> | undefined): string => {
  const specs = optionSpecs(type);
  if (specs.length === 0) return '';
  return sentence(specs.map((s) => (s.phrase as (x: OptionValue) => string)(optionVal(opts, s))));
};

/** Like buildDirective but phrases ONLY the keys present in `opts` — used by the
 *  in-preview refine, so "Update with AI" sends only what the user changed
 *  (an unchanged option contributes nothing, avoiding over-constraining the edit). */
export const buildPartialDirective = (type: string, opts: Record<string, OptionValue> | undefined): string => {
  if (!opts) return '';
  const specs = optionSpecs(type);
  const clauses = specs
    .filter((s) => opts[s.key] !== undefined)
    .map((s) => (s.phrase as (x: OptionValue) => string)(opts[s.key]));
  return sentence(clauses);
};
