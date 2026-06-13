import { describe, it, expect } from 'vitest';
import {
  parseUiDocument,
  resolveValue,
  applyConfiguredTheme,
  inferSurfaceDeliverable,
  setSurfaceTheme,
  DELIVERABLE_LABELS,
  UiSurface,
  WorkspaceThemes,
} from './uiDocument';

const doc = {
  messages: [
    { version: 'v0.10', createSurface: { surfaceId: 's1', catalogId: 'minimal' } },
    {
      version: 'v0.10',
      updateComponents: {
        surfaceId: 's1',
        components: [
          { id: 'root', component: 'Column', children: ['title', 'row'] },
          { id: 'title', component: 'Text', text: 'Hello', variant: 'h1' },
          { id: 'row', component: 'Row', children: ['field'] },
          { id: 'field', component: 'TextField', label: 'Name', value: { path: '/name' } },
        ],
      },
    },
    { version: 'v0.10', dataModelUpdate: { contents: { name: 'Ada' } } },
  ],
};

describe('parseUiDocument', () => {
  it('parses a minimal A2UI document into a surface', () => {
    const surface = parseUiDocument(doc as never);
    expect(surface).not.toBeNull();
    expect(surface!.rootId).toBe('root');
    expect(Object.keys(surface!.components)).toHaveLength(4);
    expect(surface!.components.title.text).toBe('Hello');
    expect(surface!.data).toEqual({ name: 'Ada' });
  });

  it('accepts a JSON string and a ```json fence', () => {
    expect(parseUiDocument(JSON.stringify(doc))).not.toBeNull();
    expect(parseUiDocument('```json\n' + JSON.stringify(doc) + '\n```')).not.toBeNull();
  });

  it('extracts a JSON object embedded in surrounding prose (with escaped quotes)', () => {
    // Leading prose + a string value containing an escaped quote and backslash,
    // so the balanced-block scanner walks the in-string / escape branches.
    const json = JSON.stringify({
      ...doc,
      messages: [
        { version: 'v0.10', createSurface: { surfaceId: 's1', catalogId: 'minimal' } },
        { updateComponents: { components: [{ id: 'root', component: 'Text', text: 'a "b" \\ c' }] } },
      ],
    });
    const surface = parseUiDocument(`Here is the UI document: ${json} — enjoy!`);
    expect(surface).not.toBeNull();
  });

  it('returns null for prose with an embedded array (not a UI document) and for plain prose', () => {
    // arrAt found before any object → the array branch of the embedded scan runs.
    expect(parseUiDocument('preamble [1, 2, 3] trailing')).toBeNull();
    // neither { nor [ → no embedded block
    expect(parseUiDocument('just some words, no json here')).toBeNull();
  });

  it('accepts a bare array of messages and a single message object', () => {
    expect(parseUiDocument(doc.messages as never)).not.toBeNull();
    const single = {
      updateComponents: { components: [{ id: 'root', component: 'Text', text: 'Hi' }] },
    };
    expect(parseUiDocument(single as never)).not.toBeNull();
  });

  it('falls back to the first id when there is no "root" component', () => {
    const surface = parseUiDocument({
      messages: [{ updateComponents: { components: [{ id: 'only', component: 'Text', text: 'x' }] } }],
    } as never);
    expect(surface!.rootId).toBe('only');
  });

  it('accepts "type" as an alias for the "component" discriminator (LLM variance)', () => {
    // Some models (e.g. Haiku) emit `type` instead of `component`.
    const surface = parseUiDocument({
      messages: [
        { createSurface: { surfaceId: 's1', catalogId: 'basic' } },
        {
          updateComponents: {
            surfaceId: 's1',
            components: [
              { id: 'root', type: 'Column', children: ['title', 'card1'] },
              { id: 'title', type: 'Text', text: 'Swiss Daily Briefing', variant: 'h1' },
              { id: 'card1', type: 'Card', title: 'News', children: ['n1'] },
              { id: 'n1', type: 'Text', variant: 'body', text: 'Headline…' },
            ],
          },
        },
      ],
    } as never);
    expect(surface).not.toBeNull();
    expect(surface!.rootId).toBe('root');
    // normalized to `component` so the renderer (which reads `component`) works
    expect(surface!.components.root.component).toBe('Column');
    expect(surface!.components.card1.component).toBe('Card');
    expect(Object.keys(surface!.components)).toHaveLength(4);
  });

  it('ignores components with an unknown type or missing id', () => {
    const surface = parseUiDocument({
      messages: [
        { updateComponents: { components: [
          { id: 'ok', component: 'Text', text: 'x' },
          { id: 'bad', component: 'Carousel' }, // unknown → dropped
          { component: 'Text', text: 'no id' },  // no id → dropped
        ] } },
      ],
    } as never);
    expect(Object.keys(surface!.components)).toEqual(['ok']);
  });

  it('merges data from updateDataModel alias', () => {
    const surface = parseUiDocument({
      messages: [
        { updateComponents: { components: [{ id: 'root', component: 'Text', text: 'x' }] } },
        { updateDataModel: { data: { a: 1 } } },
      ],
    } as never);
    expect(surface!.data).toEqual({ a: 1 });
  });

  it('returns null for non-JSON, non-A2UI JSON, and empty documents', () => {
    expect(parseUiDocument('not json at all')).toBeNull();
    expect(parseUiDocument('plain text')).toBeNull();
    expect(parseUiDocument('42')).toBeNull();
    expect(parseUiDocument('{ this is : not valid json')).toBeNull(); // starts with { but unparseable
    expect(parseUiDocument([{ a: 1 }, { b: 2 }] as never)).toBeNull(); // array of data, no components
    expect(parseUiDocument({ foo: 'bar' } as never)).toBeNull(); // object, no messages
    expect(parseUiDocument({ messages: [] } as never)).toBeNull(); // no components
    expect(parseUiDocument({ messages: [{ createSurface: { surfaceId: 's' } }] } as never)).toBeNull(); // surface but no components
  });

  it('ignores a data-model message with no contents/data', () => {
    const surface = parseUiDocument({
      messages: [
        { updateComponents: { components: [{ id: 'r', component: 'Text', text: 't' }] } },
        { dataModelUpdate: {} }, // neither contents nor data → no merge
      ],
    } as never);
    expect(surface!.data).toEqual({});
  });

  it('returns null for an empty string and nullish input', () => {
    expect(parseUiDocument('')).toBeNull();
    expect(parseUiDocument(null as never)).toBeNull();
    expect(parseUiDocument(undefined as never)).toBeNull();
  });

  it('tolerates malformed message entries', () => {
    const surface = parseUiDocument({
      messages: [null, 'x', { updateComponents: { components: 'not-array' } }, { updateComponents: { components: [{ id: 'r', component: 'Text', text: 't' }] } }],
    } as never);
    expect(surface!.components.r.text).toBe('t');
  });

  it('renders a UI document delivered as a JSON-ENCODED string (over-encoded result)', () => {
    // Execution results are stored stringified, so the document can arrive
    // JSON-encoded one or more times — a quoted, backslash-escaped blob like
    // "{\"messages\": …}". Strict parsing of that form fails (it starts with a
    // quote), which previously dumped raw escaped JSON into the chat instead of
    // rendering the preview. coerceJson must peel the string layer(s) first.
    const raw = JSON.stringify(doc); //            {"messages": …}
    const encodedOnce = JSON.stringify(raw); //    "{\"messages\": …}"
    const encodedTwice = JSON.stringify(encodedOnce);
    expect(parseUiDocument(encodedOnce)).not.toBeNull();
    expect(parseUiDocument(encodedTwice)).not.toBeNull();
  });

  it('extracts the document when a bracketed log prefix precedes it ("[STEP] {…}")', () => {
    // A step/log marker can prefix the output. The leading "[" must not shadow
    // the real {…} object: coerceJson tries both the first {…} and first […]
    // block, so the object still wins.
    const json = JSON.stringify(doc);
    expect(parseUiDocument(`[STEP] ${json}`)).not.toBeNull();
  });
});

describe('resolveValue', () => {
  const data = { user: { name: 'Ada' }, count: 3 };
  it('resolves a path binding', () => {
    expect(resolveValue({ path: '/user/name' }, data)).toBe('Ada');
    expect(resolveValue({ path: '/count' }, data)).toBe(3);
  });
  it('returns undefined for an unresolvable path', () => {
    expect(resolveValue({ path: '/user/missing' }, data)).toBeUndefined();
    expect(resolveValue({ path: '/nope/deep' }, data)).toBeUndefined();
    // descends into a primitive mid-path → undefined
    expect(resolveValue({ path: '/count/x' }, data)).toBeUndefined();
  });

  it('returns the whole data model for an empty path', () => {
    expect(resolveValue({ path: '' }, data)).toBe(data);
  });
  it('returns a literal value unchanged', () => {
    expect(resolveValue('literal', data)).toBe('literal');
    expect(resolveValue(undefined, data)).toBeUndefined();
  });
});

describe('parseUiDocument — surface theme', () => {
  const withRoot = (extra: object[]) => ({
    messages: [
      ...extra,
      { updateComponents: { surfaceId: 's1', components: [{ id: 'root', component: 'Text', text: 'x', variant: 'h1' }] } },
    ],
  });

  it('captures and merges a theme from createSurface.theme and a bare theme message', () => {
    const s = parseUiDocument(withRoot([
      { createSurface: { surfaceId: 's1', catalogId: 'basic', theme: { accent: '#111', font: 'serif' } } },
      { theme: { background: '#fff' } },
    ]) as never);
    expect(s!.theme).toEqual({ accent: '#111', font: 'serif', background: '#fff' });
  });

  it('captures a bare theme message when createSurface is absent', () => {
    const s = parseUiDocument(withRoot([{ theme: { accent: '#222' } }]) as never);
    expect(s!.theme).toEqual({ accent: '#222' });
  });

  it('ignores a non-object createSurface and a non-object theme value', () => {
    const s = parseUiDocument(withRoot([{ createSurface: 'nope' }, { theme: 'also-nope' }]) as never);
    expect(s!.theme).toBeUndefined();
  });

  it('leaves theme undefined when no theme is provided', () => {
    expect(parseUiDocument(doc as never)!.theme).toBeUndefined();
  });
});

describe('applyConfiguredTheme — workspace palettes are the source of truth', () => {
  const surfaceOf = (components: Record<string, string>, theme?: UiSurface['theme']): UiSurface => ({
    rootId: 'root',
    components: Object.fromEntries(
      Object.entries(components).map(([id, component]) => [id, { id, component: component as never }]),
    ),
    data: {},
    theme,
  });

  const themes: WorkspaceThemes = {
    default: { accent: '#2272B4', background: '#FFFFFF' },
    presentation: { accent: '#FF3621', background: '#0E1B21' },
    dashboard: { accent: '#7C3AED', background: '#111111' },
  };

  it('infers the deliverable from the components present', () => {
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Slides', s1: 'Slide' }))).toBe('presentation');
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Quiz' }))).toBe('quiz');
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Column', d: 'Dashboard' }))).toBe('dashboard');
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Column', t: 'Table' }))).toBe('genie');
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Column', t: 'Text' }))).toBe('default');
    // Slides outranks the Table inside a slide.
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Slides', t: 'Table' }))).toBe('presentation');
  });

  it('overrides an agent-stamped wrong palette on a deck with the configured Presentation palette', () => {
    // Regression: agents routinely copy the Default (white) palette onto every
    // surface, turning a themed deck white.
    const deck = surfaceOf({ root: 'Slides', s1: 'Slide' }, { accent: '#2272B4', background: '#FFFFFF' });
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(themes.presentation);
  });

  it('clears the theme on a deck when no Presentation palette is configured (built-in deck identity wins)', () => {
    const deck = surfaceOf({ root: 'Slides' }, { background: '#FFFFFF' });
    expect(applyConfiguredTheme(deck, { default: themes.default }).theme).toBeUndefined();
  });

  it('applies the deliverable palette, falling back to Default, then the embedded theme', () => {
    const dash = surfaceOf({ root: 'Column', d: 'Dashboard' }, { background: '#FFFFFF' });
    expect(applyConfiguredTheme(dash, themes).theme).toEqual(themes.dashboard);
    // No dashboard palette configured → Default palette.
    expect(applyConfiguredTheme(dash, { default: themes.default }).theme).toEqual(themes.default);
    // No palettes at all in the map → embedded theme survives.
    expect(applyConfiguredTheme(dash, {}).theme).toEqual({ background: '#FFFFFF' });
  });

  it('leaves the surface untouched when the workspace themes are unavailable', () => {
    const deck = surfaceOf({ root: 'Slides' }, { background: '#FFFFFF' });
    expect(applyConfiguredTheme(deck, null)).toBe(deck);
    expect(applyConfiguredTheme(deck, undefined)).toBe(deck);
  });

  it('keeps an embedded theme that deviates from every configured palette (user refine)', () => {
    // Regression: "change the background to black and the text to white"
    // edits the embedded theme — it must NOT be wiped by re-resolution.
    const refined = { accent: '#2272B4', background: '#000000', text: '#FFFFFF' };
    const deck = surfaceOf({ root: 'Slides', s1: 'Slide' }, refined);
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(refined);

    // Same for non-presentation deliverables.
    const dash = surfaceOf({ root: 'Column', d: 'Dashboard' }, refined);
    expect(applyConfiguredTheme(dash, themes).theme).toEqual(refined);
  });

  it('still re-resolves a PARTIAL copy of a configured palette', () => {
    // A subset of the Presentation palette is a copy, not a deviation —
    // re-resolve to the full configured palette.
    const deck = surfaceOf({ root: 'Slides' }, { background: '#0E1B21' });
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(themes.presentation);
  });

  it('matches palette colors case-insensitively when deciding copy vs deviation', () => {
    // Lowercase copy of the Default palette is still a copy → re-resolved.
    const deck = surfaceOf({ root: 'Slides' }, { accent: '#2272b4', background: '#ffffff' });
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(themes.presentation);
  });

  it('treats a token the palette does not define as a deviation (kept)', () => {
    // `font` exists on the embedded theme but on no configured palette —
    // a non-string vs undefined comparison → deviation → keep the theme.
    const themed = { accent: '#2272B4', background: '#FFFFFF', font: 'serif' as const };
    const deck = surfaceOf({ root: 'Slides' }, themed);
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(themed);
  });

  it('skips undefined palettes in the themes map when matching copies', () => {
    // A map entry can be explicitly undefined (e.g. no Presentation palette
    // configured): matchesPalette must treat it as "no match", not crash —
    // the embedded Default copy still re-resolves through the other entries.
    const sparse = { presentation: undefined, default: themes.default } as WorkspaceThemes;
    const deck = surfaceOf({ root: 'Slides' }, { ...themes.default });
    expect(applyConfiguredTheme(deck, sparse).theme).toBeUndefined(); // presentation rule
  });

  it('falls back to the embedded theme when the map has no palette for the deliverable', () => {
    // The embedded theme is a COPY of a configured palette (so it re-resolves),
    // but the map has neither a matching deliverable palette nor a Default —
    // the embedded theme is kept by the final fallback.
    const onlyGenie = { genie: { background: '#FFFFFF' } } as WorkspaceThemes;
    const docSurface = surfaceOf({ root: 'Column', t: 'Text' }, { background: '#FFFFFF' });
    expect(applyConfiguredTheme(docSurface, onlyGenie).theme).toEqual({ background: '#FFFFFF' });
  });

  it('re-resolves when the embedded theme is an empty object', () => {
    const deck = surfaceOf({ root: 'Slides' }, {});
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(themes.presentation);
  });

  it('keeps a user-pinned theme verbatim, never re-resolving from the palettes', () => {
    // A deterministic restyle from the in-preview "Customize" panel stamps
    // _pinned: true; even on a deck (which would normally re-resolve to the
    // Presentation palette) the pinned choice must survive unchanged.
    const pinned = { accent: '#123456', background: '#000000', _pinned: true };
    const deck = surfaceOf({ root: 'Slides', s1: 'Slide' }, pinned);
    expect(applyConfiguredTheme(deck, themes).theme).toEqual(pinned);
  });
});

describe('setSurfaceTheme — deterministic restyle', () => {
  const theme = { accent: '#FF0000', background: '#000000', font: 'serif' as const };

  it('merges the theme into an existing createSurface and stamps _pinned', () => {
    const doc = JSON.stringify({
      messages: [
        { createSurface: { surfaceId: 's1', theme: { accent: '#111111', muted: '#999999' } } },
        { updateComponents: { components: [{ id: 'root', component: 'Slides' }] } },
      ],
    });
    const out = parseUiDocument(setSurfaceTheme(doc, theme));
    expect(out?.theme).toEqual({ accent: '#FF0000', muted: '#999999', background: '#000000', font: 'serif', _pinned: true });
    // components are untouched
    expect(out?.components.root.component).toBe('Slides');
  });

  it('prepends a bare theme message when there is no createSurface', () => {
    const doc = JSON.stringify({
      messages: [{ updateComponents: { components: [{ id: 'root', component: 'Text', text: 'hi' }] } }],
    });
    const out = parseUiDocument(setSurfaceTheme(doc, theme));
    expect(out?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
    expect(out?.components.root.text).toBe('hi');
  });

  it('handles a bare-array document and a single-message document', () => {
    const arrDoc = JSON.stringify([
      { createSurface: { surfaceId: 's1' } },
      { updateComponents: { components: [{ id: 'root', component: 'Dashboard' }] } },
    ]);
    expect(parseUiDocument(setSurfaceTheme(arrDoc, theme))?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });

    const single = JSON.stringify({ createSurface: { surfaceId: 's1' }, updateComponents: { components: [{ id: 'root', component: 'Album' }] } });
    expect(parseUiDocument(setSurfaceTheme(single, theme))?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
  });

  it('prepends a theme onto a single bare message that has no createSurface', () => {
    // A lone updateComponents message (not wrapped in {messages}) → the writer
    // must promote it to a {messages:[…]} document with a theme message in front.
    const single = JSON.stringify({ updateComponents: { components: [{ id: 'root', component: 'Text', text: 'hi' }] } });
    const out = parseUiDocument(setSurfaceTheme(single, theme));
    expect(out?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
    expect(out?.components.root.text).toBe('hi');
  });

  it('returns the input unchanged when it is not valid JSON', () => {
    expect(setSurfaceTheme('not json', theme)).toBe('not json');
  });

  it('returns the input unchanged when the JSON is a primitive (no messages)', () => {
    expect(setSurfaceTheme('123', theme)).toBe('123');
  });

  it('merges into an existing bare theme message when there is no createSurface', () => {
    const doc = JSON.stringify({
      messages: [
        { theme: { muted: '#999999' } },
        { updateComponents: { components: [{ id: 'root', component: 'Mindmap' }] } },
      ],
    });
    const out = parseUiDocument(setSurfaceTheme(doc, theme));
    expect(out?.theme).toEqual({ accent: '#FF0000', background: '#000000', font: 'serif', muted: '#999999', _pinned: true });
  });

  it('a restyled document survives applyConfiguredTheme (pinned) end-to-end', () => {
    const doc = JSON.stringify({
      messages: [
        { createSurface: { surfaceId: 's1' } },
        { updateComponents: { components: [{ id: 'root', component: 'Slides' }] } },
      ],
    });
    const surface = parseUiDocument(setSurfaceTheme(doc, theme))!;
    const resolved = applyConfiguredTheme(surface, { presentation: { accent: '#FF3621' } });
    expect(resolved.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
  });
});

describe('DELIVERABLE_LABELS', () => {
  it('maps internal keys to friendly business nouns', () => {
    expect(DELIVERABLE_LABELS.album).toBe('Photo album');
    expect(DELIVERABLE_LABELS.genie).toBe('Data view');
    expect(DELIVERABLE_LABELS.default).toBe('Document');
  });
});
