import { describe, it, expect } from 'vitest';
import {
  parseUiDocument,
  findUiSurface,
  findUiDocument,
  resolveValue,
  applyConfiguredTheme,
  inferSurfaceDeliverable,
  setSurfaceTheme,
  DELIVERABLE_LABELS,
  UiSurface,
  WorkspaceThemes,
  buildResultsSurface,
  cleanContextText,
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
    expect(inferSurfaceDeliverable(surfaceOf({ root: 'Column', f: 'Flashcards' }))).toBe('flashcards');
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

  it('restyles a document wrapped in a non-fenced prose preamble (regression: instant Look no-op)', () => {
    // Agents commonly prefix the UI document with a sentence ("Here is your
    // dashboard: …"). The renderer tolerates that (coerceJson peels the prose),
    // so the preview shows fine — but the instant "Look" used a strict JSON.parse
    // that threw on the prose and returned the doc UNCHANGED, so restyling
    // silently did nothing. (It looked dashboard-specific because that agent
    // happened to add a preamble while the deck/mindmap output was pure JSON.)
    const inner = JSON.stringify({
      messages: [
        { createSurface: { surfaceId: 's1', catalogId: 'basic' } },
        {
          updateComponents: {
            components: [
              { id: 'root', component: 'Column', children: ['d'] },
              { id: 'd', component: 'Dashboard', children: ['k'] },
              { id: 'k', component: 'Stat', label: 'Revenue', value: '$1M' },
            ],
          },
        },
      ],
    });
    const prosey = 'Here is your dashboard:\n' + inner;
    const out = setSurfaceTheme(prosey, theme);
    expect(out).not.toBe(prosey); // no longer the silent no-op
    const surface = parseUiDocument(out)!;
    expect(surface.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
    expect(surface.components.k.label).toBe('Revenue'); // components preserved
    // the decorative wrapper is dropped — the persisted doc is canonical JSON
    expect(() => JSON.parse(out)).not.toThrow();
  });

  it('restyles fenced and double-encoded documents (same tolerant coercion as the renderer)', () => {
    const inner = JSON.stringify({
      messages: [{ updateComponents: { components: [{ id: 'root', component: 'Dashboard' }] } }],
    });
    const fenced = '```json\n' + inner + '\n```';
    expect(parseUiDocument(setSurfaceTheme(fenced, theme))?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
    // a doubly JSON-encoded document (e.g. a stringified execution result) too
    const doubleEncoded = JSON.stringify(inner);
    expect(parseUiDocument(setSurfaceTheme(doubleEncoded, theme))?.theme).toMatchObject({ accent: '#FF0000', _pinned: true });
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

describe('findUiSurface — A2UI nested anywhere in a result tree', () => {
  const json = JSON.stringify(doc);

  it('finds a clean top-level document (parity with parseUiDocument)', () => {
    expect(findUiSurface(doc)!.rootId).toBe('root');
    expect(findUiSurface(json)!.rootId).toBe('root');
  });

  // The shapes that defeated the old top-level-only detection. For each, assert
  // parseUiDocument on the SAME wrapped value is null (the gap) but findUiSurface
  // recovers the surface (the fix).
  it('finds a document wrapped under a single key as a PARSED object', () => {
    const wrapped = { result: doc };
    expect(parseUiDocument(wrapped as never)).toBeNull();
    expect(findUiSurface(wrapped)!.rootId).toBe('root');
  });

  it('finds a document in a MULTI-key envelope whose value is a JSON string', () => {
    const wrapped = { output: 'done', meta: json };
    expect(parseUiDocument(wrapped as never)).toBeNull();
    expect(findUiSurface(wrapped)!.rootId).toBe('root');
  });

  it('finds a document in a MULTI-key envelope whose value is a parsed object', () => {
    const wrapped = { success: true, crew_a: doc };
    expect(findUiSurface(wrapped)!.rootId).toBe('root');
  });

  it('finds a document inside a PROSE-prefixed string value', () => {
    const wrapped = { task: `Here is your dashboard: ${json}` };
    expect(parseUiDocument(wrapped as never)).toBeNull();
    expect(findUiSurface(wrapped)!.rootId).toBe('root');
  });

  it('finds a deeply nested document and respects the depth cap', () => {
    expect(findUiSurface({ a: { b: { c: doc } } })!.rootId).toBe('root');
    // Nested deeper than the cap (>6) is not found.
    const tooDeep = { a: { b: { c: { d: { e: { f: { g: doc } } } } } } };
    expect(findUiSurface(tooDeep)).toBeNull();
  });

  it('finds a document among array elements', () => {
    expect(findUiSurface([{ junk: 1 }, doc])!.rootId).toBe('root');
  });

  describe('findUiDocument — returns the RAW renderable node', () => {
    const json = JSON.stringify(doc);

    it('returns the doc object for a clean top-level document', () => {
      expect(findUiDocument(doc)).toBe(doc);
    });

    it('returns the original string for a top-level document string', () => {
      expect(findUiDocument(json)).toBe(json);
    });

    it('returns the INNER node out of a wrapped result (object)', () => {
      const wrapped = { success: true, crew_a: doc };
      expect(findUiDocument(wrapped)).toBe(doc);
    });

    it('returns the JSON string value out of a multi-key envelope', () => {
      const wrapped = { output: 'done', meta: json };
      expect(findUiDocument(wrapped)).toBe(json);
    });

    it('returns null when there is no A2UI document', () => {
      expect(findUiDocument({ output: 'just text', n: 1 })).toBeNull();
      expect(findUiDocument('plain prose, no doc')).toBeNull();
    });

    it('extracted node re-renders identically via parseUiDocument', () => {
      // The whole point: hand the extracted node to the preview pane unchanged.
      const node = findUiDocument({ result: doc })!;
      expect(parseUiDocument(node as never)!.rootId).toBe('root');
    });
  });

  it('returns the OUTERMOST/first surface when several exist', () => {
    const secondDoc = {
      messages: [{ updateComponents: { components: [{ id: 'root', component: 'Dashboard' }] } }],
    };
    // crew_a comes first in value order, so its Column-rooted surface wins.
    const surface = findUiSurface({ crew_a: doc, crew_b: secondDoc })!;
    expect(surface.components.title?.text).toBe('Hello');
  });

  it('never false-positives on non-A2UI content', () => {
    expect(findUiSurface({ messages: [] })).toBeNull(); // no components
    expect(findUiSurface({ foo: 'bar', n: 42 })).toBeNull();
    expect(findUiSurface('just some plain text')).toBeNull();
    expect(findUiSurface({ wrapper: { status: 'ok' } })).toBeNull();
    // A surface declaration with no components is not renderable.
    expect(findUiSurface({ a: { messages: [{ createSurface: { surfaceId: 's' } }] } })).toBeNull();
    expect(findUiSurface(null)).toBeNull();
    expect(findUiSurface(42)).toBeNull();
  });
});

describe('buildResultsSurface structured formatting', () => {
  const body = [
    '=== STRUCTURED CONTENT OUTLINE ===',
    'EXECUTIVE SUMMARY',
    '- AI transformation is a competitive imperative',
    '- Market opportunity $2.3T by 2030',
    '',
    'Early movers capture a 40% margin premium.',
  ].join('\n');

  it('renders banner / ALL-CAPS lines as headings (Text h4)', () => {
    const s = buildResultsSurface([{ title: 'Recalled memory', body }]);
    const texts = Object.values(s.components).filter((c) => c.component === 'Text');
    expect(texts.some((t) => t.variant === 'h4' && t.text === 'STRUCTURED CONTENT OUTLINE')).toBe(true);
    expect(texts.some((t) => t.variant === 'h4' && t.text === 'EXECUTIVE SUMMARY')).toBe(true);
  });

  it('groups a run of bullets into a single List with clean items (no markers)', () => {
    const s = buildResultsSurface([{ title: 'Recalled memory', body }]);
    const lists = Object.values(s.components).filter((c) => c.component === 'List');
    expect(lists).toHaveLength(1);
    const itemIds = (lists[0].children as string[]) || [];
    expect(itemIds).toHaveLength(2);
    const itemText = itemIds.map((id) => s.components[id].text);
    expect(itemText).toContain('AI transformation is a competitive imperative');
    expect(itemText.every((t) => !String(t).startsWith('-'))).toBe(true);
  });

  it('keeps ordinary prose as a body paragraph', () => {
    const s = buildResultsSurface([{ title: 'Recalled memory', body }]);
    const texts = Object.values(s.components).filter((c) => c.component === 'Text');
    expect(texts.some((t) => t.variant === 'body' && t.text === 'Early movers capture a 40% margin premium.')).toBe(true);
  });
});

describe('buildResultsSurface memory compaction (context-only + nice format)', () => {
  const body = [
    '(score=0.49) High-priority AI updates today.',
    'Segment Breakdown: • Enterprise AI Services: $1.2T • AI Infrastructure: $680B • AI Software: $420B',
    'categories: a, b, c, d',
    'entities: []',
    'dates: []',
    'topics: []',
    '',
    '(score=0.49) Second memory entry.',
  ].join('\n');

  const surface = () => buildResultsSurface([{ title: 'Memory', body }]);
  const texts = () =>
    Object.values(surface().components)
      .filter((c) => c.component === 'Text')
      .map((t) => String(t.text));

  it('drops provenance metadata (categories/entities/dates/topics)', () => {
    expect(texts().some((t) => /^(categories|entities|dates|topics)\b/i.test(t))).toBe(false);
  });

  it('strips the (score=…) marker and separates entries with a divider', () => {
    expect(texts().some((t) => t.startsWith('(score='))).toBe(false);
    expect(texts()).toContain('High-priority AI updates today.');
    expect(Object.values(surface().components).some((c) => c.component === 'Divider')).toBe(true);
  });

  it('splits an inline "• a • b • c" run into a labelled List', () => {
    const comps = surface().components;
    const lists = Object.values(comps).filter((c) => c.component === 'List');
    expect(lists.length).toBeGreaterThanOrEqual(1);
    expect(texts()).toContain('Segment Breakdown');
    const itemText = lists.flatMap((l) => (l.children as string[]).map((id) => String(comps[id].text)));
    expect(itemText).toContain('Enterprise AI Services: $1.2T');
  });

  it('applies a compact (smaller) font size to body text', () => {
    const body0 = Object.values(surface().components).find(
      (c) => c.component === 'Text' && c.variant === 'body',
    );
    expect((body0?.style as Record<string, unknown>)?.fontSize).toBe('0.85rem');
  });
});

describe('buildResultsSurface JSON tool output (Genie envelope)', () => {
  const body = JSON.stringify({
    content: {
      queryAttachments: [],
      textAttachments: ['The available table is X.\n- Timestamp: event_time\n- Latency: latency_ms\nThere is only one table.'],
      suggestedQuestions: ['How does TTFT vary by destination?', 'What are common status codes?'],
    },
    conversationId: '01f16e3df2f015c1a0e40714ffb80e8c',
    messageId: '01f16e3df3071dca8f438d0f5c1cb546',
    status: 'COMPLETED',
  });
  const surface = () => buildResultsSurface([{ title: 'Genie', body }]);
  const comps = () => surface().components;
  const texts = () => Object.values(comps()).filter((c) => c.component === 'Text').map((t) => String(t.text));
  const listItems = () =>
    Object.values(comps())
      .filter((c) => c.component === 'List')
      .flatMap((l) => (l.children as string[]).map((id) => String(comps()[id].text)));

  it('renders the prose attachment and drops ids/status/empty arrays', () => {
    expect(texts().some((t) => t.includes('The available table is X.'))).toBe(true);
    expect(texts().some((t) => /01f16e3df|conversationId|messageId|COMPLETED/.test(t))).toBe(false);
  });

  it('turns the embedded field lines into a List', () => {
    expect(listItems()).toContain('Timestamp: event_time');
  });

  it('renders suggested questions as a labelled list', () => {
    expect(texts()).toContain('Suggested questions');
    expect(listItems().some((t) => /How does TTFT/.test(t))).toBe(true);
  });

  it('leaves a non-JSON prose body untouched', () => {
    const s = buildResultsSurface([{ title: 'x', body: 'just plain prose, not json' }]);
    expect(Object.values(s.components).some((c) => c.component === 'Text' && c.text === 'just plain prose, not json')).toBe(true);
  });
});

describe('buildResultsSurface structured JSON readability', () => {
  const texts = (body: string) =>
    Object.values(buildResultsSurface([{ title: 'Result', body }]).components)
      .filter((c) => c.component === 'Text')
      .map((t) => String(t.text));

  it('keeps scalar fields as labelled "Key: value" lines (numbers/booleans no longer dropped)', () => {
    const t = texts(JSON.stringify({ region: 'EMEA', revenue: 1200, growth: 0.12, isFinal: true }));
    expect(t).toContain('Region: EMEA');
    expect(t).toContain('Revenue: 1200');
    expect(t).toContain('Growth: 0.12');
    expect(t).toContain('Is Final: true');
  });

  it('renders an array of records as a Table (columns + rows), not flattened text', () => {
    const surface = buildResultsSurface([{ title: 'Rows', body: JSON.stringify({ rows: [{ name: 'A', count: 2 }, { name: 'B', count: 5 }] }) }]);
    const tables = Object.values(surface.components).filter((c) => c.component === 'Table');
    expect(tables).toHaveLength(1);
    expect(tables[0].columns).toEqual(['Name', 'Count']);
    expect(tables[0].rows).toEqual([{ Name: 'A', Count: '2' }, { Name: 'B', Count: '5' }]);
  });
});

describe('buildResultsSurface — search results render as tables', () => {
  const body = JSON.stringify({
    results: {
      web: [
        { url: 'https://a.ch', title: 'News A', description: 'About A', snippets: ['x'], page_age: '2026-06-22T07:00:02', thumbnail_url: 'https://t', favicon_url: 'https://f' },
        { url: 'https://b.ch', title: 'News B', description: 'About B', snippets: [], page_age: '2026-06-21T20:00:02' },
      ],
      news: [{ title: 'N1', description: 'D1', page_age: '2026-06-22T00:00:00', url: 'https://n' }],
    },
    metadata: { query: 'Switzerland news today' },
  });

  it('emits a table per web/news array with a section heading, dropping links/ids', () => {
    const comps = Object.values(buildResultsSurface([{ title: '', body }]).components);
    const tables = comps.filter((c) => c.component === 'Table');
    expect(tables.length).toBe(2);
    const headings = comps.filter((c) => c.component === 'Text' && c.variant === 'h4').map((c) => String(c.text));
    expect(headings).toEqual(expect.arrayContaining(['Web', 'News']));
    const webTable = tables.find((t) => (t.columns as string[]).includes('Title'))!;
    expect(webTable.columns).not.toContain('Url');
    expect(webTable.columns).not.toContain('Thumbnail Url');
    expect(webTable.columns).not.toContain('Favicon Url');
  });

  it('trims ISO timestamps to a date in table cells', () => {
    const tables = Object.values(buildResultsSurface([{ title: '', body }]).components).filter((c) => c.component === 'Table');
    const webRows = tables.find((t) => (t.columns as string[]).includes('Title'))!.rows as Record<string, string>[];
    expect(Object.values(webRows[0])).toContain('2026-06-22');
  });

  it('captures each row’s source url as a link so the title can hyperlink', () => {
    const tables = Object.values(buildResultsSurface([{ title: '', body }]).components).filter((c) => c.component === 'Table');
    const webTable = tables.find((t) => (t.columns as string[]).includes('Title'))!;
    expect(webTable.links).toEqual(['https://a.ch', 'https://b.ch']);
  });
});

describe('buildResultsSurface — Genie query results render as a data table', () => {
  const body = JSON.stringify({
    content: {
      queryAttachments: [{
        query: 'SELECT ticker, avg_return, volatility FROM prices',
        description: 'Top investment by return and risk.',
        statement_response: {
          manifest: { schema: { columns: [{ name: 'ticker' }, { name: 'avg_return' }, { name: 'volatility' }] } },
          result: { data_array: [{ values: [{ string_value: 'TSLA' }, { string_value: '0.00223' }, { string_value: '0.0360' }] }] },
        },
      }],
      textAttachments: ['The most effective investment option is TSLA.'],
      suggestedQuestions: ['What are the average returns?'],
    },
    conversationId: 'x', messageId: 'y', status: 'COMPLETED',
  });
  const comps = () => Object.values(buildResultsSurface([{ title: '', body }]).components);

  it('renders the data_array as a table shaped by the manifest schema columns', () => {
    const tables = comps().filter((c) => c.component === 'Table');
    expect(tables).toHaveLength(1);
    expect(tables[0].columns).toEqual(['Ticker', 'Avg return', 'Volatility']);
    expect(tables[0].rows).toEqual([{ Ticker: 'TSLA', 'Avg return': '0.00223', Volatility: '0.0360' }]);
  });

  it('still shows the spoken answer and suggested questions, and drops ids/status', () => {
    const texts = comps().filter((c) => c.component === 'Text').map((c) => String(c.text));
    expect(texts).toContain('The most effective investment option is TSLA.');
    expect(texts).toContain('Suggested questions');
    expect(texts.some((t) => /COMPLETED|conversationId|messageId/.test(t))).toBe(false);
  });
});

describe('buildResultsSurface — raw SQL tool result renders as a table', () => {
  const body = JSON.stringify({
    statement_id: '01f1',
    status: { state: 'SUCCEEDED' },
    manifest: { format: 'JSON_ARRAY', schema: { columns: [{ name: 'event_time' }, { name: 'status_code' }, { name: 'latency_ms' }] } },
    result: { data_array: [
      { values: [{ string_value: '2026-06-22T22:35:13.779Z' }, { string_value: '200' }, { string_value: '14896' }] },
      { values: [{ string_value: '2026-06-22T22:35:07.939Z' }, { string_value: '400' }, { string_value: '1796' }] },
    ] },
  });

  it('shapes the manifest schema + data_array into a table (ISO dates trimmed)', () => {
    const tables = Object.values(buildResultsSurface([{ title: '', body }]).components).filter((c) => c.component === 'Table');
    expect(tables).toHaveLength(1);
    expect(tables[0].columns).toEqual(['Event time', 'Status code', 'Latency ms']);
    const rows = tables[0].rows as Record<string, string>[];
    expect(rows).toHaveLength(2);
    expect(rows[0]['Status code']).toBe('200');
    expect(rows[0]['Event time']).toBe('2026-06-22');
  });
});

describe('cleanContextText — plain readable text for non-technical users', () => {
  it('unwraps a MemoryMatch/MemoryRecord repr down to its content', () => {
    const repr = "[MemoryMatch(record=MemoryRecord(id='eaa6b56d', content='Focus areas: LLM advancements, multimodal AI', scope='/bi-specialist/ai-research', categories=['AI Research'], metadata={'entities': []}))]";
    const out = cleanContextText(repr);
    expect(out).toBe('Focus areas: LLM advancements, multimodal AI');
    expect(out).not.toMatch(/MemoryMatch|MemoryRecord|scope=|metadata=|id=/);
  });

  it('strips bare URLs, scope and importance from a save confirmation', () => {
    const out = cleanContextText('Saved to memory (scope=/bi-specialist/geopolitics, importance=0.8). See https://example.com/x');
    expect(out).not.toMatch(/scope=|importance=|https?:\/\//);
    expect(out).toContain('Saved to memory');
  });

  it('turns a markdown link into its text', () => {
    expect(cleanContextText('Read [Switzerland Today](https://www.swissinfo.ch/eng/)')).toBe('Read Switzerland Today');
  });

  it('leaves plain prose untouched and KEEPS (score=…) markers (used to divide entries)', () => {
    expect(cleanContextText('just plain prose')).toBe('just plain prose');
    expect(cleanContextText('(score=0.49) High-priority updates.')).toContain('(score=0.49)');
  });
});
