import { describe, it, expect } from 'vitest';
import {
  parseUiDocument,
  extractDocSummary,
  findUiSurface,
  inferSurfaceDeliverable,
  UiSurface,
} from './surfaceAdapter';

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

describe('extractDocSummary', () => {
  it('reads a top-level "summary" sibling of messages', () => {
    expect(extractDocSummary({ summary: 'Built a dashboard.', ...doc } as never)).toBe(
      'Built a dashboard.',
    );
    expect(extractDocSummary(JSON.stringify({ summary: '  trimmed  ', ...doc }))).toBe('trimmed');
  });

  it('falls back to createSurface.summary and a bare { summary } message', () => {
    expect(
      extractDocSummary({
        messages: [{ createSurface: { surfaceId: 's1', summary: 'On the surface.' } }],
      } as never),
    ).toBe('On the surface.');
    expect(
      extractDocSummary({ messages: [{ summary: 'Bare message.' }] } as never),
    ).toBe('Bare message.');
  });

  it('returns null when there is no summary, or for empty/non-string values', () => {
    expect(extractDocSummary(doc as never)).toBeNull();
    expect(extractDocSummary({ summary: '   ', ...doc } as never)).toBeNull();
    expect(extractDocSummary({ summary: 42, ...doc } as never)).toBeNull();
    expect(extractDocSummary('not json at all')).toBeNull();
  });

  it('recovers the summary from a malformed (mismatched-bracket) document', () => {
    // Same weak-model failure mode as parseUiDocument: the repair path must also
    // let the chat one-liner through instead of falling back to the generic line.
    const malformed =
      '{"summary":"Built a discovery plan.","messages":[' +
      '{"createSurface":{"surfaceId":"s1"}},' +
      '{"updateComponents":{"components":[' +
      '{"id":"root","component":"Text","text":"x"} ]}]}}]}';
    expect(() => JSON.parse(malformed)).toThrow();
    expect(extractDocSummary(malformed)).toBe('Built a discovery plan.');
  });
});

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

  it('recovers a weak-model document with mismatched/extra brackets', () => {
    // gpt-5-nano emitted this verbatim: the closing tail is `}]}]}}]}` where a
    // valid doc needs `}]}}]}` — a spurious `]}`. Strict JSON.parse rejects it,
    // which used to leak the raw JSON into the chat with an empty preview. The
    // string-aware bracket repair in coerceJson must rebalance it and render.
    const malformed =
      '{"messages":[{"createSurface":{"surfaceId":"s1","catalogId":"basic"}},' +
      '{"updateComponents":{"surfaceId":"s1","components":[' +
      '{"id":"root","component":"Column","children":["title","body"]},' +
      '{"id":"title","component":"Text","variant":"h1","text":"Title (with parens)"} ,' +
      '{"id":"body","component":"Text","variant":"body","text":"Body: (1) a, (2) b."} ]}]}}]}';
    expect(() => JSON.parse(malformed)).toThrow(); // precondition: genuinely invalid JSON
    const surface = parseUiDocument(malformed);
    expect(surface).not.toBeNull();
    expect(surface!.rootId).toBe('root');
    expect(Object.keys(surface!.components).sort()).toEqual(['body', 'root', 'title']);
    expect(surface!.components.body.text).toBe('Body: (1) a, (2) b.');
  });

  it('does not turn invalid non-A2UI text into a surface via repair', () => {
    // Repair only rebalances brackets — it must never fabricate a document.
    expect(parseUiDocument('{ this is : not valid json')).toBeNull();
    expect(parseUiDocument('{"foo": [1, 2, 3}')).toBeNull(); // repairs to valid JSON, but no A2UI shape
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

describe('inferSurfaceDeliverable', () => {
  const surfaceOf = (components: Record<string, string>, theme?: UiSurface['theme']): UiSurface => ({
    rootId: 'root',
    components: Object.fromEntries(
      Object.entries(components).map(([id, component]) => [id, { id, component: component as never }]),
    ),
    data: {},
    theme,
  });

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
