import { describe, it, expect } from 'vitest';
import { parseUiDocument, resolveValue } from './uiDocument';

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
