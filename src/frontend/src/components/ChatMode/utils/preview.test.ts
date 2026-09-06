import { describe, it, expect } from 'vitest';
import { parsePreviewContent } from './preview';

const uiDoc = JSON.stringify({
  messages: [
    {
      updateComponents: {
        surfaceId: 's1',
        components: [{ id: 'root', component: 'Text', text: 'Hello App' }],
      },
    },
  ],
});
describe('parsePreviewContent — A2UI only', () => {
  it('returns null for empty or too-short input', () => {
    expect(parsePreviewContent('')).toBeNull();
    expect(parsePreviewContent('short')).toBeNull();
  });

  it('classifies an A2UI document as ui (plain, fenced, and behind a **Title** prefix)', () => {
    expect(parsePreviewContent(uiDoc)?.type).toBe('ui');
    expect(parsePreviewContent('```json\n' + uiDoc + '\n```')?.type).toBe('ui');
    // an inner fence surrounded by prose is unwrapped too
    expect(parsePreviewContent('Here is the app:\n```json\n' + uiDoc + '\n```\nthanks')?.type).toBe('ui');
    const out = parsePreviewContent('**My Task**\n\n' + uiDoc);
    expect(out?.type).toBe('ui');
    // parsePreviewContent now returns the CANONICAL adapted Surface JSON (a flat
    // {surfaceKind, root, components[]}), not the raw legacy {messages} input.
    const surf = JSON.parse(out!.data);
    expect(surf.surfaceKind).toBeDefined();
    expect(Array.isArray(surf.components)).toBe(true);
    expect(out!.data).toContain('Hello App');
  });

  it('does NOT preview raw HTML (A2UI-only by design)', () => {
    // Raw HTML is deliberately not a preview type anymore — crews are steered
    // toward A2UI documents; ad-hoc HTML output gets no preview pane.
    expect(parsePreviewContent('<!DOCTYPE html><html><body>hi</body></html>')).toBeNull();
    expect(parsePreviewContent('<html lang="en"><body>x</body></html>')).toBeNull();
    expect(parsePreviewContent('<div><script>var x=1;</script></div>')).toBeNull();
    // …including full HTML documents embedded in mixed text output.
    const embedded =
      'some text\n<!DOCTYPE html><body>' + 'b'.repeat(160) + '</body></html>\nmore';
    expect(parsePreviewContent(embedded)).toBeNull();
  });

  it('finds an A2UI doc WRAPPED in a result envelope (so it never leaks to chat)', () => {
    // The backend hands the surface inside {result:{…}} / {output:"<json>"} /
    // {data:{result:…}}; a top-level-only parse missed these and dumped raw JSON
    // into the chat. toSurface unwraps the canonical Surface from any of them.
    const surface = {
      surfaceKind: 'document',
      root: 'root',
      components: [{ id: 'root', component: 'Text', text: 'Wrapped Hello' }],
    };
    expect(parsePreviewContent(JSON.stringify({ result: surface }))?.type).toBe('ui');
    expect(parsePreviewContent(JSON.stringify({ output: JSON.stringify(surface) }))?.type).toBe('ui');
    expect(parsePreviewContent(JSON.stringify({ data: { result: surface } }))?.type).toBe('ui');
  });

  it('does NOT preview generic JSON, markdown, or plain text', () => {
    expect(parsePreviewContent('{"a":1,"b":2}')).toBeNull();
    expect(parsePreviewContent('[{"a":1},{"a":2}]')).toBeNull();
    expect(parsePreviewContent('# Title\n\n- a\n- b')).toBeNull();
    expect(parsePreviewContent('# Big\n\n' + 'x'.repeat(600))).toBeNull();
    expect(parsePreviewContent('just a normal sentence with nothing special here at all')).toBeNull();
  });
});

