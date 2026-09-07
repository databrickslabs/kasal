import { describe, expect, it } from 'vitest';
import { builderResultContent } from './resultContent';
import { toSurface } from '../../../chat/utils/surfaceAdapter';

const surface = { surfaceKind: 'document', root: 'root', components: [{ id: 'root', component: 'Text', text: 'Report' }], dataModel: {} };
describe('builder result envelope', () => {
  it('preserves a surface alongside output text', () => {
    const content = builderResultContent({ output: 'Summary', a2ui: surface });
    expect(toSurface(content)).toEqual(surface);
    expect(JSON.parse(content!).output).toBe('Summary');
  });
  it('preserves nested result envelopes and HTML source without rewriting it', () => {
    const content = JSON.stringify({ text: 'Summary', a2ui: surface });
    expect(toSurface(builderResultContent({ output: content }))).toEqual(surface);
    const html = '<!doctype html><html>\n<body>Report</body></html>';
    expect(builderResultContent({ output: html })).toBe(html);
  });
  it('handles structured outputs and absent results', () => {
    expect(JSON.parse(builderResultContent({ output: { answer: 42 } })!)).toEqual({ answer: 42 });
    expect(builderResultContent(null)).toBeNull();
  });
});
