import React from 'react';
import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import PreviewActivity, { buildTransientSurface } from './PreviewActivity';
import { buildResultsSurface } from '../../utils/uiDocument';
import { pushTransientItem, type TransientPreviewItem } from '../../store/executionStore';

const item = (over: Partial<TransientPreviewItem> = {}): TransientPreviewItem => ({
  id: Math.random().toString(36).slice(2),
  label: 'PerplexityTool',
  sublabel: 'Stanford CS229 machine learning course syllabus',
  detail: 'Stanford **CS229** is a broad ML course.\n\nIt covers supervised learning.',
  durationMs: 3940,
  timestamp: 1,
  ...over,
});

describe('pushTransientItem', () => {
  it('appends new items', () => {
    expect(pushTransientItem([], item({ label: 'a' }))).toHaveLength(1);
  });

  it('de-dupes a re-emitted result (same label + sublabel) — newest wins', () => {
    const first = item({ id: '1', label: 'search', sublabel: 'q' });
    const dup = item({ id: '2', label: 'search', sublabel: 'q' });
    const out = pushTransientItem([first], dup);
    expect(out).toHaveLength(1);
    expect(out[0].id).toBe('2');
  });

  it('caps to the most recent `max`', () => {
    let list: TransientPreviewItem[] = [];
    for (let i = 0; i < 12; i++) list = pushTransientItem(list, item({ id: String(i), sublabel: `q${i}` }), 8);
    expect(list).toHaveLength(8);
    expect(list[list.length - 1].id).toBe('11');
    expect(list[0].id).toBe('4');
  });
});

describe('buildTransientSurface', () => {
  it('builds a Column of Cards (one per result) from the root', () => {
    const surface = buildTransientSurface([item(), item({ sublabel: 'CMU 10-701' })]);
    expect(surface.rootId).toBe('root');
    const root = surface.components.root;
    expect(root.component).toBe('Column');
    expect(root.children).toHaveLength(2);
    root.children!.forEach((id) => expect(surface.components[id].component).toBe('Card'));
  });

  it('uses the query as a heading and strips markdown from the body', () => {
    const surface = buildTransientSurface([item({ sublabel: 'My topic', detail: '**Bold** body text' })]);
    const texts = Object.values(surface.components).filter((c) => c.component === 'Text');
    expect(texts.some((t) => t.text === 'My topic' && t.variant === 'h3')).toBe(true);
    expect(texts.some((t) => t.text === 'Bold body text')).toBe(true);
    expect(texts.every((t) => !String(t.text).includes('**'))).toBe(true);
  });

  it('splits a multi-paragraph body into separate Text blocks', () => {
    const surface = buildTransientSurface([item({ detail: 'Para one.\n\nPara two.' })]);
    const bodies = Object.values(surface.components).filter((c) => c.component === 'Text' && c.variant === 'body');
    expect(bodies).toHaveLength(2);
  });
});

describe('buildResultsSurface structured formatting', () => {
  // A memory-retrieval style outline: banner heading, bullets, then prose.
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
    expect(texts()).toContain('Segment Breakdown'); // the label becomes a heading
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

describe('PreviewActivity', () => {
  it('renders the live answers through the A2UI renderer', () => {
    render(<PreviewActivity items={[item({ sublabel: 'CS229 syllabus', detail: 'Covers supervised learning.' })]} />);
    expect(screen.getByLabelText('Live results')).toHaveAttribute('aria-busy', 'true');
    expect(screen.getByText('CS229 syllabus')).toBeInTheDocument();
    expect(screen.getByText('Covers supervised learning.')).toBeInTheDocument();
  });
});
