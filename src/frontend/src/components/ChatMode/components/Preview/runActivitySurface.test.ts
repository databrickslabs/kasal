import { describe, it, expect } from 'vitest';
import {
  buildResultsSurface,
  cleanContextText,
} from './runActivitySurface';

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

describe('buildResultsSurface — JSON envelope with a prose string value is decoded', () => {
  // A tool that returns {"results": "<one big string>"} (not an array of records).
  // The string carries a URL that abuts the closing quote. Regression:
  // cleanContextText's greedy URL strip used to run BEFORE the JSON parse and
  // swallow the trailing quote/brace, so the parse failed and the raw envelope
  // leaked verbatim. We must parse + clean it instead. (Synthetic, non-PII data.)
  const body = JSON.stringify({
    results: 'Found 2 results:\n\n1. Sample Widget Guide | Example Co\n URL: https://example.com/widget',
  });

  const texts = () =>
    Object.values(buildResultsSurface([{ title: '', body }]).components)
      .filter((c) => c.component === 'Text')
      .map((c) => String(c.text));

  it('parses the envelope instead of leaking it (wrapper gone, URL stripped)', () => {
    const all = texts().join('\n');
    expect(all).not.toContain('{"results"'); // the JSON wrapper never shows
    expect(all).not.toMatch(/https?:\/\//); // the abutting URL is stripped, not leaked
  });

  it('splits the prose into readable lines (heading + list item)', () => {
    const all = texts();
    expect(all).toContain('Found 2 results'); // ":" heading, marker trimmed
    expect(all).toContain('Sample Widget Guide | Example Co'); // "1." numbered → list item
  });

  it('salvages + decodes a CLAMPED envelope (backend cut it mid-string → invalid JSON)', () => {
    // The backend caps tool output and appends "…[truncated]", so JSON.parse fails.
    // We must still decode escape sequences and drop the {"results":" wrapper instead
    // of leaking raw \uXXXX codes. The string literal below holds REAL backslash-u
    // sequences (\\u → \u at runtime): "é" decodes to "é". Synthetic, non-PII.
    const escaped = '{"results": "Sample caf\\u00e9 report\\nmore body text long enough to be cut by the backend trace cap and then some';
    const body = escaped + '…[truncated]';
    const all = Object.values(buildResultsSurface([{ title: '', body }]).components)
      .filter((c) => c.component === 'Text')
      .map((c) => String(c.text))
      .join('\n');
    expect(all).toContain('Sample café report'); // é decoded to é
    expect(all).not.toContain('\\u00e9'); // no raw escape codes
    expect(all).not.toContain('{"results"'); // wrapper stripped
    expect(all).not.toMatch(/\[truncated\]/); // marker removed
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
